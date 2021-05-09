---
date: 2021-03-01
layout: post
title: 再来谈谈 mysql 连接池
thread: 2021-03-01-python_mysql_pool_2.md
categories: 数据库
tags: python
---

## 为什么要连接池？

在开发中, 只要涉及到类似 TCP 长连接资源的, 通常都要考虑到使用连接池封装来提高资源的可复用率, 避免频繁建立连接带来的网络请求开销.

在没有使用连接池的时候, Client 想执行一条 MySQL UPDATE 语句, 先要与 MySQL 进行 3 次网络传输("握手")建立一个 TCP 连接, 然后再发送 1 次命令,
接收 1 次命令执行结果. 如果使用了连接池, 虽然第一次执行上述过程也需要经历 3 次握手, 但是第二次, 第三次等等就省略了这个握手过程.

另外, 使用连接池, 除了可以减少网络交互的时间消耗, 更重要的是 TCP 连接数是有限制的. 当你的服务要面临短时间高并发请求的时候(比如突刺流量), 连接数很容易超出限制抛出错误.

- MySQL 最大连接数限制: `show variables like '%max_connections%';`
- 服务器文件描述符数量限制: `ulimit -n`

题外话, 模拟测试的朋友还要注意本地临时端口的限制. TCP 客户端连接服务端的时候, 需要获取本地的临时端口, 传输层协议限制了最多只有 65535 个端口, 不少都预先占用了.

- 可用的临时端口范围
  - 查看: `cat /proc/sys/net/ipv4/ip_local_port_range`
  - 修改: `echo "start-number  end-number"` , start-number 和 end-number 是 0-65536 端口号范围内的数, 0-1024 最好不要用, 通常是熟知端口, 如果是专门的代理服务器的话, 很多熟知端口没有使用, 当然可以考虑！


## 连接池的核心需求

- 存储一定数量的连接
- 线程安全 (支持协程的情况下要保障协程安全)
- 线程阻塞唤醒机制 (基于锁实现的条件变量, 支持线程的阻塞 wait, 和唤醒 notify_all)


## 常用的数据结构

- 队列 queue/deque/channel
- 链表 LinkedList
- 数组 array/slice
- 集合 set

以上是一些常见的作为连接池的数据结构, 不管是比较底层的数据结构, 还是封装了的高级数据结构, 最终都是要实现一个 `队列` 的功能, 满足 `连接池的核心需求`.

如果你使用的编程语言是 Golang, 可以使用 channel 作为队列, 协程安全, golang runtime 会自动处理 channel 上阻塞的协程以及唤醒. (底层是 gopark goready 调用, golang 中条件变量的实现同理)

如果你使用的编程语言是 Python, 可以使用 queue (基于 deque 封装), 线程安全, 使用条件变量 condition 可支持线程的阻塞和唤醒.

如果你使用的编程语言标准库没有 `队列` 的数据结构, 可以基于现有的数据结构实现一个类似的队列, 或者通过第三方库满足功能.


## 一个封装 PyMySQL 连接池的实际案例

PyMySQL 是 python 访问 MySQL 的一个很好用的库, 可惜的是不支持连接池.我们线上的一些项目中使用了 PyMySQL, 随着用户量的上涨,  系统压力会陡然上升, mysql 连接池化就成为了优化的重点.

因为这些项目使用 PyMySQL 的姿势也是独有的,  所以没法利用一些普适的连接池库来改造, 就只能自己撸了.


```
# coding: utf-8
""" PyMySQL 连接池

实现一个线程安全, 高效复用的连接池, 支持高并发场景.

Support environment:
Python >= 3.6.7
PyMySQL >= 0.9.3

Usage:

eg1:

```
pool = DBConnectionPool(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)
conn = pool.connection()
with conn as cursor:
    cursor.execute(sql)
```

eg2:

```
pool = DBConnectionPool(MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)
conn = pool.connection()
with conn:
    conn.begin()
    with conn as cursor:
        cursor.execute(sql)
```

"""
import logging
import time
import threading

from collections import deque
from pymysql import err
from pymysql.constants import CR
from pymysql.cursors import DictCursor
from pymysql.connections import Connection


__version__ = '2.1'


class DBConnectionError(Exception):

    def __init__(self, err_code=-1, err_msg=""):
        self.err_code = int(err_code)
        self.err_msg = str(err_msg)

    def __str__(self):
        return "%s<%s %s>" % (self.__class__.__name__, self.err_code, self.err_msg)

    __repr__ = __str__


class PoolIsFullError(DBConnectionError):
    """ 连接池溢出 """
    pass


class DBCursor(DictCursor):
    """ 重写 PyMysql 的游标

    """
    pass


class DBConnection(Connection):

    def __init__(self, pool, idle_at, *args, **kwargs):
        """ 重写 PyMysql 的连接

        :param pool: 连接池, DBConnectionPool
        :param idle_at: 连接空闲时间, int, 单位: 秒
        :param args: Connection 实例化的参数
        :param kwargs: Connection 实例化的参数
        """
        super(DBConnection, self).__init__(*args, **kwargs)
        self.pool = pool
        self.idle_at = idle_at
        self.enter_count = 0
        self.transaction_started = 0
        self.current_cursor = None
        self._json_obj = {
            "_sock": self._sock.getsockname(),
            "enter_count": self.enter_count,
            "transaction_started": self.transaction_started,
            "idle_at": self.idle_at,
        }

    def __str__(self):
        return str(self._json_obj)

    __repr__ = __str__

    def __enter__(self):
        self.enter_count += 1
        logging.debug("DB:__enter__: enter count: %s", self.enter_count)
        if self.enter_count == 1:
            self.current_cursor = self.cursor()
        return self.current_cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.enter_count -= 1
        logging.debug("DB:__exit__: enter count: %s", self.enter_count)
        if self.enter_count != 0:
            return
        self.pool.increase_drop_count()
        self.current_cursor.close()
        if exc_type:
            # 有异常, 无脑回滚
            # 1. 通过 begin 开启的事务, 会得到 rollback
            # 2. 通过设置 autocommit 的事务, 也会得到 rollback
            # 3. 确实没有事务的, 无脑 rollback 也没有副作用
            self.rollback()
        elif self.transaction_started:
            # 执行成功, 提交事务
            # 只有通过 begin 开启的事务, 且无抛出异常, 需要自动 commit
            self.commit()
        del (exc_type, exc_val, exc_tb)
        # 如果中途 commit rollback 遇到网络错误, recycle 将不会执行, 避免回收到失效连接
        self.pool.recycle(self)
        self.pool.decrease_drop_count()

    # def query(self, sql, unbuffered=False):
    #     """  sql 统一的执行方法
    #         - cursor 的 execute execute 方法会调用 query
    #         - 如果是非事务操作, 遇到网络错误, 尝试 ping 一下, 然后重试一次
    #         - 如果是事务操作, 遇到网络错误, 忽略, 在最开始的 begin 会检查网络情况
    #         - TODO: 因为 socket 超时的情况, 底层没有把错误暴露出来, 可能会有问题
    #     """
    #     func = super(DBConnection, self).query
    #     try:
    #         return func(sql, unbuffered)
    #     except err.OperationalError as e:
    #         if self.transaction_started == 0 and e.args[0] in (CR.CR_SERVER_LOST, CR.CR_SERVER_GONE_ERROR):
    #             logging.warning("DB:query: Maybe lost connection to MySQL: %s, try one more time" % str(e))
    #             self.ping()
    #             return func(sql, unbuffered)
    #         raise

    def begin(self):
        """ 事务开启

           - 如果 begin 开启事务遇到网络错误, 尝试 ping 一下, 然后重试一次
           - 如果 begin 后续的事务操作, 中途遇到网络错误, 因为会 rollback, 所以不会有问题; 即使 rollback 失败, 也得不到 commit
        """
        func = super(DBConnection, self).begin
        # 标记为事务连接
        self.transaction_started = 1
        try:
            func()
        except err.OperationalError as e:
            if e.args[0] in (CR.CR_SERVER_LOST, CR.CR_SERVER_GONE_ERROR):
                logging.warning("DB:reconnect_if_exc: Maybe lost connection to MySQL: %s, try one more time" % str(e))
                self.ping()
                func()
            raise

    def commit(self):
        """ 事务提交 """
        self.transaction_started = 0
        super(DBConnection, self).commit()

    def rollback(self):
        """ 事务回滚 """
        self.transaction_started = 0
        super(DBConnection, self).rollback()

    def close(self):
        """ 真实关闭 mysql 连接 """
        super(DBConnection, self).close()


class DBConnectionPool(object):
    version = __version__

    def __init__(self, host, port, user, password, database,
                 charset="utf8mb4", max_connection=32, idle_timeout=15, check_timeout=6*60,
                 wait_timeout=5, read_timeout=5, write_timeout=5):
        """ 自定义的连接池

        :param host: 主机地址, string
        :param port: 数据库端口, int
        :param user: 用户名, string
        :param password: 密码, string
        :param database: 库名, string
        :param charset: 编码, string, 默认 utf8mb4 可以存储 emoji
        :param max_connection: 最大限制连接数, int
        :param idle_timeout: 连接空闲时间, int, 单位: 秒
        :param check_timeout: 清理空闲连接时间, int, 单位: 秒
        :param wait_timeout: 等待连接时间, int, 单位: 秒 (默认 None 没有超时)
        :param read_timeout: DBConnection读超时, int, 单位: 秒 (默认 None 没有超时)
        :param write_timeout: DBConnection写超时, int, 单位: 秒  (默认 None 没有超时)

          v2.1 版本引入 _drop_count, 记录被丢弃的连接数, 避免 _active_count 计数不对 `伪溢出` 现象
        """
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._charset = charset
        self._max_connection = max_connection
        self._idle_queue = deque()
        self._idle_timeout = idle_timeout
        self._check_timeout = check_timeout
        self._wait_timeout = wait_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._active_count = 0        # 创建后没有回收的总连接数 (包含丢弃的连接)
        self._drop_count = 0          # 创建后因为异常而丢弃的连接数 (等待 GC 回收)
        self._last_check_at = self.ts

    @property
    def ts(self):
        return int(time.time())

    def _qsize(self):
        return len(self._idle_queue)

    def _put(self, c):
        self._idle_queue.append(c)

    def _get(self, right_side=False):
        try:
            return self._idle_queue.pop() if right_side else self._idle_queue.popleft()
        except IndexError:
            return None

    def _contain(self, c):
        return self._idle_queue.count(c)

    def _close_connection(self, c):
        """ 关闭 mysql 连接 """
        c.close()
        with self._lock:
            self._active_count -= 1

    def increase_drop_count(self):
        with self._lock:
            self._drop_count += 1

    def decrease_drop_count(self):
        with self._lock:
            self._drop_count -= 1

    def _get_connection(self, block=False, timeout=None, right_side=True):
        """ 从空闲池拿 mysql 连接

           - 非阻塞模式:
            - block=False, timeout=None, 直接从连接池获取连接, 如果没有返回 None

           - 阻塞模式:
            - block=True, timeout=None, 直接从连接池获取连接, 如果没有一直等待被 notify 唤醒
            - block=True, timeout>0, 在 timeout 时间内等待被 notify 唤醒, 如果成功唤醒或者超时, 直接从连接池获取连接, 如果没有返回 None
        """
        with self._not_empty:
            logging.debug("DB:_get_connection: pool max connection count: %s, idle count: %s, active_count: %s",
                          self._max_connection, len(self._idle_queue), self._active_count)
            logging.debug("DB:_get_connection: _idle_queue: %s", self._idle_queue)
            if not block:
                if not self._qsize():
                    return None
            elif timeout is None:
                while not self._qsize():
                    self._not_empty.wait()
            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")
            else:
                self._not_empty.wait_for(self._qsize, timeout=timeout)
            return self._get(right_side=right_side)

    def _new_connection(self):
        """ 创建新的 mysql 连接
           如果连接数已经达到上限, 不在创建新的连接, 除非 _check_idle_timeout 清理掉空闲的连接, _active_count 下降
        """
        with self._lock:
            if self._active_count - self._drop_count >= self._max_connection:
                return None
            # 初始化连接实例, 并真实连上 mysql (defer_connect=False)
            conn = DBConnection(
                self,
                self.ts,
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                db=self._database,
                charset=self._charset,
                cursorclass=DBCursor,
                autocommit=True,
                defer_connect=False,
                read_timeout=self._read_timeout,
                write_timeout=self._write_timeout,
            )
            self._active_count += 1
            return conn

    def _check_idle_timeout(self):
        """ 检查并清理空闲超时的连接 """
        self._last_check_at = self.ts
        c = self._get_connection(block=False, right_side=False)
        while c:
            if not self._is_idle_timeout(c):
                self.recycle(c)
                break
            idle_seconds = self._last_check_at - c.idle_at
            logging.info("DB:_check_idle_timeout: connection %s idle for %s seconds, idle count: %s, active_count: %s, drop_count: %s"
                         % (c, idle_seconds, self._qsize(), self._active_count, self._drop_count))
            self._close_connection(c)
            c = self._get_connection(block=False, right_side=False)

    def _is_idle_timeout(self, c):
        """ 是否空闲超时 """
        return c.idle_at + self._idle_timeout < self.ts

    def _is_check_timeout(self):
        """ 是否检查空闲超时 """
        return self.ts - self._last_check_at > self._check_timeout

    @staticmethod
    def _log_connection_cost(c, started_at, pos, is_check):
        cost = time.time() * 1000 - started_at
        if cost >= 100:
            # 耗时过长时，需排查: 是否连接池过小 or 代码bug
            logging.warning('DB:connection: pos:%s, cost:%.2fms, is_check:%s', pos, cost, is_check)
        return c

    def connection(self):
        """ 从连接池获取一个连接 """
        started_at = time.time() * 1000
        # 定期清理空闲连接
        is_check = self._is_check_timeout()
        if is_check:
            self._check_idle_timeout()
        # 非阻塞模式获取连接
        c = self._get_connection(block=False, right_side=True)
        if c:
            return self._log_connection_cost(c, started_at, 1, is_check)
        c = self._new_connection()
        if c:
            return self._log_connection_cost(c, started_at, 2, is_check)

        # 阻塞模式获取连接
        c = self._get_connection(block=True, timeout=self._wait_timeout, right_side=True)
        if c:
            return self._log_connection_cost(c, started_at, 3, is_check)
        logging.error("DB:_new_connection: pool exceed max connection count: %s, idle count: %s, active_count: %s, drop_count: %s" %
                      (self._max_connection, self._qsize(), self._active_count, self._drop_count))
        raise PoolIsFullError(err_msg="DB:connection: pool exceed max connection")

    def recycle(self, c):
        """ 回收连接到连接池 """
        c.idle_at = self.ts
        with self._not_empty:
            if not self._contain(c):  # TODO：O(n)操作太耗时，确认不会进入else分支后移除此行代码
                self._put(c)
                # 唤醒等待获取连接的线程 _get_connection(block=True)
                self._not_empty.notify()
            else:
                logging.error('DB:recycle: unexpected error, connection:%s', c)


```


## 后话

MySQL 连接池只是 TCP 连接池的一种,  类似的我之前也有分享 Thrift RPC 连接池化的案例.
只要是长连接池化, 实现起来都差不多, 要根据不同业务场景和编程语言, 选择更合适的姿势.

最后,  要理解各种边界情况,  极端情况, 做好充分的压力测试.线上环境是很复杂的, 看似简洁的代码里尽是细节.

















