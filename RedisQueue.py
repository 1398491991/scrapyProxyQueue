#coding=utf-8
"""
基于 redis 的一个队列
线程安全(测试)
"""
from time import time as _time
try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading


__all__ = ['RedisQueueEmpty', 'RedisQueueFull', 'RedisQueueTimeOut','RedisQueue', 'LifoRedisQueue']

class RedisQueueEmpty(Exception):
    "当此队列元素个数为0，且 block==False 或者 timeout=0 抛出这个错误"
    pass

class RedisQueueFull(Exception):
    "当此队列元素个数大于队列限制最大元素个数时，且 block==False 或者 timeout=0 抛出这个错误"
    pass

class RedisQueueTimeOut(Exception):
    "put 或者 get 方法,并且 timeout 参数非零时，会抛出这个错误"
    pass


class RedisQueue:
    """先进先出队列"""
    def __init__(self,max_size,redis_conn,redis_key_default="RedisQueue"):
        """
        :param max_size: 限制队列最大元素个数 只能是数字
        :param redis_conn: 一个关于redis数据库的连接
        :param redis_key_default:存储在redis数据库中的键值的名称,默认为 RedisQueue 只能是字符串
        """
        self._max_size=max_size
        assert isinstance(self._max_size,int) is True,"Only allow 'max_size' type is int"
        self._redis_key=redis_key_default # 存储在 redis 中的键值名称 默认为 RedisQueue
        assert isinstance(self._redis_key,str) is True,"Only allow 'redis_key' type is str"
        self._redis_conn=redis_conn # 一个 redis 的连接

        self._not_full = _threading.Condition()

    def _get(self,timeout=0):
        return self._redis_conn.blpop(self._redis_key,timeout)

    def _put(self,item):
        return self._redis_conn.rpush(self._redis_key,item)

    def r_qsize(self):
        """ 返回的有序集合成员的数量 """
        return self._redis_conn.llen(self._redis_key)


    def r_get(self,block=True,timeout=None):
        """
        参见 python Queue.get()
        """
        try:
            if not block:
                if not self.r_qsize():
                    raise RedisQueueEmpty

            elif timeout is None:
                   return self._get()[-1] # 堵塞,直到有为止

            elif timeout < 0:
                raise ValueError("'timeout' must be a non-negative number")

            else:
                item=self._get(timeout) # 等待时间
                if item is None:
                    raise RedisQueueTimeOut
                else:
                    return item[-1]

        except Exception as e:
            raise e

    def r_put(self, item, block=True, timeout=None):
        """
        参见 python Queue.put()
        """
        try:
            if self._max_size > 0:
                if not block:
                    if self.r_qsize() >= self._max_size:
                        raise RedisQueueFull

                elif timeout is None:
                    return self._put(item)

                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")

                else:
                    end_time = _time() + timeout
                    while self.r_qsize() >= self._max_size:
                        remaining = end_time - _time()
                        if remaining <= 0.0:
                            raise RedisQueueTimeOut
                        self._not_full.wait(remaining)
                    return self._put(item)
            else:
                return self._put(item)

        except Exception as e:
            raise e

class LifoRedisQueue(RedisQueue):
    """后进先出队列"""
    def _get(self,timeout=0):
        """继承修改 _get 方法"""
        return self._redis_conn.brpop(self._redis_key,timeout)


