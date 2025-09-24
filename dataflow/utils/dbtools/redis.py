import redis
import uuid
import redis.typing
import time
from contextlib import contextmanager
from dataflow.utils.log import Logger

_logger = Logger('utils.dbtools.redis')


# REDIS_CONFIG = {
#         'host':'192.168.18.145', 
#         'port':6379, 
#         'db':14, 
#         'password':'Lszx)hz@redis_20201014'
# }

class RedisTools:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.__redis_client__ = redis.StrictRedis(host=host, port=port, db=db, password=password, decode_responses=True)

    @contextmanager
    def with_lock(self, lock_name, acquire_timeout=10, lock_timeout=600):
        try:
            lockid = self.acquire_lock(lock_name, acquire_timeout, lock_timeout)
            if lockid is None:
                raise RuntimeError(f'{lock_name}获取锁失败')
            # if lockid is None:
            #     return
            yield lockid
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e
        finally:
            self.release_lock(lock_name, lockid)
            
    def do_with_lock(self, lock_name, acquire_timeout=10, lock_timeout=600, func:callable=None):
        try:
            lockid = self.acquire_lock(lock_name, acquire_timeout, lock_timeout)
            if lockid is None:
                raise RuntimeError(f'{lock_name}获取锁失败')
            # if lockid is None:
            #     return
            if func is not None:
                func()
        except Exception as e:
            _logger.ERROR("[Exception]", e)
            raise e
        finally:
            self.release_lock(lock_name, lockid)
            
    def acquire_lock(self, lock_name, acquire_timeout=10, lock_timeout=600)->str:
        identifier = str(uuid.uuid4())
        end = time.time() + acquire_timeout
        while time.time() < end:
            if self.__redis_client__.set(lock_name, identifier, nx=True, ex=lock_timeout):
                _logger.DEBUG(f'Acquire lock with {identifier}[lock_name={lock_name} acquire_timeout={acquire_timeout},lock_timeout={lock_timeout}]')
                return identifier 
            time.sleep(0.01)
        _logger.DEBUG(f'Not acquire lock with {identifier}[lock_name={lock_name} acquire_timeout={acquire_timeout},lock_timeout={lock_timeout}]')
        return None

    def release_lock(self, lock_name, identifier)->bool:
        script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        rtn = self.__redis_client__.eval(script, 1, lock_name, identifier) == 1
        _logger.DEBUG(f'Release lock with {identifier}[lock_name={lock_name} result={rtn}]')
        return rtn

    def set(self, key, value, ex=None):
        """
        设置键值对
        :param key: 键
        :param value: 值
        :param ex: 过期时间（秒）
        :return: 是否成功
        """
        return self.__redis_client__.set(key, value, ex=ex)

    def get(self, key):
        """
        获取键的值
        :param key: 键
        :return: 值
        """
        return self.__redis_client__.get(key)

    def delete(self, key):
        """
        删除键
        :param key: 键
        :return: 是否成功
        """
        return self.__redis_client__.delete(key)

    def hset(self, name, key, value):
        """
        设置哈希表中的键值对
        :param name: 哈希表名称
        :param key: 键
        :param value: 值
        :return: 是否成功
        """
        return self.__redis_client__.hset(name, key, value)

    def hget(self, name, key):
        """
        获取哈希表中的键值
        :param name: 哈希表名称
        :param key: 键
        :return: 值
        """
        return self.__redis_client__.hget(name, key)

    def hgetall(self, name):
        """
        获取整个哈希表
        :param name: 哈希表名称
        :return: 哈希表
        """
        return self.__redis_client__.hgetall(name)

    def ttl(self, key)->redis.typing.ResponseT:
        return self.__redis_client__.ttl(key)