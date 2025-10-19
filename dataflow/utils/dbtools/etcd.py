from dataflow.utils.log import Logger
from dataflow.utils.utils import json_to_str, current_datetime_str
from dataflow.utils.reflect import is_not_primitive
import time
from dataflow.utils.thread import newThread
from typing import Callable
import functools
import os
import importlib.metadata
version = importlib.metadata.version("protobuf")
_logger = Logger('dataflow.utils.dbtools.etcd')
if version > '3.22.0':
    os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'
    _logger.WARN(f'protobuf版本={version} 不兼容版本，或者降级protobuf版本至3.22以下，或者使用环境变量PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python，可能影响到性能')
else:
    _logger.WARN(f'protobuf版本={version} 兼容版本')
# from dataflow.utils.config import Settings  # noqa: F401

import etcd3  # noqa: E402
import etcd3.utils    # noqa: E402
import etcd3.exceptions  # noqa: E402
import grpc  # noqa: E402


class EtcdTools:
    @staticmethod
    def increment_last_byte(key:str)->bytes:
        return etcd3.utils.increment_last_byte(etcd3.utils.to_bytes(key))
    
    @staticmethod
    def lease_to_id(lease)->any:
        return etcd3.utils.lease_to_id(lease=lease)
    
    def __init__(self, host='localhost', port=2379, **kwargs):
        self._client = etcd3.client(host=host, port=port, **kwargs)
        _logger.DEBUG(f"EtcdTools {self._client}")
        
    def put(self, key, value, ex=None, prev_kv=False)-> any:
        """
        设置键值对
        :param key: 键
        :param value: 值
        :param ex: 过期时间（秒）        
        :param prev_kv: return the previous key-value pair
        :return: 是否成功
        """
        if is_not_primitive(value):
            value = json_to_str(value)
        
        if ex:
            ex = self._client.lease(ex)
        return self._client.put(key, value, lease=ex, prev_kv=prev_kv)
    
    
    def put_if_not_exists(self, key, value, ex=None):
        if is_not_primitive(value):
            value = json_to_str(value)
        
        if ex:
            ex = self._client.lease(ex)
        return self._client.put_if_not_exists(key, value, lease=ex)
    
    def get(self, key)->tuple[any,None]:
        """
        获取键的值
        :param key: 键
        :return: 值
        """
        return self._client.get(key)
    
    def delete(self, key, prev_kv=False, return_response=False):
        """
        删除键
        :param key: 键
        :return: 是否成功
        """
        return self._client.delete(key,prev_kv=False, return_response=False)
    
    def delete_prefix(self, prefix):
        return self._client.delete_prefix(prefix=prefix)
    
    def get_prefix(self, key_prefix, **kwargs):
        return self._client.get_prefix(key_prefix=key_prefix, **kwargs)
    
    def get_all(self):
        return self.get_all()

    def get_range(self, range_start, range_end, **kwargs):
        return self._client.get_range(range_start, range_end, **kwargs)
       
    def replace(self, key, initial_value, new_value):
        return self._client.replace(key, initial_value, new_value)
    
    def add_watch_callback(self, key, callback:Callable[[any,str],bool], **kwargs):
        return self.watch(key=key, callback=callback, **kwargs)
    
    def add_watch_prefix_callback(self, key_prefix, callback:Callable[[any,str],bool], **kwargs):        
        return self.watch_prefix(key_prefix=key_prefix, callback=callback, **kwargs)
    
    def watch(self, key, callback:Callable[[any,str],bool], start_revision=None,
                     progress_notify=False, filters=None, prev_kv=False):
        return self._watch(key, callback, None, start_revision=start_revision
                           ,progress_notify=progress_notify, filters=filters, prev_kv=prev_kv)
    
    def watch_prefix(self, key_prefix, callback:Callable[[any,str],bool], start_revision=None,
                     progress_notify=False, filters=None, prev_kv=False):
        return self._watch(key_prefix, callback, EtcdTools.increment_last_byte(key_prefix), start_revision=start_revision
                           ,progress_notify=progress_notify, filters=filters, prev_kv=prev_kv)
    
    def _watch(self, key, callback:Callable[[any,str],bool], range_end=None, start_revision=None,
                     progress_notify=False, filters=None, prev_kv=False):
        def watch_loop():
            
            def load_once()->any:
                """首次拉取 & 记录 revision"""            
                _, meta = self._client.get(key)
                if meta is not None:
                    last_revision = meta.mod_revision            
                    return last_revision
                return None
            
            last_revision = start_revision if start_revision else load_once()
            
            while True:
                try:
                    # 1. 先做一次全量，防止重连间隙丢数据                    
                    # last_revision = load_once()
                    # 2. 建立 watch：从 last_revision+1 开始
                    for event in self._client.watch(key, start_revision=(last_revision or 0)+1, range_end=range_end, 
                                                    progress_notify=progress_notify, 
                                                    filters=filters, 
                                                    prev_kv=prev_kv):
                        if event is None:          # 30s 内无事件，继续循环
                            continue                        
                        last_revision = event.mod_revision
                        try:                        
                            rtn = callback(event, key)
                            if rtn:
                                _logger.WARN(f'执行回调函数返回{rtn}，退出观察点')
                                return 
                        except Exception as e:
                            _logger.WARN(f'执行回调函数出错{e}')
                except grpc.RpcError as e:                    
                    _logger.WARN(f"watch broken ({e}), retry in 3s")
                    time.sleep(3)
                except StopIteration as e:
                    _logger.WARN(f"watch broken ({e}) by cancel, retry in 3s")
                    break
                except Exception as e:
                    _logger.WARN(f"watch broken ({e}), exit watch loop")
                    break 

        t = newThread(watch_loop, None, daemon=True)
        t.start()
        
    def lock(self, key, callback, ttl:int=60, timeout:int=None, **kwargs)->any:
        if ttl is None:
            ttl = 60
            
        try:
            lock = self._client.lock(key, ttl)
            lock.acquire(timeout=timeout)
            if callback and callable(callback):
                rtn = callback(**kwargs)
                return rtn
            else:
                pass
        except etcd3.exceptions.LockTimeout as e:
            _logger.WARN(f'申请锁已经超时{e}')
            raise e
        finally:
            if lock:
                lock.release()
                
    def Lock(self, key, ttl:int=60, timeout:int=None)->Callable:
        def decorater(func:Callable):
            @functools.wraps(func)
            def wrap(*args, **kwargs):
                def _call():
                    return func(*args, **kwargs)
                return self.lock(key, _call, ttl, timeout)                
            return wrap
        return decorater


def initEtcdWithConfig(config)->EtcdTools:
    if config is None:
        _CONFIG = {}
    else:
        if hasattr(config, '__dict__'):
            _CONFIG = vars(config)
        else:
            if isinstance(config, dict):
                _CONFIG = dict(config)
            else:
                _CONFIG = config
    
    _logger.DEBUG(f'Etcd初始化 {_CONFIG}')
    
    test_key = "test_key"
    
    if 'test' in _CONFIG:
        test_key =_CONFIG.pop('test')
                                
    _cs = EtcdTools(**_CONFIG)
    
    
    test = _cs.put(test_key, current_datetime_str(), 100)
        
    if test is None :
        raise Exception(f'Etcd不能访问 {_CONFIG}')
    
    return _cs
                

if __name__ == "__main__":    
    config = {
        'host':'localhost',
        'port':12379
    }
    _cs = initEtcdWithConfig(config)
    _logger.DEBUG(f'Etcd={_cs}')
    
    # _cs.put('python-test/test-key', 'Liuyong', 1000)
    
    _logger.DEBUG(_cs.get('python-test/test-key'))
    
    # def test_lock(a, b=100):
    #     _logger.DEBUG(f'测试Lock：{a}={b}')
    #     time.sleep(100)
    
    # _cs.lock('python-test/lock/aaa', test_lock, 100, None, a='LiuYong')
    
    # input('输入任何键退出')

                
                