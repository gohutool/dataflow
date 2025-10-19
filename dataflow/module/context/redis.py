from dataflow.module import Context
from dataflow.utils.log import Logger
from dataflow.utils.reflect import get_fullname
from dataflow.utils.dbtools.redis import RedisTools, initRedisWithConfig
from typing import Callable
import functools
from dataflow.utils.utils import str_isEmpty,str_strip,json_to_str
from dataflow.utils.sign import b64_encode
from fastapi import Request

prefix = 'context.redis'

_logger = Logger('dataflow.module.context.redis')


class RedisContext:
    ENABLED:bool = False
    @staticmethod    
    def getTool()->RedisTools:                
        return Context.getContext().getBean(get_fullname(RedisTools))
    @staticmethod ## 过期时间（秒）
    def redis_cache(*,ttl:int=None,prefix:str=None,single:bool=False):
        rs_prefix = None
        if str_isEmpty(prefix):
            rs_prefix = 'context:redis:cache'
        else:
            rs_prefix = str_strip(prefix)        
        def _redis_cache_decorator(func: Callable):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):                
                # 调用原始的路由处理函数
                
                if RedisContext.ENABLED:
                    t:RedisTools = RedisContext.getTool()                                        
                    if single:
                        k = rs_prefix
                    else:
                        param = {}          
                        # param.update(kwargs)
                        # param.pop('request', None)
                        for k, v in kwargs.items():
                            if isinstance(v, Request):
                                continue
                            param[k] = v
                        
                        k = rs_prefix+':'+b64_encode(json_to_str(param))
                    result = t.getObject(k)
                    if not result:
                        result = await func(*args, **kwargs)
                        t.set(k, result, ttl)
                        _logger.DEBUG(f'没有命中缓存，获取值放入缓存[{k}-{ttl}]=>{result}')
                    else:
                        _logger.DEBUG(f'命中缓存，从缓存中获取值[{k}=>{result}]')
                else:
                    result = await func(*args, **kwargs)
                
                # 在请求处理完成之后执行的逻辑
                # print("After the request is processed")
                return result
            return wrapper
        _logger.DEBUG(f'创建Redis_cache装饰器[{rs_prefix},{single}]=>{_redis_cache_decorator}')            
        return _redis_cache_decorator
    

@Context.Configurationable(prefix=prefix)
def _init_redis_context(config):
    c = config
    if c:
        _logger.INFO(f'初始化Redis源{prefix}[{c}]开始')
        r = initRedisWithConfig(c)            
        Context.getContext().registerBean(get_fullname(RedisTools), r)
        _logger.INFO(f'初始化Redis源{prefix}[{c}]={r}结束')      
        RedisContext.ENABLED = True  
    else:
        _logger.INFO('没有配置Redis源，跳过初始化')

