from dataflow.module import Context
from dataflow.utils.log import Logger
from dataflow.utils.dbtools.redis import RedisTools, initRedisWithConfig


prefix = 'context.redis'

_logger = Logger('module.context.redis')


class RedisContext:
    @staticmethod    
    def getTool(ds_name:str=None)->RedisTools:                
        return Context.getContext().getBean(prefix)
    

@Context.Configurationable(prefix=prefix)
def _init_redis_context(config):
    c = config
    if c:
        r = initRedisWithConfig(c)            
        Context.getContext().registerBean(prefix, r)
        _logger.INFO(f'初始化Redis源{prefix}[{c}]={r}')        
    else:
        _logger.INFO('没有配置Redis源，跳过初始化')

_init_redis_context()
