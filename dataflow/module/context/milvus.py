from dataflow.module import Context
from dataflow.utils.reflect import get_fullname
from dataflow.utils.log import Logger
from dataflow.utils.dbtools.milvus import MilvusTools, initMilvusWithConfig


prefix = 'context.milvus'

_logger = Logger('dataflow.module.context.milvus')


class MilvusContext:
    @staticmethod    
    def getTool()->MilvusTools:                
        return Context.getContext().getBean(get_fullname(MilvusTools))
    

@Context.Configurationable(prefix=prefix)
def _init_redis_context(config):
    c = config
    if c:
        _logger.INFO(f'初始化Milvus源{prefix}[{c}]开始') 
        r = initMilvusWithConfig(c)            
        Context.getContext().registerBean(get_fullname(MilvusTools), r)
        _logger.INFO(f'初始化Milvus源{prefix}[{c}]={r}结束') 
    else:
        _logger.INFO('没有配置Milvus源，跳过初始化')

