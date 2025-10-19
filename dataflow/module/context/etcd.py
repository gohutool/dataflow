from dataflow.module import Context
from dataflow.utils.log import Logger
from dataflow.utils.reflect import get_fullname
from dataflow.utils.dbtools.etcd import initEtcdWithConfig,EtcdTools


prefix = 'context.etcd'

_logger = Logger('dataflow.module.context.etcd')



class EtcdContext:
    ENABLED:bool = False
    @staticmethod    
    def getTool()->EtcdTools:                
        return Context.getContext().getBean(get_fullname(EtcdTools))
    @staticmethod ## 过期时间（秒）
    def lock(key, ttl:int=60, timeout:int=None):
        _client:EtcdTools = EtcdContext.getTool()
        return _client.Lock(key, ttl, timeout)
        


@Context.Configurationable(prefix=prefix)
def _init_etcd_context(config):
    c = config
    if c:
        _logger.INFO(f'初始化Etcd源{prefix}[{c}]开始')
        r = initEtcdWithConfig(c)            
        Context.getContext().registerBean(get_fullname(EtcdTools), r)
        _logger.INFO(f'初始化Etcd源{prefix}[{c}]={r}结束')      
        EtcdContext.ENABLED = True  
    else:
        _logger.INFO('没有配置Etcd源，跳过初始化')

