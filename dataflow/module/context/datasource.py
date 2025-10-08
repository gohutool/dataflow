from dataflow.module import Context, Bean
from dataflow.utils.dbtools.pydbc import PydbcTools,Propagation, TX as _tx

from dataflow.utils.utils import str_isEmpty
from dataflow.utils.log import Logger
from dataflow.utils.reflect import get_fullname
from typing import Union,Type,Tuple

prefix = 'context.database'

_logger = Logger('dataflow.module.context.datasource')

class DataSourceContext:
    @staticmethod
    def getDefaultKey():
        return get_fullname(PydbcTools)
    
    @staticmethod    
    def getDS(ds_name:str=None)->PydbcTools:        
        if str_isEmpty(ds_name):
            ds_name = DataSourceContext.getDefaultKey()
                        
        return Context.getContext().getBean(f'{ds_name}')
    
class TransactionManager:
    def __init__(self, pydbc:PydbcTools):
        self._pydbc = pydbc
    
def TX(tx_name:str=None, *, propagation:Propagation=Propagation.REQUIRED,rollback_for:Union[Type[Exception], Tuple[Type[Exception], ...]] = Exception):
    
    if tx_name and isinstance(tx_name, str):
        tx = Bean(tx_name)
    elif tx_name:
        raise Context.ContextExceptoin('只能使用TransacrtionManager实例名称作为参数')
    else:
        tx = Bean(get_fullname(TransactionManager))
        
    tx:TransactionManager = tx    
    return _tx(tx._pydbc, propagation, rollback_for)

@Context.Configurationable(prefix=prefix)
def _init_datasource_context(config):
    c = config
    if c:
        default_ok:bool = False
        _default_c = {}
        for k, v in c.items(): 
            if isinstance(v, dict):
                _logger.INFO(f'初始化数据源{prefix}.{k}[{v}]开始')
                pt = PydbcTools(**v)
                Context.getContext().registerBean(f'{k}', pt)
                _logger.INFO(f'初始化数据源{prefix}.{k}[{v}]={pt}成功')
                if not default_ok:
                    Context.getContext().registerBean(DataSourceContext.getDefaultKey(), pt)
                    default_ok = True
                    _logger.INFO(f'设置默认数据源={pt}')
            else:
                _default_c[k] = v
                
        if _default_c and 'url' in _default_c:            
            pt = PydbcTools(**_default_c)
            _logger.INFO(f'初始化DEFAULT数据源{prefix}.ds[{_default_c}]={pt}成功')
            Context.getContext().registerBean(DataSourceContext.getDefaultKey(), pt)
            _logger.INFO(f'设置默认数据源={pt}')
        
        # 注册默认的TransactionManager实例，作为TX默认事务管理器
        tm:TransactionManager = TransactionManager(DataSourceContext.getDS(DataSourceContext.getDefaultKey()))
        Context.getContext().registerBean(get_fullname(TransactionManager), tm)
        _logger.INFO(f'设置默认TX事务管理器={tm}')
            
    else:
        _logger.INFO('没有配置数据源，跳过初始化')
