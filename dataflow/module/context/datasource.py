from dataflow.module import Context
from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.utils import str_isEmpty
from dataflow.utils.log import Logger

prefix = 'context.database'

_logger = Logger('module.context.datasource')

class DataSourceContext:
    @staticmethod    
    def getDS(ds_name:str=None)->PydbcTools:        
        if str_isEmpty(ds_name):
            ds_name = 'ds'            
        return Context.getContext().getBean(f'{ds_name}')
    

@Context.Configurationable(prefix=prefix)
def _init_datasource_context(config):
    c = config
    if c:
        default_ok:bool = False
        for k, v in c.items():                
            _logger.INFO(f'初始化数据源{prefix}.{k}[{v}]开始')
            pt = PydbcTools(**v)
            Context.getContext().registerBean(f'{k}', pt)
            _logger.INFO(f'初始化数据源{prefix}.{k}[{v}]={pt}成功')
            if not default_ok:
                Context.getContext().registerBean('ds', pt)
                default_ok = True
                _logger.INFO(f'设置默认数据源={pt}')
    else:
        _logger.INFO('没有配置数据源，跳过初始化')

_init_datasource_context()