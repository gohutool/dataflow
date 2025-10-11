from dataflow.utils.log import Logger
from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.dbtools.pybatis import Mapper as _Mapper, SELECT as _SELECT, UPDATE as _UPDATE, XMLConfig
from dataflow.module.context.datasource import DataSourceContext
from dataflow.module import Context

_logger = Logger('dataflow.module.context.pybatisplus')

def _get_datasource(datasource:str|PydbcTools):
    if datasource:
        if isinstance(datasource, str):
            datasource = DataSourceContext.getDS(datasource)
        elif isinstance(datasource, PydbcTools):
            datasource = datasource
        else:
            raise KeyError(f'缺少 datasource：{id}')
    else:
        datasource = DataSourceContext.getDS()
            
    return datasource
    

def Mapper(datasource:str|PydbcTools,namespace:str=None, table:str=None, id_col=None):
    datasource = _get_datasource(datasource)        
    return _Mapper(datasource, namespace=namespace, table=table, id_col=id_col)

def Selete(datasource:str|PydbcTools, sql:str=None, *, resultType:type|str=dict):
    datasource = _get_datasource(datasource)        
    return _SELECT(datasource, sql=sql, resultType=resultType)

def Update(datasource:str|PydbcTools, sql:str=None):
    datasource = _get_datasource(datasource)        
    return _UPDATE(datasource, sql=sql)


prefix = 'context.pybatisplus'


@Context.Configurationable(prefix=prefix)
def _init_datasource_context(config:dict):
    config.setdefault('root','conf')
    config.setdefault('pattern','/**/*Mapper.xml')    
    root:str = config['root']
    pattern:str = config['pattern']    
    XMLConfig.scan_mapping_xml(root=root, pattern=pattern)