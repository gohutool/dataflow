from functools import wraps
from dataflow.module.context.datasource import DataSourceContext
from dataflow.utils.log import Logger
from dataflow.utils.reflect import inspect_own_method
from dataflow.utils.utils import str_isEmpty,PageResult
from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.dbtools.pybatis import PageMode


_logger = Logger('dataflow.module.context.pybatisplus')

def Mapper(datasource:str=None, *, table:str=None,id_col='id'):
    def mapper_decorator(cls):
        _table = table
        _id_col = id_col
        
        if str_isEmpty(_table):
            _table = cls.__name__
            
        if str_isEmpty(_table):
            _id_col = 'id'
            
        _logger.DEBUG(f'{cls}=>{_table}[{_id_col}] {datasource}')
                
        def getDataSource()->PydbcTools:
            return DataSourceContext.getDS(datasource)
        
        # ---------- CRUD 方法 ----------
        @classmethod
        def select_by_id(cls, pk:any)->dict:
            return getDataSource().queryOne(f'select * from {_table} where {_id_col}=:id ', {'id':pk})
                # return s.query(cls).get(pk)

        @classmethod
        def select_list(cls,page:PageMode=PageMode(pageno=1,pagesize=0))->PageResult:
            if not page:
                page = PageMode(pageno=1,pagesize=0)
                
            return getDataSource().queryPage(f'select * from {_table} order by id desc', {}, page.pageno, page.pagesize)

        @classmethod
        def insert(cls, entity:dict)->int:
            return getDataSource().insertT(_table, entity)

        @classmethod
        def update_by_id(cls, entity:dict)->int:
            return getDataSource().updateT(_table, entity,{_id_col:entity['id']})

        @classmethod
        def delete_by_id(cls, pk:any)->int:
            return getDataSource().deleteT(_table, {_id_col:pk})
        
        funcs = inspect_own_method(cls)
        
        for func in funcs:
            _logger.DEBUG(f'{func[0]}.{func[1]}')
        # def say(self, pk:any)->int:
        #     print(f'=========={dir(self)} {_id_col}={pk}')

        # 把方法挂到类上
        cls.select_by_id = select_by_id
        cls.select_list = select_list
        cls.insert = insert
        cls.update_by_id = update_by_id
        cls.delete_by_id = delete_by_id
        
        return cls
    
    return mapper_decorator

