from dataflow.utils.antpath import find
from dataflow.utils.reflect import getType, get_fullname, getInstance,isList,isType,is_not_primitive,is_user_defined
# from dataflow.utils.reflect import inspect_own_method, inspect_class_method, inspect_static_method, getPydanticInstance
from jinja2 import Template
import re
import xml.etree.ElementTree as ET
from typing import Self
from dataflow.utils.utils import str_isEmpty,PageResult
from datetime import datetime, date
from pydantic import BaseModel,Field
from enum import Enum
from dataflow.utils.log import Logger
from dataflow.utils.reflect import inspect_own_method, inspect_class_method, inspect_static_method,getType
from dataflow.utils.dbtools.pydbc import PydbcTools
from typing import get_type_hints
import inspect
import functools


_logger = Logger('dataflow.utils.dbtools.pybatis')

# from jinja2 import Environment

# env = Environment(
#     loader=...,
#     trim_blocks=True,      # 去掉块级标签后的第一个空行
#     lstrip_blocks=True,    # 去掉块级标签前的空白
# )


class PageMode(BaseModel):    
    pageno: int = Field(..., min=1)  # 必填，长度限制
    pagesize: int = 20


_p = r'\{\$\s*.*?\s*\$\}'
_ip = r'^\{\$\s*|\s*\$\}$'

def _get_result_type(resultType:str):    
    if str_isEmpty(resultType):
        return dict
    if resultType == 'int':
        return int
    if resultType == 'str':
        return str
    if resultType == 'float':
        return float
    if resultType == 'dict':
        return dict
    if resultType == 'list':
        return list
    if resultType == 'datetime':
        return datetime
    if resultType == 'date':
        return date        
    return getType(resultType)

class SQLItem:
    class SQLType(Enum):
        SELECT = "SELECT" 
        UPDATE = "UPDATE"
        DELETE = "DELETE"
        INSERT = "INSERT"
        REF = "REF"
    def __init__(self, id:str, txt:str, type:str, sql:str, resultType:str=None, references:list[tuple[str, str, str]]=None, options:dict={}):
        self.txt = txt
        self.sql = sql
        self.type = type
        self.id = id
        self.resultType = None if str_isEmpty(resultType) else resultType.strip()
        self.references = references
        self.options = options
        self.sqlTemplate:Template = None
    
    def __repr__(self):
        # return f'type={self.type} txt={self.txt} sql={self.sql} resultType={self.getReulstType()}[{self.resultType}] references={self.references} options={self.options}'
        return f'type={self.type} txt={self.txt} sql={self.sql} resultType={self.getReulstType()}[{self.resultType}] references={self.references}'
    
    def hasReference(self):
        return self.references
    
    def build_sql(self, data:any)->str:
        return self.sqlTemplate.render(data)
    
    def getReulstType(self):
        return _get_result_type(self.resultType)
        # if str_isEmpty(self.resultType):
        #     return dict
        # if self.resultType == 'int':
        #     return int
        # if self.resultType == 'str':
        #     return str
        # if self.resultType == 'float':
        #     return float
        # if self.resultType == 'dict':
        #     return dict
        # if self.resultType == 'list':
        #     return list
        # if self.resultType == 'datetime':
        #     return datetime
        # if self.resultType == 'date':
        #     return date        
        # return getType(self.resultType)


class XMLConfig:
    _ALL_CONFIG:dict[str,str] = {}
    @staticmethod
    def scan_mapping_xml(root:str='conf', pattern:str='/**/*Mapper.xml'):
        xml_files = find(root, pattern)
        for p, xml_file in xml_files:
            xc:XMLConfig = XMLConfig.parseXML(xml_file)
            XMLConfig.putOne(xc)
    def __init__(self, namespace:str, sqls:dict[str,SQLItem]={}):
        self.namespace = namespace
        self.sqls = sqls
        self.references = {}        
        if sqls:
            # self.sqls.setdefault('id')
            for k, sql in sqls.items():
                if sql.type == SQLItem.SQLType.REF:
                    self.references[k] = sql
                    
        self.ready = False
        
    def __repr__(self):
        # print('\n'.join(self.sqls.items()))
        return f'namespace={self.namespace} sqls={'\n'.join(f'{k}: {v}' for k, v in self.sqls.items())} ready={self.ready}'
    
    def getSql(self, id:str)->SQLItem:
        if id not in self.sqls:
            raise KeyError(f'缺少 key：{id}')
        
        return self.sqls.get(id)
    
    def buildSql(self, id:str,data:any)->str:
        sql:SQLItem = self.getSql(id)
        return sql.build_sql(data)
    
    @staticmethod
    def sqlItem(namespace:str, id:str)->SQLItem:
        # if namespace not in XMLConfig._ALL_CONFIG:
        #     raise KeyError(f'缺少 namespace：{namespace}')
        xmlConfig:XMLConfig = XMLConfig.getXmlConfig(namespace)
        return xmlConfig.getSql(id)
    @staticmethod
    def getXmlConfig(namespace:str)->Self:
        if namespace not in XMLConfig._ALL_CONFIG:
            raise KeyError(f'缺少 namespace：{namespace}')
        xmlConfig:XMLConfig = XMLConfig._ALL_CONFIG[namespace]
        return xmlConfig
    @staticmethod    
    def build_sql(namespace:str, id:str, data:any)->str:
        return XMLConfig.sqlItem(namespace, id).build_sql(data)
            
    @staticmethod
    def placeholder_references(refs:dict[str, SQLItem])->dict[str, SQLItem]:        
        """原地解析嵌套占位符，返回新字典；循环依赖抛 ValueError。"""
        resolved: dict[str, SQLItem] = {}          # 已解析的最终值
        visiting: set[str] = set()             # 当前解析链（用于判环）
        
        def dfs(key: str) -> SQLItem:
            if key in resolved:
                return resolved[key]
            if key in visiting:
                raise ValueError(f'循环依赖检测到：{" -> ".join(visiting | {key})}')
            if key not in refs:
                raise KeyError(f'缺少 key：{key}')
            
            visiting.add(key)
            raw:SQLItem = refs[key]
            
            _refers = raw.references
            if _refers:
                _sql = raw.txt
                for o in _refers:
                    replace_k = o[0]
                    replace_id = o[1]
                    _sql = _sql.replace(replace_k, dfs(replace_id).sql)
                raw.sql = _sql                            
            else:
                raw.sql = raw.txt
            
            resolved[key] = raw
            visiting.remove(key)
            return resolved[key]
        
        for k in refs:
            dfs(k)
            
        return resolved
    
    def binding_references(self):        
        self.references = XMLConfig.placeholder_references(self.references)
        # print(self.references)
        for k, v in self.sqls.items():
            v:SQLItem = v
            if not v.type == SQLItem.SQLType.REF:
                if v.hasReference():
                    _sql = v.txt
                    for k in v.references:
                        replace_key = k[0]
                        ref_id = k[1]
                        if ref_id not in self.references:
                            raise KeyError(f'缺少 ref_id：{ref_id}')
                        else:                            
                            ref_sql = self.references[ref_id].sql
                            # print(f'ref_id={ref_id} {ref_sql}')
                                                    
                        _sql = _sql.replace(replace_key, ref_sql)
                    v.sql = _sql
                else:
                    v.sql = v.txt
                
                v.sqlTemplate = Template(v.sql, trim_blocks=True, lstrip_blocks=True)
        self.ready = True
            
    @staticmethod
    def putOne(xc:Self):
        xc:XMLConfig = xc
        XMLConfig._ALL_CONFIG[xc.namespace] = xc        
    @staticmethod
    def parseXML(xmlFile:str):
        xc:XMLConfig = _parse_xml(xmlFile)
        XMLConfig.putOne(xc)
        xc.binding_references()
        return xc
    @classmethod
    def is_test(cls):
        pass

def get_ref_name(txt:str)->list:
    a = re.findall(_p, txt)
    return [(blk, re.sub(_ip, '', blk)) for blk in a]
    
def _parse_xml(file:str)->XMLConfig:
    tree = ET.parse(file) 
    root = tree.getroot() 
    ns = root.attrib['namespace']
    
    
    # sqlnodes = root.iter('sql')
    sqls = {}
    for sqlnode in root:
        txt = sqlnode.text.strip()
        id = sqlnode.attrib['id']        
        _list = get_ref_name(txt)
        references = []
        for v in _list:
            references.append((v[0], v[1], None))
            
        tag = sqlnode.tag.strip()
        nodeType = 'select'
        opt = {}        
        if tag == 'update':
            nodeType = SQLItem.SQLType.UPDATE
            resultType = 'int'
        elif tag == 'delete':
            nodeType = SQLItem.SQLType.DELETE
            resultType = 'int'
        elif tag == 'insert':
            nodeType = SQLItem.SQLType.INSERT     
            sqlnode.attrib.setdefault('autoKey')
            autoKey = sqlnode.attrib['autoKey']
            if not str_isEmpty(autoKey):
                opt['autoKey'] = autoKey
            resultType = 'int'
        elif tag == 'ref':
            nodeType = SQLItem.SQLType.REF
            resultType = 'str'
        elif tag == 'select':
            nodeType = SQLItem.SQLType.SELECT
            sqlnode.attrib.setdefault('resultType')
            resultType = sqlnode.attrib['resultType']
        else:
            raise ValueError(f'不支持标签{tag}, 本版本目前可以支持select, update, delete, insert, ref')
            
        sqlItem = SQLItem(id, txt, nodeType, None, resultType, references, opt)
        sqls[sqlItem.id] = sqlItem
         
    xmlConfig = XMLConfig(ns, sqls)
    
    _logger.DEBUG(f'Mapper文件{file}解析到{len(sqls)}个SQL模板')
    # xmlConfig.sqls = sqls        
    return xmlConfig
    
def _binding_sql_with_func(func:callable, sql:str, sqlType:str, resultType:type, ds:PydbcTools, options:dict={}):    
    sig = inspect.signature(func)        
    # params = sig.parameters               # 2. 有序参数字典
    # return_ann = sig.return_annotation  
    type_hints = get_type_hints(func)
    
    # sqlType:str = sqlItem.type
    # resultType:type = sqlItem.getReulstType()
    return_type = type_hints.get('return') or resultType or dict 
    _sqlTemplate = Template(sql, trim_blocks=True, lstrip_blocks=True)
    # if sqlType == 'select':        

    @functools.wraps(func)        
    def _sql_proxy(*args, **kwargs)->any:
        bound = sig.bind_partial(*args, **kwargs)
        bound.apply_defaults()        
        _logger.DEBUG(f'{bound.arguments}=>{return_type}')
        # bound.arguments.pop('self')        
        sql = _sqlTemplate.render(bound.arguments)
        
        if sqlType == SQLItem.SQLType.DELETE: 
            return ds.delete(sql, bound.arguments)
        elif sqlType == SQLItem.SQLType.UPDATE: 
            return ds.update(sql, bound.arguments)
        elif sqlType == SQLItem.SQLType.INSERT: 
            autokey = None
            if options and 'autoKey' in options:
                autokey = options['autoKey']
            return ds.insert(sql, bound.arguments, autokey)
        elif sqlType == SQLItem.SQLType.SELECT:
            pageMode = None
            for k,v in bound.arguments.items():
                if isinstance(v, PageMode) or isinstance(sig.parameters[k], PageMode):
                    pageMode = v
            _isList = False
            _isPage = False
            if isList(return_type):
                _isList = True                                                                        
            if isType(return_type, PageResult) or pageMode:
                _isList = True
                _isPage = True
                
            print(f'================ _isPage={_isPage} _isList={_isList} return_type={return_type} resultType={resultType}')                                
            if _isPage:
                if pageMode is None:
                    pageMode = PageMode(pageno=1)                    
                rtn = ds.queryPage(sql, bound.arguments, pageMode.pageno, pageMode.pagesize)
                
                if not rtn.list:
                    return rtn
                
                if is_not_primitive(resultType):                    
                    if is_user_defined(resultType):
                        _l = [getInstance(resultType, one) for one in rtn]
                        rtn.list = _l
                        return rtn
                    else:
                        return rtn
                else:
                    _l = [next(iter(one.values())) for one in rtn.list]
                    rtn.list = _l
                    return rtn
            else:
                if _isList:
                    rtn = ds.queryMany(sql, bound.arguments)
                    if not rtn :
                        return rtn
                    
                    if is_not_primitive(resultType):
                        if is_user_defined(resultType):
                            _l = [getInstance(resultType, one) for one in rtn]
                            return _l
                        else:
                            return rtn                        
                    else:
                        _l = [next(iter(one.values())) for one in rtn]
                        return _l
                else:
                    rtn = ds.queryOne(sql, bound.arguments)
                    if rtn is None:
                        return rtn
                    else:
                        if is_not_primitive(return_type):
                            if isType(return_type, dict):
                                return rtn
                            else:
                                return getInstance(return_type, rtn)
                        else:
                            return next(iter(rtn.values()))
                            
        return func(*args, **kwargs)
        
    return _sql_proxy

    
def _binding_function_with_pybatis(cls, func_name:str, func:callable, xmlConfig:XMLConfig, ds:PydbcTools):
    _logger.DEBUG(f'{func_name}.{func}')
    
    sig = inspect.signature(func)        
    # params = sig.parameters               # 2. 有序参数字典
    # return_ann = sig.return_annotation  
    type_hints = get_type_hints(func)
    
    sqlItem:SQLItem = xmlConfig.getSql(func_name)
    sqlType:str = sqlItem.type
    resultType:type = sqlItem.getReulstType()
    return_type = type_hints.get('return') or resultType or dict 
    
    # if sqlType == 'select':        
        
    def _sql_proxy(self, *args, **kwargs)->any:
        bound = sig.bind_partial(self, *args, **kwargs)
        bound.apply_defaults()        
        _logger.DEBUG(f'{bound.arguments}=>{return_type}')
        bound.arguments.pop('self')
        
        sql = sqlItem.build_sql(bound.arguments)
        
        if sqlType == SQLItem.SQLType.DELETE: 
            return ds.delete(sql, bound.arguments)
        elif sqlType == SQLItem.SQLType.UPDATE: 
            return ds.update(sql, bound.arguments)
        elif sqlType == SQLItem.SQLType.INSERT: 
            autokey = None
            if sqlItem.options and 'autoKey' in sqlItem.options:
                autokey = sqlItem.options['autoKey']
            return ds.insert(sql, bound.arguments, autokey)
        elif sqlType == SQLItem.SQLType.SELECT:
            pageMode = None
            for k,v in bound.arguments.items():
                if isinstance(v, PageMode) or isinstance(sig.parameters[k], PageMode):
                    pageMode = v
            _isList = False
            _isPage = False
            if isList(return_type):
                _isList = True                                                                        
            if isType(return_type, PageResult) or pageMode:
                _isList = True
                _isPage = True
                
            print(f'================ _isPage={_isPage} _isList={_isList} return_type={return_type} resultType={resultType}')                                
            if _isPage:
                if pageMode is None:
                    pageMode = PageMode(pageno=1)                    
                rtn = ds.queryPage(sql, bound.arguments, pageMode.pageno, pageMode.pagesize)
                
                if not rtn.list:
                    return rtn
                
                if is_not_primitive(resultType):                    
                    if is_user_defined(resultType):
                        _l = [getInstance(resultType, one) for one in rtn]
                        rtn.list = _l
                        return rtn
                    else:
                        return rtn
                else:
                    _l = [next(iter(one.values())) for one in rtn.list]
                    rtn.list = _l
                    return rtn
            else:
                if _isList:
                    rtn = ds.queryMany(sql, bound.arguments)
                    if not rtn :
                        return rtn
                    
                    if is_not_primitive(resultType):
                        if is_user_defined(resultType):
                            _l = [getInstance(resultType, one) for one in rtn]
                            return _l
                        else:
                            return rtn                        
                    else:
                        _l = [next(iter(one.values())) for one in rtn]
                        return _l
                else:
                    rtn = ds.queryOne(sql, bound.arguments)
                    if rtn is None:
                        return rtn
                    else:
                        if is_not_primitive(return_type):
                            if isType(return_type, dict):
                                return rtn
                            else:
                                return getInstance(return_type, rtn)
                        else:
                            return next(iter(rtn.values()))
                            
        return func(self, *args, **kwargs)
        
    setattr(cls, func_name, _sql_proxy)

def Mapper(datasource:PydbcTools, *, namespace:str=None, table:str=None,id_col='id'):
    def mapper_decorator(cls):
        _table = table
        _id_col = id_col
        _namespace = namespace or get_fullname(cls)
        
        if str_isEmpty(_table):
            _table = cls.__name__
            
        if str_isEmpty(_table):
            _id_col = 'id'
            
        _logger.DEBUG(f'{cls}=>{_table}[{_id_col}] {datasource}')
                
        def getDataSource()->PydbcTools:
            return datasource
        
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
            
        # def say(self, pk:any)->int:
        #     print(f'=========={dir(self)} {self.name}{_id_col}={pk}')
        # cls.say = say
        # _logger.DEBUG(f'say.{say}')            
        
        _logger.DEBUG(f'namespace={_namespace}')
        xmlCOnfig:XMLConfig = XMLConfig.getXmlConfig(_namespace)
        
        funcs = inspect_own_method(cls)        
        for func in funcs:            
            _binding_function_with_pybatis(cls, func[0], func[1], xmlCOnfig, getDataSource())

        # 把方法挂到类上
        cls.select_by_id = select_by_id
        cls.select_list = select_list
        cls.insert = insert
        cls.update_by_id = update_by_id
        cls.delete_by_id = delete_by_id
        
        return cls
    
    return mapper_decorator


def SELECT(datasource:PydbcTools, sql:str=None, *, resultType:type|str=dict):
    if isinstance(resultType, str):
        resultType = _get_result_type(resultType)
    
    def decorator(func:callable)->callable:        
        return _binding_sql_with_func(func, sql, SQLItem.SQLType.SELECT, resultType=resultType, ds=datasource, options={})
    return decorator

def UPDATE(datasource:PydbcTools, *, sql:str=None):
    def decorator(func:callable)->callable:        
        return _binding_sql_with_func(func, sql, SQLItem.SQLType.UPDATE, resultType=int, ds=datasource, options={})
    return decorator


if __name__ == "__main__":
    
    rtn = find('conf', '**/sql/**')
    # rtn = find('conf', '**/sql/**')
    for o in rtn:
        print(o)
        
    template = Template('Hello, {{ user }}!')
    data = {
        'user1': 'Alice',
        'items': ['Book', 'Pen', 'Notebook']
    }
    output = template.render(data)
    print(output)
    
    s = 'xxx {$ common_condition $} yyy  {$ common_condition2 $} '
    print(get_ref_name(s))
    
    tname = get_fullname(XMLConfig)
    print(tname)
    t = getType(tname)
    print(t)
    data = {
        'namespace':tname
    }
    obj = getInstance(tname, data)
    print(obj)
    
    xc = XMLConfig.parseXML('conf/sql/userMapper.xml')
    print(xc)
    
    sqlItem:SQLItem = XMLConfig.sqlItem('application.user.mapper.UserMapper', 'select_by_id')
    print(sqlItem.sql)
    
    sql = XMLConfig.build_sql('application.user.mapper.UserMapper', 'select_by_id', data)
    print(f'SQL={sql}')
    # xc = _parse_xml('conf/sql/userMapper.xml')
    # print(xc)
    
    print(inspect_own_method(t))
    print(inspect_static_method(t))
    print(inspect_class_method(t))
    
    XMLConfig.scan_mapping_xml('conf', '**/*Mapper.xml')

    url = 'mysql+pymysql://u:p@localhost:61306/dataflow_test?charset=utf8mb4'
    p = PydbcTools(url=url, username='stock_agent', password='1qaz2wsx', test='select 1')    

    @Mapper(p, namespace='application.user.dao.UserMapper',table='sys_user',id_col='user_id')
    class TestMapper:    
        def __init__(self, name:str='LiuYong'):
            self.name = name
            
        def say(self, word, pageMode:PageMode)->PageResult:
            return f'{self.name} Say: {word}'
        
        def run(self, road)->str:
            return f'{self.name} run: {road}'

    print('== start')
    t = TestMapper('LiuYong')
    print(dir(t))    
    print(t.select_by_id('2'))
    print(f'Say={t.say('Liuyong', PageMode(pageno=2))}')
    print(f'Run={t.run('Shenzhen')}')
    
    @SELECT(p, 'select * from sys_user where user_name=:name and sex=:sex',resultType=dict)
    def select_test(name:str, sex:int, pm:PageMode):
        pass
    
    @UPDATE(p, sql='update sys_user set sex=sex where sex=:old_sex and sex<>:new_sex') 
    def update_test(old_sex:int, new_sex:int):
        pass
    
    print(select_test('ry', 1))
    print(update_test('1', 0))
    
    # t = TestMapper('Dataflow')
    # print(t.select_by_id('2'))
    # print(f'Say={t.say('Liuyong')}')
    # print(f'Run={t.run('Shenzhen')}')
