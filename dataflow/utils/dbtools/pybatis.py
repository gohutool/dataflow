from dataflow.utils.antpath import find
from dataflow.utils.reflect import getType, get_fullname, getInstance, getPydanticInstance
from dataflow.utils.reflect import inspect_own_method, inspect_class_method, inspect_static_method
from jinja2 import Template
import re
import xml.etree.ElementTree as ET
from typing import Self
from dataflow.utils.utils import str_isEmpty
from datetime import datetime, date
_p = r'\{\$\s*.*?\s*\$\}'
_ip = r'^\{\$\s*|\s*\$\}$'

class SQLItem:
    def __init__(self, id:str, txt:str, type:str, sql:str, resultType:str=None, references:list[tuple[str, str, str]]=None, options:dict={}):
        self.txt = txt
        self.sql = sql
        self.type = type
        self.id = id
        self.resultType = None if str_isEmpty(resultType) else resultType.strip()
        self.references = references
        self.options = options
    
    def __repr__(self):
        # return f'type={self.type} txt={self.txt} sql={self.sql} resultType={self.getReulstType()}[{self.resultType}] references={self.references} options={self.options}'
        return f'type={self.type} txt={self.txt} sql={self.sql} resultType={self.getReulstType()}[{self.resultType}] references={self.references}'
    
    def hasReference(self):
        return self.references
    
    def getReulstType(self):
        if str_isEmpty(self.resultType):
            return dict
        if self.resultType == 'int':
            return int
        if self.resultType == 'str':
            return str
        if self.resultType == 'float':
            return float
        if self.resultType == 'dict':
            return dict
        if self.resultType == 'datetime':
            return datetime
        if self.resultType == 'date':
            return date        
        return getType(self.resultType)

class XMLConfig:
    _ALL_CONFIG:dict[str,str] = {}
    def __init__(self, namespace:str, sqls:dict[str,SQLItem]={}):
        self.namespace = namespace
        self.sqls = sqls
        self.references = {}        
        if sqls:
            # self.sqls.setdefault('id')
            for k, sql in sqls.items():
                if sql.type == 'ref':
                    self.references[k] = sql
                    
        self.ready = False
        
    def __repr__(self):
        # print('\n'.join(self.sqls.items()))
        return f'namespace={self.namespace} sqls={'\n'.join(f'{k}: {v}' for k, v in self.sqls.items())} ready={self.ready}'
    
    def getSql(self, id:str)->str:
        return self.sqls.get(id)
    
    @staticmethod
    def rebulid_references(refs:dict[str, SQLItem])->dict[str, SQLItem]:        
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
        self.references = XMLConfig.rebulid_references(self.references)
        # print(self.references)
        for k, v in self.sqls.items():
            v:SQLItem = v
            if not v.type == 'ref':
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
        txt = sqlnode.text
        id = sqlnode.attrib['id']        
        _list = get_ref_name(txt)
        references = []
        for v in _list:
            references.append((v[0], v[1], None))
            
        tag = sqlnode.tag.strip()
        nodeType = 'select'
        opt = {}        
        if tag == 'update':
            nodeType = 'update'
            resultType = 'int'
        elif tag == 'delete':
            nodeType = 'delet'
            resultType = 'int'
        elif tag == 'insert':
            nodeType = 'insert'     
            sqlnode.attrib.setdefault('autoKey')
            autoKey = sqlnode.attrib['autoKey']
            opt['autoKey'] = autoKey
            resultType = 'int'
        elif tag == 'ref':
            nodeType = 'ref'
            resultType = 'str'
        elif tag == 'select':
            nodeType = 'select'
            sqlnode.attrib.setdefault('resultType')
            resultType = sqlnode.attrib['resultType']
        else:
            raise ValueError(f'不支持标签{tag}, 本版本目前可以支持select, update, delete, insert, ref')
            
        sqlItem = SQLItem(id, txt, nodeType, None, resultType, references, opt)
        sqls[sqlItem.id] = sqlItem
         
    xmlConfig = XMLConfig(ns, sqls)
    # xmlConfig.sqls = sqls        
    return xmlConfig
    
    pass

if __name__ == "__main__":
    
    rtn = find('conf', '**/sql/**')
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
    
    # xc = _parse_xml('conf/sql/userMapper.xml')
    # print(xc)
    
    # print(inspect_own_method(t))
    # print(inspect_static_method(t))
    # print(inspect_class_method(t))