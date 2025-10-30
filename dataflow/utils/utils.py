from datetime import datetime,date
from urllib import parse
from pandas import DataFrame
from typing import List
import pandas as pd
import numpy as np
import traceback
import importlib
import pytz
from typing import Any, Optional
import time
import re
import json
from pydantic import BaseModel, ConfigDict
import decimal
import threading
from itertools import count
import uuid
from uuid import UUID as UUIDOBJ
import sys
import os

# from pydantic import BaseModel, Field

def set_cn_timezone():    
    # 设置时区（必须在导入其他时间相关模块前设置）
    os.environ["TZ"] = "Asia/Shanghai"
    if hasattr(time, 'tzset'):          # Unix / macOS / WSL
        time.tzset()

def date_datetime_cn(dt:datetime=None):
    if dt is None:
        now = datetime.now(pytz.timezone('Asia/Shanghai')) 
        return now
    else :
        # 定义中国时区（UTC+8）
        china_tz = pytz.timezone('Asia/Shanghai')
        # 将 datetime 对象转换为带有时区信息的中国时区对象
        china_time = dt.replace(tzinfo=pytz.utc).astimezone(china_tz)
        return china_time

def date_date_cn():    
    now = date_datetime_cn().date()
    return now

def date_which_week_day_info(target_date:str|date|datetime=None):
    # 如果 target_date 为 None，则使用当前日期
    if target_date is None:
        target_date = date_date_cn()
     
    # 如果 target_date 是 datetime.datetime 类型，转换为 date 类型
    if isinstance(target_date, datetime):
        target_date = target_date.date()
    
    # 如果 target_date 是 str 类型，转换为 date 类型
    if isinstance(target_date, str):
        try:
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("日期格式错误，请使用 YYYY-MM-DD 格式。")    
    c = target_date.isocalendar()
    return c[1], c[2]
    # if isinstance(target_date, date):
    #     target_date = target_date.strftime("%Y-%m-%d")        

def find_index(datas:list[dict], key:str='id', value:any=None)->int:
    # 使用 enumerate 和列表推导式找到索引
    index = next((i for i, data in enumerate(datas) if data[key] == value), None)

    return index


def dynamic_instantiate(full_class_path, **kwargs):
    # 分割模块路径和类名
    module_path, class_name = full_class_path.rsplit('.', 1)
    
    # 动态加载模块
    module = importlib.import_module(module_path)
    
    # 获取类对象
    cls = getattr(module, class_name, None)
    
    # 检查是否是类
    if cls and isinstance(cls, type):
        # 实例化类
        instance = cls(**kwargs)
        return instance
    else:
        raise ValueError(f"Class {class_name} not found in module {module_path}.")

def test_na():
    # 创建一个示例 DataFrame
    data = {
        'A': [1, 2, np.nan, 4],
        'B': [5, 'nan', 7, 'NaN'],
        'C': [9, 10, 11, np.nan]
    }

    df = pd.DataFrame(data)

    # 打印原始 DataFrame
    print("原始 DataFrame:")
    print(df)

    # 定义一个函数，将 NaN 或 'nan'（忽略大小写）替换为 None
    def replace_nan_with_none(value):
        # 检查是否为 NaN
        if pd.isna(value):
            return None
        # 检查是否为字符串类型
        if isinstance(value, str):
            # 将字符串转换为小写后与 'nan' 比较
            if value.lower() == 'nan':
                return None
        return value

    # 使用 apply 方法结合 lambda 函数应用该函数
    # df = df.apply(lambda x: x.apply(replace_nan_with_none))
    # 使用 numpy 的向量化操作
    # df = df.apply(lambda x: np.where(
    #     pd.isna(x) | (x.astype(str).str.lower() == 'nan'),
    #     None,
    #     x
    # ))

    df = dataframe_fillna(df)
    # 打印处理后的 DataFrame
    print("\n处理后的 DataFrame:")
    print(df)


def str_strip(txt:str)->str:
    if str_isEmpty(txt):
        return ''
    return re.sub(r'\s+', ' ', txt).strip()

def str_isEmpty(txt:str)->bool:
    if txt is None or str(txt).strip() == '':
        return True
    return False

def int_to_str(dict:dict, key:str):
    if key in dict:
        v = dict[key]        
        if v is not None:
            # print(format(dict[key], '.0f'))
            # dict[key] = format(dict[key], '.4f').rstrip('0').rstrip('.')
            dict[key] = number_to_str(v)
            
def number_to_str(v):    
    if v is None:
        return None    
    if isinstance(v, str) and (v == '-' or v == ''):
        return None
    
    return format(v, '.4f').rstrip('0').rstrip('.')


# def replace_nan_with_none(value):
#     if pd.isna(value):
#         return None
#     if isinstance(value, str) and value.lower() == 'nan':
#         return None
#     return value

def dataframe_fillna(df:DataFrame)->DataFrame:
    if df.empty:
        return df
    # 使用 applymap 方法应用该函数
    # df = df.applymap(replace_nan_with_none)
    # df = df.apply(lambda x: x.apply(replace_nan_with_none))
    # return df
    # return df.where(pd.notnull(df), None)
    # return df.fillna(None)
    # df = df.apply(lambda x: x.apply(replace_nan_with_none))
    df = df.apply(lambda x: np.where(
        pd.isna(x) | (x.astype(str).str.lower() == 'nan'),
        None,
        x
    ))
    return df

def dataframe_to_dict(df:DataFrame, key_field:str='item', value_field:str='value')->dict:
    df = dataframe_fillna(df)
    return df.set_index(key_field)[value_field].to_dict()

# df.to_dict(orient='records')：将 DataFrame 转换为一个列表，其中每一行是一个字典。每个字典的键是列名，值是对应行的值。
# orient='records'：指定转换的格式为记录格式，即每一行是一个字典。
def dataframe_to_list(df:DataFrame)->List[dict]:
    # print(df)
    df = dataframe_fillna(df)
    # print(df)
    return df.to_dict(orient='records')

# # 创建日志目录
# log_dir = "logs"
# os.makedirs(log_dir, exist_ok=True)

# __logger = logging.getLogger("Logger")
# __logger.setLevel(logging.DEBUG) 
# # 创建格式化器
# formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(message)s')
# # 创建日志格式
# formatter = logging.Formatter(
#     fmt="%(levelname)s - %(asctime)s - %(name)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S"
# )

# # file_handler = logging.handlers.RotatingFileHandler(f"{log_dir}/ea.log")
# # 创建文件处理器（带日志轮转）
# file_handler = logging.handlers.RotatingFileHandler(
#     filename=os.path.join(log_dir, "ea.log"),
#     maxBytes=1024 * 1024 * 5,  # 5MB
#     backupCount=3  # 保留 3 个备份文件
# )

# file_handler.setLevel(logging.DEBUG)
# file_handler.setFormatter(formatter)
# __logger.addHandler(file_handler)

# # 创建并添加控制台处理器
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# console_handler.setFormatter(formatter)
# __logger.addHandler(console_handler)



# 一劳永逸
# def _datetime_converter(obj: object) -> str:
#     """
#     把 datetime / date 对象转成 ISO-8601 字符串
#     """    
#     if isinstance(obj, (datetime, date)):        
#         return date2str_yyyymmddddmmss(obj)
    
    
# json.JSONEncoder.default = staticmethod(_datetime_converter)

# class DateTimeEncoder(json.JSONEncoder):
#     """自定义 JSON 编码器，处理 datetime 和 date 对象"""
#     def default(self, obj):
#         if isinstance(obj, (datetime, date)):
#             # 返回 ISO 格式字符串
#             return obj.isoformat()
#         # 对于其他类型，使用默认的序列化方法
#         return super().default(obj)

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        
        if isinstance(obj, datetime):
            # datetime转换
            return date2str_yyyymmddddmmss(obj)
        elif isinstance(obj, date):
            # date转换
            return date2str_yyyymmddddmmss(obj)   
        elif isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            if has_method(obj, 'dict'):
                return obj.dict()
                                        
        raise TypeError(f'Unknown type {type(obj)}')

# # json.JSONEncoder.default = DateTimeEncoder().default

def has_method(o, m):
    return callable(getattr(o, m, None))

def json_to_str(obj:any):
    return json.dumps(obj, ensure_ascii=False, separators=(',', ':'), cls=DateEncoder)
    # if has_method(obj, 'dict'):
    #     obj = obj.dict()        
    # return orjson.dumps(obj).decode()
    
# 每个线程独享的计数器
_local = threading.local()    
def get_unique_seq()->int:
    # return int(time.time() * 100000000000000000)    
    """线程内唯一整数，从 1 开始递增"""
    if not hasattr(_local, 'cnt'):
        _local.cnt = count(1)      # 新建迭代器
    return next(_local.cnt)

def str_to_json(txt:str)->dict|list:
    return json.loads(txt)

def current_time()->float:
    return time.time()

def current_millsecond()-> int:
    return int(current_time() * 1000)

def current_datetime():
    return datetime.now()

def current_datetime_str():
    current_time = current_datetime().strftime('%Y-%m-%d %H:%M:%S') 
    return current_time

def str2date_yyyymmddddmmss(date_str):    
    date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
    return date

def str2date_yyyymmdd(date_str):    
    date = datetime.strptime(date_str, "%Y%m%d").date()
    return date

def str2date_yyyymmddhhmmsss(date_str):    
    date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S.%f").date()
    return date

def date2str_yyyymmdd(date):
    return date.strftime("%Y%m%d")

def date2str_yyyymmddddmmss(date):
    return date.strftime("%Y-%m-%d %H:%M:%S")

def date2str_yyyymmddhhmmsss(date):
    return date.strftime("%Y-%m-%d %H:%M:%S.%f")[:-2]

def str2datestr_yyyymmddhhmmsss(date_str):    
    return date2str_yyyymmdd(str2date_yyyymmddhhmmsss(date_str))

def fill_error_stack(e):
    tb = traceback.format_exc()
    return f"异常: {e}\n{tb}"

def utf8_urldecode(txt):
    if txt is None:
        return txt
    str1 = txt.replace('%u', '\\u')    
    s = str1.encode('utf-8').decode('unicode_escape')  
    s = parse.unquote(s)
    # print("1:", s)
    # print("2:", txt)
    # escape_str = '%23%u5DF4%u5F66%u6DD6%u5C14%u5E02%u5965%u9686%u5DE5%u7A0B%u5EFA%u8BBE%u6709%u9650%u516C%u53F8'  
    # str1 = escape_str.replace('%u', '\\u').encode('utf-8').decode('unicode_escape')  
    # print("3:", str1)s    
    return s

def str2Bool(s: any, dv=False) -> bool:    
    if str_isEmpty(s) :
        return dv
    s = str(s)
    return s.lower().strip() in ('true', '1', 'yes', 'on')

def str2Num(s, dv=None):
    if isinstance(s, (int, float)) and not isinstance(s, bool):
        return s
    try:
        f = float(s.strip())
        # 去掉 .0 的小数可以转 int
        return int(f) if f.is_integer() else f
    except ValueError:
        try:
            return float(s)
        except ValueError as e:            
            if dv is None:
                raise e
            return dv  # 或返回默认值    
    except Exception as e:    
        # raise e
        
        return dv

def str_is_text_include_checkstr_in_checklist(txt:str, checks:list[str])->bool:  
    if txt is None or len(txt.strip()) == 0:
        return False
    if checks is None or len(checks) == 0:
        return False
    
    for item in checks:
        if item in txt:
            return True
    return False

def dataframe_from_xls(filename:str, header=None, ignore:list[str]=None)->pd.DataFrame:
    # 使用 pandas 的 ExcelFile 类获取所有工作表的名称
    xls = pd.ExcelFile(filename)

    # 创建一个空的 DataFrame 用于存储所有数据
    all_data = pd.DataFrame()

    # 遍历所有工作表
    for sheet_name in xls.sheet_names:
        # 读取当前工作表的数据
        if not str_is_text_include_checkstr_in_checklist(sheet_name, ignore):
            df = pd.read_excel(xls, sheet_name=sheet_name, header=header)
            # 将当前工作表的数据添加到总的 DataFrame 中
            all_data = pd.concat([all_data, df], ignore_index=True)
        
    return all_data

def dataframe_get_column_data(df:pd.DataFrame, column_idx:int=0)->list:
    if df.empty:
        return []
    
    return df[df.columns[column_idx]]

def dataframe_get_rows_from_xls(filename:str)->tuple:
        df = dataframe_from_xls(filename, ignore=['忽略'])
        # print(df)
        rows = dataframe_get_column_data(df)
        rows = list(set(rows))
        stocks = []
        bankuais = []
        for row in rows:
            if not str_isEmpty(row):
                if row.endswith('板块'):
                    row = row.replace('板块','')
                    bankuais.append(row)
                else:
                    stocks.append(row)
                    
        stocks = list(set(stocks))
        bankuais = list(set(bankuais))                    
        return (stocks, bankuais)
    
def obj_2_json_file(obj:any, filepath:str):
    json_data = json_to_str(obj)
        # 将 JSON 数据保存到文件，指定编码为 UTF-8
    with open(filepath, "w", encoding="utf-8") as file:
        file.write(json_data)
        
def obj_from_json_file(filepath:str)->any:       
    # 打开 JSON 文件并读取内容
    with open(filepath, "r", encoding="utf-8") as file:
        # 使用 json.load() 将 JSON 数据转换为 Python 对象（列表或字典）
        loaded_data = json.load(file)
        return loaded_data
    
def list_add_field(datas:list, key:str, v:any)->list:
    # 遍历列表，为每个字典增加 type 字段
    for data in datas:
        data[key] = v
    return datas

def list_update_data(datas:list, updateFunc)->list:
    # 遍历列表，为每个字典增加 type 字段
    for data in datas:
        updateFunc(data,datas)
    return datas

def build_full_code(code:str, market:str='SH'):
    market = 'SZ' if code.startswith('4') else market
    return f'{market}{code}'

def build_market(code:str, market:str='SH'):
    market = 'SZ' if code.startswith('4') or code.startswith('83') or code.startswith('87')  or code.startswith('92') else market
    return market


def getAttrPlus(data:dict, field:str, dv:any=None)->any:
    if data is None:
        return dv
    rtn = None
    
    obj = data
    
    """'a.b.c' -> 逐层取值"""
    for key in field.split('.'):        
        if isinstance(obj, dict):
            if key in obj:
                obj = obj[key]
            else:
                obj = None
        else:
            obj = getattr(obj, key, None)    
            
        if obj is None:
            break       
        
    if obj is None:
        rtn = dv
    else:
        rtn = obj
            
    return rtn

def get_str_from_dict(d:dict, k:any, kv:str=None):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
    if v is None:
        v = kv
    return str(v)


def get_int_from_dict(d:dict, k:any, kv:int=0):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
    if v is None:
        v = kv
    rtn = str2Num(v, kv)
    if rtn is None:
        return rtn        
    return int(rtn)

def get_float_from_dict(d:dict, k:any, kv:float=0):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
    if v is None:
        v = kv
    return str2Num(v, kv)

def get_bool_from_dict(d:dict, k:any, kv:bool=False):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
    if v is None:
        v = kv
    return str2Bool(v, kv)

def get_from_dict(d:dict, k:any, kv:any=None):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
    if v is None:
        v = kv
    return v

def get_list_from_dict(d:dict, k:any, kv:any=None):
    v = None
    if k in d:
        v = getAttrPlus(d, k, kv)
        if isinstance(v, str):
            v = str_strip(v).split(',')
    if v is None:
        v = kv
    return v

def center_str(txt:str, width:int, fillchar=' ', trim:bool=False):
    txt = str_strip(txt)
    if trim and len(txt) >= width:
        return txt[:width]
    return txt.center(width, fillchar)

def l_str(txt:str, width:int, fillchar=' ', trim:bool=False):
    txt = str_strip(txt)
    if trim and len(txt) >= width:
        return txt[:width]
    return txt.ljust(width, fillchar)

def r_str(txt:str, width:int, fillchar=' ', trim:bool=False):
    txt = str_strip(txt)
    if trim and len(txt) >= width:
        return txt[:width]
    return txt.rjust(width, fillchar)

def center_byts(d:bytes, width:int, fillchar=b'\0', trim:bool=False):
    if d is not None:
        if trim and len(d) >= width:
            return d[:width]
        return d.center(width, fillchar)
    return d

def l_bytes(d:bytes, width:int, fillchar=b'\0', trim:bool=False):
    if d is not None:
        if trim and len(d) >= width:
            return d[:width]
        return d.ljust(width, fillchar)
    return d

def r_bytes(d:bytes, width:int, fillchar=b'\0', trim:bool=False):
    if d is not None:
        if trim and len(d) >= width:
            return d[:width]
        return d.rjust(width, fillchar)
    return d


def parse_long_args(argv=sys.argv[1:]) -> dict[str, any]:
    """
    把 --a.b.c=value 变成 {'a': {'b': {'c': value}}}
    支持自动类型推断 int/float/bool
    """
    def cast(v: str):
        if v.isdigit(): 
            return int(v)
        try: 
            return float(v)
        except ValueError: 
            pass
        
        if v.lower() in ('true', 'false', 'yes', 'no', 'on', 'off'): 
            return v.lower() == 'true' or v.lower() == 'on' or v.lower() == 'yes'        
        return v

    cfg: dict = {}
    
    for token in argv:
        if not token.startswith('--') or '=' not in token:
            continue
        k, v = token[2:].split('=', 1)
        keys = k.split('.')
        d = cfg
        for kk in keys[:-1]:
            d = d.setdefault(kk, {})
        d[keys[-1]] = cast(v)
    return cfg    

def UUID():
    return uuid.uuid4()

def SNAKE_UUID(worker:str):
    return uuid.uuid5(UUIDOBJ('6ba7b814-9dad-11d1-80b4-00c04fd430c8'), worker)

# def copy2Dict(
#                 obj: Any, 
#                 include_private: bool = False,
#                 include_methods: bool = False,
#                 exclude_attrs: Optional[List[str]] = None,
#                 max_depth: int = 1
#     )->dict[str,any]: 
#     """
#     万能对象转字典方法
    
#     Args:
#         obj: 要转换的对象
#         include_private: 是否包含私有属性
#         include_methods: 是否包含方法
#         exclude_attrs: 要排除的属性名列表
#         max_depth: 最大递归深度（用于处理嵌套对象）
#     """
#     if exclude_attrs is None:
#         exclude_attrs = []
    
#     if max_depth <= 0:
#         return obj
    
#     # 如果是基本类型，直接返回
#     if isinstance(obj, (str, int, float, bool, type(None))):
#         return obj
    
#     # 如果是字典，递归处理值
#     if isinstance(obj, dict):
#         return {k: copy2Dict(v, include_private, include_methods, exclude_attrs, max_depth-1) 
#                 for k, v in obj.items()}
    
#     # 如果是列表或元组，递归处理元素
#     if isinstance(obj, (list, tuple, set)):
#         return [copy2Dict(item, include_private, include_methods, exclude_attrs, max_depth-1) 
#                 for item in obj]
    
#     # 处理对象
#     result = {}
    
#     for attr_name in dir(obj):
#         # 跳过特殊方法
#         if attr_name.startswith('__') and attr_name.endswith('__'):
#             continue
        
#         # 过滤私有属性
#         if not include_private and attr_name.startswith('_'):
#             continue
        
#         # 过滤排除的属性
#         if attr_name in exclude_attrs:
#             continue
        
#         try:
#             attr_value = getattr(obj, attr_name)
            
#             # 过滤方法（如果不包含方法）
#             if not include_methods and callable(attr_value):
#                 continue
            
#             # 递归处理嵌套对象
#             result[attr_name] = copy2Dict(
#                 attr_value, include_private, include_methods, exclude_attrs, max_depth-1
#             )
            
#         except (AttributeError, Exception):
#             # 跳过无法访问的属性
#             continue
    
#     return result

class MyBase(BaseModel):
    model_config = ConfigDict(
        json_encoders={          # v1 语法，v2 也可用
            datetime: lambda v: date2str_yyyymmddddmmss(v),
            date: lambda v: date2str_yyyymmddddmmss(v)
        }
    )

class PageResult:
    # total :int = Field(0, description="总条数")
    # pagesize :int = Field(10, description="每页大小")    
    # page :int = Field(1, description="页序号")
    # totalPage :int = Field(0, description="总页数")
    # list : Optional[List[Any]] = Field([], description="行记录")
    # total,pagesize,page,totalPage,list
    
    # def __init__(self, total=0, pagesize=10, page=1, totalPage=0, list=None):
    def __init__(
        self,
        total: int  = 0 ,
        pagesize: int =10,
        page: int = 1,
        totalPage: int = 0,
        list: Optional[List[Any]] = None
    ):
        """
        初始化 ValueObject 类的实例。
        :param total: 总记录数
        :param pagesize: 每页显示的记录数
        :param page: 当前页码，默认为 1
        :param totalPage: 总页数，默认为 1
        :param list: 当前页的记录列表，默认为空列表
        """
        self.total = total
        self.pagesize = pagesize
        self.page = page
        self.totalPage = totalPage
        self.list = list if list is not None else []

    def __eq__(self, other):
        """
        定义对象的相等性。
        """
        if isinstance(other, PageResult):
            return (self.total == other.total and
                    self.pagesize == other.pagesize and
                    self.page == other.page and
                    self.totalPage == other.totalPage and
                    self.list == other.list)
        return False

    def __hash__(self):
        """
        定义对象的哈希值。
        """
        return hash((self.total, self.pagesize, self.page, self.totalPage, tuple(self.list)))

    
    # ① 供 Pydantic 序列化
    def dict(self) -> dict:
        return {
            "total": self.total,
            "pagesize": self.pagesize,
            "page": self.page,
            "totalPage": self.totalPage,
            "list": self.list,
        }

    
    def __repr__(self):
        """
        定义对象的字符串表示。
        """
        return (f"PageResult(total={self.total}, pagesize={self.pagesize}, "
                f"page={self.page}, totalPage={self.totalPage}, list={self.list})")

class ReponseVO:
    # status: bool = Field(True, description="响应状态")
    # msg: str = Field('成功', description="返回消息")
    # data: Any = Field(None, description="返回数据")
    def __init__(self, status:bool=True, msg:str='成功', code:int=200, data:any=None): 
        self.status = status
        self.msg = msg
        self.data = data
        self.code = code
        
    # ① 供 Pydantic 序列化
    def dict(self) -> dict:
        return {
            "status": self.status,
            "msg": self.msg,
            "data": self.data,
            "code": self.code,
        }
        
    def __repr__(self):
        """
        定义对象的字符串表示。
        """
        return (f"ReponseVO(status={self.status}, code={self.code}, msg={self.msg}, data={self.data}")
    
if __name__ == "__main__":
    t = date_datetime_cn()
    print(f'{t}=={json_to_str(t)}')
    
    print(str2datestr_yyyymmddhhmmsss('2024-01-12 00:00:00.0')) 
    txt = '\\u5e7f\\u4e1c\\u7701\\u6df1\\u5733\\u5e02\\u5b9d\\u5b89\\u533a\\u77f3\\u5ca9\\u8857\\u9053\\u6c34\\u7530\\u77f3\\u9f99\\u5927\\u905326\\u53f7'   
    # txt = '%u7701%u4EFD%23'
    print(utf8_urldecode(txt))
    txt = '%23%u5DF4%u5F66%u6DD6%u5C14%u5E02%u5965%u9686%u5DE5%u7A0B%u5EFA%u8BBE%u6709%u9650%u516C%u53F8'   
    # txt = '%u7701%u4EFD%23'
    print(utf8_urldecode(txt))
    # 要编码的URL
    # encoded_string = '%3Ch3%3E%u4EBA%u5747%u4E8C%u6C27%u5316%u78B3'

    # decoded_string = parse.unquote(encoded_string,'unicode_escape')
    # print(decoded_string)
    
    # print(parse.quote('中文','unicode_escape'))
    # print(decode_unicode_string('%u7701%u4EFD%23'))
    
    # s = "陕中"  
    # encoded_s = parse.quote(s.replace("%u", "%%u"))  
    # print(encoded_s)
    
    # s = "Hello, world中国!"  
    # unicode_s = s.encode("unicode_escape")  
    # print(unicode_s)
    
    
    
    # s = "Hello, world中国!"  
    # unicode_s = s.encode("unicode_escape")  
    # print(unicode_s)
    
  
    # encoded_url = "%u7701%%u4EFD%23"  
    # decoded_url = parse.unquote(encoded_url)  
    # print(decoded_url)
    
    # aa = '\\u5e7f\\u4e1c\\u7701\\u6df1\\u5733\\u5e02\\u5b9d\\u5b89\\u533a\\u77f3\\u5ca9\\u8857\\u9053\\u6c34\\u7530\\u77f3\\u9f99\\u5927\\u905326\\u53f7'
    # print(aa.encode('utf-8').decode("unicode_escape"))
    
    # #解码
    # escape_str = '%23%u5DF4%u5F66%u6DD6%u5C14%u5E02%u5965%u9686%u5DE5%u7A0B%u5EFA%u8BBE%u6709%u9650%u516C%u53F8'
    # str1 = escape_str.replace('%u', '\\u')
    # str = str1.encode('utf-8').decode('unicode_escape')
    # print(str)
    
    # escape_str = '%23%u5DF4%u5F66%u6DD6%u5C14%u5E02%u5965%u9686%u5DE5%u7A0B%u5EFA%u8BBE%u6709%u9650%u516C%u53F8'  
    # str1 = escape_str.replace('%u', '\\u').encode('utf-8').decode('unicode_escape')  
    # print(str1)
    # print(utf8_urldecode(escape_str))

    # #编码
    # escape_str = '巴彦淖尔市奥隆工程建设有限公司'
    # #decode()解码
    # l = escape_str.encode('unicode_escape').decode("utf-8")
    # s = l.replace('\\u', '%u')
    # print(s)
