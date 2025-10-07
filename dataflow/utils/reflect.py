from importlib import import_module
from typing import Any, Optional, List, Type, get_origin, get_args,Callable,get_type_hints
from types import FunctionType
import importlib
import pkgutil
from dataflow.utils.log import Logger
from dataflow.utils.utils import current_millsecond
import sys 
import inspect

_logger = Logger('dataflow.utils.reflect')


def haveAttr(obj:any, attr:str)->bool:
    if obj is None:
        return False
    
    if isinstance(obj, dict):
        return attr in obj
    
    return hasattr(obj, attr)

def newInstance(fully_qualified_name: str, *args, **kwargs)->any:
    try:
        mod_name, cls_name = fully_qualified_name.rsplit(".", 1)
        mod = import_module(mod_name)
        cls = getattr(mod, cls_name)
    except (ValueError, ModuleNotFoundError, AttributeError) as e:
        raise RuntimeError(f"Cannot instantiate {fully_qualified_name}: {e}") from e
    return cls(*args, **kwargs)

def getType(fully_qualified_name:str|type|object)->type:
    try:
        if isinstance(fully_qualified_name, str):
            mod_name, cls_name = fully_qualified_name.rsplit(".", 1)
            mod = import_module(mod_name)
            cls = getattr(mod, cls_name)
        elif isinstance(fully_qualified_name, type):
            cls = fully_qualified_name
        elif is_not_primitive(object):
            cls = type(fully_qualified_name)
        else:
            raise RuntimeError(f"Cannot instantiate {fully_qualified_name}")
    except (ValueError, ModuleNotFoundError, AttributeError) as e:
        raise RuntimeError(f"Cannot instantiate {fully_qualified_name}: {e}")
    return cls

def getPydanticInstance(fully_qualified_name:str|type, properties:dict)->any:
    cls = getType(fully_qualified_name)
    if properties:
        return cls(**property)
    else:
        return cls()

def getInstance(fully_qualified_name:str|type, properties:dict)->any:
    obj = newInstance(fully_qualified_name, *[], **properties)
    return dict2obj(obj, properties)

def is_instance_method(obj) -> bool:
    return (
        inspect.ismethod(obj) and          # 绑定方法
        isinstance(obj.__self__, object) and  # 有实例宿主
        not inspect.isclass(obj.__self__)     # 排除 @classmethod 的绑定类
    )
        
def getTypeAttr(fully_qualified_name:str|type|object):
    t = getType(fully_qualified_name)
    return vars(t).items()
    
# 获取该类自己定义的所有实例方法（不包括继承的、不包括特殊方法如 __init__，或按需包括）。    
def inspect_own_method(cls:type|str|object,excludePriviate:bool=True)->list:
    cls_type = getType(cls)
    methods = []
    # for name, method in inspect.getmembers(cls_type, predicate=inspect.isfunction):
    #     # print(f'{name} {method}')
    #     # 只保留该类自己定义的（不是继承的）
    #     if method.__qualname__.startswith(cls_type.__name__ + '.'):
    #         if not excludePriviate or not name.startswith('_'):
    #             methods.append((name, method))
    # return methods    
    
    for name, attr_value in getTypeAttr(cls_type):
        # print(f'{name} {attr_value}') 
        
        if isinstance(attr_value, FunctionType):
            method = attr_value
            if method.__qualname__.startswith(cls_type.__name__ + '.'):
                # print(f'{method.__qualname__} {attr_value}')
                if not excludePriviate or not name.startswith('_'):
                    methods.append((name, method))
    return methods


def inspect_class_method(cls:type|str|object,excludePriviate:bool=True)->list:
    cls_type = getType(cls)
    methods = []    
    # for name, value in dict(cls_type.__dict__).items():
    #     print(f'{name} {value}')
    # for name in dir(cls_type):
    #     attr_value = getattr(cls_type, name)
    #     print(f'{name} {attr_value}')
    for name, attr_value in getTypeAttr(cls_type):
        # print(f'{name} {attr_value}') 
        if isinstance(attr_value, classmethod):
            method = attr_value
            if method.__qualname__.startswith(cls_type.__name__ + '.'):
                if not excludePriviate or not name.startswith('_'):
                    methods.append((name, method))
    return methods

def inspect_static_method(cls:type|str|object,excludePriviate:bool=True)->list:
    cls_type = getType(cls)
    methods = []
    for name, attr_value in getTypeAttr(cls_type):        
        # print(f'{type(attr_value)} {attr_value}')
        if isinstance(attr_value, staticmethod):
            method = attr_value
            if method.__qualname__.startswith(cls_type.__name__ + '.'):
                if not excludePriviate or not name.startswith('_'):
                    methods.append((name, method))
    return methods

def getAttr(data:dict, field:str, dv:any=None)->any:
    if data is None:
        return dv
    rtn = None
    if isinstance(data, dict):
        if field in data:
            rtn = data[field]
    else:
        rtn = getattr(data, field, None)
        
    if rtn is None:
        rtn = dv   
        
    return rtn

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

def to_dict(
                obj: Any, 
                include_private: bool = False,
                include_methods: bool = False,
                exclude_attrs: Optional[List[str]] = None,
                max_depth: int = 1
    )->dict[str,any]: 
    """
    万能对象转字典方法
    
    Args:
        obj: 要转换的对象
        include_private: 是否包含私有属性
        include_methods: 是否包含方法
        exclude_attrs: 要排除的属性名列表
        max_depth: 最大递归深度（用于处理嵌套对象）
    """
    if exclude_attrs is None:
        exclude_attrs = []
    
    if max_depth <= 0:
        return obj
    
    # 如果是基本类型，直接返回
    if isinstance(obj, (str, int, float, bool, type(None))):
        return obj
    
    # 如果是字典，递归处理值
    if isinstance(obj, dict):
        return {k: to_dict(v, include_private, include_methods, exclude_attrs, max_depth-1) 
                for k, v in obj.items()}
    
    # 如果是列表或元组，递归处理元素
    if isinstance(obj, (list, tuple, set)):
        return [to_dict(item, include_private, include_methods, exclude_attrs, max_depth-1) 
                for item in obj]
    
    # 处理对象
    result = {}
    
    for attr_name in dir(obj):
        # 跳过特殊方法
        if attr_name.startswith('__') and attr_name.endswith('__'):
            continue
        
        # 过滤私有属性
        if not include_private and attr_name.startswith('_'):
            continue
        
        # 过滤排除的属性
        if attr_name in exclude_attrs:
            continue
        
        try:
            attr_value = getattr(obj, attr_name)
            
            # 过滤方法（如果不包含方法）
            if not include_methods and callable(attr_value):
                continue
            
            # 递归处理嵌套对象
            result[attr_name] = to_dict(
                attr_value, include_private, include_methods, exclude_attrs, max_depth-1
            )
            
        except (AttributeError, Exception):
            # 跳过无法访问的属性
            continue
    
    return result

def dict2obj(obj: object, d: dict) -> object:
    for k, v in d.items():
        if hasattr(obj, k):
            setattr(obj, k, v)
    return obj    

def import_lib(base):   
    start = current_millsecond()
    _logger.INFO(f'import_lib-->加载包{base}开始')
    mod = importlib.import_module(base)
    cost = (current_millsecond() - start)
    _logger.INFO(f'import_lib-->加载包{base}[{"PKG" if hasattr(mod, '__path__') else "MOD" }] 耗时{cost:.2f}毫秒')
    return mod

def loadlib_by_path(path: str|list[str]) -> List:
    """
    按 uvicorn 风格字符串加载包/模块
    返回 [模块对象, ...]
    """
    # 1. 解析模式
    if path.endswith('.**'):
        base, recursive = path[:-3], True
    elif path.endswith('.*'):
        base, recursive = path[:-2], False
    else:
        base, recursive = path, None

    # 3. 加载根
    root_mod = import_lib(base)
    _logger.DEBUG(f'{dir(root_mod)}')
    loaded = [(base, hasattr(root_mod, '__path__'))]

    if recursive is None:               # 仅单个模块
        return loaded

    if not hasattr(root_mod, '__path__'):
        raise ValueError(f'{base} 不是包，无法使用 * / **')

    # 4. 手动递归
    def walk(path, prefix):
        for _, name, ispkg in pkgutil.iter_modules(path):
            full_name = prefix + name
            sub = import_lib(full_name)
            loaded.append((full_name, ispkg))
            if recursive and ispkg:          # ** 模式才继续深入
                # import_lib(full_name)
                walk(sub.__path__, full_name + '.')

    walk(root_mod.__path__, base + '.')
    return loaded

def get_fullname(obj:any|Type)->str:
    if obj is not None and not isinstance(obj, Type):
        obj = type(obj)
                
    full_name = f"{obj.__module__}.{obj.__name__}"
    return full_name

def get_generic(obj:any)->Type:    
    return get_origin(obj), get_args(obj)

def get_methodname(func:callable)->str:
    # 自己拼出想要的字符串
    # sig = inspect.signature(func)
    params = ','.join(inspect.signature(func).parameters)
    full_name =  f"{func.__module__}.{func.__qualname__}({params})"
    # full_name = f"{func.__module__}.{func.__qualname__}"    
    return full_name    

# list = *args
# dict = **kwargs
def bind_call_parameter(func:Callable, args:list, kwargs:dict, bind_func:Callable, new_params:dict)->tuple[list, dict]:
    
    sig = inspect.signature(func)        
    type_hints = get_type_hints(func)
    bound = sig.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    new_args, new_kwargs = [], {}

    for name, param_value in bound.arguments.items():        
        param_info = sig.parameters[name]
        # param_info = sig.parameters[name]        
        # 获取参数类型
        _typ = type_hints.get(name)
        # 实际类型
        # actual_type = type(param_value)
        binded = True
        
        if name in new_params:
            if bind_func:
                binded, value = bind_func(old_value=param_value, type=_typ, name=name, new_value=new_params[name])
            else:
                value = new_params[name]
        else:
            value = param_value
                                        
         # print(f"参数: {param_name}")
        # print(f"  参数类型: {_typ}")
        # print(f"  实际类型: {actual_type}")
        # print(f"  参数种类: {param_info.kind.name}")
        # print(f"  默认值: {param_info.default if param_info.default != param_info.empty else '无'}")
        # print(f"  传入值: {param_value}")
        # 普通参数原样透传
                
        if param_info.kind in (inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD):
            new_args.append(value)
        else:
            new_kwargs[name] = value
    pass

# 定义原始类型
primitive_types = (int, float, bool, str, type(None))

def is_user_object(obj):
    return hasattr(type(obj), '__dict__') and not inspect.isbuiltin(obj)

def is_not_primitive(obj):
    return not isinstance(obj, primitive_types)

# ------------------- demo -------------------
if __name__ == '__main__':
    
    _logger.DEBUG('====')
    
    path = "dataflow.main"
    if len(sys.argv) >=2:
        path = sys.argv[1]
    # 1. 仅加载模块
    # print(loader.load("dataflow"))

    # 2. 加载 db + 第一级子模块/子包
    # print(loader.load("dataflow.*"))

    # 3. 加载 db + 全部递归子模块
    # print(loader.load("dataflow.**"))
    print(f'========== {path}')
    print(loadlib_by_path(path))
    
    print(get_fullname(''))
    print(get_fullname(Logger()))
    print(get_fullname(Logger))
    
    print(get_generic(list[int]))        # (<class 'list'>, (<class 'int'>,))
    print(get_generic(dict[str, int]))   # (<class 'dict'>, (<class 'str'>, <class 'int'>))
    print(get_generic(int))              # (None, ())
    
    print(get_methodname(get_methodname))    
    print(get_methodname(getAttrPlus))