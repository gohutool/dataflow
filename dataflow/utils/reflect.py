from importlib import import_module
from typing import Any, Optional, List

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
    