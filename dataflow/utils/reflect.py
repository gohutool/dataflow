from importlib import import_module
from typing import Any


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

def getAttr(data:dict, field:str, dv:any)->any:
    if data is None:
        return dv
    rtn = None
    if isinstance(data, dict):
        if haveAttr(data, field):
            rtn = data[field]
    else:
        obj = getattr(data, field, None)
        
    if rtn is None:
        rtn = dv   
        
    return rtn

def getAttrPlus(data:dict, field:str, dv:any)->any:
    if data is None:
        return dv
    rtn = None
    
    obj = data
    
    """'a.b.c' -> 逐层取值"""
    for key in field.split('.'):        
        if isinstance(obj, dict):
            if haveAttr(obj, key):
                obj = obj[key]
            else:
                obj = None
        else:
            obj = getattr(obj, key, None)    
            
        if obj is None:
            break       
        
    if obj is None:
        rtn = dv
            
    return rtn


    