from dataflow.utils.log import Logger
from dataflow.utils.reflect import loadlib_by_path
from fastapi import FastAPI
from typing import Callable
from functools import wraps
from dataflow.utils.config import YamlConfigation


_logger = Logger('module.context')

_logger.DEBUG('加载module.context')

class Context:    
    @staticmethod
    def getContext():
        if _contextContainer._context is None:
            raise Exception('没有初始化上下文，请先使用Context.initContext进行初始化')
        return _contextContainer._context
    
    @staticmethod
    def initContext(applicationConfig_file:str, scan_path:str):
        _contextContainer._context = Context(applicationConfig_file, scan_path)        
        _logger.INFO(f'实例化容器={_contextContainer._context}')
        _contextContainer._context._parseContext()
        _logger.DEBUG(f'开始加载模块路径{scan_path}')
        loadlib_by_path(_contextContainer._context.scan_path)
        
        
    def __init__(self, applicationConfig_file:str, scan_path:str):
        self._CONTEXT = {}     
        self.appcaltion_file=applicationConfig_file
        self.scan_path = scan_path
        self._application_config:YamlConfigation = YamlConfigation.loadConfiguration(self.appcaltion_file)        
        _logger.INFO(f'实例化容器={applicationConfig_file},{scan_path}')                
    
    def registerBean(self, service_name, service):
        self._CONTEXT[service_name] = service
    
    def getBean(self, service_name):
        return self._CONTEXT[service_name]
    
    def _parseContext(self):        
        module_path = 'dataflow.module.**'
        _logger.DEBUG(f'开始加载内部模块路径{module_path}')
        loadlib_by_path(module_path)
        pass
            
    @staticmethod
    def Context(*,app:FastAPI, application_yaml:str='conf/application.yaml', scan:str='dataflow.application'):
        if _contextContainer._webcontext is None:
            WebContext.initContext(app)
            
        if _contextContainer._context is None:
            Context.initContext(application_yaml, scan)  
            _logger.WARN('Context启动成功')                        
        else:
            _logger.WARN('Context已经启动')
            
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):                
                result = func(*args, **kwargs)                
                return result
            return wrapper
        return decorator
    
    @staticmethod    
    def Configurationable(*, prefix:str):
        c:YamlConfigation = Context.getContext()._application_config
        config = c.getConfig(prefix)        
        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(*args, **kwargs):                
                if config is not None:
                    if kwargs is None:
                        kwargs = {}
                    kwargs['config'] = config
                    result = func(*args, **kwargs)
                else:
                    _logger.WARN(f'{prefix}没有对应值，配置函数只能进行配置相关操作，跳过')
                    result = None
                    #result = func(*args, **kwargs)
                return result
            return wrapper
        return decorator
        


class WebContext: 
    @staticmethod
    def getContext():
        if _contextContainer._webcontext is None:
            raise Exception('没有初始化上下文，请先使用WebContext.initContext进行初始化')
        return _contextContainer._webcontext
    
    @staticmethod
    def getRoot()->FastAPI:
        return WebContext.getContext().getApp()
    
    @staticmethod
    def initContext(app: FastAPI):        
        _contextContainer._webcontext = WebContext(app)
        _logger.INFO(f'实例化WEB容器={app} {_contextContainer._webcontext}')
            
    def __init__(self, app: FastAPI):
        self._app = app
        pass       
    
    def getApp(self)->FastAPI:
        return self._app
    

class _ContextContainer:
    def __init__(self):
        self._context:Context = None
        self._webcontext:WebContext = None
        
_contextContainer = _ContextContainer()


