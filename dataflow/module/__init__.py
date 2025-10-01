from dataflow.utils.log import Logger
from dataflow.utils.reflect import loadlib_by_path
from fastapi import FastAPI
from typing import Callable
from functools import wraps
from dataflow.utils.config import YamlConfigation


_logger = Logger('module.context')

_logger.DEBUG('加载module.context')

_onloaded = []
_onstarted = []
_onexit = []
_oninit = []

_web_onloaded = []
_web_onstarted = []


class Context:
    MOCK:bool = False
    class Event:
        @staticmethod        
        def on_init(func):
            _oninit.append(func)
            _logger.DEBUG(f'on_init增加处理函数{func}')
            
        def on_loaded(func):
            _onloaded.append(func)
            _logger.DEBUG(f'on_loaded增加处理函数{func}')
            
        @staticmethod
        def on_started(func):
            _onstarted.append(func)
            _logger.DEBUG(f'on_started增加处理函数{func}')
            
        @staticmethod
        def on_exit(func):
            _onexit.append(func)
            _logger.DEBUG(f'on_exit增加处理函数{func}')
            
        def emit(event:str, *args, **kwargs):
            """广播事件"""
            _handlers = None
            if not event :
                event = 'loaded'
            _logger.DEBUG(f'Context触发{event}开始')    
            if event.strip().lower()=='loaded':
                _handlers = _onloaded
            elif event.strip().lower()=='started':
                _handlers = _onstarted
            elif event.strip().lower()=='exit':
                _handlers = _onexit
            elif event.strip().lower()=='init':
                _handlers = _oninit
            else:
                return             
            for f in _handlers:
                f(*args, **kwargs)
            _logger.DEBUG(f'Context触发{event}结束')
        
    @staticmethod
    def getContext():
        if _contextContainer._context is None and not Context.MOCK :
            raise Exception('没有初始化上下文，请先使用Context.initContext进行初始化')
        return _contextContainer._context
    
    @staticmethod
    def initContext(applicationConfig_file:str, scan_path:str):
        _contextContainer._context = Context(applicationConfig_file, scan_path)        
        _logger.INFO(f'实例化容器={_contextContainer._context}')
        _contextContainer._context._parseContext()
        _logger.DEBUG(f'加载模块路径{scan_path}开始')        
        _modules = loadlib_by_path(_contextContainer._context.scan_path)
        _logger.DEBUG(f'加载模块路径{scan_path}结束')        
        Context.Event.emit('loaded', _contextContainer._context, _modules)
        
    def __init__(self, applicationConfig_file:str, scan_path:str):
        self._CONTEXT = {}     
        self.appcaltion_file=applicationConfig_file
        self.scan_path = scan_path
        self._application_config:YamlConfigation = YamlConfigation.loadConfiguration(self.appcaltion_file)        
        _logger.INFO(f'实例化容器={applicationConfig_file},{scan_path}')    
        
    def getConfigContext(self)->YamlConfigation:
        return self._application_config            
    
    def registerBean(self, service_name, service):
        self._CONTEXT[service_name] = service
    
    def getBean(self, service_name):
        return self._CONTEXT[service_name]
    
    def _parseContext(self):        
        module_path = 'dataflow.module.**'
        _logger.DEBUG(f'初始化内部模块路径{module_path}开始')
        _modules = loadlib_by_path(module_path)
        _logger.DEBUG(f'初始化内部模块路径{module_path}结束')
        Context.Event.emit('init', self, _modules)
        pass
    
    @staticmethod
    def Value(placeholder:str)->any:
        return Context.getContext().getConfigContext().value(placeholder)
        
    
    @staticmethod
    def Start_Context(*,app:FastAPI=None, application_yaml:str='conf/application.yaml', scan:str='dataflow.application'):
        if _contextContainer._webcontext is None:            
            WebContext.initContext(app)         
            
        if _contextContainer._context is None:
            Context.initContext(application_yaml, scan)              
        else:
            _logger.WARN('Context已经启动')        
            
        WebContext.Event.emit('loaded', app)
            
    @staticmethod
    def Context(*,app:FastAPI, application_yaml:str='conf/application.yaml', scan:str='dataflow.application'):                
        Context.Start_Context(app, application_yaml, scan)
                
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
    class Event:
        @staticmethod
        def on_loaded(func):
            _web_onloaded.append(func)
            _logger.DEBUG(f'on_loaded增加处理函数{func}')
            
        @staticmethod
        def on_started(func):
            _web_onstarted.append(func)
            _logger.DEBUG(f'on_started增加处理函数{func}')
            
        def emit(event:str, *args, **kwargs):
            """广播事件"""
            _handlers = None
            if not event :
                event = 'loaded'
            _logger.DEBUG(f'WebContext触发{event}开始')
            if event.strip().lower()=='loaded':
                _handlers = _web_onloaded
            elif event.strip().lower()=='started':
                _handlers = _web_onstarted            
            else:
                return             
            for f in _handlers:
                f(*args, **kwargs)
            _logger.DEBUG(f'WebContext触发{event}结束')
            
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


