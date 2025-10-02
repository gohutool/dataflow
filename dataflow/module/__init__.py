from dataflow.utils.log import Logger
from dataflow.utils.reflect import loadlib_by_path,get_fullname,get_methodname
from dataflow.utils.utils import str_isEmpty
from fastapi import FastAPI, Request, HTTPException
from typing import Callable,Type,Any,Optional,Dict
from typing import get_type_hints
from functools import wraps
from dataflow.utils.config import YamlConfigation
import contextvars
from typing_extensions import Annotated, Doc
import inspect

_logger = Logger('module.context')

_logger.DEBUG('加载module.context')

_onloaded = []
_onstarted = []
_onexit = []
_oninit = []

_web_onloaded = []
_web_onstarted = []

class Context:
    class ContextExceptoin(HTTPException):
        def __init__(
            self,
            detail: Annotated[
                Any,
                Doc(
                    """
                    Any data to be sent to the client in the `detail` key of the JSON
                    response.
                    """
                ),
            ] = None,
            status_code: Annotated[
                int,
                Doc(
                    """
                    HTTP status code to send to the client.
                    """
                ),
            ]=503,
            headers: Annotated[
                Optional[Dict[str, str]],
                Doc(
                    """
                    Any headers to send to the client in the response.
                    """
                ),
            ] = None,
        ) -> None:
            super().__init__(status_code=status_code, detail=detail, headers=headers)
        
    SERVICE_PREFIX :str = 'context.service'
    class Event:
        @staticmethod        
        def on_init(func):  ### (context, modules)
            _oninit.append(func)
            _logger.DEBUG(f'on_init增加处理函数{func}')
            
        def on_loaded(func):  ### (context, modules)
            _onloaded.append(func)
            _logger.DEBUG(f'on_loaded增加处理函数{func}')
            
        @staticmethod
        def on_started(func):  ### (context)
            _onstarted.append(func)
            _logger.DEBUG(f'on_started增加处理函数{func}')
            
        @staticmethod
        def on_exit(func):  ### ()
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
        if _contextContainer._context is None:
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
        self._CONTEXT_DEP :dict[list[tuple[str,any]]]= {}
        self._INJECT_METHOD_CONTEXT = {}
        self.appcaltion_file=applicationConfig_file
        self.scan_path = scan_path
        self._application_config:YamlConfigation = YamlConfigation.loadConfiguration(self.appcaltion_file)        
        _logger.INFO(f'实例化容器={applicationConfig_file},{scan_path}')    
        
    def getConfigContext(self)->YamlConfigation:
        return self._application_config            
    
    def registerBean(self, service_name, service):
        self._CONTEXT[Context.SERVICE_PREFIX + '.' + service_name] = service
        _logger.INFO(f'注册服务{service_name}={service}成功') 
    
    def getBean(self, service_name):
        k = Context.SERVICE_PREFIX + '.' + service_name
        if k in self._CONTEXT:
            return self._CONTEXT[k]
        else:
            raise Context.ContextExceptoin(f'不能找到{service_name}服务，先注册实例')
    
    def add_dep_info(self, service, target):
        k = str(service)
        deps = None
        if k not in Context.getContext()._CONTEXT_DEP:
            deps = []
            Context.getContext()._CONTEXT_DEP[k] = deps
        else:
            deps = Context.getContext()._CONTEXT_DEP[k]
        deps.append(target)
        _logger.DEBUG(f'添加服务依赖信息{target}->{service}')
    
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
    def Start_Context(app:FastAPI=None, application_yaml:str='conf/application.yaml', scan:str='dataflow.application'):
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
    
    # ---------------- 核心：@service 装饰器 -----------------
    @staticmethod        
    def service(name: str = None):
        """
        类装饰器
        :param name: 指定注册名，None 则按类本身注册
        """
        def decorator(cls):
            # # 1. 实例化（单例）
            # sig = inspect.signature(cls)
            # # 支持构造函数里也有 Autowired 参数            
            # deps = {}
            
            # for param in sig.parameters.values():
            #     ann = param.annotation
            #     if ann is not inspect.Parameter.empty:
            #         if hasattr(ann, '__metadata__'):  # Annotated[T, Autowired(...)]
            #             meta = ann.__metadata__[0]
            #             if isinstance(meta, Autowired):
            #                 deps[param.name] = obtain(meta.key)
            # impl = cls(**deps) if deps else cls()

            # # 2. 注册到容器
            # key = name if name else cls
            # register(key, impl, singleton=True)
            t_name = get_fullname(cls)
            impl = cls()
            service_name = name.strip() if not str_isEmpty(name) else t_name
            Context.getContext().registerBean(service_name, impl)
            Context.getContext().registerBean(t_name, impl)
            _logger.DEBUG(f'Service注册服务实例{service_name},{t_name}={impl}成功')
            return cls  # 返回原类，不影响后续继承/使用
        return decorator
    
    @staticmethod
    def  Inject(func:Callable)->Callable:
        sig = inspect.signature(func)        
        type_hints = get_type_hints(func)
        @wraps(func)
        def wrapper(*args, **kwargs):    
            if kwargs or args:                
                # _logger.DEBUG(f"=== 分析函数: {func.__name__} ===")                   
                # 把位置参数变成 (name, value) 列表，方便统一处理
                bound = sig.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                new_args, new_kwargs = [], {}
                
                is_autowired = False

                for name, param in sig.parameters.items():
                                        
                    # print(f"参数: {param_name}")
                    # print(f"  类型注解: {param_type}")
                    # print(f"  实际类型: {actual_type}")
                    # print(f"  参数种类: {param_info.kind.name}")
                    # print(f"  默认值: {param_info.default if param_info.default != param_info.empty else '无'}")
                    # print(f"  传入值: {param_value}")
        
                    value = bound.arguments[name]
                    param_info = param
                    v = param_info.default
                    if v and isinstance(v, Context.Autowired):                        
                        b_name = v.name
                        f_name = name
                        _typ = type_hints.get(name)
                        if str_isEmpty(b_name) or str_isEmpty(f_name) or _typ:
                            serviceimpl = None
                            inject_k = str(func)
                            incache = False
                            if inject_k in Context.getContext()._INJECT_METHOD_CONTEXT:
                                serviceimpl = Context.getContext()._INJECT_METHOD_CONTEXT[inject_k]
                                _logger.DEBUG(f'从缓存中获取服务实例{serviceimpl}={inject_k}')
                                incache = True
                            
                            if serviceimpl is None and not str_isEmpty(b_name):                                
                                try:
                                    service_name = b_name
                                    serviceimpl = Context.getContext().getBean(service_name)
                                    _logger.DEBUG(f'从名称{b_name}获取服务实例{serviceimpl}')
                                except Exception:
                                    _logger.DEBUG(f'从名称{b_name}没有获取服务实例')
                                    pass
                                                                                                
                            if serviceimpl is None and not str_isEmpty(f_name):                
                                try:
                                    service_name = f_name
                                    serviceimpl = Context.getContext().getBean(service_name)
                                    _logger.DEBUG(f'从属性名{f_name}获取服务实例{serviceimpl}')
                                except Exception:
                                    _logger.DEBUG(f'从属性名{f_name}没有获取服务实例')
                                    pass
                                
                            if serviceimpl is None:                     
                                try:
                                    service_name = get_fullname(_typ)        
                                    serviceimpl = Context.getContext().getBean(service_name)
                                    _logger.DEBUG(f'从类型{service_name}获取服务实例{serviceimpl}')
                                except Exception:
                                    _logger.DEBUG(f'从类型{service_name}没有获取服务实例')
                                    pass
                                
                            if not serviceimpl:
                                _logger.WARN(f'没有找到注入{name}服务实例，注入失败，直接使用实参{value}进行调用')
                                serviceimpl = value 
                            else:
                                is_autowired = True
                                if not incache:                                                                
                                    Context.getContext()._INJECT_METHOD_CONTEXT[inject_k] = serviceimpl
                                    Context.getContext().add_dep_info(serviceimpl, ('Func', func))
                                
                            # 根据参数种类决定放哪
                            if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                            inspect.Parameter.POSITIONAL_OR_KEYWORD):
                                new_args.append(serviceimpl)
                            else:
                                new_kwargs[name] = serviceimpl
                        else:
                            raise Exception(f'{func.__name__}注入参数{name}属性失败，必须指定Type或者使用Name进行实例化')
                    else:
                        # 普通参数原样透传
                        if param.kind in (inspect.Parameter.POSITIONAL_ONLY,
                                        inspect.Parameter.POSITIONAL_OR_KEYWORD):
                            new_args.append(value)
                        else:
                            new_kwargs[name] = value
                    
                if is_autowired:
                    return func(*new_args, **new_kwargs)
                else:        
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        _logger.DEBUG(f'装载{func.__name__}成功，hints={type_hints.items()}，函数={wrapper}')
        return wrapper
    
    class Autowired:
        _all_inject:dict[str, tuple[Type, any, str, Type]]={}
        def __init__(self, *, name:str=None):
            self.name = name
        def _get_binded_key(self, typ:Type, name:str)->str:
            return Context.SERVICE_PREFIX+get_fullname(typ) + '.'+ name
        def __set_name__(self, owner, name):
            self._serviceimpl = None
            hints = get_type_hints(owner)
            typ = hints.get(name)
            # if str_isEmpty(self.name):
            #     self.name = name
            if typ is None and str_isEmpty(self.name) and str_isEmpty(name):
                raise Exception(f'{get_fullname(owner)}注入{name}属性失败，必须指定Type或者使用Name进行实例化')
            else:
                k = self._get_binded_key(owner, name)  ### 先缓存，最后Context的import装载结束后，在把Autowired进行装配
                self._typ = typ
                self._name = name
                Context.Autowired._all_inject[k] = (typ, self, name, owner)   
                _logger.DEBUG(f'{get_fullname(owner)}登记注入{name}属性[{k}]，指定Type={get_fullname(typ)}')
        def __get__(self, instance, owner):
            if instance is None:
                return self            
            _logger.DEBUG(f'获取注册实例{self._serviceimpl}[{self._name}]')
            return self._serviceimpl
                
        def __set__(self, instance, value):    
            raise Exception('只能使用Autowired的方式注入属性值')
            
        
# 定义一个上下文变量
_current_requst_context = contextvars.ContextVar('_current_requst_context', default=None)

class WebContext:     
    @staticmethod
    def getRequest()->Request:
        return _current_requst_context.get()
    @staticmethod
    def setRequest(request:Request):
        _current_requst_context.set(request)
    @staticmethod
    def resetRequest():
        _current_requst_context.set(None)
    class Event:
        @staticmethod
        def on_loaded(func):  #### (app)
            _web_onloaded.append(func)
            _logger.DEBUG(f'on_loaded增加处理函数{func}')
            
        @staticmethod
        def on_started(func):  #### (app)
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
    
@WebContext.Event.on_loaded
def _register_router_for_actuctor(app:FastAPI):
    @app.get('/actuator/infos')
    def actuator_infos():
        rtn = {}
        for k, v in Context.getContext()._CONTEXT.copy().items():
            rtn[k] = get_fullname(v)
        return rtn
    
    @app.get('/actuator/info/{itemid}')
    def actuator_info(itemid:str):
        k = Context.SERVICE_PREFIX + '.' + itemid
        if k not in Context.getContext()._CONTEXT:
            raise Context.ContextExceptoin('没有找到{itemid}对应服务')
        else:
            serviceImpl = Context.getContext()._CONTEXT[k]
            rtn = {
                    'service_id': Context.SERVICE_PREFIX + '.' + itemid,
                    'name': itemid,
                    'service': get_fullname(serviceImpl),
                    'references':[]
            }
            k = str(serviceImpl)
            if k in Context.getContext()._CONTEXT_DEP:
                for o in Context.getContext()._CONTEXT_DEP[k]:
                    rtn['references'].append({
                        'Type':o[0],
                        'Name':get_fullname(o[1]) if o[0]=='Class' else get_methodname(o[1])
                    })
            
            return rtn    

        
@Context.Event.on_loaded
def _assemble_bindding_service(context:Context, modules:list):
    total:int = 0
    for k, v in Context.Autowired._all_inject.items():
        _typ = v[0]
        _aw:Context.Autowired = v[1]
        _name = v[2]
        _owner = v[3]
        if _typ is None and str_isEmpty(_aw.name) and str_isEmpty(_name):
            raise Exception(f'{get_fullname(_owner)}注入{_name}属性失败，必须指定Type或者使用Name进行实例化')
        
        serviceimpl = None
        
        if not str_isEmpty(_aw.name):
            service_name = _aw.name
            serviceimpl = Context.getContext().getBean(service_name)
            _logger.DEBUG(f'从名称{_aw.name}获取服务实例{serviceimpl}')
            
        if serviceimpl is None and _typ is not None:
            service_name = get_fullname(_typ)        
            serviceimpl = Context.getContext().getBean(service_name)
            _logger.DEBUG(f'从类型{service_name}获取服务实例{serviceimpl}')
        
        if serviceimpl is None:
            service_name = _name
            serviceimpl = Context.getContext().getBean(service_name)
            _logger.DEBUG(f'从属性名{_name}获取服务实例{serviceimpl}')
            
        if serviceimpl is None:
            raise Exception(f'注入装载{get_fullname(_owner)}属性{_name}出现问题，没有找到服务实例{service_name}')
        _aw._serviceimpl = serviceimpl        
        Context.getContext().add_dep_info(serviceimpl, ('Class', _owner))
        total += 1
        _logger.DEBUG(f'注入装载{get_fullname(_owner)}属性{_name}成功，找到服务实例{service_name}')
        
    _logger.DEBUG(f'注入装载成功，共装载服务实例{total}个')

class _ContextContainer:
    def __init__(self):
        self._context:Context = None
        self._webcontext:WebContext = None
        
_contextContainer = _ContextContainer()

        
    
