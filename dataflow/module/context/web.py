
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from dataflow.utils.log import Logger
from dataflow.utils.utils import str_isEmpty,str_strip, ReponseVO, get_list_from_dict, get_float_from_dict, get_int_from_dict, get_bool_from_dict,current_millsecond,l_str,str2Num
from dataflow.utils.web.asgi import get_remote_address, CustomJSONResponse,get_ipaddr
from dataflow.utils.web.asgi_proxy import AdvancedProxyService,ProxyConfig
from dataflow.utils.reflect import get_methodname,get_fullname
from dataflow.module import Context, WebContext
from antpathmatcher import AntPathMatcher
from fastapi import Request, FastAPI, APIRouter, Depends
from slowapi import Limiter
import functools
# from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError,HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid
from dataflow.utils.jwt import create_token as _create_token, verify_token as _verify_token
import inspect
from pathlib import Path
import anyio  # noqa: F401
from starlette.staticfiles import StaticFiles as _SF
from starlette.types import Scope, Receive, Send  # noqa: F401


_logger = Logger('dataflow.module.context.web')

antmatcher = AntPathMatcher()    
# # 提取路径中的变量
# variables = matcher.extract_uri_template_variables("/users/{id}", "/users/123")
# print(variables) # 输出: {'id': '123'}

# # 提取多个变量
# variables = matcher.extract_uri_template_variables(
# "/users/{user_id}/posts/{post_id}", "/users/123/posts/456"
# )
# print(variables) # 输出: {'user_id': '123', 'post_id': '456'}

# 自定义装饰器，实现@Controller 注解
def Controller(app:FastAPI|APIRouter=None, *, prefix: str = "", **ka):
    def decorator(cls):
        # 创建路由器
        router = APIRouter(prefix=prefix, **ka)
        _logger.WARN(f'{get_fullname(cls)}=>Path:{prefix}')
        cls_inst = cls()
        # 获取类的所有方法
        for name, method in inspect.getmembers(cls_inst, predicate=inspect.ismethod):
            # 检查方法是否有路由元数据
            if hasattr(method, '__route_metadata__'):
                metadata = getattr(method, '__route_metadata__')                
                # 'path': path,                    
                # 'methods': tag,
                # 'kwargs': kwargs                   
                path = metadata['path']
                kwargs = metadata['kwargs']
                tag = metadata['methods']
                
                # 为方法添加依赖注入（如果需要）
                endpoint = method
                if hasattr(method, '_dependencies'):
                    # dependencies = getattr(method, '_dependencies')
                    endpoint = Depends(method)
                
                # 添加路由到路由器
                RequestBind._RequestMapping(path=path, api=router, tag=tag, **kwargs)(endpoint)                
                _logger.WARN(f'绑定{get_methodname(method)}方法，Method:{tag}, Path:{path}')
                
                # router.add_api_route(
                #     path, 
                #     endpoint, 
                #     methods=methods,
                #     **metadata.get('kwargs', {})
                # )        
        # 将路由器保存到类中
        cls.router = router
        if app:
            if isinstance(app, FastAPI):
                _app:FastAPI = app
                _app.include_router(cls.router)                
                _logger.WARN(f'{get_fullname(cls)}已经进行RequestMapping路径{prefix}')                 
            elif isinstance(app, APIRouter):
                _app:APIRouter = app
                _app.include_router(cls.router)
                _logger.WARN(f'{get_fullname(cls)}已经进行RequestMapping路径{prefix}')                
            else:
                raise Context.ContextException(f'{get_fullname(app)}参数类型出错，app只能是FastAPI或者APIRouter对象，或者为空，通过cls.router获取后设置')
        else:
            _logger.WARN(f'{get_fullname(cls)}没有设置FastAPI或者APIRouter对象，需要通过手动获取cls.router后进行request绑定映射到路径{prefix}')
        return cls
    
    return decorator

    # @staticmethod
    # def GetMapping(*args, **kwargs):
    #     return RequestBind.GetMapping(None, *args, **kwargs)
    
    # @staticmethod
    # def PostMapping(api:FastAPI|APIRouter=None, *args, **kwargs):
    #     return RequestBind.PostMapping(None, *args, **kwargs)
    
    # @staticmethod
    # def PutMapping(api:FastAPI|APIRouter=None, *args, **kwargs):
    #     return RequestBind.PutMapping(None, *args, **kwargs)
    
    # @staticmethod
    # def DeleteMapping(api:FastAPI|APIRouter=None, *args, **kwargs):
    #     return RequestBind.DeleteMapping(None, *args, **kwargs)
        
    # @staticmethod
    # def OptionsMapping(api:FastAPI|APIRouter=None, *args, **kwargs):
    #     return RequestBind.OptionsMapping(None, *args, **kwargs)
    
    # @staticmethod
    # def RequestMapping(api:FastAPI|APIRouter=None, *args, **kwargs):
    #     return RequestBind.RequestMapping(None, *args, **kwargs)
        

class RequestBind:
    @staticmethod
    def GetMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api
        #     return api.get(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.get(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=['GET'], **kwargs)
            
    @staticmethod
    def PostMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api
        #     return api.post(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.post(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=['POST'], **kwargs)
        
    @staticmethod
    def PutMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api
        #     return api.put(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.put(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=['PUT'], **kwargs)
        
    @staticmethod
    def DeleteMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api
        #     return api.delete(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.delete(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=['DELETE'], **kwargs)
        
    @staticmethod
    def OptionsMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api
        #     return api.options(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.options(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=['OPTIONS'], **kwargs)
    
    @staticmethod
    def _RequestMapping(path:str, *, api:FastAPI|APIRouter=None, tag=['GET'], **kwargs):
        if api:
            if not tag:
                tag = ['GET']
                
            if not kwargs:
                kwargs = {}
            kwargs['methods']=tag
            
            if 'api' in kwargs:
                kwargs.pop('api')
                
            if 'tag' in kwargs:
                kwargs.pop('tag')
            
            _logger.DEBUG(f'{path}进行RequestMapping注册={kwargs}')
            
            if isinstance(api, FastAPI):
                api:FastAPI = api
                return api.api_route(path, **kwargs)
            elif isinstance(api, APIRouter):
                api:APIRouter = api
                return api.api_route(path, **kwargs)
            else:
                raise Context.ContextException(f'{path}只能设置FastAPI或者APIRouter对象')
        else:
            # @functools.wraps(func)
            _tag = tag
            if tag is None:
                tag = ["GET"]
        
            def decorator(func):
                # 存储路由元数据
                func.__route_metadata__ = {
                    'path': path,                    
                    'methods': tag,
                    'kwargs': kwargs
                }
                _logger.DEBUG(f'{get_methodname(func)}没有设定FastAPI或者APIRouter对象，__route_metadata__={func.__route_metadata__}')
                return func
            return decorator
        
    @staticmethod
    def RequestMapping(path:str, *, api:FastAPI|APIRouter=None, **kwargs):
        # if not kwargs:
        #     kwargs = {}
        # kwargs['methods']=['GET','POST','PUT','DELETE']
        
        # if isinstance(api, FastAPI):
        #     api:FastAPI = api            
        #     return api.api_route(*args, **kwargs)
        # else:            
        #     api:APIRouter = api
        #     return api.api_route(*args, **kwargs)
        return RequestBind._RequestMapping(path, api=api, tag=["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"], **kwargs)

_filter = []

def filter(app:FastAPI=None, *, path:list[str]|str='*', excludes:list[str]|str=None, order=1):       
    paths = None
    if isinstance(path, list):
        paths = []
        for o in path:
            paths.append(o.strip())
    else:
        if str_isEmpty(path) or path.strip() == '*':
            paths = None
        else:
            paths = str_strip(path).split(',')
        
    _excludes = None
    if isinstance(excludes, list):
        _excludes = []
        
        for o in excludes:
            _excludes.append(o.strip())
    else:
        if str_isEmpty(excludes):
            _excludes = None
        else:
            _excludes = str_strip(excludes).split(',')
        
    def decorator(func: Callable) -> Callable:
        _filter.append((order, app, path, excludes, func, paths, _excludes))
        @functools.wraps(func)
        async def wrapper(request: Request, call_next):
             return await call_next(request) 
        return wrapper 
        # if (paths is None or len(paths) == 0) and (_excludes is None or len(_excludes) == 0):
        #     app.add_middleware(BaseHTTPMiddleware, dispatch=func)
        # else:
        #     async def new_func(request: Request, call_next):   
        #         if _excludes is not None and len(_excludes)>0 :
        #             for o in _excludes:
        #                 if antmatcher.match(o, request.url.path):                        
        #                     return await call_next(request)                                                
                
        #         matched = False
        #         if paths is not None and len(paths)>0:
        #             for o in paths:
        #                 if antmatcher.match(o, request.url.path):
        #                     matched = True
        #                     break
        #         else:
        #             matched = True
                        
        #         if not matched:
        #             return await call_next(request)
        #         else:
        #             _logger.DEBUG(f'{request.url.path}被拦截器拦截')
        #             try:
        #                 return await func(request, call_next)                                
        #             except HTTPException as e:
        #                 raise e
        #             except RequestValidationError as e:
        #                 raise e
        #             except StarletteHTTPException as e:
        #                 raise e
        #             except Exception as e:
        #                 raise Context.ContextExceptoin(detail=e.__str__())
                    
        #     app.add_middleware(BaseHTTPMiddleware, dispatch=new_func)      
    # _logger.DEBUG(f'创建过滤器装饰器={decorator} path={path} excludes={excludes}')
    return decorator

def _global_id(request:Request):
    return '_global_'

_default_limit_rate = Context.getContext().getConfigContext().getList('context.limiter.default_limit_rate')
_default_limit_rate = _default_limit_rate if _default_limit_rate else ["200000/day", "50000/hour"]

_ip_limiter = Limiter(key_func=get_remote_address, default_limits=_default_limit_rate)
_global_limiter = Limiter(key_func=_global_id, default_limits=_default_limit_rate)

_limiters = {}
_limiters['IP'] = _ip_limiter
_limiters['GLOBAL'] = _global_limiter

# def _wrap_limiter(func:Callable)->Callable:
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         had_request = False
#         for k, v in                 
#         result = func(*args, **kwargs)                
#         return result
#     return wrapper

def limiter(rule:str, *, key:Callable|str=None):
    if key is None:
        # key = 'ip'
        key = 'global'
    if isinstance(key, str):
        if key.strip().upper()=='IP':
            _logger.DEBUG(f'使用默认访问IP限流器[{rule}]=>{_ip_limiter}')
            return _ip_limiter.limit(rule)
        else:
            _logger.DEBUG(f'使用默认访问限流器[{rule}]=>{_global_limiter}')
            return _global_limiter.limit(rule)
    else:
        _limiter = None
        key = str(key)        
        if key in _limiters:
            _limiter = _limiters[key]
        else:
            _limiter = Limiter(key_func=key, default_limits=_default_limit_rate)
            _limiters[key] = _limiter
        _logger.DEBUG(f'使用自定义访问限流器[{rule}]=>{_limiter}')
        return _limiter.limit(rule)


@WebContext.Event.on_loaded
def init_error_handler(app:FastAPI):
    
    
    @app.exception_handler(Exception)
    async def exception_handler(request: Request, exception:Exception):
        
        
        if isinstance(exception, Context.ContextException):
            return await context_exception_handler(request, exception)
        if isinstance(exception, RequestValidationError):
            return await validation_exception_handler(request, exception)
        if isinstance(exception, HTTPException):
            return await http_exception_handler(request, exception)
        
        _logger.ERROR(f'处理Expcetion: {exception}')
        # _logger.ERROR(f'处理Expcetion: {exc}', exc)
        # _logger.ERROR(f'处理Expcetion: {exc}')
        code = getattr(exception, 'code', 500)
        
        return CustomJSONResponse(
            status_code=code,
            # content={"code": exc.status_code, "message": exc.detail}
            content=ReponseVO(False, code=code, msg=exception.__str__(), data=exception.__str__())
        )
              
        
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exception:HTTPException):
        # _logger.ERROR(f'处理HttpExpcetion: {exc}', exc)
        _logger.WARN(f'处理HTTPException: {exception}')
        return CustomJSONResponse(            
            status_code=exception.status_code,
            # content={"code": exc.status_code, "message": exc.detail}
            content=ReponseVO(False, code=exception.status_code, msg=exception.detail, data=exception.detail)
        )
    
    # 覆盖校验错误
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exception:RequestValidationError):        
        # _logger.ERROR(f'处理RequestValidationError: {exc}', exc)
        _logger.WARN(f'处理Expcetion: {exception.errors()}')
        return CustomJSONResponse(
            status_code=200,
            # content={"code": 422, "message": "参数校验失败", "errors": exc.errors()}
            content=ReponseVO(False, code=422, msg=exception.errors(), data=exception.errors())
        )
    
    # 覆盖校验错误
    @app.exception_handler(Context.ContextException)
    async def context_exception_handler(request: Request, exception:Context.ContextException):        
        # _logger.ERROR(f'处理RequestValidationError: {exc}', exc)
        _logger.WARN(f'处理Expcetion: {exception}')
        return CustomJSONResponse(
            status_code=exception.status_code,
            # content={"code": 422, "message": "参数校验失败", "errors": exc.errors()}
            content=ReponseVO(False, code=exception.code, msg=exception.detail, data=exception.detail)
        )    

_ttl_minutes = str2Num(Context.Value('${context.jwt.ttl_minutes:21600}'))
_secret = l_str(Context.Value('${context.jwt.secret:replace-with-256-bit-secret}'), 32, '0')

_logger.DEBUG(f'JWT参数 ttl_minutes={_ttl_minutes} secret={_secret}')

def create_token(user_key: str, user_name:str)->str:
    return _create_token(user_key, user_name, _ttl_minutes, _secret)

def verify_token(token:str)->dict:
    return _verify_token(token, _secret)

@WebContext.Event.on_started
def _register_all_filter(_app:FastAPI):
    _logger.DEBUG(f'自定义{len(_filter)}个过滤器进行初始化')
    # 排序：先按 order 升序，再按插入序号降序（后插入在前）
    # _filter.sort(key=lambda t: (t[1], -t[2]))
    # 排序：先按 order 升序，再按插入序号升序（先插入在前）
    # _filter.append((order, app, path, excludes, func, paths, _excludes))
    _filter.sort(key=lambda t: (t[0], t[2]), reverse=False)
    # _filter = sorted(_filter, key=lambda t: (t[0], -_filter.index(t)))
    for v in _filter:
        _o,app,_path,_ex,func,paths,_excludes=v
        _logger.DEBUG(f'初始化过滤器{paths}=<{_excludes}>')
        app:FastAPI = app
        if not app:
            app = _app        
        if (paths is None or len(paths) == 0) and (_excludes is None or len(_excludes) == 0):
            app.add_middleware(BaseHTTPMiddleware, dispatch=func)
        else:
            def middleware_wrapper(
                paths_snap=paths,
                excludes_snap=_excludes,
                func_snap=func,
            ):
                async def new_func(request: Request, call_next):   
                    if excludes_snap is not None and len(excludes_snap)>0 :
                        for o in excludes_snap:
                            if antmatcher.match(o, request.url.path):                        
                                return await call_next(request)                                                
                    
                    matched = False
                    _logger.DEBUG(f'检查{paths_snap} = {request.url.path}')
                    if paths_snap is not None and len(paths_snap)>0:
                        for o in paths_snap:
                            if antmatcher.match(o, request.url.path):
                                matched = True
                                break
                    else:
                        matched = True
                            
                    if not matched:
                        return await call_next(request)
                    else:
                        _logger.DEBUG(f'{request.url.path}被拦截器拦截')
                        try:
                            return await func_snap(request, call_next)                                
                        except HTTPException as e:
                            raise e
                        except RequestValidationError as e:
                            raise e
                        except StarletteHTTPException as e:
                            raise e
                        except Exception as e:
                            # raise Context.ContextExceptoin(detail=e.__str__()) from e
                            raise Context.ContextException(detail=e.__str__())
                            # raise HTTPException(200, detail=e.__str__())
                        
                return new_func
                    
            app.add_middleware(BaseHTTPMiddleware, dispatch=middleware_wrapper(paths, _excludes, func)) 
            
        _logger.DEBUG(f'注册过滤器={get_methodname(func)}[{_o}] path={_path} excludes={_ex}')
    
        
@WebContext.Event.on_started
def init_web_common_filter(app:FastAPI):    
    @app.middleware("http")
    async def wrap_exception_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        rid = ''
        if hasattr(request.state, 'xid'):
            rid = request.state.xid
        try:                        
            response = await call_next(request)
        except Context.ContextException as e:
            raise e
        except HTTPException as e:
            raise e
        except RequestValidationError as e:
            raise e
        except StarletteHTTPException as e:
            raise e
        except Exception as e:
            # _logger.ERROR(f"[{rid}] {request.method} {request.url}", e)
            # raise Context.ContextExceptoin(detail=str(e)) from e
            raise Context.ContextException(detail=str(e)) from e
        
        _logger.INFO(f"[{rid}] {request.method} {request.url}")        
        return response    
    _logger.DEBUG(f'注册过滤器={wrap_exception_handler}') 
        
    @app.middleware("http")
    async def xid_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        start = current_millsecond()
        
        rid = uuid.uuid4().hex
        request.state.xid = rid    
        ip = get_ipaddr(request)
        
        _logger.INFO(f"[{rid}] {request.method} {request.url}")
        
        # txt = body.decode("utf-8", errors="replace")
        path_params = request.path_params
        # 2. 查询参数
        query_params = dict(request.query_params)
        # 3. 请求头
        headers = dict(request.headers)
        # 4. Cookie
        cookies = dict(request.cookies)
        
        body = await request.body()
        # 构造新作用域 request，后续路由再读 body() 时实际读的是缓存
        async def receive():
            return {"type": "http.request", "body": body, "more_body": False}
        request = Request(request.scope, receive=receive)
        
        _logger.DEBUG(f'Path_params={path_params}')        
        _logger.DEBUG(f'Query_params={query_params}')
        _logger.DEBUG(f'Headers={headers}')
        _logger.DEBUG(f'Cookies={cookies}')
        _logger.DEBUG(f'Body={body.decode("utf-8", errors="replace")}')
        
        WebContext.setRequest(request)        
        response = await call_next(request)
        WebContext.resetRequest()
                
        # ====== 响应阶段 ======
        cost = (current_millsecond() - start)
        response.headers["X-Request-ID"] = rid      
        response.headers["X-Cost-ms"] = str(cost)
        
        
        _logger.INFO(f"[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
        return response        
    _logger.DEBUG(f'注册过滤器={xid_handler}')  

class AsyncStaticFiles(_SF):
    async def get_response(self, path: str, scope):
        # 确保文件操作是异步的
        try:
            return await super().get_response(path, scope)
        except Exception as e:
            _logger.ERROR(f"Static file error: {e}")
            raise


@Context.Configurationable(prefix='context.web.static')
def _config_init_staticFiles(config):
    if not config:
        return
    
    @WebContext.Event.on_started
    def print_web_start_test(app):
        if 'mapping' in config and config['mapping'] and isinstance(config['mapping'], dict):
            idx = 1            
            for k, path in config['mapping'].items():
                k:str = k
                if not k.startswith('/'):
                    k = '/' + k
                    
                app.mount(f"{k}", AsyncStaticFiles(directory=Path(path), html=True), name=f"static-{k[1:]}")
                idx += 1
                _logger.DEBUG(f'静态文件目录路径映射{k}-{path}')        
            
        if 'root' in config and not str_isEmpty(config['root']):
            app.mount("/", AsyncStaticFiles(directory=Path(config['root']), html=True), name="static-root")            
            _logger.DEBUG(f'静态文件ROOT目录路径映射/-{config['root']}')
    

@Context.Configurationable(prefix='context.web.proxy')
def _config_init_proxy(config):
    if not config:
        return
    _logger.DEBUG(f'代理服务配置={config}')
    ison = get_bool_from_dict(config, 'enabled', False)
    
    if not ison:
        return 
    
    @Context.Service()
    def init_proxy_config():
        _proxy_config = ProxyConfig(
            timeout=get_float_from_dict(config, "timeout", 30.0),
            max_connections=get_int_from_dict(config, "max_connections", 100),
            enable_caching=get_bool_from_dict(config, "enable_caching", False),
            cache_ttl=get_int_from_dict(config, "cache_ttl", 300),
            rate_limit=get_int_from_dict(config, "rate_limit", None),
            blocked_user_agents=get_list_from_dict(config, "blocked_user_agents", None)
        )
        
        _aps = AdvancedProxyService(_proxy_config)
        # Context.getContext().registerBean(get_fullname(AdvancedProxyService))
        _logger.DEBUG(f'初始化代理服务成功,并进行注册，可以使用getBean(AdvancedProxyService),获取服务实例=>{_aps}[{_proxy_config}]')
        return _aps
        
    

@Context.Configurationable(prefix='context.web.cors')
def _config_cors_filter(config):
    _logger.DEBUG(f'CORS过滤器装饰器信息=[{config}]')
    
    @WebContext.Event.on_started
    def _init_cros_filter(app:FastAPI):        
        # origins = ["*"]        
        opts = {
            'allow_origins':get_list_from_dict(config, 'allow_origins', ["*"]),
            'allow_methods':get_list_from_dict(config, 'allow_methods', ["*"]),
            'allow_headers':get_list_from_dict(config, 'allow_headers', ["*"]),            
            'expose_headers':get_list_from_dict(config, 'expose_headers', ["*"]),
            'allow_credentials':get_bool_from_dict(config, 'allow_credentials', True),
        }
        app.add_middleware(
            CORSMiddleware,
            **opts
            # # allow_origins=origins,
            # allow_origins=["*"],
            # allow_credentials=True,
            # allow_methods=["*"],
            # allow_headers=["*"],
        )
        _logger.DEBUG(f'添加CORS过滤器[{opts}]={CORSMiddleware}成功')
        
        
if __name__ == "__main__":    
    matcher = AntPathMatcher()
    def test_match(str1,str2):
        print(f'matcher.match("{str1}", "{str2}") = {matcher.match(str1, str2)}')       # 输出: True
        
    test_match("/api/?", "/api/d")       # 输出: True
    test_match("/api/?", "/api/dd")      # 输出: False
    test_match("/api/*", "/api/data")    # 输出: True
    test_match("/api/*", "/api/data-test.jsp")    # 输出: True
    test_match("/api/**", "/api/data/info") # 输出: True    
    test_match("/api/**", "/api/data/test.jsp")    # 输出: True
    test_match("/api/**", "/api/") # 输出: True    
    test_match("/api/**", "/api") # 输出: True    
    test_match("*/api/**", "/aaa/api/") # 输出: True    
    test_match("*/api/**", "aaa/api/") # 输出: True    
    test_match("**/api/**", "/test/aaa/api/") # 输出: True    


