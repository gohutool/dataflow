
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from dataflow.utils.log import Logger
from dataflow.utils.utils import str_isEmpty,str_strip, ReponseVO, get_list_from_dict, get_bool_from_dict,current_millsecond  # noqa: F401
from dataflow.utils.web.asgi import get_remote_address, CustomJSONResponse,get_ipaddr
from dataflow.module import Context, WebContext
from antpathmatcher import AntPathMatcher
from fastapi import Request, FastAPI
from slowapi import Limiter
# from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError,HTTPException
from starlette.exceptions import HTTPException as StarletteHTTPException
from fastapi.middleware.cors import CORSMiddleware
import uuid

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
_filter = []

def filter(app:FastAPI=None, *, path:list[str]|str='*', excludes:list[str]|str=None, order=1):    
    
    if not app:
        app = WebContext.getRoot()
    
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
        if (paths is None or len(paths) == 0) and (_excludes is None or len(_excludes) == 0):
            app.add_middleware(BaseHTTPMiddleware, dispatch=func)
        else:
            async def new_func(request: Request, call_next):   
                if _excludes is not None and len(_excludes)>0 :
                    for o in _excludes:
                        if antmatcher.match(o, request.url.path):                        
                            return await call_next(request)                                                
                
                matched = False
                if paths is not None and len(paths)>0:
                    for o in paths:
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
                        return await func(request, call_next)                                
                    except HTTPException as e:
                        raise e
                    except RequestValidationError as e:
                        raise e
                    except StarletteHTTPException as e:
                        raise e
                    except Exception as e:
                        raise Context.ContextExceptoin(detail=e.__str__())
                    
            app.add_middleware(BaseHTTPMiddleware, dispatch=new_func)      
    _logger.DEBUG(f'创建过滤器装饰器={decorator} path={path} excludes={excludes}')
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
    
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc:HTTPException):
        _logger.ERROR(f'处理HttpExpcetion: {exc}', exc)
        return CustomJSONResponse(            
            status_code=exc.status_code,
            # content={"code": exc.status_code, "message": exc.detail}
            content=ReponseVO(False, code=exc.status_code, msg=exc.detail, data=exc.detail)
        )
        
    
    @app.exception_handler(Exception)
    async def exception_handler(request: Request, exc:Exception):
        _logger.ERROR(f'处理Expcetion: {exc}', exc)
        code = getattr(exc, 'code') if hasattr(exec, 'code') else 500
        return CustomJSONResponse(
            status_code=code,
            # content={"code": exc.status_code, "message": exc.detail}
            content=ReponseVO(False, code=code, msg=exc.__str__(), data=exc.__str__())
        )
              
    # 覆盖 HTTPException
    @app.exception_handler(StarletteHTTPException)
    async def http_fastapi_exception_handler(request: Request, exc:StarletteHTTPException):
        return CustomJSONResponse(
            status_code=exc.status_code,
            # content={"code": exc.status_code, "message": exc.detail}
            content=ReponseVO(False, code=exc.status_code, msg=exc.detail, data=exc.detail)
        )
  
    # 覆盖校验错误
    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc:RequestValidationError):
        return CustomJSONResponse(
            status_code=422,
            # content={"code": 422, "message": "参数校验失败", "errors": exc.errors()}
            content=ReponseVO(False, code=422, msg=exc.detail, data=exc.errors)
        )

@Context.Configurationable(prefix='context.web.cors')
def _config_cors_filter(config):
    _logger.DEBUG(f'CORS过滤器装饰器信息=[{config}]')
    
    @WebContext.Event.on_loaded
    def _init_cros_filter(app:FastAPI):        
        # origins = ["*"]        
        opts = {
            'allow_origins':get_list_from_dict(config, 'allow_origins', ["*"]),
            'allow_methods':get_list_from_dict(config, 'allow_methods', ["*"]),
            'allow_headers':get_list_from_dict(config, 'allow_headers', ["*"]),
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
        _logger.DEBUG(f'添加CORS过滤器装饰器[{opts}]={CORSMiddleware}成功')
        
        
@WebContext.Event.on_loaded
def init_web_common_filter(app:FastAPI):    
    @app.middleware("http")
    async def wrap_exception_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        rid = ''
        if hasattr(request.state, 'xid'):
            rid = request.state.xid
        try:                        
            response = await call_next(request)
        except HTTPException as e:
            raise e
        except RequestValidationError as e:
            raise e
        except StarletteHTTPException as e:
            raise e
        except Exception as e:
            _logger.ERROR(f"[{rid}] {request.method} {request.url}", e)
            raise Context.ContextExceptoin(detail=e.__str__())
        
        _logger.INFO(f"[{rid}] {request.method} {request.url}")        
        return response    
    _logger.DEBUG(f'添加过滤器装饰器={wrap_exception_handler}') 
    
    @app.middleware("http")
    async def xid_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        start = current_millsecond()
        
        rid = uuid.uuid4().hex
        request.state.xid = rid    
        ip = get_ipaddr(request)
        
        _logger.INFO(f"[{rid}] {request.method} {request.url}")
        
        WebContext.setRequest(request)        
        response = await call_next(request)
        WebContext.resetRequest()
                
        # ====== 响应阶段 ======
        cost = (current_millsecond() - start)
        response.headers["X-Request-ID"] = rid      
        response.headers["X-Cost-ms"] = str(cost)                      
        
        _logger.INFO(f"[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
        return response        
    _logger.DEBUG(f'添加过滤器装饰器={xid_handler}')  

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


