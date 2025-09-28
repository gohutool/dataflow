
from functools import wraps
from typing import Callable
from fastapi import Request, HTTPException, FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from dataflow.utils.log import Logger
from dataflow.utils.utils import str_isEmpty,str_strip
from antpathmatcher import AntPathMatcher

_logger = Logger('ASGI')

# 定义一个自定义装饰器
def custom_authcheck_decorator(func: Callable):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # 模拟身份验证逻辑
        if 'request' in kwargs:
            request: Request = kwargs.get("request")
            # 在请求到达路由之前执行的逻辑
            _logger.INFO(f"Uri={request.url} args={args} kwargs={kwargs}")
            if not request.headers.get("api-key"):
                raise HTTPException(status_code=401, detail="Unauthorized")
            if request.headers.get("api-key")!='liuyong10221022':
                raise HTTPException(status_code=401, detail="api-key is wrong")

        # 调用原始的路由处理函数
        result = await func(*args, **kwargs)
        # 在请求处理完成之后执行的逻辑
        # print("After the request is processed")
        return result
    return wrapper

"""
按优先级解析真实客户端 IP。
返回第一个有效 IPv4/IPv6，取不到就退回到 request.client.host
"""
# 常用代理头，按优先级排
__headers_proxy = (
    "cf-connecting-ip",      # Cloudflare
    "x-real-ip",             # Nginx
    "x-forwarded-for",       # 通用
    "x-client-ip",
    "x-cluster-client-ip",
)

def get_ipaddr(request: Request) -> str:
    """
    Returns the ip address for the current request (or 127.0.0.1 if none found)
     based on the X-Forwarded-For headers.
     Note that a more robust method for determining IP address of the client is
     provided by uvicorn's ProxyHeadersMiddleware.
    """
    for hdr in __headers_proxy:
        value = request.headers.get(hdr, "").strip()
        if not value:
            continue
        # X-Forwarded-For 可能是一串，取最左边第一个
        ip = value.split(",")[0].strip()
        if ip:
            return ip
        
    # if "X_FORWARDED_FOR" in request.headers:
    #     return request.headers["X_FORWARDED_FOR"]
    # else:
    if not request.client or not request.client.host:
        return "127.0.0.1"
    
    return request.client.host
    
def get_remote_address(request: Request) -> str:
    """
    Returns the ip address for the current request (or 127.0.0.1 if none found)
    """
    # if not request.client or not request.client.host:
    #     return "127.0.0.1"

    # return request.client.host
    return get_ipaddr(request)

antmatcher = AntPathMatcher()    
# # 提取路径中的变量
# variables = matcher.extract_uri_template_variables("/users/{id}", "/users/123")
# print(variables) # 输出: {'id': '123'}

# # 提取多个变量
# variables = matcher.extract_uri_template_variables(
# "/users/{user_id}/posts/{post_id}", "/users/123/posts/456"
# )
# print(variables) # 输出: {'user_id': '123', 'post_id': '456'}
def filter(app:FastAPI, *, path:str='*', excludes:str=None):
    paths = None
    if str_isEmpty(path) or path.strip() == '*':
        paths = None
    else:
        paths = str_strip(path).split(',')
        
    _excludes = None
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
                    return await func(request, call_next)
            app.add_middleware(BaseHTTPMiddleware, dispatch=new_func)        
    return decorator




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


