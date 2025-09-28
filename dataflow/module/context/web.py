
from typing import Callable
from starlette.middleware.base import BaseHTTPMiddleware
from dataflow.utils.log import Logger
from dataflow.utils.utils import str_isEmpty,str_strip
from antpathmatcher import AntPathMatcher
from fastapi import Request, FastAPI

_logger = Logger('module.context.web')

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
                    _logger.DEBUG(f'{request.url.path}被拦截器拦截')
                    return await func(request, call_next)
            app.add_middleware(BaseHTTPMiddleware, dispatch=new_func)      
    _logger.DEBUG(f'创建过滤器装饰器={decorator}')
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


