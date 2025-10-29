
from functools import wraps
from typing import Callable,Mapping
from fastapi import Request, HTTPException
from dataflow.utils.log import Logger
from antpathmatcher import AntPathMatcher
from fastapi.responses import JSONResponse

from dataflow.utils.reflect import is_not_primitive
from dataflow.utils.utils import json_to_str

from fastapi.encoders import jsonable_encoder as original_jsonable_encoder
from dataflow.utils.utils import date2str_yyyymmddddmmss, has_method
from datetime import datetime, date
import fastapi.encoders

_logger = Logger('dataflow.utils.web.asgi')

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

class CustomJSONResponse(JSONResponse):
    def render(self, content):
        if is_not_primitive(content):
            return json_to_str(content).encode("utf-8")
        return super().render(content)
    
def Init_fastapi_jsonencoder_plus():
    # 保存原始函数
    _original_jsonable_encoder = original_jsonable_encoder

    def custom_jsonable_encoder(obj, **kwargs):
        """自定义的 jsonable_encoder，全局处理时间格式化"""
        # 设置自定义编码器
        
        # if obj:
        #     if has_method(obj, 'dict'):
        #         return _original_jsonable_encoder(obj.dict(), **kwargs)
        
        custom_encoders = {
            datetime: lambda x: date2str_yyyymmddddmmss(x),
            date: lambda x: date2str_yyyymmddddmmss(x)            
        }
        
        # 合并用户可能传入的其他编码器
        if 'custom_encoder' in kwargs:
            custom_encoders.update(kwargs['custom_encoder'])
        
        # 使用合并后的编码器
        kwargs['custom_encoder'] = custom_encoders
        
        return _original_jsonable_encoder(obj, **kwargs)

    fastapi.encoders.jsonable_encoder = custom_jsonable_encoder
    # fastapi.encoders.dumps = custom_jsonable_encoder
    
def getRequestURLPath(request:Request)->str:
    return request.url.path

    
def getRequestHeaders(request:Request)->Mapping[str,str]:
    return request.headers

def getRequestHeader(request:Request, name:str, dv:str=None)->str:
    v = getRequestHeaders(request)
    v = v.get(name)
    if v:
        return v
    return dv
    

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


