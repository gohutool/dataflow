
from functools import wraps
from typing import Callable
from fastapi import Request, HTTPException

from dataflow.utils.log import Logger

_logger = Logger('ASGI')

# 定义一个自定义装饰器
def custom_authcheck_decorator(func: Callable):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        # 模拟身份验证逻辑
        if 'request' in request:
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

