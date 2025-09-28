
# 导入 FastAPI 框架
from fastapi import FastAPI, Request, Depends, HTTPException # noqa: F401
from dataflow.utils.utils import current_millsecond
import uuid
from dataflow.utils.web.asgi import custom_authcheck_decorator  # noqa: F401
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import status
from contextlib import asynccontextmanager
from dataflow.utils.log import Logger
from dataflow.utils.web.asgi import get_ipaddr
from dataflow.module.context.metrics import setup_metrics 
from dataflow.module.context.web import filter
from dataflow.module import Context
from dataflow.utils.reflect import is_not_primitive
from dataflow.utils.utils import json_to_str
# from datetime import datetime,date
import json


_logger = Logger('router.endpoint')

# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    
    yield
    # 关闭时执行的代码
    _logger.INFO("Application shutdown")


# class CustomJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (datetime, date)):
#             return obj.strftime("%Y-%m-%d %H:%M:%S")  # 自定义格式
#         return super().default(obj)

class CustomJSONResponse(JSONResponse):
    def render(self, content):
        if is_not_primitive(content):
            return json_to_str(content).encode("utf-8")
        return super().render(content)
    
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",  
              default_response_class=CustomJSONResponse,            
              version="1.0.0")
 
@Context.Context(app=app, scan='dataflow.application.**')
def initApp(app:FastAPI):
    _logger.INFO(f'开始初始化App={app}')
            
    setup_metrics(app)
    @app.middleware("http")
    async def authcheck_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        rid = ''
        if hasattr(request.state, 'xid'):
            rid = request.state.xid    
            
        response = await call_next(request)
        _logger.INFO(f"[{rid}] {request.method} {request.url}")        
        return response    
    _logger.DEBUG(f'创建过滤器装饰器={authcheck_handler}')

    @app.middleware("http")
    async def xid_handler(request: Request, call_next):
        rid = uuid.uuid4().hex
        request.state.xid = rid    
        _logger.INFO(f"[{rid}] {request.method} {request.url}")
        
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response        
    _logger.DEBUG(f'创建过滤器装饰器={xid_handler}')
    
    @filter(app, excludes='/test,/test/**')
    async def costtime_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        start = current_millsecond()
        # 可选择直接返回，不继续往后走（熔断、IP 黑名单）
        # if request.client.host in BLACKLIST:
        #     return JSONResponse({"msg": "blocked"}, 403)

        # ====== 继续往后走（路由、业务） ======
        response = await call_next(request)
        # ====== 响应阶段 ======
        cost = (current_millsecond() - start)
        
        ip = get_ipaddr(request)

        response.headers["X-Cost-ms"] = str(cost)
        _logger.INFO(f"[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
        return response

    # origins = ["*"]

    app.add_middleware(
        CORSMiddleware,
        # allow_origins=origins,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    _logger.DEBUG(f'创建过滤器装饰器={CORSMiddleware}')

    @app.get("/test")
    async def test_endpoint():
        _logger.INFO('测试中间件顺序')
        return JSONResponse(
            status_code=status.HTTP_200_OK, 
            content={"message": "测试中间件顺序"}
        )
        # return {"message": "测试中间件顺序"}            
        

initApp(app=app)    
    