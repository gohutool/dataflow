
# 导入 FastAPI 框架
from fastapi import FastAPI, Request, Depends, HTTPException # noqa: F401
from dataflow.utils.utils import current_millsecond
import uuid
from dataflow.utils.web.asgi import custom_authcheck_decorator  # noqa: F401
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import status
from contextlib import asynccontextmanager
from dataflow.utils.dbtools.mysql import initMysqlWithYaml
from dataflow.utils.dbtools.redis import initRedisWithYaml
from dataflow.utils.dbtools.milvus import initMilvusWithYaml
from dataflow.utils.log import Logger
from dataflow.utils.web.asgi import get_ipaddr
from dataflow.utils.config import settings
from dataflow.module.context.metrics import setup_metrics 
from dataflow.module.context.web import filter
from dataflow.module.context import WebContext, Context

_logger = Logger('endpoint')

# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    
    initMysqlWithYaml('conf/db.yaml')
    initRedisWithYaml('conf/redis.yaml')
    initMilvusWithYaml('conf/milvus.yaml')
    
    _logger.INFO(f'DS={settings.getList('DataSource')}')
    _logger.INFO(f'MYSQLDS={settings.getDict('MYSQLDS')}')
    _logger.INFO(f'REDIS={settings.getDict('REDIS')}')
    _logger.INFO(f'MILVUS={settings.getDict('MILVUS')}')
    
    
    yield
    # 关闭时执行的代码
    _logger.INFO("Application shutdown")
    

# 基础全局依赖：验证 API Key
# 这种方式针对Router方式，其他方式不拦截
# 中间件拦截所有请求，根据
# async def verify_api_key(request: Request):
#     api_key = request.headers.get("X-API-Key")
#     print(f'api_key={api_key}')    
#     if not api_key:
#         raise HTTPException(status_code=401, detail="API Key missing")
#     # 简单的验证逻辑（实际应用中应该更复杂）
#     if api_key != "your-secret-api-key":
#         raise HTTPException(status_code=403, detail="Invalid API Key")    
#     # # 验证通过，可以继续
#     # return {"api_key": api_key}
    

# # 创建一个 FastAPI 应用实例
# app = FastAPI(lifespan=lifespan,
#               title="DataFlow API",
#               version="1.0.0",
#               dependencies=[Depends()]
#             )   

app = FastAPI(lifespan=lifespan,
              title="DataFlow API",
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

    @app.middleware("http")
    async def xid_handler(request: Request, call_next):
        rid = uuid.uuid4().hex
        request.state.xid = rid    
        _logger.INFO(f"[{rid}] {request.method} {request.url}")
        
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response
    
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

    @app.get("/test")
    async def test_endpoint():
        _logger.INFO('测试中间件顺序')
        return JSONResponse(
            status_code=status.HTTP_200_OK, 
            content={"message": "测试中间件顺序"}
        )
        # return {"message": "测试中间件顺序"}
        

initApp(app=app)    
    