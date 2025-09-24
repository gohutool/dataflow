
# 导入 FastAPI 框架
from fastapi import FastAPI, Request, HTTPException
import requests
from dataflow.utils.log import Logger
from dataflow.utils.utils import current_millsecond
import uuid
from dataflow.utils.asgi import custom_authcheck_decorator
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi import status
from contextlib import asynccontextmanager
from dataflow.utils.dbtools.mysql import initMysqlWithConfig, initMysqlWithYaml
from dataflow.utils.dbtools.redis import initRedisWithConfig, initRedisWithYaml
from dataflow.utils.dbtools.milvus import initMilvusWithConfig, initMilvusWithYaml

_logger = Logger('endpoint')

# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    
    initMysqlWithYaml('conf/db.yaml')
    initRedisWithYaml('conf/redis.yaml')
    initMilvusWithYaml('conf/milvus.yaml')
    
    yield
    # 关闭时执行的代码
    _logger.INFO("Application shutdown")
    

# 创建一个 FastAPI 应用实例
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",
              version="1.0.0",
            )   


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

@app.middleware("http")
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

    response.headers["X-Cost-ms"] = str(cost)
    _logger.INFO(f"[{{request.url}}] {response.status_code} {cost:.2f}ms")
    return response


origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
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