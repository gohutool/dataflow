
# 导入 FastAPI 框架
from fastapi import FastAPI, Request, HTTPException
import requests
from dataflow.utils.log import Logger
from dataflow.utils.asgi import custom_authcheck_decorator
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

_logger = Logger('endpoint')

# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    
    yield
    # 关闭时执行的代码
    _logger.INFO("Application shutdown")
    

# 创建一个 FastAPI 应用实例
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",
              version="1.0.0",
            )   

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)





