
# 导入 FastAPI 框架
from fastapi import FastAPI, Request# noqa: F401
from dataflow.boot import ApplicationBoot
from dataflow.utils.web.asgi import Init_fastapi_jsonencoder_plus
from contextlib import asynccontextmanager
from dataflow.utils.log import Logger
from dataflow.module import Context, WebContext
# from fastapi.middleware.cors import CORSMiddleware

_logger = Logger('router.endpoint')


# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    WebContext.Event.emit('post_started', app)
    Context.Event.emit('started', Context.getContext())
    
    yield
    # 关闭时执行的代码
    Context.Event.emit('exit')
    _logger.INFO("Application shutdown")

Init_fastapi_jsonencoder_plus()
    
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",  
            #   default_response_class=CustomJSONResponse,            
              version="1.0.0")       
    
@Context.Context(app=app, application_yaml=ApplicationBoot.application_yaml, scan=ApplicationBoot.scan)
def initApp(app:FastAPI,context1:Context):
    _logger.INFO(f'开始初始化App={app} {context1}')
    
# initApp(app=app)

WebContext.Event.emit('started', app)
    