
# 导入 FastAPI 框架
from fastapi import FastAPI, Request# noqa: F401
from dataflow.boot import ApplicationBoot
from dataflow.utils.web.asgi import Init_fastapi_jsonencoder_plus
from contextlib import asynccontextmanager
from dataflow.utils.log import Logger
from dataflow.utils.config import YamlConfigation
from dataflow.module import Context, WebContext
# from fastapi.middleware.cors import CORSMiddleware

_logger = Logger('dataflow.router.endpoint')


# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    WebContext.Event.emit('post_started', app)
    Context.Event.emit('started', Context.getContext())
    
    _c:YamlConfigation = Context.getContext().getConfigContext()    
    _logger.INFO(f"{_c.getStr('application.name', 'DataFlow Application')} {_c.getStr('application.version', '1.0.0')} Start server on {_c.getStr('application.server.host', '127.0.0.1')}:{_c.getStr('application.server.port', 8080)} 启动完成")
        
    yield
    # 关闭时执行的代码
    Context.Event.emit('exit')
    _logger.INFO("Application shutdown")

Init_fastapi_jsonencoder_plus()
    
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",  
            #   default_response_class=CustomJSONResponse,            
              version="1.0.0")       
    
@Context.Context(app=app, applicationConfiguration=ApplicationBoot.applicationConfig, scan=ApplicationBoot.scan)
def initApp(app:FastAPI,context:Context):
    _logger.INFO(f'开始初始化App={app} {context}')
    
# initApp(app=app)

WebContext.Event.emit('started', app)
    