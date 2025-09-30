
# 导入 FastAPI 框架
from fastapi import FastAPI, Request# noqa: F401
from dataflow.utils.utils import current_millsecond
import uuid
from dataflow.utils.web.asgi import Init_fastapi_jsonencoder_plus

from contextlib import asynccontextmanager
from dataflow.utils.log import Logger
from dataflow.utils.web.asgi import get_ipaddr
from dataflow.module import Context, WebContext
# from fastapi.middleware.cors import CORSMiddleware

_logger = Logger('router.endpoint')

# 定义 lifespan 上下文管理器
@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时执行的代码
    _logger.INFO("Application startup")
    Context.Event.emit('started')
    
    yield
    # 关闭时执行的代码
    Context.Event.emit('exit')
    _logger.INFO("Application shutdown")

Init_fastapi_jsonencoder_plus()
    
app = FastAPI(lifespan=lifespan,
              title="DataFlow API",  
            #   default_response_class=CustomJSONResponse,            
              version="1.0.0")

    
@Context.Context(app=app, scan='dataflow.application.**')
def initApp(app:FastAPI):
    _logger.INFO(f'开始初始化App={app}')
    
    @app.middleware("http")
    async def authcheck_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        rid = ''
        if hasattr(request.state, 'xid'):
            rid = request.state.xid    
            
        response = await call_next(request)
        _logger.INFO(f"[{rid}] {request.method} {request.url}")        
        return response    
    _logger.DEBUG(f'添加过滤器装饰器={authcheck_handler}')

    @app.middleware("http")
    async def xid_handler(request: Request, call_next):
        # ====== 请求阶段 ======
        start = current_millsecond()
        rid = uuid.uuid4().hex
        request.state.xid = rid    
        _logger.INFO(f"[{rid}] {request.method} {request.url}")
        
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid        
        # ====== 响应阶段 ======
        cost = (current_millsecond() - start)
        ip = get_ipaddr(request)
        _logger.INFO(f"[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
        return response        
    _logger.DEBUG(f'添加过滤器装饰器={xid_handler}')
         
    # app.add_middleware(
    #     CORSMiddleware,
    #     # **opts
    #     # # allow_origins=origins,
    #     allow_origins=["*"],
    #     # allow_credentials=True,
    #     allow_methods=["*"],
    #     allow_headers=["*"],
    # )
    # _logger.DEBUG(f'添加CORS过滤器装饰器={CORSMiddleware}成功')     

initApp(app=app)    
WebContext.Event.emit('started', app)
    