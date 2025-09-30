from dataflow.utils.utils import current_millsecond
from dataflow.utils.web.asgi import get_remote_address
from dataflow.module import Context,WebContext
from dataflow.utils.log import Logger
from fastapi import FastAPI, Request, status # noqa: F401
from fastapi.responses import JSONResponse
from dataflow.module.context.web import filter, limiter

_logger = Logger('application.test')
app:FastAPI = WebContext.getRoot()

@Context.Configurationable(prefix='context.test')
def config_all(config):
    _logger.DEBUG(f'========={config}')
    pass

config_all()

@app.get("/test")
@limiter(rule='1/minute')
async def test_endpoint(request:Request):
    _logger.INFO('测试中间件顺序')
    return JSONResponse(
        status_code= status.HTTP_200_OK, 
        content={"message": "测试中间件顺序"}
    )
    # return {"message": "测试中间件顺序"}  
    
@filter(path='/test,/test/**')
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
    
    ip = get_remote_address(request)
    response.headers["X-Cost-ms"] = str(cost)
    _logger.INFO(f"测试过滤器==[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
    return response    


@Context.Event.on_exit
def print_exit_test():
    _logger.DEBUG('@Context.Event.on_exit======================= 退出程序')
    

@Context.Event.on_started
def print_start_test():
    _logger.DEBUG('@Context.Event.on_start======================= 启动程序')
    

@Context.Event.on_init
def print_init_test(context, modules):
    _logger.DEBUG(f'@Context.Event.on_init=======================  {context} {modules}')        
    
@Context.Event.on_loaded
def print_load_test(context, modules):
    _logger.DEBUG(f'@Context.Event.on_loaded ======================= {context} {modules}')        
        

@WebContext.Event.on_started
def print_web_start_test(app):
    _logger.DEBUG('@WebContext.Event.on_start======================= 启动程序')
    
@WebContext.Event.on_loaded
def print_web_load_test(app):
    _logger.DEBUG(f'@WebContext.Event.on_loaded ======================= {app}')            