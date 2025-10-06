from dataflow.utils.utils import current_millsecond, ReponseVO, date_datetime_cn,date2str_yyyymmddhhmmsss
from dataflow.utils.web.asgi import get_remote_address
from dataflow.module import Context,WebContext
from dataflow.utils.log import Logger
from fastapi import FastAPI, Request, status, HTTPException # noqa: F401
from fastapi.responses import JSONResponse
from dataflow.module.context.web import filter, limiter
from dataflow.module.context.redis import RedisContext
from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.schedule import ScheduleContext

_logger = Logger('application.test')
app:FastAPI = WebContext.getRoot()

@Context.Service('userService')
class UerService:
    pydbc:PydbcTools=Context.Autowired(name='ds01')
    def getItemInfo(self, item_id:str)->any:
        return self.pydbc.queryOne('select * from sa_security_realtime_daily where code=:code order by tradedate desc limit 1', {'code':item_id})
    
userService = UerService()

class ItemService:
    userService:UerService=Context.Autowired(name='userService')
    def getItems(self, item_id:str)->any:
        _logger.DEBUG(f'调用Itemservice={self.name}')
        return self.userService.getItemInfo(item_id)
    def __init__(self, name):
        self.name = name

itemService = ItemService('NoName')

@Context.Service('itemService2')
def getItemService2():
    return ItemService('2')

@Context.Service('itemService1')
def getItemService1():
    return ItemService('1')

@Context.Inject
def getInfos(code, ds01:PydbcTools=Context.Autowired()):
    return ds01.queryPage('select * from sa_security_realtime_daily where code<>:code order by tradedate', {'code':code}, pagesize=20, page=2)

@Context.Configurationable(prefix='context.test')
def config_all(config):
    _logger.DEBUG(f'========={config}')
    pass

@app.get("/test")
@limiter(rule='1/minute')
async def test_endpoint(request:Request):
    _logger.INFO('测试中间件顺序')
    return JSONResponse(
        status_code= status.HTTP_200_OK, 
        content={"message": "测试中间件顺序"}
    )
    # return {"message": "测试中间件顺序"}  
    

@app.get("/test/redis/{itemid}")
@RedisContext.redis_cache(ttl=60, prefix='cache:data:test:items')
async def test_redis_cache(request:Request, itemid:str):
    _logger.INFO('测试Redis_Cache组件')
    return ReponseVO(data={"itemid":itemid, "time":date_datetime_cn()})


@app.get("/test/service/{itemid}")
@RedisContext.redis_cache(ttl=60, prefix='cache:data:test:items')
async def test_service(request:Request, itemid:str):
    _logger.INFO('测试Service组件')    
    return ReponseVO(data=userService.getItemInfo(itemid))

@app.get("/test/services/{itemid}")
async def test_services(request:Request, itemid:str):
    _logger.INFO('测试Services组件')    
    return ReponseVO(data=getInfos(itemid))

@app.get("/test/reg_services/{itemid}")
async def test_services_reg(request:Request, itemid:str):
    _logger.INFO('测试注册Services组件')    
    return ReponseVO(data=itemService.getItems(itemid))

@app.get("/test/itemservice1/{itemid}")
async def test_itemservice1(request:Request, itemid:str):
    _logger.INFO('测试注册ItemService1组件')    
    _is:ItemService = Context.getContext().getBean('itemService1')
    return ReponseVO(data=_is.getItems(itemid))


@app.get("/test/itemservice2/{itemid}")
async def test_itemservice2(request:Request, itemid:str):
    _logger.INFO('测试注册ItemService2组件')    
    _is:ItemService = Context.getContext().getBean('itemService2')
    return ReponseVO(data=_is.getItems(itemid))

@app.get("/test/exception")
async def test_exception(request:Request):
    _logger.INFO('测试Exception异常')
    raise Exception('测试Exception异常')

@app.api_route("/test/httpexception", methods=["GET", "POST"])
async def test_httpexception(request:Request):
    _logger.INFO('测试HttpException异常')
    raise HTTPException(501, detail='测试HttpException异常')


@app.get("/test/context_value")
async def test_context_value(request:Request):
    _logger.INFO('测试Value组件')
    return ReponseVO(data=Context.Value('${env:LANGFUSE.secret_key:1-sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}'))


@app.get("/test/getrequest")
async def test_get_request():
    _logger.INFO('测试获取Request组件')
    request = WebContext.getRequest()
    return ReponseVO(data={"base_url":request.base_url, 
                           'method': request.method})
        
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
    response.headers["test-Cost-ms"] = str(cost)
    _logger.INFO(f"测试过滤器==[{request.url}][{ip}] {response.status_code} {cost:.2f}ms")
    return response    

@ScheduleContext.Event.on_Listener(event=ScheduleContext.Event.EVENT_ALL)
def _print_schedule_event(je:ScheduleContext.Event.JobEvent):
    _logger.DEBUG(f'触发Scheduler事件{je.code}={je}')
    
@ScheduleContext.on_Trigger(trigger=ScheduleContext.Event.CronTrigger(second='*/10'), args=[1,2,3], id='JOB_2')
@ScheduleContext.on_Trigger(trigger=ScheduleContext.Event.CronTrigger(second='*/30'), kwargs={'a':123}, id='JOB_1')
def _print_date_info(*args, **kwargs):
    _logger.DEBUG(f'当前时间==={date2str_yyyymmddhhmmsss(date_datetime_cn())} {args} {kwargs}')
    

@Context.Event.on_exit
def print_exit_test():
    _logger.DEBUG('@Context.Event.on_exit======================= 退出程序')
    

@Context.Event.on_started
def print_start_test(context):
    _logger.DEBUG(f'@Context.Event.on_start======================= 启动程序 {context} ')
    

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