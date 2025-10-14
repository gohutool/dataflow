from dataflow.utils.web.asgi import get_remote_address
from dataflow.utils.utils import current_millsecond,date2str_yyyymmddhhmmsss,date_datetime_cn
from fastapi import Request
from dataflow.module.context.web import filter
from dataflow.utils.schedule import ScheduleContext
from dataflow.utils.log import Logger
from dataflow.module import Context, WebContext
from dataflow.module.context.kafka import KafkaContext

_logger = Logger('application.test.config')


@Context.Configurationable(prefix='context.test')
def config_all(config):
    _logger.DEBUG(f'========={config}')
        

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

@KafkaContext.ON_Consumer(inbound='server-1', subscribe='input_others')
def on_consumer_1(err, msg):
    if err:
        _logger.DEBUG(f'================ ⚠️ {err}')
    else:
        _logger.DEBUG(f'================ 💬 {msg.value().decode()}')
    

@KafkaContext.ON_Consumer(inbound='server-2', subscribe='input_cardata')
def on_consumer_2(err, msg):
    if err:
        _logger.DEBUG(f'================ ⚠️ {err}')
    else:
        _logger.DEBUG(f'================ 💬 {msg.value().decode()}')
        
    