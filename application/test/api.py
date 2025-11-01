from dataflow.utils.utils import ReponseVO, date_datetime_cn, date2str_yyyymmddhhmmsss,json_to_str

from dataflow.module import Context,WebContext
from dataflow.utils.log import Logger
from fastapi import FastAPI, Request, status, HTTPException # noqa: F401
from fastapi.responses import JSONResponse
from dataflow.module.context.web import limiter
from dataflow.module.context.redis import RedisContext
from dataflow.module.context.kafka import KafkaContext
from dataflow.utils.web.asgi_proxy import AdvancedProxyService,StreamResponse

from application.test.service import ItemService, getInfos, UerService
import httpx
import asyncio
from fastapi.responses import StreamingResponse


_logger = Logger('application.test.api')

app:FastAPI = WebContext.getRoot()


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


@app.get("/test/redis_service/{itemid}")
@RedisContext.redis_cache(ttl=60, prefix='cache:data:test:redis-items')
async def test_service(request:Request, itemid:str):
    _logger.INFO('测试Redis和UserService组件')
    _is:UerService = Context.getContext().getBean('userService')    
    return ReponseVO(data=_is.getItemInfo(itemid, 'redis_cache'))

@app.get("/test/services/{itemid}")
async def test_services(request:Request, itemid:str):
    _logger.INFO('测试Services组件')    
    return ReponseVO(data=getInfos(itemid))

@app.get("/test/itemservice/{itemid}")
async def test_services_reg(request:Request, itemid:str):
    _logger.INFO('测试注册ItemService-Noname组件')    
    _is:ItemService = Context.getContext().getBean('itemService-Noname')
    return ReponseVO(data=_is.getItems(itemid))


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

@app.get("/test/tx")
async def test_tx(request:Request):
    _logger.INFO('<<<<<<<<<<测试TX事务管理器')    
    _is:UerService = Context.getContext().getBean('userService')
    _is.test_tx_2()
    _logger.INFO('>>>>>>>>>>测试TX事务管理器')
    return ReponseVO(data='测试TX事务管理器')
    


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

async def sse_gen(request:Request):
    idx = 0
    while not await request.is_disconnected():   # ← 关键：客户端断线就跳出
        idx += 1
        _logger.WARN(f'CURRENT={idx}')
        obj = {
            "result": {
                "header": {
                    "cluster_id": "14841639068965178418",
                    "member_id": "10276657743932975437",
                    "revision": str(1329880+idx),
                    "raft_term": "5"
                },
                "watch_id": str(1761887538896+idx)
            }
        }
        if idx ==1 :
            obj["result"]["created"] = True
        else: 
            obj["result"]["events"] = [
                        {
                            "kv": {
                                "key": "dGVzdA==",
                                "create_revision": "1329615",
                                "mod_revision": "1329884",
                                "version": "4",
                                "value": "dGVzdDExMTExMXZ2dnZ2dmJiYmJ2dnY="
                            }
                        }
                    ]
            
        yield json_to_str(obj)
        # yield json_to_str({
        #     'data':idx
        #     })
        # idx += 1
        try:
            await asyncio.sleep(1)
        except asyncio.CancelledError:           # 额外保险：sleep 被取消也结束
            break

@app.post("/v3/stream")
async def sse(request: Request):
    return StreamingResponse(sse_gen(request),
                             media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache"})

@app.post("/v3/watch")
async def watch(request: Request):
    try:        
        def check_func(_request:Request):
            async def check(chunk:bytes)->bool:
                return not await _request.is_disconnected()
            return check
            
        aps:AdvancedProxyService = Context.getContext().getBean(AdvancedProxyService)
        body = await request.body()
        
        sr:StreamResponse = await aps.async_stream_http_request(
            "http://localhost:8080/v3/stream",
            # "http://localhost:12379/v3/watch",
                                    'POST',
                                    data=None,
                                    content=body,
                                    headers=None,
                                    params=None,
                                    checkfunc=check_func(request))
        response = sr.getResponse()
        return StreamingResponse(await sr.chunkstreams(),
                                    status_code=response.status_code,
                                    headers=response.headers)
        
        # response, sr = await aps.async_response_stream(
        #         "http://localhost:8080/v3/stream",
        #         # "http://localhost:12379/v3/watch",
        #         'POST',
        #         data=None,
        #         content=body,
        #         headers=None,
        #         params=None,                
        #     )
        
        # return StreamingResponse(sr,
        #                         status_code=response.status_code,
        #                         headers=response.headers)
        
    except Exception as e:
        raise e
    
@app.get("/test/getrequest")
async def test_get_request():
    _logger.INFO('测试获取Request组件')
    request = WebContext.getRequest()
    return ReponseVO(data={"base_url":request.base_url, 
                           'method': request.method})

def on_callback(err, msg):
    _logger.DEBUG(f'====={msg}')
    if err:
        _logger.DEBUG(f'❌ {err}')
    else:
        _logger.DEBUG(f'✅ {msg.topic()} {msg.partition()} {msg.offset()}')


@app.get("/test/kafka1")
async def test_kafka_1(request:Request, itemid:str=None):
    _logger.INFO('测试Kafka1组件')
    KafkaContext.getOutBoud('server-1').send('input_others', {
        "time": date_datetime_cn(),
        "time_str": date2str_yyyymmddhhmmsss(date_datetime_cn()),
        'itemid':itemid,
        "inbound":"server-1",
        "subscribe": "python.test.1"
    }, on_callback)
    return ReponseVO(data=Context.Value('${env:LANGFUSE.secret_key:1-sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}'))

@app.get("/test/kafka2")
async def test_kafka_2(request:Request, itemid:str=None):
    _logger.INFO('测试Kafka2组件')
    KafkaContext.getOutBoud('server-2').send('input_cardata', {
        "time": date_datetime_cn(),
        "time_str": date2str_yyyymmddhhmmsss(date_datetime_cn()),
        'itemid':itemid,
        "inbound":"server-2",
        "subscribe": "python.demo.2"
    }, on_callback)
    return ReponseVO(data=Context.Value('${env:LANGFUSE.secret_key:1-sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da}'))