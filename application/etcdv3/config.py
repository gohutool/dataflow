from dataflow.utils.log import Logger
from dataflow.module.context.web import filter,verify_token
from fastapi import Request
from fastapi.responses import JSONResponse
from dataflow.utils.utils import current_millsecond,date2str_yyyymmddhhmmsss,date_datetime_cn
from dataflow.utils.web.asgi import get_remote_address,getRequestURLPath,getRequestHeader
from dataflow.module import Context,WebContext

_logger = Logger('application.etcdv3')


@filter(path='/etcdv3/api,/etcdv3/api/**')
async def costtime_handler(request: Request, call_next):
    # ====== 请求阶段 ======
    start = current_millsecond()
    # 可选择直接返回，不继续往后走（熔断、IP 黑名单）
    # if request.client.host in BLACKLIST:
    #     return JSONResponse({"msg": "blocked"}, 403)
    
    path = getRequestURLPath(WebContext.getRequest())
    _logger.INFO(f"ETCDV3测试过滤器==[{request.url}][{path}]")
    if 'login' not in path:
        auth = getRequestHeader(request, 'authorization', None)
        if not auth:
            raise Context.ContextException("没有登录信息，请先进行登录")
        auth = auth.replace('Bearer ','')
        _logger.DEBUG(f'authorization = {auth}')
        
        try:
            user = verify_token(auth)
            _logger.DEBUG(f'username = {user}')
            WebContext.setRequestUserObject(user['username'])
        except Exception as e:
            _logger.DEBUG(f'verify_token={str(e)}')
            return JSONResponse(
                content={"msg": "用户登录过期"},
                status_code=400,
                headers={               # ← 这里加任意头
                    "is-session-timeout": "1",
                    "is-application-exception": "1",
                },
            )

    # ====== 继续往后走（路由、业务） ======
    try:
        response = await call_next(request)
        # ====== 响应阶段 ======                
        # response.headers["test-Cost-ms"] = str(cost)
        return response    
    finally:
        cost = (current_millsecond() - start)
        ip = get_remote_address(request)
        _logger.INFO(f"ETCDV3测试过滤器==[{request.url}][{ip}]{cost:.2f}ms")
        WebContext.resetRequestUserObject()
        
    