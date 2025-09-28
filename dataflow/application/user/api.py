from fastapi import APIRouter
from dataflow.module import WebContext
from dataflow.module.context.datasource import DataSourceContext
from dataflow.module.context.redis import RedisContext
from dataflow.module.context.milvus import MilvusContext
from dataflow.utils.log import Logger
from dataflow.utils.utils import ReponseVO


_logger = Logger('application.user')

router = APIRouter(prefix="/users", tags=["用户"])
_logger.INFO('实例化用户模块')

@router.post("/add")
def create_item(title: str):
    return {"id": 42, "title": title}

@router.get("/{item_id}")
def read_item(item_id: str):    
    key = f'cache:data:user:{item_id}'
    usr = RedisContext.getTool().getObject(key)
    
    if not usr:
        # usr = DataSourceContext.getDS().queryPage('select * from sa_security_realtime_daily where code=:code order by tradedate desc', {'code':'300492'}, page=1, pagesize=10)
        usr = DataSourceContext.getDS().queryOne('select * from sa_security_realtime_daily where code=:code order by tradedate desc limit 1', {'code':item_id})
        RedisContext.getTool().set(key, usr, 60*5)
    else:
        _logger.INFO(f'从redis中获取数据={usr}')
    
    MilvusContext.getTool().create_collection('login_info', 768)
    
    return ReponseVO(
        data=usr
    )
    
    

WebContext.getRoot().include_router(router)