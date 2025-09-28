from fastapi import APIRouter
from dataflow.module.context import WebContext,Context
from dataflow.utils.log import Logger

_logger = Logger('application.user')

router = APIRouter(prefix="/users", tags=["用户"])
_logger.INFO('实例化用户模块')

@router.post("/add")
def create_item(title: str):
    return {"id": 42, "title": title}

@router.get("/{item_id}")
def read_item(item_id: int):    
    return Context.getContext().getDataSource().queryPage('select * from sa_security_realtime_daily where code=:code order by tradedate desc', {'code':'300492'}, page=1, pagesize=10)

WebContext.getRoot().include_router(router)