from fastapi import APIRouter
from dataflow.module.context import WebContext
from dataflow.utils.log import Logger

_logger = Logger('application.user')

router = APIRouter(prefix="/users", tags=["用户"])
_logger.INFO('实例化用户模块')

@router.post("/add")
def create_item(title: str):
    return {"id": 42, "title": title}

@router.get("/{item_id}")
def read_item(item_id: int):
    return {"item_id": item_id}

WebContext.getRoot().include_router(router)