from fastapi import APIRouter,Body,Query,Form  # noqa: F401
from dataflow.module import WebContext,Context
from dataflow.utils.dbtools.pydbc import PydbcTools 
from dataflow.utils.log import Logger
from dataflow.utils.utils import UUID, get_str_from_dict
from dataflow.module.context.web import RequestBind, create_token,Controller
from application import AppReponseVO
from application.etcdv3.service import EtcdV3Service
from dataflow.utils.sign import b64_decode
from httpx import AsyncClient
import httpx
from fastapi.responses import StreamingResponse

_logger = Logger('application.etcdv3')

@Controller(WebContext.getRoot(), prefix='/etcdv3/api', tags=["ETCDV3接口"])
class ETCDV3Controller:
    pydbcTools:PydbcTools = Context.Autowired(name="ds04")
    userService:EtcdV3Service = Context.Autowired()
    
    @RequestBind.PostMapping('/login')
    # def login(self, payload: dict = Body(...)):
    async def login(self, username:str=Form(), password:str=Form(),grant_type:str=Form()):        
        username = b64_decode(username)
        password = b64_decode(password)
        grant_type = b64_decode(grant_type)
        _logger.DEBUG(f'username={username} password={password} grant_type={grant_type}')
        self.userService.login(username, password)        
        token = create_token(username, username)        
        return AppReponseVO(data={                
                'token':token
            }).dict()
        # pass
        
    @RequestBind.GetMapping('/logout')
    # def login(self, payload: dict = Body(...)):
    async def logout(self):
        _logger.DEBUG(f'username={WebContext.getRequestUserObject()}成功退出')
        
       
    @RequestBind.PostMapping('/modifypwd')
    async def modifypwd(self, payload: dict):
        password = get_str_from_dict(payload, 'password')
        newpassword = get_str_from_dict(payload, 'newpassword')
        username = WebContext.getRequestUserObject()
        cnt = self.userService.modifypwd(username, password, newpassword)        
        return AppReponseVO(data={      
                "count":cnt                          
            }).dict()
        
    @RequestBind.GetMapping('/config/getall')
    async def getallconfig(self):
        # username = WebContext.getRequestUserObject()
        data = self.userService.getallconfig()    
        return AppReponseVO(data={      
                "data":data
            }).dict()
    
    @RequestBind.PostMapping('/config/saveone')
    async def saveoneconfig(self, payload: dict):
        data = self.userService.saveoneconfig(payload)    
        return AppReponseVO(data={      
                "data":data
            }).dict()

    
    @RequestBind.PostMapping('/config/remove/{id}')
    async def removeoneconfig(self, id:str):
        data = self.userService.removeoneconfig(id)
        return AppReponseVO(data={      
                "data":data
            }).dict()
          
    @RequestBind.GetMapping('/ginghan/bar')
    async def ginghan_bar(self):
        """
        转发远程 pie.json 内容
        """
        async with AsyncClient(timeout=10) as cli:
            remote = await cli.get("https://www.ginghan.com/bar.json")
            # 如需校验状态
            remote.raise_for_status()
            # 直接返回 bytes，让 FastAPI 按远程 Content-Type 走
            return remote.json()
        
    @RequestBind.GetMapping('/ginghan/line')        
    async def ginghan_line(self):
        """
        转发远程 pie.json 内容
        """
        async with AsyncClient(timeout=10) as cli:
            remote = await cli.get("https://www.ginghan.com/line.json")
            # 如需校验状态
            remote.raise_for_status()
            # 直接返回 bytes，让 FastAPI 按远程 Content-Type 走
            return remote.json()
        
    @RequestBind.GetMapping('/ginghan/pie')
    async def ginghan_pie(self):
        """
        转发远程 pie.json 内容
        """
        async with AsyncClient(timeout=10) as cli:
            remote = await cli.get("https://www.ginghan.com/pie.json")
            # 如需校验状态
            remote.raise_for_status()
            # 直接返回 bytes，让 FastAPI 按远程 Content-Type 走
            return remote.json()
        
    @RequestBind.GetMapping('/ginghan/info')
    async def ginghan_info(self):
        """
        远程字节 → 本地字节（Content-Type 与远程保持一致）
        """
        async def byte_stream():
            async with httpx.AsyncClient(timeout=10) as cli:
                async with cli.stream("GET", "https://www.ginghan.com/info.json") as r:
                    r.raise_for_status()
                    async for chunk in r.aiter_bytes():
                        yield chunk

        # 先拉一次响应头，把远程 Content-Type 带回来
        async with httpx.AsyncClient(timeout=10) as cli:
            head = await cli.head("https://www.ginghan.com/info.json")
            content_type = head.headers.get("content-type", "application/octet-stream")

        return StreamingResponse(
            byte_stream(),
            media_type=content_type,
            # headers={
            #     "Content-Disposition": "inline; filename=info.json"
            # }
        )
        
