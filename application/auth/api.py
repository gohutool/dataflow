from fastapi import APIRouter,Body
from dataflow.module import WebContext,Context
from dataflow.utils.log import Logger
from dataflow.utils.utils import UUID, get_str_from_dict
from dataflow.module.context.redis import RedisContext
from dataflow.module.context.web import RequestBind, create_token,Controller

from captcha.image import ImageCaptcha
import random
import string

from dataflow.utils.sign import b64_encode
from application import AppReponseVO
from application.user.service import UserService
from dataflow.utils.sign import matches

_logger = Logger('application.auth')

# 过滤易混字符
SAFE_CHARS = "".join(set(string.ascii_uppercase + string.digits) - set("0O1lI"))


_logger.INFO('实例化认证模块')

def _generate_code(length: int = 4) -> str:
    return "".join(random.choices(SAFE_CHARS, k=length))

_CAPTCHA_CODE_CACHE_KEY = 'app:captcha:code:'


# router = APIRouter(prefix="/auth", tags=["认证"])

# # @router.get('/captchaImage')
# @RequestBind.GetMapping('/captchaImage', api=router)
# def captchaImage():
#     code = _generate_code(4)
#     captcha_id = UUID().hex
    
#     # 生成图片
#     img = ImageCaptcha(width=160, height=60)
#     data = img.generate(code)
#     data = data.read()
#     img_str = b64_encode(data)
    
#     RedisContext.getTool().set(_CAPTCHA_CODE_CACHE_KEY+':'+captcha_id, code, 60)
    
#     return AppReponseVO(data={
#         'captchaEnabled':True,
#         'img':img_str,
#         'uuid':captcha_id
#     }).dict()
    

# # @router.post('/login')    
# @RequestBind.RequestMapping('/login', api=router)
# def login(payload: dict = Body(...)):
#     username = payload['username']
#     password = payload['password']
#     code = payload['code']
#     uuid = payload['uuid']
    
#     _logger.DEBUG(f'usernmae={username} password={password} code={code}, uuid={uuid}')
    
#     _code = RedisContext.getTool().get(_CAPTCHA_CODE_CACHE_KEY+':'+uuid)
    
#     if not _code:
#         raise Context.ContextExceptoin('验证码已经失效')
    
#     if not code.lower() == _code.lower():
#         raise Context.ContextExceptoin('验证码输入错误')
    
#     userService:UserService = Context.getContext().getBean(UserService)
#     user = userService.loadUserByUsername(username)
    
#     if not matches(password, user['password']):
#         raise Context.ContextExceptoin('用户名和密码错误')
    
#     token = create_token(get_str_from_dict(user, 'user_id'), get_str_from_dict(user, 'user_name'))
    
#     return AppReponseVO(data={
#             'token':token
#         }).dict()

# WebContext.getRoot().include_router(router)




@Controller(WebContext.getRoot(), prefix='/auth', tags=["认证"])
class AuthController:
    @RequestBind.GetMapping('/captchaImage')
    def captchaImage(self):
        code = _generate_code(4)
        captcha_id = UUID().hex
        
        # 生成图片
        img = ImageCaptcha(width=160, height=60)
        data = img.generate(code)
        data = data.read()
        img_str = b64_encode(data)
        
        RedisContext.getTool().set(_CAPTCHA_CODE_CACHE_KEY+':'+captcha_id, code, 60)
        
        return AppReponseVO(data={
            'captchaEnabled':True,
            'img':img_str,
            'uuid':captcha_id
        }).dict()
        
    @RequestBind.RequestMapping('/login')
    def login(self, payload: dict = Body(...)):
        username = payload['username']
        password = payload['password']
        code = payload['code']
        uuid = payload['uuid']
        
        _logger.DEBUG(f'usernmae={username} password={password} code={code}, uuid={uuid}')
        
        _code = RedisContext.getTool().get(_CAPTCHA_CODE_CACHE_KEY+':'+uuid)
        
        if not _code:
            raise Context.ContextExceptoin('验证码已经失效')
        
        if not code.lower() == _code.lower():
            raise Context.ContextExceptoin('验证码输入错误')
        
        userService:UserService = Context.getContext().getBean(UserService)
        user = userService.loadUserByUsername(username)
        
        if not matches(password, user['password']):
            raise Context.ContextExceptoin('用户名和密码错误')
        
        token = create_token(get_str_from_dict(user, 'user_id'), get_str_from_dict(user, 'user_name'))
        
        return AppReponseVO(data={
                'token':token
            }).dict()