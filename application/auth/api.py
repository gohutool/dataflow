from fastapi import APIRouter,Body
from dataflow.module import WebContext,Context
from dataflow.utils.log import Logger
from dataflow.utils.utils import UUID
from dataflow.module.context.redis import RedisContext
from captcha.image import ImageCaptcha
import random
import string

from dataflow.utils.sign import b64_encode
from application import AppReponseVO

_logger = Logger('application.auth')

# 过滤易混字符
SAFE_CHARS = "".join(set(string.ascii_uppercase + string.digits) - set("0O1lI"))

router = APIRouter(prefix="/auth", tags=["认证"])
_logger.INFO('实例化认证模块')

def _generate_code(length: int = 4) -> str:
    return "".join(random.choices(SAFE_CHARS, k=length))

_CAPTCHA_CODE_CACHE_KEY = 'app:captcha:code:'

@router.get('/captchaImage')
def captchaImage():
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
    

@router.get('/login')    
def login(payload: dict = Body(...)):
    username = payload['username']
    password = payload['password']
    code = payload['code']
    uuid = payload['uuid']
    
    _code = RedisContext.getTool().get(_CAPTCHA_CODE_CACHE_KEY+':'+uuid)
    
    if not _code:
        raise Context.ContextExceptoin('验证码已经失效')
    
    if code == _code:
        raise Context.ContextExceptoin('验证码输入错误')
    
    
    pass

WebContext.getRoot().include_router(router)