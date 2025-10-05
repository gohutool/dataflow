from fastapi import APIRouter
from dataflow.module import WebContext
from dataflow.utils.log import Logger
from dataflow.utils.utils import UUID
from fastapi.responses import StreamingResponse
from captcha.image import ImageCaptcha
import random
import string
import io
from dataflow.utils.sign import b64_encode
from application import AppReponseVO

_logger = Logger('application.auth')

# 过滤易混字符
SAFE_CHARS = "".join(set(string.ascii_uppercase + string.digits) - set("0O1lI"))

router = APIRouter(prefix="/auth", tags=["认证"])
_logger.INFO('实例化认证模块')

def _generate_code(length: int = 4) -> str:
    return "".join(random.choices(SAFE_CHARS, k=length))


@router.get('/captchaImage')
def captchaImage():
    code = _generate_code(4)
    captcha_id = UUID().hex
    
    # 生成图片
    img = ImageCaptcha(width=160, height=60)
    data = img.generate(code)
    data = data.read()
    img_str = b64_encode(data)
    
    return AppReponseVO(data={
        'captchaEnabled':True,
        'img':img_str,
        'uuid':captcha_id
    }).dict()
    
    

WebContext.getRoot().include_router(router)