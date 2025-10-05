from langfuse import Langfuse, observe
from dataflow.utils.config import settings
from dataflow.utils.log import Logger
import functools
from typing import Callable
import atexit

# Optionally, initialize the client with configuration options
# langfuse = Langfuse(public_key="pk-lf-...", secret_key="sk-lf-...")
 
# Get the default client
# client = get_client()

_logger = Logger('dataflow.utils.trace.langfuse')

class LangfusePlugin:    
    name:str = 'LangfusePlugin'    
    ENABLE_LANGFUSE:bool = False
    _langfuse:Langfuse=None
    @staticmethod
    def getLangfuser()->Langfuse:
        return LangfusePlugin._langfuse
    
    @staticmethod
    def flush():
        if LangfusePlugin._langfuse:
            LangfusePlugin._langfuse.flush()
            _logger.INFO('Langfuse清理缓存')
    
    @staticmethod
    def observe(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            if LangfusePlugin.ENABLE_LANGFUSE:
                observe_func = observe(func)
                print('Langfuse call')
                return observe_func(*args, **kw)
            else:
                return func(*args, **kw)
        return wrapper 


def Setup_Langfuser(secret_key, public_key, host, **kwargs)->Langfuse:
    # secret_key=settings.getStr("LANGFUSE_PUBLIC_KEY", "sk-lf-b60f4b33-ff5a-46ac-9086-e776373c86da")
    # public_key=settings.getStr("LANGFUSE_SECRET_KEY", "pk-lf-4172303b-f7c4-4dc0-9d77-184d99c06131")
    # host=settings.getStr("LANGFUSE_HOST", "https://us.cloud.langfuse.com")

    # secret_key="sk-lf-32a7fc51-dad9-4977-950b-c00b9cf8c12b"
    # public_key="pk-lf-13e8d88d-4f15-4bba-bb3e-609c4095ed41"
    # host="https://us.cloud.langfuse.com"

    _logger.DEBUG(f'public_key={public_key} secret_key={secret_key} host={host}')
        
    # Initialize Langfuse
    langfuse = Langfuse(
        public_key=public_key,
        secret_key=secret_key,
        host=host,    
        **kwargs
    )
    
    LangfusePlugin.ENABLE_LANGFUSE = True
    LangfusePlugin._langfuse = langfuse
    
    _logger.DEBUG(f'{LangfusePlugin.name} {LangfusePlugin.observe} {LangfusePlugin.ENABLE_LANGFUSE}')
    
    def on_exit():        
        langfuse.flush()
        _logger.INFO('Langfuse清理缓存')
    
    atexit.register(on_exit)   
    return langfuse

if __name__ == "__main__":
    
    lang = Setup_Langfuser()
    
    @LangfusePlugin.observe
    # @observe2
    def test_observe():
        print(f'====={observe}')
        return settings
    
    test_observe()
    
    # # 1. 初始化一个Trace（追踪链路）
    # trace = langfuse.trace(
    #     name="my-first-trace",
    #     user_id="user-123"
    # )

    # # 2. 正常使用OpenAI，但额外传递 trace_id 参数
    # completion = openai.chat.completions.create(
    #     model="gpt-3.5-turbo",
    #     messages=[{"role": "user", "content": "对我说Hello, World!"}],
    #     temperature=0,
    #     trace_id=trace.id, # 关键步骤：关联Trace
    # )

    # print(completion.choices[0].message.content)