from dataflow.module import Context
from dataflow.utils.log import Logger
from dataflow.utils.trace.langfuse import LangfusePlugin, Setup_Langfuser
from typing import Callable

prefix = 'context.langfuse'

_logger = Logger('dataflow.module.context.langfuse')

class LangfuseContext:    
    observe:Callable = LangfusePlugin.observe
        

@Context.Configurationable(prefix=prefix)
def _init_langfuse_context(config):
    c = config
    if c:
        _logger.INFO(f'初始Langfuse日志跟踪{prefix}[{c}]开始')  
        Setup_Langfuser(**config)        
        _logger.INFO(f'初始Langfuse日志跟踪{prefix}[{c}]成功')
    else:
        _logger.INFO('没有配置Langfuse日志跟踪，跳过初始化')
        
    @Context.Event.on_exit
    def _on_exit():
        LangfusePlugin.flush()


