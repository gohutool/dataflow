from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.log import Logger

_logger = Logger('module.context')

_logger.DEBUG('加载module.context')

class Context:    
    @staticmethod
    def getContext():
        if _context is None:
            raise Exception('没有初始化上下文，请先使用Context.initContext进行初始化')
        return _context
    
    @staticmethod
    def initContext(applicationConfig_file:str, scan_path:str):
        _context = Context()
        
    def __init__(self, applicationConfig_file:str, scan_path:str):        
        pass
    
    def _parseContext(self):
        pass
        
_context :Context = None

                    
