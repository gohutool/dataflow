from dataflow.utils.log import Logger
from dataflow.utils.utils import current_millsecond,str_to_json
from dataflow.module import Context
from dataflow.utils.dbtools.pydbc import PydbcTools
from dataflow.utils.dbtools.pydbc import NULL, SimpleExpression
import time

_logger = Logger('application.test.service')

@Context.Service('userService')
class UerService:
    pydbc:PydbcTools=Context.Autowired()
    def getItemInfo(self, item_id:str, name:str)->any:
        _logger.DEBUG(f'调用UerService参数={name}')
        return self.pydbc.queryOne('select * from sa_security_realtime_daily where code=:code order by tradedate desc limit 1', {'code':item_id})
    
    def test_tx_3(self):
        _logger.DEBUG("BEGIN TX3 ========================")
        sample = '''
            {"id":435177,"tradedate":"2025-09-30","code":"920819","name":"颖泰生物","price":"4.25","changepct":"-0.47","change":"-0.02","volume":"56537","turnover":"24137761.32","amp":"1.17","high":"4.3","low":"4.25","topen":"4.3","lclose":"4.27","qrr":"0.62","turnoverpct":"0.47","pe_fwd":"170.35","pb":"1.02","mc":"5209650000","fmc":"5131906875","roc":"-0.23","roc_5min":"-0.23","changepct_60day":"1.67","changepct_currentyear":"19.72","hot_rank_em":5116,"market":"SZ","createtime":"2025-09-30 09:32:17","updatetime":"2025-09-30 17:06:09","enable":1}
            '''
        sample:dict = str_to_json(sample)
        sample['low']=NULL    
        sample['tradedate']='2025-01-05'
        sample['code']=f'3_{current_millsecond()}'
        rtn = self.pydbc.insertT('dataflow_test.sa_security_realtime_daily', sample)        
        _logger.DEBUG(f"END TX3 Result={rtn}  {sample}")
        time.sleep(60)
        
    
    def test_tx_2(self):
        _logger.DEBUG("BEGIN TX2 ========================")
        
        exp = SimpleExpression()
        exp = exp.AND('code','=','920819')
        exp = exp.AND('price','=',4.25)
        exp = exp.AND('code','=','920819').AND('price','=',4.25).AND_ISNULL('volume',False)
        exp = exp.AND_IN('code',['920819','920813'])
        exp = exp.AND_BETWEEN('tradedate','2025-01-05','2026-01-06')
        exp = exp.AND('tradedate','in', ['2025-01-05','2025-01-06','2025-09-30'])
    
        rtn = self.pydbc.queryMany('select * from dataflow_test.sa_security_realtime_daily where 1=1 AND ' + exp.Sql(), exp.Param())
        _logger.DEBUG(f'Result={rtn}')
        
        sample = '''
            {"id":435177,"tradedate":"2025-09-30","code":"920819","name":"颖泰生物","price":"4.25","changepct":"-0.47","change":"-0.02","volume":"56537","turnover":"24137761.32","amp":"1.17","high":"4.3","low":"4.25","topen":"4.3","lclose":"4.27","qrr":"0.62","turnoverpct":"0.47","pe_fwd":"170.35","pb":"1.02","mc":"5209650000","fmc":"5131906875","roc":"-0.23","roc_5min":"-0.23","changepct_60day":"1.67","changepct_currentyear":"19.72","hot_rank_em":5116,"market":"SZ","createtime":"2025-09-30 09:32:17","updatetime":"2025-09-30 17:06:09","enable":1}
            '''
                        
        sample:dict = str_to_json(sample)
        sample['code']=f'2_{current_millsecond()}'
        rtn = self.pydbc.insertT('dataflow_test.sa_security_realtime_daily', sample)     
        time.sleep(0.1)
        self.test_tx_3()
        time.sleep(60)
        _logger.DEBUG("END TX2 ========================")
    
class ItemService:
    userService:UerService=Context.Autowired(name='userService')
    def getItems(self, item_id:str)->any:
        _logger.DEBUG(f'调用Itemservice={self.name}')
        return self.userService.getItemInfo(item_id, self.name)
    def __init__(self, name):
        self.name = name            
    
@Context.Service('itemService2')
def _getItemService2():
    return ItemService('itemService2')

@Context.Service('itemService1')
def _getItemService1():
    return ItemService('itemService1')

@Context.Service('itemService-Noname')
def _getItemServiceNoname():
    return ItemService('itemService-Noname')

@Context.Inject
def getInfos(code, ds01:PydbcTools=Context.Autowired()):
    return ds01.queryPage('select * from sa_security_realtime_daily where code<>:code order by tradedate', {'code':code}, pagesize=20, page=2)

