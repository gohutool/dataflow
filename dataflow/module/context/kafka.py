from dataflow.module import Context
from dataflow.utils.log import Logger
from dataflow.utils.reflect import get_methodname
from dataflow.utils.mq.kafka import getProducer, getConsumer,produce, subscribe as _subscribe
from confluent_kafka import Producer, Consumer
import functools


prefix = 'context.stream.kafka'


class Channel:
    def __init__(self, name:str, _producer:Producer, topic:str, options:dict={}):
        self._producer = _producer
        self._options = options
        self.name = name
        self.topic = topic
        
    def __repr__(self)->str:
        return f'name={self.name} topic={self.topic} options={self._options} producer={self._producer}'
    
class Subscribe:
    def __init__(self, name:str, _consumer:Consumer, topic:str, options:dict={}):
        self._consumer = _consumer
        self._options = options
        self.name = name
        self.topic = topic
        
    def __repr__(self)->str:
        return f'name={self.name} topic={self.topic} options={self._options} consumer={self._consumer}'

class OUTBOUND:
    def __init__(self, _producer:Producer, name:str, _channels:dict=[]):
        self._producer:Producer = _producer
        self._channelMaps:dict[str, Channel] = {}
        self.name = name
            
        for k, v in _channels.items():
            channel = Channel(k, _producer, v['destination'], v)
            self._channelMaps[k] = channel        
            _logger.DEBUG(f'通道{k}实例化=>{channel}')
            
    def send(self, channel:str, payload:str|dict|object, callback:callable):
        if channel in self._channelMaps:
            topic = self._channelMaps[channel].topic
            _logger.DEBUG(f'找到通道{channel}=>{topic}=>{self._channelMaps[channel]}')
            produce(self._producer, topic, payload, callback)
        else:
            produce(self._producer, channel, payload, callback)

class INBOUND:
    def __init__(self, _consumer:Consumer, _subscribes:list[dict]=[]):
        self._consumer:Consumer = _consumer
        self._subscribeMaps:dict[str, Subscribe] = {}
        for k, v in _subscribes.items():
            channel = Subscribe(k, _consumer, v['destination'], v)
            self._subscribeMaps[k] = channel        
            _logger.DEBUG(f'消息主题{k}实例化=>{channel}')
            
    def subscribe(self, subscribe:str, onconsumer:callable):
        topic = subscribe
        if subscribe in self._subscribeMaps:
            topic = self._subscribeMaps[subscribe].topic
            _logger.DEBUG(f'找到订阅主题{subscribe}=>{topic}=>{self._subscribeMaps[subscribe]}')
        else:
            topic = subscribe
            
        _subscribe(self._consumer, topic, onconsumer)

_logger = Logger('dataflow.module.context.kafka')

class KafkaContext:
    _INBOUND:dict[str, ] = {}
    _OUTBOUND:dict = {}
    @staticmethod
    def getOutBoud(outbound:str)->OUTBOUND:
        return KafkaContext._OUTBOUND[outbound]
    
    @staticmethod
    def getInBound(inbound:str)->INBOUND:
        return KafkaContext._INBOUND[inbound]
    
    @staticmethod
    def ON_Consumer(inbound:str, subscribe:str)->callable:
        inbound:INBOUND = KafkaContext.getInBound(inbound)
        topic = subscribe
        if subscribe in inbound._subscribeMaps:
            topic = inbound._subscribeMaps[subscribe].topic
            _logger.DEBUG(f'找到订阅主题{subscribe}=>{topic}=>{inbound._subscribeMaps[subscribe]}')            
        else:
            topic = subscribe
            _logger.DEBUG(f'没有找到订阅主题{subscribe},创建主题')
                    
        def decorator(func:callable)->callable:
            
            _logger.DEBUG(f'订阅主题{subscribe}=>{topic}=>{get_methodname(func)}=>{inbound._subscribeMaps[subscribe]}') 
            _subscribe(inbound._consumer, topic, func)
            
            @functools.wraps(func)
            def wrap(*args, **kwargs):
                return func(*args, **kwargs)
            return wrap        
        return decorator
        

@Context.Configurationable(prefix=prefix)
def _init_kafka_context(config:dict):        
    if config:
        _logger.DEBUG(f'初始化Kafka源{prefix}[{config}]开始')
        c:dict = config.copy()    
        
        if 'outbound' in c:                                    
            producers:dict = c['outbound']
            for k, v in producers.items():                
                pc = {}
                channels:list[dict] = v.pop('channels') if 'channels' in v else []                
                for k1, v1 in v.items():
                    k1 = k1.replace('-', '.')
                    pc[k1] = v1
                _logger.DEBUG(f'初始化Kafka-OUTBOUND源{k}[{pc}]')
                _p = getProducer(pc)
                KafkaContext._OUTBOUND[k] = OUTBOUND(_p, k, channels)                
                Context.getContext().registerBean(f'kafka.outbound.{k}', _p)
        else:
            _logger.DEBUG('没有配置Kafka-OUTBOUND源，跳过初始化')   
            
        
        if 'inbound' in c:                                    
            consumers:dict = c['inbound']
            for k, v in consumers.items():                
                pc = {}
                subscribes:list[dict] = v.pop('subscribes') if 'subscribes' in v else []                
                for k1, v1 in v.items():
                    k1 = k1.replace('-', '.')
                    pc[k1] = v1
                _logger.DEBUG(f'初始化Kafka-INBOUND源{k}[{pc}]')
                _p = getConsumer(pc)
                KafkaContext._INBOUND[k] = INBOUND(_p, subscribes)          
                Context.getContext().registerBean(f'kafka.inbound.{k}', _p)      
        else:
            _logger.DEBUG('没有配置Kafka-INBOUND源，跳过初始化')                
    else:
        _logger.DEBUG('没有配置Kafka源，跳过初始化')
    pass
