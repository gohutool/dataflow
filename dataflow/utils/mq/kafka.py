from confluent_kafka import Producer ,Consumer
import time
from dataflow.utils.utils import current_millsecond,json_to_str
from dataflow.utils.thread import Sleep,LoopDaemonThread
from dataflow.utils.reflect import is_user_object
from dataflow.utils.log import Logger

_logger = Logger('dataflow.utils.mq.kafka')

# https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md

def getProducer(config:dict)->Producer:    
    p = Producer(config)
    
    def _producerFlush():
        try:
            remaining_messages = p.flush(5)
            if remaining_messages > 0:
                _logger.WARN(f"⚠️  flush() 超时，仍有 {remaining_messages} 条消息未完成交付")
            else:
                _logger.DEBUG("所有消息均已交付。")
        except Exception as e:
            _logger.WARN(f'通道发生错误-{e}')
                
    LoopDaemonThread(_producerFlush, name=f'Kafka-produce-{config['client.id']}-{current_millsecond()}', sleep=5)        
    return p

def produce(producer:any, topic:str, payload:str|dict|object, cb:callable):
    if is_user_object(payload):
        # payload = json.dumps(payload, ensure_ascii=False)
        payload = json_to_str(payload)
    if isinstance(payload, (list, dict)):
        # payload = json.dumps(payload, ensure_ascii=False)
        payload = json_to_str(payload)
    else:
        # payload = json.dumps(payload, ensure_ascii=False)        
        payload = json_to_str(payload)
    producer.produce(topic, payload, callback=cb)
    # producer.flush()
    producer.poll(0)

def getConsumer(config:dict)->any:
    c = Consumer(config)
    return c

def subscribe(consumer:Consumer, topic:str|list[str], onConsumer:callable):        
    # obj = {
    #     'is_running':True
    # }    
    if isinstance(topic, str):
        topic = [topic]
        
    consumer.subscribe(topic)    
    # def startSubscribe():
    #     while obj['is_running']:
    #         msg = consumer.poll(1.0)
    #         if msg is None: 
    #             continue
    #         if msg.error():                
    #             onConsumer(err=msg.error(), msg=msg)
    #             continue
    #         onConsumer(err=None, msg=msg)                    
    # t = newThread(startSubscribe, name=f'Kafka-subscribe-{'-'.join(topic)}-{current_millsecond()}', daemon=True)
    # setattr(t, '__end__', obj)
    # t.start()
    
    # def on_exit():
    #     obj['is_running'] = False
        
    # atexit.register(on_exit)
    
    def _subscribe():                
        msg = consumer.poll(1.0)
        if msg is None: 
            return 
        if msg.error():
            # print('⚠️', msg.error())            
            onConsumer(err=msg.error(), msg=msg)            
            return
        try:
            onConsumer(err=None, msg=msg)
        except Exception as e:
            _logger.WARN(f'消息订阅发生错误-{e}')
            # if consumer.
            # pass
            
    LoopDaemonThread(_subscribe, name=f'Kafka-consumer-{current_millsecond()}', sleep=0.001)

def test_producer():
    p = getProducer({
        'bootstrap.servers': '192.168.18.145:9092',
        'client.id': 'python-producer',
        # 可根据需要配置重试、确认机制等
        'retries': 5,
        'acks': 'all'
        
        # 'debug': 'all'
    })    

    def cb(err, msg):
        print(f'====={msg}')
        if err:
            print('❌', err)
        else:
            print('✅', msg.topic(), msg.partition(), msg.offset())

    def send():
        for i in range(10):
            print(f'==={i+1}')
            payload = {'id': i, 
                        'ts': time.time(),
                        'no': current_millsecond()
            }
            
            produce(p, 'python.test', payload, cb)    
            # p.produce('python.test', payload, callback=cb)
            # print(p.poll(0))          # 触发回调
            Sleep(1)
    LoopDaemonThread(send, sleep=0.001)

def test_consumer():
    c = getConsumer({
        'bootstrap.servers': '192.168.18.145:9092',
        'group.id': 'python-demo',
        'auto.offset.reset': 'earliest'
    })
    
    # print(f'consumer={dir(c)} ')
    print(f'consumer={c.list_topics().topics} ')
    
    def on_consumer(err, msg):
        if err:
            print('⚠️', err)
        else:
            print('💬', msg.value().decode())
    
    
    subscribe(c, 'python.test', on_consumer)

# p = Producer({
#     'bootstrap.servers': '192.168.18.145:9092',
#     'debug': 'all'
#     })

# def cb(err, msg):
#     if err:
#         print('❌', err)
#     else:
#         print('✅', msg.topic(), msg.partition(), msg.offset())

# for i in range(10):
#     print(f'==={i+1}')
#     payload = json.dumps({'id': i, 'ts': time.time()})
#     p.produce('python.test', payload, callback=cb)
#     print(p.poll(0))          # 触发回调
# print('====end')
# # p.flush()
# print('flush end')

# c = Consumer({
#     'bootstrap.servers': '192.168.18.145:9092',
#     'group.id': 'python-demo',
#     'auto.offset.reset': 'earliest'
# })
# c.subscribe(['python.test'])

# while True:
#     msg = c.poll(1.0)
#     if msg is None: 
#         continue
#     if msg.error():
#         print('⚠️', msg.error())
#         continue
#     print('💬', msg.value().decode())


if __name__ == "__main__":
    print('start')
    
    test_consumer()
    # test_producer()
        
    input('输入任何字符退出')
    print('退出')