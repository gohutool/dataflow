from confluent_kafka import Producer ,Consumer
import json
import time
from dataflow.utils.thread import newThread
import atexit

def getProducer(config:dict)->Producer:
    return Producer(config)

def produce(producer:any, topic:str, playload:str, cb:callable):
    producer.produce(topic, payload, callback=cb)

def getConsumer(config:dict)->any:
    c = Consumer(config)
    return c

def subscribe(consumer:Consumer, topic:str, onConsumer:callable):        
    obj = {
        'is_running':True
    }
    
    def startSubscribe():
        while obj['is_running']:
            msg = c.poll(1.0)
            if msg is None: 
                continue
            if msg.error():
                # print('⚠️', msg.error())
                onConsumer(error=msg.error(), msg=msg)
                continue
            onConsumer(error=None, msg=msg)
            # print('💬', msg.value().decode())
            
    t = newThread(startSubscribe, name=f'{consumer}-{topic}', daemon=True)
    setattr(t, '__end__', obj)
    t.start()
    
    def on_exit():
        obj['is_running'] = False
        
    atexit.register(on_exit)
    
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