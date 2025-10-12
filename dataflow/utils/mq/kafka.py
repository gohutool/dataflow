from confluent_kafka import Producer
import json, time

p = Producer({'bootstrap.servers': 'localhost:9092'})

def cb(err, msg):
    if err:
        print('❌', err)
    else:
        print('✅', msg.topic(), msg.partition(), msg.offset())

for i in range(10):
    payload = json.dumps({'id': i, 'ts': time.time()})
    p.produce('demo', payload, callback=cb)
    p.poll(0)          # 触发回调
p.flush()



if __name__ == "__main__":
    print('start')