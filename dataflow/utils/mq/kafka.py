from confluent_kafka import Producer,Consumer
import json
import time

p = Producer({'bootstrap.servers': '192.168.18.145:9092'})

def cb(err, msg):
    if err:
        print('❌', err)
    else:
        print('✅', msg.topic(), msg.partition(), msg.offset())

for i in range(10):
    print(f'==={i+1}')
    payload = json.dumps({'id': i, 'ts': time.time()})
    p.produce('python.test', payload, callback=cb)
    p.poll(0)          # 触发回调
print('====end')
p.flush()
print('flush end')

c = Consumer({
    'bootstrap.servers': '192.168.18.145:9092',
    'group.id': 'python-demo',
    'auto.offset.reset': 'earliest'
})
c.subscribe(['python.test'])

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error():
        print('⚠️', msg.error())
        continue
    print('💬', msg.value().decode())


if __name__ == "__main__":
    print('start')