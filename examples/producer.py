import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8") if k else None,
)

topic = "demo-topic"

for i in range(10):
    key = f"user-{i % 3}"
    msg = {"no": i, "hello": "สวัสดี", "key": key}
    future = producer.send(topic, key=key, value=msg)
    record = future.get(timeout=10)
    print(f"sent {msg} -> partition={record.partition} offset={record.offset}")
    time.sleep(0.5)

producer.flush()
producer.close()
