import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "demo-topic",
    bootstrap_servers=["localhost:9092"],
    group_id="demo-group",
    auto_offset_reset="earliest",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    key_deserializer=lambda m: m.decode("utf-8") if m else None,
)

print("กำลังฟัง topic: demo-topic ... กด Ctrl+C เพื่อหยุด")
try:
    for msg in consumer:
        print(f"partition={msg.partition} offset={msg.offset} key={msg.key} value={msg.value}")
except KeyboardInterrupt:
    consumer.close()
