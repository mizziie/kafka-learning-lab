# Lessons Learned

บันทึกสิ่งที่ได้เรียนรู้ขณะทำโปรเจกต์นี้

## 1. Kafka vs RabbitMQ

- RabbitMQ ใช้แนวคิด **Queue + Exchange**: ข้อความถูก consume แล้วจะหายไป (ถ้า ack)
- Kafka ใช้แนวคิด **Distributed Log**: ข้อความยังคงอยู่ตาม retention อ่านย้อนหลังได้
- Kafka เหมาะกับ **event streaming**, log aggregation, replay ส่วน RabbitMQ เหมาะกับ **task queue, RPC, complex routing**

## 2. Topic, Partition และ Offset

- **Topic** คือช่องทางส่งข้อความ
- **Partition** แบ่ง topic ออกเป็น log ย่อย ๆ เพื่อ parallelism
- **Offset** คือตำแหน่งข้อความใน partition
- ลำดับข้อความรับประกันเฉพาะภายใน partition เดียวกัน

## 3. Consumer Group

- Consumer ที่มี `group_id` เดียวกันจะแบ่งกันอ่าน partition
- Consumer group ต่างกันมี offset แยกกัน อ่านข้อมูลเดียวกันได้อิสระ
- ห้ามมี consumer ใน group เดียวกันมากกว่า partition เกินไป เพราะบางตัวจะว่าง

## 4. Kafka Listeners ภายใน/ภายนอก Docker

- Container ด้วยกัน connect ผ่าน `broker:29092`
- Host machine connect ผ่าน `localhost:9092`
- ต้องตั้งค่า `KAFKA_ADVERTISED_LISTENERS` ให้ถูกต้อง ไม่งั้น client จะเชื่อมต่อไม่ได้

## 5. Python Kafka Client

- `kafka-python-ng` เป็น pure-Python client ใช้งานง่าย แต่ throughput ไม่สูง
- สำหรับ production ควรพิจารณา `confluent-kafka-python` ซึ่งเป็น binding รอบ `librdkafka`

## 6. Production Considerations

- ควรมีหลาย broker และ replication factor > 1
- ควรใช้ Schema Registry เพื่อกำหนด schema ให้ producer/consumer ตกลงกัน
- ควร monitor consumer lag ผ่าน Kafka UI หรือ Prometheus/Grafana
