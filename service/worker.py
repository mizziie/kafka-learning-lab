import json
import os
import time

from kafka import KafkaConsumer, KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
REQUEST_TOPIC = os.getenv("KAFKA_REQUEST_TOPIC", "requests")
RESPONSE_TOPIC = os.getenv("KAFKA_RESPONSE_TOPIC", "responses")


def connect_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                REQUEST_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="backend-service",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda m: m.decode("utf-8") if m else None,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            return consumer
        except Exception as exc:
            last_error = exc
            print(f"Consumer connect attempt {attempt}/{retries} failed: {exc}")
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect consumer to Kafka at {KAFKA_BOOTSTRAP}: {last_error}")


def main():
    consumer = connect_consumer()
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    print(f"Worker started, listening on topic: {REQUEST_TOPIC}")
    try:
        for msg in consumer:
            request = msg.value or {}
            msg_id = request.get("id") or msg.key
            print(f"[{msg_id}] received: {request}")

            response = {
                "id": msg_id,
                "status": "processed",
                "result": f"Executed action='{request.get('action')}'",
            }
            producer.send(RESPONSE_TOPIC, key=msg_id, value=response)
            print(f"[{msg_id}] sent response to {RESPONSE_TOPIC}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
