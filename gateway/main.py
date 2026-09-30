import json
import os
import time
import uuid
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from kafka import KafkaProducer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
REQUEST_TOPIC = os.getenv("KAFKA_REQUEST_TOPIC", "requests")

producer: KafkaProducer | None = None


def _create_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            if p.bootstrap_connected():
                return p
        except Exception as exc:
            last_error = exc
            print(f"Kafka connection attempt {attempt}/{retries} failed: {exc}")
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP}: {last_error}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = _create_producer()
    yield
    producer.close()


app = FastAPI(title="Kafka Gateway", lifespan=lifespan)


def _kafka_ready() -> bool:
    if producer is None:
        return False
    try:
        # partitions_for จะขอ metadata จาก broker ถ้าเชื่อมต่อหลุดจะพยายาม reconnect ให้
        producer.partitions_for(REQUEST_TOPIC)
        return True
    except Exception:
        return False


@app.get("/health")
def health():
    ready = _kafka_ready()
    return {"status": "ok" if ready else "error", "kafka_connected": ready}


@app.post("/call")
def call_service(payload: dict):
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    if not _kafka_ready():
        raise HTTPException(status_code=503, detail="Kafka not ready")

    msg_id = str(uuid.uuid4())
    message = {
        "id": msg_id,
        "action": payload.get("action"),
        "data": payload.get("data", {}),
    }

    future = producer.send(REQUEST_TOPIC, key=msg_id, value=message)
    record = future.get(timeout=10)

    return {
        "status": "queued",
        "message_id": msg_id,
        "topic": REQUEST_TOPIC,
        "partition": record.partition,
        "offset": record.offset,
    }
