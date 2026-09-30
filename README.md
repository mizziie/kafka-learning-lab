# Kafka Gateway with Docker Compose

> โปรเจกต์นี้สร้าง Kafka messaging layer ด้วย Docker Compose พร้อมตัวอย่าง Gateway (FastAPI) และ Worker Service เพื่อให้เห็นการใช้งาน Kafka เป็นด่านหน้าในการเรียก service ผ่านข้อความ
>
> Learning materials in Thai are available under [`docs/`](docs/).

## What is this?

A minimal event-driven gateway built on Kafka:

- **HTTP Gateway** accepts requests via `POST /call` and publishes them to a Kafka topic.
- **Worker Service** consumes the topic, processes the message, and writes a response back to another topic.
- **Kafka UI** lets you inspect topics, messages, partitions and consumer groups.
- Everything runs in Docker Compose for a single-command setup.

## Tech Stack

- Apache Kafka (Confluent Platform `7.6.1`)
- Zookeeper
- FastAPI (Gateway)
- Python + `kafka-python-ng` (Worker & examples)
- Docker & Docker Compose
- Kafka UI (`provectuslabs/kafka-ui`)

## Architecture

```mermaid
%%{init: {'theme':'base'}}%%
flowchart LR
    classDef nodeStyle fill:#ffffff,stroke:#000000,stroke-width:2px,color:#000000

    Client["Client / curl"]:::nodeStyle
    Gateway["Gateway (FastAPI)<br/>localhost:8000"]:::nodeStyle
    Requests["Kafka Topic: requests"]:::nodeStyle
    Worker["Worker Service"]:::nodeStyle
    Responses["Kafka Topic: responses"]:::nodeStyle
    UI["Kafka UI<br/>localhost:8081"]:::nodeStyle

    Client -->|"POST /call<br/>{action, data}"| Gateway
    Gateway -->|"producer.send(key=message_id)"| Requests
    Requests -->|"consume"| Worker
    Worker -->|"process"| Worker
    Worker -->|"producer.send"| Responses
    UI -->|"inspect"| Requests
    UI -->|"inspect"| Responses

    linkStyle default stroke:#000000,stroke-width:2px
```

## Quick Start

### Prerequisites

- Docker Desktop (WSL2 backend recommended on Windows)
- Docker Compose v2.20+ (supports `depends_on` conditions)

### Run everything

```bash
docker compose up -d --build
```

Wait a few seconds for Zookeeper, Kafka and the topic initialization to finish.

### Verify the gateway

```bash
curl http://localhost:8000/health
```

Expected output:

```json
{"status":"ok","kafka_connected":true}
```

### Send a request through the gateway

```bash
curl -X POST http://localhost:8000/call \
  -H "Content-Type: application/json" \
  -d '{"action":"greet","data":{"name":"krisa"}}'
```

Expected output:

```json
{
  "status": "queued",
  "message_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "topic": "requests",
  "partition": 0,
  "offset": 0
}
```

### Watch the worker process the message

```bash
docker compose logs -f worker
```

You should see the worker receive the request and produce a response to the `responses` topic.

### Explore Kafka UI

Open [http://localhost:8081](http://localhost:8081) and browse:

- **Topics**: `requests`, `responses`, `demo-topic`
- **Messages**: payload, key, offset and partition
- **Consumer Groups**: current offset and lag

## Project Structure

```
.
├── docker-compose.yml
├── .env
├── .gitignore
├── README.md
├── gateway/
│   ├── Dockerfile
│   ├── main.py
│   └── requirements.txt
├── service/
│   ├── Dockerfile
│   ├── worker.py
│   └── requirements.txt
├── examples/
│   ├── producer.py
│   ├── consumer.py
│   └── requirements.txt
└── docs/
    ├── 01-kafka-basics.md
    ├── 02-architecture.md
    └── 03-lessons-learned.md
```

## Learning Path

If you are new to Kafka, follow the guides in order:

1. [01 - Kafka Basics (Thai)](docs/01-kafka-basics.md)
2. [02 - Architecture](docs/02-architecture.md)
3. [03 - Lessons Learned](docs/03-lessons-learned.md)

## Common Commands

```bash
# Start the stack
docker compose up -d --build

# View logs
docker compose logs -f broker
docker compose logs -f worker

# Stop the stack
docker compose down

# Stop and remove all data (volumes)
docker compose down -v
```

## Kafka Connection

- **Inside Docker network**: use `broker:29092`
- **From the host machine**: use `localhost:9092`

This is configured by `KAFKA_ADVERTISED_LISTENERS` in `docker-compose.yml`.

## Future Improvements

- Replace Zookeeper with KRaft mode
- Add Schema Registry for Avro / JSON Schema validation
- Add Kafka Streams or ksqlDB for stream processing
- Add multiple Kafka brokers for real HA
- Switch Python gateway/worker to `confluent-kafka-python` for higher throughput

## License

MIT
