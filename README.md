# Real-time Pageview Pipeline

A containerized real-time pipeline that ingests pageview events via REST API, delivers raw data to S3, and aggregates pageviews by (postcode, 1-minute window) using Kafka Streams.

## Architecture

```
REST API (POST /api/pageviews) --> Kafka (pageviews topic) --> Raw S3 Writer --> S3 raw/
                                    |
                                    v
                              Kafka Streams
                              (aggregate by postcode, 1-min window)
                                    |
                                    v
                           Kafka (pageview-aggregates) --> Aggregates S3 Writer --> S3 aggregates/
```

## Tech Stack

- **Java 21**, **Spring Boot 3.4**, **Gradle**
- **Kafka** (KRaft mode), **Kafka Streams**
- **LocalStack** for S3
- **Terraform** for infrastructure (S3 bucket)
- **docker** for containerization
- 
## Prerequisites

- Java 21
- docker and docker-compose
- Terraform

## Quick Start

### Single command (LocalStack → Terraform → full stack)

```bash
./up.sh
```

This script:
1. Starts LocalStack
2. Waits for it to be healthy
3. Runs `terraform init` and `terraform apply -auto-approve` (creates S3 bucket)
4. Starts the full stack with `docker compose up --build` (Kafka, kafka-init, app)

### 3. Produce Pageviews

```bash
curl -X POST http://localhost:8080/api/pageviews \
  -H "Content-Type: application/json" \
  -d '{"user_id":1234,"postcode":"SW19","webpage":"www.website.com/index.html","timestamp":1611662684}'
```

Batch:
```bash
curl -X POST http://localhost:8080/api/pageviews/batch \
  -H "Content-Type: application/json" \
  -d '[{"user_id":1,"postcode":"SW19","webpage":"www.a.com","timestamp":1611662684},{"user_id":2,"postcode":"EC1","webpage":"www.b.com","timestamp":1611662685}]'
```

### 4. Verify S3 Output

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://pageview-data/raw/
aws --endpoint-url=http://localhost:4566 s3 ls s3://pageview-data/aggregates/
```

Or with `awslocal`: `awslocal s3 ls s3://pageview-data/`

## Run Locally (without containers for app)

1. Start Kafka and LocalStack: `docker compose up -d kafka localstack`
2. Create Kafka topics: `docker compose run --rm kafka-init` (or create `pageviews` and `pageview-aggregates` manually)
3. Run Terraform to create bucket (see above)
4. Run the app: `./gradlew bootRun --args='--spring.profiles.active=local'`

## Running Tests

```bash
./gradlew test
```

- **Unit tests**: Topology (TopologyTestDriver), controller, serialization
- **Integration tests**: Full Spring context with EmbeddedKafka, API acceptance (POST single/batch)
- **E2E tests** (excluded by default, requires docker/Docker):

```bash
./gradlew e2eTest
```

The E2E test verifies the full pipeline: POST pageviews → Kafka Streams aggregation → consume aggregates from output topic. It uses Testcontainers (LocalStack for S3).

### Using docker instead of Docker

Testcontainers works with docker. `testcontainers.properties` disables Ryuk (the container reaper) for docker compatibility. Ensure `DOCKER_HOST` points to your docker socket:

**macOS** (docker Machine):

```bash
docker machine start
export DOCKER_HOST=unix://$(docker machine inspect --format '{{.ConnectionInfo.dockerSocket.Path}}')
./gradlew test
```

**Linux** (rootless docker):

```bash
systemctl --user enable --now docker.socket
export DOCKER_HOST=unix://${XDG_RUNTIME_DIR}/docker/docker.sock
./gradlew test
```

## Project Structure

```
src/main/java/com/pageview/pipeline/
├── PageviewPipelineApplication.java
├── factory/        # Kafka, Kafka Streams, S3 bean factories
├── controller/     # PageviewController (REST API)
├── model/          # Pageview, PageviewAggregate
├── consumer/       # RawPageviewS3Writer, AggregateS3Writer
└── streams/        # PageviewAggregationTopology, PageviewTimestampExtractor

terraform/          # S3 bucket (LocalStack provider)
docker-compose.yml  # Kafka, LocalStack, app
up.sh               # Single command: LocalStack → Terraform → docker compose up
```

## Configuration

| Property | Default | Description |
|----------|---------|-------------|
| `pipeline.pageviews-topic` | pageviews | Kafka input topic |
| `pipeline.aggregates-topic` | pageview-aggregates | Kafka output topic |
| `pipeline.stream.threads` | 2 | Kafka Streams thread count |
| `aws.s3.bucket-name` | pageview-data | S3 bucket |
| `aws.s3.endpoint-override` | (empty) | LocalStack endpoint when set |
| `management.metrics.export.cloudwatch.namespace` | PageviewPipeline | CloudWatch namespace for metrics |
| `management.metrics.export.cloudwatch.enabled` | true | Enable CloudWatch metrics export (disabled for local/test) |

## Monitoring

Metrics are exposed via Spring Boot Actuator and published to **AWS CloudWatch** (`PageviewPipeline` namespace) when running in AWS.

- **Health**: `/actuator/health` (Kafka, Kafka Streams, S3), `/actuator/health/liveness`, `/actuator/health/readiness`
- **Custom metrics**: `pageviews.produced`, `s3.writes`, `s3.errors`, `s3.write.duration` (tagged by writer: raw, aggregate, late)
- **CloudWatch export** is disabled for the `local` and `test` profiles (LocalStack has no CloudWatch)

See [monitoring/README.md](monitoring/README.md) for CloudWatch alarms and Kubernetes probe config.

## API Reference

| Endpoint | Method | Body | Response |
|----------|--------|------|----------|
| `/api/pageviews` | POST | Pageview JSON | 202 Accepted |
| `/api/pageviews/batch` | POST | Array of Pageview JSON | 202 Accepted |

**Pageview**:
```json
{"user_id": 1234, "postcode": "SW19", "webpage": "www.website.com/index.html", "timestamp": 1611662684}
```

**PageviewAggregate** (output):
```json
{"postcode": "SW19", "window_start": "2021-01-26T14:44:00", "pageview_count": 42}
```