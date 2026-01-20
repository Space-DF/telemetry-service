# Telemetry Service

A robust Go-based telemetry aggregation and alert management service for the SpaceDF IoT platform. Processes device telemetry data, stores metrics in TimescaleDB, and manages real-time alerts.

## Overview

The Telemetry Service is a high-performance message processor that:

- **Aggregates telemetry data** from IoT devices via AMQP
- **Stores time-series data** in TimescaleDB for historical analysis
- **Manages alert rules** and generates alerts based on configurable thresholds
- **Processes messages in batches** for optimal database performance
- **Provides REST API** for querying metrics and alerts

## Architecture

```
RabbitMQ (AMQP)
       ↓
┌──────────────────────────────────┐
│   Telemetry Service              │
│  ┌─────────────────────────────┐ │
│  │ AMQP Consumer (Multi-tenant)│ │
│  └──────────────┬──────────────┘ │
│                 ↓                │
│  ┌──────────────────────────────┐│
│  │  Message Processing Engine   ││
│  │  - Data Transformation       ││
│  │  - Alert Evaluation          ││
│  │  - Batch Aggregation         ││
│  └──────────┬───────────────────┘│
│             ↓                    │
│  ┌──────────────────────────────┐│
│  │ TimescaleDB (PostgreSQL)     ││
│  │ - Device Metrics             ││
│  │ - Alert History              ││
│  │ - Rule Configurations        ││
│  └──────────────────────────────┘│
│             ↕                    │
│  ┌──────────────────────────────┐│
│  │ REST API                     ││
│  │ - Query Metrics              ││
│  │ - Manage Alerts              ││
│  │ - Health Checks              ││
│  └──────────────────────────────┘│
└──────────────────────────────────┘
```

## Key Features

### 📊 Data Processing
- **Multi-tenant support** - Isolated data per organization/tenant
- **Batch processing** - Efficient database writes with configurable flush intervals
- **Device location tracking** - Processes location coordinates with trilateration
- **Message enrichment** - Adds metadata and context to raw messages

### 🗄️ Data Storage
- **TimescaleDB** - Time-series optimized PostgreSQL extension
- **Hypertables** - Automatic partitioning for scalability
- **Retention policies** - Automatic old data cleanup
- **Indexed queries** - Fast metric retrieval

### 🔌 Integration
- **AMQP/RabbitMQ** - Message consumer with auto-reconnect
- **Multiple vhosts** - Support for RabbitMQ virtual hosts
- **REST API** - Query and manage telemetry data
- **Health checks** - Liveness and readiness endpoints

## Prerequisites

- **Go 1.24+**
- **TimescaleDB 2.10+** (PostgreSQL with TimescaleDB extension)
- **RabbitMQ 3.12+**
- **Docker & Docker Compose** (for containerized deployment)

## Installation

### Clone and Setup

```bash
cd telemetry-service

# Download dependencies
go mod download
go mod tidy

# Copy environment template
cp .env.example .env

# Edit configuration
nano .env
```

### Configuration
Create a `.env` file with the following example settings:

```bash
# Server
SERVER_LOG_LEVEL=info
SERVER_API_PORT=8080

# TimescaleDB Configuration
DB_NAME="spacedf_telemetry"
DB_USERNAME="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5437"
DB_BATCH_SIZE=1000
DB_FLUSH_INTERVAL=1s
DB_MAX_CONNECTIONS=25
DB_MAX_IDLE_CONNS=5

# RabbitMQ Configuration (Multi-tenant mode)
AMQP_BROKER_URL=amqp://admin:password@rabbitmq:5672/
AMQP_CONSUMER_TAG=telemetry-service
AMQP_PREFETCH_COUNT=100
AMQP_ALLOWED_VHOSTS=
AMQP_RECONNECT_DELAY=5s
```

### Database Setup

Initialize TimescaleDB:

```bash
# Using dbmate migration tool
dbmate up

# Or manually create the database
createdb telemetry_db
psql telemetry_db -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
```

## Usage

### Run the Service

```bash
# Start the telemetry service
go run ./cmd/telemetry serve

# Or build and run
make build
./bin/telemetry serve
```

### Docker

```bash
# Build image
docker build -t telemetry-service:latest .

# Run container
docker run -p 8080:8080 \
  --env-file .env \
  -e AMQP_BROKER_URL=amqp://rabbitmq:5672/ \
  -e DB_HOST=timescaledb \
  telemetry-service:latest

# Using Docker Compose
docker-compose up -d telemetry-service
```
## Development

### Running Tests

```bash
# Run all tests
go test -v ./...

# Run specific package tests
go test -v ./internal/services/...

# With coverage
go test -cover ./...
```

### Code Quality

```bash
# Format code
gofmt -w .

# Install tools
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install github.com/securego/gosec/v2/cmd/gosec@latest

# Lint code
golangci-lint run ./...

# Security scan
gosec ./...
```

### Database Migrations

The service uses `dbmate` for database migrations:

```bash
# Create new migration
dbmate new create_metrics_table

# Apply migrations
dbmate up

# Rollback
dbmate down
```
## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License
Licensed under the Apache License, Version 2.0  
See the LICENSE file for details.

[![SpaceDF - A project from Digital Fortress](https://df.technology/images/SpaceDF.png)](https://df.technology/)