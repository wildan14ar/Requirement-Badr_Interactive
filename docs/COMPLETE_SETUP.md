# 🚀 Complete Stack Setup Guide

## Overview

Satu **docker-compose.yml** yang mencakup seluruh data pipeline stack untuk Badr Interactive recruitment project.

---

## 📊 Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│              COMPLETE DATA PIPELINE STACK                     │
└──────────────────────────────────────────────────────────────┘

┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Airflow    │────▶│     dbt      │────▶│  ClickHouse  │
│ (Orchestrator)│    │ (Transform)  │    │   (OLAP)     │
└──────────────┘     └──────────────┘     └──────┬───────┘
                                                  │
                                        ┌─────────┴─────────┐
                                        │     Superset      │
                                        │   (Dashboard)     │
                                        └───────────────────┘

┌──────────────┐     ┌──────────────┐
│    Kafka     │────▶│    Spark     │
│ (Streaming)  │     │ (Processing) │
└──────────────┘     └──────────────┘
```

---

## 🛠️ Services & Ports

| Service | Container Name | Port(s) | Purpose |
|---------|---------------|---------|---------|
| **PostgreSQL** | airflow-postgres | 5432 (internal) | Airflow metadata database |
| **Redis** | redis-badr | 6379 | Airflow Celery + Superset cache |
| **Airflow Webserver** | airflow-webserver | **8081** | Airflow UI |
| **Airflow Scheduler** | airflow-scheduler | 8974 (internal) | DAG scheduler |
| **Airflow Worker** | airflow-worker | - | Celery worker |
| **Kafka** | kafka-badr | **9092** (internal), **9094** (external) | Event streaming (KRaft) |
| **Kafka UI** | kafka-ui-badr | **8080** | Kafka monitoring UI |
| **Spark Master** | spark-master | **8082** (UI), **7077** (master) | Spark cluster master |
| **Spark Worker** | spark-worker | - | Spark worker |
| **ClickHouse** | clickhouse-badr | **8123** (HTTP), **9000** (Native) | OLAP data mart |
| **Superset** | superset-badr | **8088** | Dashboard & BI |

---

## 🚀 Quick Start

### 1. Start All Services

```bash
# Start everything
docker compose up -d

# Check status
docker compose ps
```

### 2. Verify Services

```bash
# Airflow UI
curl http://localhost:8081/health

# Kafka
docker exec kafka-badr kafka-topics --bootstrap-server localhost:9092 --list

# ClickHouse
curl http://localhost:8123/ping

# Superset
curl http://localhost:8088/health
```

### 3. Access UIs

| UI | URL | Credentials |
|----|-----|-------------|
| **Airflow** | http://localhost:8081 | airflow / airflow |
| **Kafka UI** | http://localhost:8080 | No auth (local) |
| **Spark Master** | http://localhost:8082 | No auth |
| **Superset** | http://localhost:8088 | admin / admin |

---

## 📁 Required Directory Structure

```
Requirement-Badr_Interactive/
├── docker-compose.yml              ✅ Main compose file
├── Dockerfile.airflow              ⚠️ Need to create
├── Dockerfile.spark                ⚠️ Optional
│
├── airflow/
│   ├── dags/                       ⚠️ Create + add DAGs
│   ├── logs/                       ⚠️ Auto-created
│   ├── config/                     ⚠️ Auto-created
│   └── plugins/                    ⚠️ Optional
│
├── kafka/
│   └── topics.sh                   ✅ Already created
│
├── spark/
│   ├── jobs/                       ⚠️ Create for Spark jobs
│   └── data/                       ⚠️ Optional
│
├── scripts/
│   ├── etl/                        ✅ Already created
│   └── ddl/                        ✅ Already created
│
└── dbt/
    ├── models/                     ⚠️ Create dbt models
    └── tests/                      ⚠️ Create dbt tests
```

---

## 🔧 Dockerfile.airflow

Create this file to extend Airflow image with required packages:

```dockerfile
FROM apache/airflow:3.0.6

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    clickhouse-driver==0.2.6 \
    clickhouse-connect==0.7.0 \
    pymysql==1.1.0 \
    pandas==2.2.0 \
    numpy==1.26.3 \
    dbt-clickhouse==1.7.2 \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    python-dotenv==1.0.1 \
    tqdm==4.66.1
```

---

## 📊 Service Usage by Phase

### Phase 1: BATCH PROCESSING (Primary)

**Services Used:**
- ✅ Airflow (scheduler, webserver, worker)
- ✅ PostgreSQL (Airflow metadata)
- ✅ Redis (Celery broker)
- ✅ ClickHouse (Data mart)
- ✅ Superset (Dashboard)

**NOT Used:**
- ❌ Kafka (future-proof setup)
- ❌ Spark (future-proof setup)

**Commands:**
```bash
# Start only Phase 1 services
docker compose up -d postgres redis airflow-init airflow-webserver airflow-scheduler airflow-worker clickhouse superset

# Run ETL DAG in Airflow
# 1. Go to http://localhost:8081
# 2. Trigger DAG: stock_etl_pipeline
```

---

### Phase 2: REAL-TIME (When Needed)

**Add Services:**
- ✅ Kafka (event streaming)
- ✅ Spark (stream processing)

**Commands:**
```bash
# Start full stack
docker compose up -d

# Check Kafka topics
docker exec kafka-badr kafka-topics --bootstrap-server localhost:9092 --list

# Submit Spark job
docker exec spark-master spark-submit --master spark://spark-master:7077 /opt/spark/jobs/streaming_job.py
```

---

## 🎯 Resource Requirements

### Minimum Requirements

| Resource | Requirement | Notes |
|----------|-------------|-------|
| **RAM** | 8 GB | Can run but may be slow |
| **CPU** | 4 cores | Minimum for all services |
| **Disk** | 20 GB | For volumes and logs |

### Recommended

| Resource | Requirement | Notes |
|----------|-------------|-------|
| **RAM** | 16 GB | Smooth operation |
| **CPU** | 8 cores | Better performance |
| **Disk** | 50 GB | For data growth |

### Resource Allocation per Service

| Service | Memory Limit | CPU Limit |
|---------|-------------|-----------|
| PostgreSQL | 512 MB | 0.5 |
| Redis | 256 MB | 0.25 |
| Airflow (each component) | 1 GB | 0.5 |
| Kafka | 600 MB | 1 |
| Spark Master | 1 GB | 1 |
| Spark Worker | 2 GB | 1 |
| ClickHouse | 4 GB | 2 |
| Superset | 2 GB | 1 |
| **TOTAL** | **~15 GB** | **~8 cores** |

---

## 🐛 Troubleshooting

### Issue: Out of Memory

```bash
# Check resource usage
docker stats

# Stop unnecessary services
docker compose stop spark-master spark-worker kafka kafka-ui kafka-init

# Or reduce ClickHouse memory
# Edit docker-compose.yml: clickhouse deploy.resources.limits.memory
```

### Issue: Airflow not starting

```bash
# Check init logs
docker logs airflow-init

# Reset Airflow
docker compose down
docker compose up -d postgres redis
docker compose up -d airflow-init
docker compose up -d airflow-webserver airflow-scheduler airflow-worker
```

### Issue: Kafka topics not created

```bash
# Run init manually
docker compose up kafka-init

# Or create topics manually
docker exec kafka-badr kafka-topics --bootstrap-server localhost:9092 \
  --create --topic stock_events --partitions 6 --replication-factor 1
```

### Issue: ClickHouse connection refused

```bash
# Wait for ClickHouse to be ready
docker logs clickhouse-badr

# Should see: "Ready for connections"

# Test connection
curl http://localhost:8123/ping
# Should return: Ok.
```

---

## 💡 Tips & Best Practices

### 1. Start Services Gradually

```bash
# First: databases
docker compose up -d postgres redis clickhouse

# Wait for health checks
docker compose ps

# Then: processing
docker compose up -d airflow-init kafka

# Finally: UIs
docker compose up -d airflow-webserver airflow-scheduler airflow-worker superset kafka-ui
```

### 2. Use Profiles (Optional)

Add profiles to docker-compose.yml to start subsets:

```yaml
services:
  kafka:
    profiles:
      - streaming
  
  spark-master:
    profiles:
      - streaming
```

Then start with:
```bash
# Batch only
docker compose up -d

# Include streaming
docker compose --profile streaming up -d
```

### 3. Monitor Resources

```bash
# Real-time stats
docker stats

# Service health
docker compose ps

# Logs
docker compose logs -f airflow-webserver
docker compose logs -f kafka
```

### 4. Backup Data

```bash
# Backup ClickHouse data
docker exec clickhouse-badr clickhouse-client --query "BACKUP DATABASE datamart_badr_interactive TO Disk('backups', 'backup.zip')"

# Backup Airflow metadata
docker exec airflow-postgres pg_dump -U airflow airflow > airflow_backup.sql
```

---

## 📚 Next Steps

1. ✅ docker-compose.yml created
2. ⚠️ Create `Dockerfile.airflow`
3. ⚠️ Create `airflow/dags/` with ETL DAGs
4. ⚠️ Create `dbt/models/` with transformation models
5. ⚠️ Test full pipeline end-to-end
6. ⚠️ Create Superset dashboard

---

## ✅ Checklist

- [ ] Docker Desktop installed & running
- [ ] docker-compose.yml in place
- [ ] Dockerfile.airflow created
- [ ] Directory structure created
- [ ] Start services: `docker compose up -d`
- [ ] Verify all services healthy
- [ ] Access Airflow: http://localhost:8081
- [ ] Access Superset: http://localhost:8088
- [ ] Access Kafka UI: http://localhost:8080
- [ ] Run first ETL DAG
- [ ] Verify data in ClickHouse
- [ ] Create Superset dashboard

---

**Last Updated:** April 7, 2026  
**Status:** Docker Compose Ready - Need Dockerfiles & DAGs
