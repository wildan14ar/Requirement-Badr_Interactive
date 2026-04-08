# Kafka Integration Guide

## Overview

Setup Apache Kafka (KRaft mode) untuk sistem logistik kesehatan Badr Interactive.

---

## 📊 Kafka Topics Strategy

### Topics yang Dibuat

| Topic | Partitions | Retention | Purpose | Phase |
|-------|-----------|-----------|---------|-------|
| `stock_events` | 6 | 24 hours | Stock insertions/updates | Phase 2 (Future) |
| `stock_updates` | 6 | 24 hours | Stock level changes | Phase 2 (Future) |
| `entity_changes` | 3 | 24 hours | Entity/facility updates | Phase 2 (Future) |
| `stock_alerts` | 3 | 7 days | Critical stock alerts | Phase 2 (Near RT) |
| `cold_storage_telemetry` | 12 | 24 hours | IoT sensor data | Phase 3 (Future) |

---

## 🚀 Quick Start

### 1. Start Kafka Stack

```bash
# Start Kafka + UI + ClickHouse + Superset
docker compose up -d

# Check services status
docker compose ps

# View Kafka logs
docker logs -f kafka-badr
```

### 2. Verify Kafka is Running

```bash
# Test connection
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 --list

# Should show:
# stock_events
# stock_updates
# entity_changes
# stock_alerts
# cold_storage_telemetry
```

### 3. Access Kafka UI

- **URL:** http://localhost:8080
- **Cluster Name:** badr-local
- **Bootstrap Server:** kafka:9092

---

## 📝 Using Kafka (Phase 2 - When Needed)

### Produce Events (Python Example)

```python
from kafka import KafkaProducer
import json
from datetime import datetime

# Initialize producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='lz4'
)

# Produce stock event
event = {
    'stock_id': 12345,
    'material_id': 67,
    'entity_id': 89,
    'quantity': 150.5,
    'batch_id': 'BATCH-2026-001',
    'timestamp': datetime.now().isoformat(),
    'event_type': 'stock_update'
}

producer.send('stock_events', value=event)
producer.flush()

print(f"✓ Event sent: {event}")
```

### Consume Events (Python Example)

```python
from kafka import KafkaConsumer
import json

# Initialize consumer
consumer = KafkaConsumer(
    'stock_events',
    bootstrap_servers=['localhost:9094'],
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    group_id='stock_processor_group'
)

# Consume events
print("⏳ Waiting for events...")
for message in consumer:
    event = message.value
    print(f"✓ Received: {event}")
    
    # Process event
    # - Update ClickHouse
    # - Send alert if critical
    # - etc.
```

---

## 🔄 Integration with Current Pipeline

### Current Architecture (Phase 1 - Batch Only)

```
MySQL → Airflow (4x/day) → dbt → ClickHouse → Superset
```

**Kafka NOT used in Phase 1** because:
- ✅ Stock data doesn't change real-time
- ✅ 4x/day batch update sufficient
- ✅ Keep it simple, no over-engineering

---

### Future Architecture (Phase 2 - Add Real-time)

```
┌─────────────────────────────────────────────────────┐
│              BATCH PATH (Primary)                   │
│  MySQL → Airflow → dbt → ClickHouse → Superset     │
└─────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────┐
│         REAL-TIME PATH (When Needed)                │
│                                                     │
│  MySQL CDC → Kafka → Spark → ClickHouse            │
│                      │                              │
│                      └→ Alerts System              │
└─────────────────────────────────────────────────────┘
```

**When to enable Kafka:**
1. Need real-time stock alerts
2. IoT sensors deployment (cold storage)
3. High-frequency transactions
4. Live monitoring requirement

---

## 🛠️ Kafka Commands Reference

### Topic Management

```bash
# List topics
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 --list

# Describe topic
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 --describe --topic stock_events

# Create topic
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 \
  --create --topic my_topic --partitions 3 --replication-factor 1

# Delete topic
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 \
  --delete --topic my_topic
```

### Produce/Consume Messages

```bash
# Produce messages (CLI)
docker exec -it kafka-badr kafka-console-producer --bootstrap-server localhost:9092 --topic stock_events

# Consume messages (CLI)
docker exec -it kafka-badr kafka-console-consumer --bootstrap-server localhost:9092 --topic stock_events --from-beginning

# Consume with key
docker exec -it kafka-badr kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic stock_events --from-beginning --property print.key=true --property key.separator=":"
```

### Consumer Groups

```bash
# List consumer groups
docker exec -it kafka-badr kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe consumer group
docker exec -it kafka-badr kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group stock_processor_group

# Reset offsets
docker exec -it kafka-badr kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group stock_processor_group --reset-offsets --to-earliest --topic stock_events --execute
```

---

## 📦 Python Dependencies

```bash
# For Kafka integration
pip install kafka-python
pip install confluent-kafka
```

**Add to requirements.txt:**
```
kafka-python==2.0.2
confluent-kafka==2.3.0
```

---

## 🎯 Use Cases di Project Ini

### Use Case 1: Stock Alerts (Near Real-time)

**Scenario:** Alert ketika stock mencapai level kritis

**Implementation:**
```
1. MySQL CDC (Debezium) → Kafka topic: stock_updates
2. Python consumer monitors stock levels
3. If stock < threshold → send alert to Kafka topic: stock_alerts
4. Alert consumer sends notification (email/WhatsApp/Slack)
```

**Code Structure:**
```
kafka/
├── producers/
│   └── stock_event_producer.py    # Publish stock updates
├── consumers/
│   ├── stock_alert_checker.py     # Check critical levels
│   └── alert_notifier.py          # Send notifications
└── topics.sh                       # Topic initialization
```

---

### Use Case 2: Cold Storage Monitoring (Real-time)

**Scenario:** Monitor temperature vaksin dari IoT sensors

**Implementation:**
```
1. IoT sensors → MQTT → Kafka topic: cold_storage_telemetry
2. Spark Streaming consumes events
3. If temperature out of range → alert
4. Store to ClickHouse for historical analysis
```

---

## ✅ Checklist Implementation

### Phase 1: Setup (Current)

- [x] Docker Compose with Kafka KRaft
- [x] Topics initialization script
- [x] Kafka UI for monitoring
- [x] Network configuration

### Phase 2: Integration (Future)

- [ ] Python producer for stock events
- [ ] Python consumer for alerts
- [ ] CDC setup (Debezium)
- [ ] Spark Streaming jobs
- [ ] ClickHouse integration

---

## 🐛 Troubleshooting

### Issue: Kafka not starting

```bash
# Check logs
docker logs kafka-badr

# Common issues:
# - Port 9092 or 9094 already in use
# - Insufficient memory
# - Volume permission issues
```

**Solution:**
```bash
# Stop all services
docker compose down

# Remove volumes
docker compose down -v

# Restart
docker compose up -d
```

### Issue: Topics not created

```bash
# Run initialization manually
docker exec -it kafka-init-badr /bin/bash /topics.sh

# Or create topics manually
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 \
  --create --topic stock_events --partitions 6 --replication-factor 1
```

### Issue: Cannot connect from host

```bash
# Check port mapping
docker ps | grep kafka

# Should show:
# 0.0.0.0:9092->9092/tcp, 0.0.0.0:9094->9094/tcp

# Test connection
docker exec -it kafka-badr kafka-topics --bootstrap-server localhost:9092 --list
```

---

## 📚 Resources

- **Kafka KRaft:** https://kafka.apache.org/documentation/#kraft
- **Kafka UI:** https://github.com/provectus/kafka-ui
- **Python Kafka Client:** https://kafka-python.readthedocs.io/
- **Confluent Kafka:** https://github.com/confluentinc/confluent-kafka-python

---

**Last Updated:** April 7, 2026  
**Status:** Kafka Ready - Not used in Phase 1 (Batch Only)
