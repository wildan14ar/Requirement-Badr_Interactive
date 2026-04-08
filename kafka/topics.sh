#!/bin/bash
# ==============================================================================
# Kafka Topics Initialization Script
# Purpose: Create all required topics when Kafka starts for the first time
# ==============================================================================

echo "⏳ Waiting for Kafka to be ready..."

# Wait for Kafka to be ready
until kafka-topics --bootstrap-server kafka:9092 --list &> /dev/null; do
  echo "  Kafka not ready yet, waiting..."
  sleep 2
done

echo "✅ Kafka is ready, creating topics..."

# ==============================================================================
# TOPICS FOR STOCK EVENTS (Real-time - Future Phase)
# ==============================================================================

echo "📝 Creating topic: stock_events"
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic stock_events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

echo "📝 Creating topic: stock_updates"
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic stock_updates \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# ==============================================================================
# TOPICS FOR ENTITY CHANGES (Real-time - Future Phase)
# ==============================================================================

echo "📝 Creating topic: entity_changes"
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic entity_changes \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# ==============================================================================
# TOPICS FOR ALERTS (Near Real-time - Phase 2)
# ==============================================================================

echo "📝 Creating topic: stock_alerts"
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic stock_alerts \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# ==============================================================================
# TOPICS FOR IOT/SENSORS (Real-time - Future Phase)
# ==============================================================================

echo "📝 Creating topic: cold_storage_telemetry"
kafka-topics --bootstrap-server kafka:9092 \
  --create \
  --topic cold_storage_telemetry \
  --partitions 12 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete \
  --config compression.type=lz4

# ==============================================================================
# LIST ALL TOPICS
# ==============================================================================

echo ""
echo "✅ All topics created successfully!"
echo ""
echo "📊 Topics list:"
kafka-topics --bootstrap-server kafka:9092 --list

echo ""
echo "📋 Topic details:"
kafka-topics --bootstrap-server kafka:9092 --describe

echo ""
echo "🎉 Kafka initialization complete!"
