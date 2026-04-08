# 🏗️ Hybrid Data Pipeline Architecture

## Overview

Arsitektur hybrid yang menggabungkan:
- **Batch Processing** (Airflow + dbt) untuk ETL terjadwal
- **Stream Processing** (Kafka + Spark) untuk real-time (future-proof)
- **Unified Data Mart** (ClickHouse) untuk kedua path
- **Dashboard** (Superset) untuk visualisasi

---

## 📊 Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     HYBRID DATA PIPELINE ARCHITECTURE                        │
└─────────────────────────────────────────────────────────────────────────────┘

═══════════════════════════════════════════════════════════════════════════════
                         SOURCE LAYER (OLTP - MySQL)
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────────┐
│  recruitment_dev @       │
│  10.10.0.30:3306         │
│  • stocks                │
│  • batches               │
│  • master_materials      │
│  • entities              │
│  • entity_tags           │
│  • provinces/regencies   │
└────────┬─────────────────┘
         │
    ┌────┴────┐
    │         │
    │         │ (Real-time CDC - Future)
    │         │ Debezium/Maxwell
    ▼         ▼
╔══════════════╗  ╔══════════════════════════════════════════╗
║  BATCH PATH  ║  ║         STREAMING PATH (Future)          ║
║  (Primary)   ║  ║                                        ║
╚══════╤═══════╝  ╚══════╤═══════════════════════════════════╝
       │                  │
       ▼                  ▼
══════════════════  ═══════════════════════════════════════════════════════════
   INGESTION              STREAMING INGESTION LAYER
══════════════════  ═══════════════════════════════════════════════════════════

┌──────────────┐      ┌──────────────────────────────────────────┐
│  Python ETL  │      │          Apache Kafka Cluster            │
│  (PyMySQL)   │      │  ┌──────────────────────────────────┐   │
│              │      │  │  Topic: stock_events             │   │
│  Schedule:   │      │  │  • stock_insertions              │   │
│  4x/day      │      │  │  • stock_updates                 │   │
│  (Airflow)   │      │  │  • stock_deletions               │   │
└──────┬───────┘      │  └──────────────────────────────────┘   │
       │              │                                          │
       │              │  ┌──────────────────────────────────┐   │
       │              │  │  Topic: inventory_transactions   │   │
       │              │  │  • material_movements            │   │
       │              │  │  • entity_changes                │   │
       │              │  └──────────────────────────────────┘   │
       │              └──────────┬───────────────────────────────┘
       │                         │
       │                         ▼
       │              ┌──────────────────────┐
       │              │  Apache Spark        │
       │              │  Streaming           │
       │              │                      │
       │              │  • Real-time agg     │
       │              │  • Window functions  │
       │              │  • State management  │
       │              └──────────┬───────────┘
       │                         │
       ▼                         ▼
═══════════════════════════════════════════════════════════════════════════════
                     PROCESSING & TRANSFORMATION LAYER
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────────┐
│                    Apache Airflow (Orchestration)                     │
│                                                                      │
│  DAG: stock_etl_pipeline                                            │
│  Schedule: 0 0,6,12,18 * * * (4x/day)                               │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐          │
│  │  Task 1:     │───▶│  Task 2:     │───▶│  Task 3:     │          │
│  │  Extract     │    │  dbt Run     │    │  Validate    │          │
│  │  (MySQL)     │    │  Transform   │    │  & Alert     │          │
│  └──────────────┘    └──────────────┘    └──────────────┘          │
│         │                     │                     │               │
│         ▼                     ▼                     ▼               │
│    Staging Tables         Data Marts           Quality Tests       │
│    (ClickHouse)          (ClickHouse)          (dbt tests)         │
└──────────────────────────────────────────────────────────────────────┘

                          ▼

┌──────────────────────────────────────────────────────────────────────┐
│                          dbt (Transform)                              │
│                                                                      │
│  Models:                                                             │
│  ├── staging/                                                        │
│  │   ├── stg_stocks.sql                                             │
│  │   ├── stg_batches.sql                                            │
│  │   ├── stg_materials.sql                                          │
│  │   ├── stg_entities.sql                                           │
│  │   └── stg_locations.sql                                          │
│  ├── marts/                                                          │
│  │   ├── dim_date.sql                                               │
│  │   ├── dim_material.sql                                           │
│  │   ├── dim_entity.sql                                             │
│  │   ├── dim_location.sql                                           │
│  │   ├── dim_activity.sql                                           │
│  │   ├── dim_entity_tag.sql                                         │
│  │   └── fact_stock.sql                                             │
│  └── reporting/                                                      │
│      ├── rpt_daily_stock.sql                                        │
│      ├── rpt_entity_summary.sql                                     │
│      └── rpt_material_summary.sql                                   │
│                                                                      │
│  Tests:                                                              │
│  ├── unique & not_null on primary keys                              │
│  ├── relationships on foreign keys                                  │
│  ├── accepted_values on categories                                  │
│  └── custom: stock_quantity >= 0                                    │
└──────────────────────────────────────────────────────────────────────┘

       │                          ▲
       │ (Batch)                  │ (Streaming)
       ▼                          ▼
═══════════════════════════════════════════════════════════════════════════════
                          STORAGE LAYER (OLAP)
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────┐
│                  ClickHouse Data Mart                             │
│                                                                  │
│  Database: datamart_badr_interactive                             │
│                                                                  │
│  Staging Layer (Raw):                                            │
│  ├── stg_stocks                                                  │
│  ├── stg_batches                                                 │
│  └── stg_entities                                                │
│                                                                  │
│  Dimension Layer:                                                │
│  ├── dim_date                                                    │
│  ├── dim_material                                                │
│  ├── dim_entity                                                  │
│  ├── dim_location                                                │
│  ├── dim_activity                                                │
│  └── dim_entity_tag                                              │
│                                                                  │
│  Fact Layer:                                                     │
│  ├── fact_stock (batch)                                         │
│  └── fact_stock_realtime (streaming)                            │
│                                                                  │
│  Reporting Layer:                                                │
│  ├── rpt_daily_stock                                            │
│  ├── rpt_entity_summary                                         │
│  └── rpt_material_summary                                       │
│                                                                  │
│  Materialized Views (Real-time Aggregations):                    │
│  ├── mv_stock_by_material                                       │
│  ├── mv_stock_by_entity                                         │
│  └── mv_stock_by_location                                       │
└──────────────────────────────────────────────────────────────────┘

       │
       ▼
═══════════════════════════════════════════════════════════════════════════════
                        VISUALIZATION LAYER
═══════════════════════════════════════════════════════════════════════════════

┌──────────────────────────────────────────────────────────────────┐
│                  Apache Superset Dashboard                        │
│                                                                  │
│  Charts:                                                         │
│  ├── Big Number: Total Stock                                    │
│  ├── Bar Chart: Stock by Entity Tag                             │
│  ├── Bar Chart: Stock by Material                               │
│  ├── Line Chart: Stock Trend Over Time                          │
│  ├── Pie Chart: Vaccine vs Non-Vaccine                          │
│  └── Table: Top 10 Entities                                     │
│                                                                  │
│  Filters:                                                        │
│  ├── Date Range                                                 │
│  ├── Province                                                   │
│  ├── Material Type (Vaccine/Non-Vaccine)                        │
│  ├── Entity Tag                                                 │
│  └── Activity                                                   │
│                                                                  │
│  Caching: Redis (1 hour TTL)                                    │
└──────────────────────────────────────────────────────────────────┘

```

---

## 🔄 Data Flow

### Batch Path (Primary - Airflow + dbt)

```
1. Airflow triggers DAG (4x/day)
   ↓
2. Python ETL extracts from MySQL
   ↓
3. Load to ClickHouse staging tables
   ↓
4. dbt runs transformations:
   - staging → marts → reporting
   ↓
5. dbt tests data quality
   ↓
6. Airflow sends alert (email/Slack)
   ↓
7. Superset refreshes dashboard
```

**Schedule:** `0 0,6,12,18 * * *` (every 6 hours)

---

### Streaming Path (Future - Kafka + Spark)

```
1. MySQL CDC (Debezium) captures changes
   ↓
2. Publish to Kafka topics
   ↓
3. Spark Streaming consumes events
   ↓
4. Real-time aggregations (window functions)
   ↓
5. Insert to ClickHouse fact_stock_realtime
   ↓
6. Materialized Views auto-update
   ↓
7. Superset shows near-real-time data (5 min delay)
```

**Latency:** 1-5 minutes

---

## 📋 When to Use Which Path?

| Scenario | Use Batch | Use Streaming |
|----------|-----------|---------------|
| **Dashboard update** | ✅ 4x/day sufficient | ⚠️ If need < 5 min |
| **Stock alerts** | ❌ Too slow | ✅ Instant notification |
| **Historical analysis** | ✅ Perfect | ⚠️ Overkill |
| **Cold storage monitoring** | ❌ Need real-time temp alerts | ✅ IoT sensors |
| **Report generation** | ✅ Daily/weekly reports | ❌ Not needed |
| **Audit trail** | ✅ Daily snapshots | ✅ Complete event log |

---

## 🛠️ Tech Stack Summary

| Component | Technology | Purpose | When to Enable |
|-----------|-----------|---------|----------------|
| **MySQL** | Source DB | OLTP operational data | Always |
| **Python ETL** | Extract & Load | Batch extraction | Always |
| **Airflow** | Orchestration | Schedule & monitor | Phase 1 ✅ |
| **dbt** | Transformation | SQL-based transforms | Phase 1 ✅ |
| **ClickHouse** | Data Mart | Columnar storage | Always |
| **Superset** | Dashboard | BI visualization | Always |
| **Kafka** | Event Streaming | Real-time events | Phase 2 (Future) |
| **Spark** | Stream Processing | Real-time aggregations | Phase 2 (Future) |
| **Redis** | Cache | Dashboard caching | Phase 1 ✅ |

---

## 📅 Implementation Phases

### Phase 1: Batch Processing (Current)
**Timeline:** 1-2 weeks
**Components:** Airflow + dbt + ClickHouse

**Deliverables:**
- ✅ Airflow DAGs for scheduling
- ✅ dbt models for transformations
- ✅ dbt tests for data quality
- ✅ ClickHouse schema (staging + marts)
- ✅ Superset dashboard

**Status:** **In Progress**

---

### Phase 2: Real-time Streaming (Future)
**Timeline:** 2-4 weeks (when needed)
**Components:** Kafka + Spark + CDC

**Deliverables:**
- Kafka cluster setup
- Debezium CDC from MySQL
- Spark Streaming jobs
- Real-time materialized views
- Alert system (low stock)

**Status:** **Planned (code structure ready)**

---

### Phase 3: Advanced Analytics (Optional)
**Timeline:** 1-2 months
**Components:** ML + Forecasting

**Deliverables:**
- Stock demand forecasting
- Anomaly detection
- Automated reordering suggestions
- Predictive maintenance (cold storage)

**Status:** **Future Enhancement**

---

## 💰 Cost Analysis

| Component | Phase 1 | Phase 2 | Phase 3 |
|-----------|---------|---------|---------|
| **Infrastructure** | $50-100/month | $200-500/month | $500-1000/month |
| **Complexity** | Low | Medium-High | High |
| **Team Size** | 1-2 DE | 2-3 DE + DS | 3-5 team |
| **Maintenance** | Low | Medium | High |

---

## ✅ Recommendation

### **For Recruitment Project:**

**Implement Phase 1 FULLY** with **Phase 2 structure prepared**

```
Priority:
1. ✅ Airflow orchestration (MUST HAVE)
2. ✅ dbt transformations (MUST HAVE)
3. ✅ ClickHouse data mart (MUST HAVE)
4. ✅ Superset dashboard (MUST HAVE)
5. 🔄 Kafka setup (NICE TO HAVE - structure only)
6. 🔄 Spark streaming (NICE TO HAVE - structure only)
```

**Presentasi Strategy:**
1. Demo Phase 1 (working batch pipeline)
2. Show Phase 2 architecture (future-proof design)
3. Explain migration path
4. Discuss trade-offs and decisions

**This shows:**
- ✅ Practical thinking (start simple)
- ✅ Future-proof architecture
- ✅ Understanding of scaling needs
- ✅ Cost-awareness

---

**Next Steps:**
1. Implement Airflow DAGs
2. Create dbt project
3. Setup ClickHouse schema
4. Build Superset dashboard
5. Prepare Kafka/Spark structure (optional demo)

---

**Last Updated:** April 7, 2026  
**Status:** Architecture Approved - Ready for Implementation
