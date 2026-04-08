# 📊 Pipeline Strategy: Batch vs Real-time

## Pemahaman Kebutuhan Data

Tidak semua data di sistem logistik kesehatan butuh real-time. Mari kita **pisahkan berdasarkan use case**.

---

## 🎯 Klasifikasi Data

### ✅ **Data yang TIDAK Butuh Real-time (Batch)**

| Data | Update Frequency | Alasan |
|------|------------------|--------|
| **Stock levels** | 4x/day | Stock tidak berubah setiap detik |
| **Master materials** | 1x/day | Data master jarang berubah |
| **Entities (facilities)** | 1x/day | Data fasilitas stabil |
| **Provinces & Regencies** | 1x/week | Data wilayah hampir tidak pernah berubah |
| **Historical reports** | 1x/day | Report based on historical data |

**Pipeline:** Airflow + dbt + ClickHouse

**Schedule:** 
- Stock: Every 6 hours (00:00, 06:00, 12:00, 18:00)
- Master data: Daily (01:00)

---

### ⚡ **Data yang Butuh Real-time / Near Real-time**

| Data | Update Frequency | Alasan |
|------|------------------|--------|
| **Stock alerts (critical level)** | < 5 minutes | Perlu alert segera saat stock habis |
| **Cold storage temperature** | < 1 minute | Temperature monitoring untuk vaksin |
| **Emergency distribution** | < 5 minutes | Distribusi urgent (wabah/outbreak) |
| **Transaction events** | < 1 minute | Audit trail lengkap |

**Pipeline:** Kafka + Spark Streaming + ClickHouse

**Latency:** 1-5 minutes

---

## 🏗️ Architecture (Simple & Practical)

```
┌──────────────────────────────────────────────────────────────┐
│                    SOURCE: MySQL OLTP                        │
│                  recruitment_dev @ 10.10.0.30                │
└────────────────────────┬─────────────────────────────────────┘
                         │
                    ┌────┴────┐
                    │         │
        Data tidak  │         │  Data butuh real-time
        real-time   │         │  (alerts, temperature)
                    │         │
                    ▼         ▼
        ╔══════════════╗  ╔══════════════════════╗
        ║  BATCH PATH  ║  ║   STREAMING PATH     ║
        ║  (Primary)   ║  ║   (Only if needed)   ║
        ╚══════╤═══════╝  ╚══════════╤═══════════╝
               │                      │
               │                      │
               ▼                      ▼
    ┌──────────────────┐    ┌────────────────────┐
    │   Apache Airflow │    │   Apache Kafka     │
    │   (Scheduler)    │    │   (Event Queue)    │
    │   4x/day         │    │                    │
    └────────┬─────────┘    └────────┬───────────┘
             │                       │
             ▼                       ▼
    ┌──────────────────┐    ┌────────────────────┐
    │       dbt        │    │   Spark Streaming  │
    │   (Transform)    │    │   (Process events) │
    └────────┬─────────┘    └────────┬───────────┘
             │                       │
             └───────────┬───────────┘
                         ▼
              ┌────────────────────┐
              │    ClickHouse      │
              │   (Data Mart)      │
              │                    │
              │  • fact_stock      │
              │  • fact_stock_rt   │
              │  • dim_* tables    │
              └─────────┬──────────┘
                        │
                        ▼
              ┌────────────────────┐
              │   Apache Superset  │
              │   (Dashboard)      │
              └────────────────────┘
```

---

## 💡 Rekomendasi: Kapan Pakai Yang Mana?

### Decision Tree

```
Apakah data perlu di-update dalam hitungan menit?
│
├─ ❌ NO → Pakai BATCH (Airflow + dbt)
│   └─ Contoh: Dashboard stock, reports, master data
│
└─ ✅ YES → Apakah data volumenya tinggi (>100 events/min)?
    │
    ├─ ❌ NO → Pakai POLLING (Python script every 1-5 min)
    │   └─ Contoh: Stock alerts, simple monitoring
    │
    └─ ✅ YES → Pakai STREAMING (Kafka + Spark)
        └─ Contoh: IoT sensors, high-frequency transactions
```

---

## 📋 Use Cases di Sistem Logistik Kesehatan

### 1️⃣ **Dashboard Stock (BATCH)**

**Requirement:**
- Tampilkan total stock per material
- Filter: date range, province, entity type
- Update: 4x per hari cukup

**Pipeline:** Airflow + dbt

```
Schedule: 0 0,6,12,18 * * *

Airflow DAG:
  1. Extract dari MySQL (stocks, batches, materials, entities)
  2. Load ke staging tables di ClickHouse
  3. dbt transform: staging → dimensions → fact_stock
  4. Validate data quality
  5. Refresh Superset dashboard
```

**Kenapa Batch:**
- ✅ Stock tidak berubah setiap detik
- ✅ User cek dashboard periodik (beberapa kali sehari)
- ✅ Simple, mudah di-maintain
- ✅ Cost rendah

---

### 2️⃣ **Stock Alert - Critical Level (NEAR REAL-TIME)**

**Requirement:**
- Alert ketika stock material tertentu mencapai level kritis
- Notification via email/WhatsApp/Slack
- Response time: < 5 minutes

**Pipeline Option A: Simple Polling** (Recommended)

```
Python Script (every 2 minutes):
  1. Query MySQL: SELECT qty FROM stocks WHERE qty < threshold
  2. If critical stock detected:
     → Send alert (email/WhatsApp/Slack)
     → Log to ClickHouse for tracking
```

**Pipeline Option B: Kafka + Spark** (Only if high volume)

```
MySQL CDC (Debezium) → Kafka → Spark Streaming → Alert
```

**Kenapa Polling Cukup:**
- ✅ Simple implementation
- ✅ 2-minute latency acceptable
- ✅ No need complex infrastructure
- ✅ Easy to debug

**Kafka/Spark hanya jika:**
- Ada 1000+ facilities sending updates per minute
- Butuh exact real-time (sub-minute)

---

### 3️⃣ **Cold Storage Temperature Monitoring (REAL-TIME)**

**Requirement:**
- Monitor temperature cold storage (vaksin)
- Alert jika temperature di luar range
- Update: setiap 10-30 detik dari IoT sensors

**Pipeline:** Kafka + Spark Streaming

```
IoT Sensors → MQTT → Kafka → Spark Streaming → ClickHouse
                                        │
                                        └→ Alert if temp out of range
```

**Kenapa Real-time:**
- ✅ Vaksin rusak jika temperature salah
- ✅ Perlu alert segera (< 1 minute)
- ✅ High-frequency data (setiap 10-30 detik)

---

### 4️⃣ **Emergency Distribution (NEAR REAL-TIME)**

**Requirement:**
- Track distribusi urgent (wabah/outbreak)
- Monitor stock movement in near real-time
- Update: setiap 1-5 minutes

**Pipeline:** Simple REST API + Polling

```
Distribution App → POST /api/stock-movement
                        │
                        └→ Insert to MySQL
                              │
                              └→ Polling script (every 1 min)
                                    │
                                    └→ Update ClickHouse
```

**Kenapa Tidak Kafka:**
- ✅ Volume rendah (beberapa transaksi per menit)
- ✅ Simple REST API cukup
- ✅ Polling 1 minute acceptable

---

## 🛠️ Implementation Priority

### Phase 1: BATCH (MUST HAVE) ⭐⭐⭐⭐⭐

**Implement first:**
- Airflow DAGs untuk stock ETL (4x/day)
- dbt models untuk transformations
- ClickHouse data mart
- Superset dashboard

**Timeline:** 1-2 minggu

**Code:** Sudah ada structure, tinggal implement

---

### Phase 2: NEAR REAL-TIME (NICE TO HAVE) ⭐⭐⭐

**Implement when needed:**
- Stock alert system (polling every 2 min)
- Simple monitoring dashboard
- Alert notifications

**Timeline:** 1 minggu

**Code:** Python script sederhana

```python
# stock_alert_checker.py
import schedule
import time
from datetime import datetime

def check_critical_stock():
    """Check stock levels every 2 minutes"""
    # Query MySQL for critical stock
    critical_items = query_critical_stock()
    
    if critical_items:
        send_alert(critical_items)
        log_to_clickhouse(critical_items)

# Run every 2 minutes
schedule.every(2).minutes.do(check_critical_stock)

while True:
    schedule.run_pending()
    time.sleep(1)
```

---

### Phase 3: REAL-TIME (FUTURE - ONLY IF NEEDED) ⭐

**Implement only if:**
- Ada IoT sensors (cold storage)
- High-frequency transactions
- Real-time tracking requirement

**Timeline:** 2-4 minggu

**Code:** Kafka + Spark setup

---

## 📊 Comparison Table

| Aspect | BATCH (Airflow+dbt) | NEAR RT (Polling) | REAL-TIME (Kafka+Spark) |
|--------|---------------------|-------------------|-------------------------|
| **Latency** | 6 hours | 1-5 minutes | < 1 minute |
| **Complexity** | Low-Medium | Low | High |
| **Setup Time** | 1-2 days | 2-4 hours | 1-2 weeks |
| **Infrastructure** | Airflow + ClickHouse | Python script | Kafka + Spark + ZooKeeper |
| **Maintenance** | Low | Very Low | High |
| **Cost** | $50/month | $10/month | $300+/month |
| **Best For** | Dashboard, reports | Alerts, monitoring | IoT, high-frequency |

---

## ✅ Rekomendasi Final untuk Project Ini

### **Untuk Recruitment Dashboard:**

#### Use **BATCH ONLY** (Phase 1)

```
Airflow (4x/day) → dbt → ClickHouse → Superset
```

**Kenapa:**
1. ✅ Requirement tidak butuh real-time
2. ✅ Simple dan mudah di-presentasikan
3. ✅ Cost minimal
4. ✅ Easy to maintain
5. ✅ Sufficient untuk use case

**Presentasi Points:**
- "Kami memulai dengan batch processing karena data stock tidak berubah real-time"
- "Update 4x sehari sudah cukup untuk decision making"
- "Architecture bisa di-upgrade ke near real-time jika perlu di masa depan"

---

### **Future Enhancement (Jika Ada Requirement Tambahan):**

#### Add Near Real-time untuk Alerts

```
Polling Script (every 2 min) → Alert System
                                       │
                                       └→ Same ClickHouse for tracking
```

**Presentasi Points:**
- "Jika perlu alert system, kami bisa tambah polling script sederhana"
- "Tidak perlu Kafka/Spark karena volume rendah"
- "Keep it simple, solve the problem"

---

## 🎯 Summary

| Data Type | Update | Pipeline | Status |
|-----------|--------|----------|--------|
| Dashboard Stock | 4x/day | Airflow + dbt | ✅ Implement First |
| Master Data | 1x/day | Airflow + dbt | ✅ Implement First |
| Stock Alerts | 2 min | Polling | 🔄 Phase 2 (if needed) |
| Cold Storage IoT | 30 sec | Kafka + Spark | ❌ Future only |

---

## 💬 Kesimpulan

**"Start simple, scale when needed"**

1. **Mulai dengan BATCH** (Airflow + dbt) → cover 90% use cases
2. **Tambah NEAR RT** (Polling) → jika perlu alerts
3. **Upgrade ke REAL-TIME** (Kafka + Spark) → HANYA jika benar-benar perlu

**Jangan over-engineer dari awal!**

---

**Last Updated:** April 7, 2026  
**Recommendation:** Start with BATCH (Phase 1), add streaming only when proven necessary
