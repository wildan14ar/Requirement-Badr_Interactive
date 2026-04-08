# Apache Superset Setup Guide

## Overview

This guide provides step-by-step instructions for setting up Apache Superset dashboard with ClickHouse data mart for the Badr Interactive project.

---

## 🚀 Quick Start

### Option 1: Using Docker Compose (Recommended)

```bash
# Start all services (ClickHouse + Superset + Redis)
docker compose up -d

# Check services status
docker compose ps

# View logs
docker compose logs -f superset
```

**Access Superset:**
- URL: http://localhost:8088
- Username: `admin`
- Password: `admin`

### Option 2: Manual Installation

```bash
# Install Superset
pip install apache-superset

# Initialize database
export FLASK_APP=superset
superset db upgrade

# Create admin user
superset fab create-admin

# Load default roles and permissions
superset init

# Start Superset
superset run -p 8088 --host 0.0.0.0
```

---

## 🔗 Connecting ClickHouse to Superset

### Step 1: Install ClickHouse Driver for Superset

```bash
# Inside Superset container or environment
pip install clickhouse-connect
# OR
pip install clickhouse-driver
```

### Step 2: Add Database Connection

1. **Login to Superset** → Go to **Settings** → **Database Connections**

2. **Click "+ Database"**

3. **Configure Connection:**
   - **Database:** ClickHouse
   - **Display Name:** `Badr Interactive Data Mart`
   - **SQLAlchemy URI:** 
     ```
     clickhouse://default:@localhost:8123/datamart_badr_interactive
     ```
   - **OR** (if using clickhouse-driver):
     ```
     clickhouse+native://default:@localhost:9000/datamart_badr_interactive
     ```

4. **Test Connection** → Should show "Connection looks good"

5. **Save**

### Step 3: Verify Tables

After adding the database:
1. Go to **SQL** → **SQL Lab**
2. Select "Badr Interactive Data Mart" database
3. Run query:
   ```sql
   SHOW TABLES
   ```
4. You should see:
   - dim_date
   - dim_material
   - dim_entity
   - dim_location
   - dim_activity
   - dim_entity_tag
   - fact_stock

---

## 📊 Creating Dashboard Charts

### Chart 1: Total Stock (Big Number)

**Purpose:** Display total stock quantity

**Steps:**
1. **Charts** → **+ Chart**
2. **Data Source:** Select `fact_stock`
3. **Chart Type:** Big Number
4. **Configuration:**
   - **Metric:** `SUM(stock_quantity)` → Rename to "Total Stock"
   - **Filters:** Add date range filter if needed
5. **Customize:**
   - **Title:** "Jumlah Stok"
   - **Font Size:** Large
   - **Color:** Blue
6. **Save** → Add to dashboard

**SQL Equivalent:**
```sql
SELECT 
    SUM(stock_quantity) AS total_stock
FROM fact_stock
WHERE date_key BETWEEN 20260101 AND 20260407
```

---

### Chart 2: Stock by Entity Tag (Bar Chart)

**Purpose:** Show stock distribution by entity type

**Steps:**
1. **Charts** → **+ Chart**
2. **Data Source:** Create dataset with this query:
   ```sql
   SELECT 
       det.tag_name,
       det.tag_category,
       SUM(fs.stock_quantity) AS total_stock,
       COUNT(DISTINCT fs.entity_key) AS entity_count
   FROM fact_stock fs
   INNER JOIN dim_entity_tag det ON fs.tag_key = det.tag_key
   WHERE fs.date_key >= toYYYYMMDD(today() - toIntervalDay(30), 'YYYYMMDD')
   GROUP BY det.tag_name, det.tag_category
   ORDER BY total_stock DESC
   ```
3. **Chart Type:** Bar Chart
4. **Configuration:**
   - **X Axis:** `tag_name`
   - **Y Axis:** `SUM(total_stock)`
   - **Group By:** `tag_category` (optional)
   - **Sort:** Descending by total_stock
5. **Customize:**
   - **Title:** "Stok per Tag Entitas"
   - **Colors:** Categorical palette
   - **Show Labels:** Yes
6. **Save** → Add to dashboard

---

### Chart 3: Stock by Material (Bar Chart)

**Purpose:** Show stock distribution per material

**Steps:**
1. **Charts** → **+ Chart**
2. **Dataset Query:**
   ```sql
   SELECT 
       dm.material_name,
       dm.category,
       dm.is_vaccine,
       SUM(fs.stock_quantity) AS total_stock,
       COUNT(DISTINCT fs.entity_key) AS entity_count
   FROM fact_stock fs
   INNER JOIN dim_material dm ON fs.material_key = dm.material_key
   WHERE fs.date_key >= toYYYYMMDD(today() - toIntervalDay(30), 'YYYYMMDD')
   GROUP BY dm.material_name, dm.category, dm.is_vaccine
   ORDER BY total_stock DESC
   LIMIT 20
   ```
3. **Chart Type:** Horizontal Bar Chart
4. **Configuration:**
   - **X Axis:** `SUM(total_stock)`
   - **Y Axis:** `material_name`
   - **Group By:** `category` (Vaccine vs Non-Vaccine)
   - **Limit:** 20 materials
5. **Customize:**
   - **Title:** "Stok per Material"
   - **Legend:** Show category
6. **Save** → Add to dashboard

---

### Chart 4: Stock Trend Over Time (Line Chart)

**Purpose:** Show stock trend over time

**Steps:**
1. **Dataset Query:**
   ```sql
   SELECT 
       dd.full_date,
       dd.year,
       dd.month,
       SUM(fs.stock_quantity) AS total_stock
   FROM fact_stock fs
   INNER JOIN dim_date dd ON fs.date_key = dd.date_key
   WHERE dd.full_date >= today() - toIntervalDay(90)
   GROUP BY dd.full_date, dd.year, dd.month
   ORDER BY dd.full_date ASC
   ```
2. **Chart Type:** Line Chart
3. **Configuration:**
   - **X Axis:** `full_date` (Time Series)
   - **Y Axis:** `SUM(total_stock)`
   - **Show Markers:** Yes
4. **Customize:**
   - **Title:** "Stock Trend (Last 90 Days)"
   - **Line Color:** Green
   - **Smooth Line:** Yes
5. **Save** → Add to dashboard

---

### Chart 5: Vaccine vs Non-Vaccine (Pie Chart)

**Purpose:** Compare vaccine and non-vaccine stocks

**Steps:**
1. **Dataset Query:**
   ```sql
   SELECT 
       dm.category,
       SUM(fs.stock_quantity) AS total_stock
   FROM fact_stock fs
   INNER JOIN dim_material dm ON fs.material_key = dm.material_key
   GROUP BY dm.category
   ```
2. **Chart Type:** Pie Chart
3. **Configuration:**
   - **Group By:** `category`
   - **Metric:** `SUM(total_stock)`
4. **Customize:**
   - **Title:** "Vaccine vs Non-Vaccine"
   - **Show Labels & Values:** Yes
   - **Colors:** Blue (Vaccine), Orange (Non-Vaccine)
5. **Save** → Add to dashboard

---

### Chart 6: Top 10 Entities by Stock (Table)

**Purpose:** Show top 10 entities with highest stock

**Steps:**
1. **Dataset Query:**
   ```sql
   SELECT 
       de.entity_name,
       de.entity_type,
       dl.province_name,
       dl.regency_name,
       SUM(fs.stock_quantity) AS total_stock,
       COUNT(DISTINCT fs.material_key) AS material_count
   FROM fact_stock fs
   INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
   INNER JOIN dim_location dl ON fs.location_key = dl.location_key
   WHERE fs.date_key >= toYYYYMMDD(today() - toIntervalDay(30), 'YYYYMMDD')
   GROUP BY de.entity_name, de.entity_type, dl.province_name, dl.regency_name
   ORDER BY total_stock DESC
   LIMIT 10
   ```
2. **Chart Type:** Table
3. **Configuration:**
   - **Columns:** entity_name, entity_type, province_name, regency_name, total_stock, material_count
   - **Sort:** Descending by total_stock
4. **Customize:**
   - **Title:** "Top 10 Entities by Stock"
   - **Pagination:** 10 rows per page
5. **Save** → Add to dashboard

---

## 🎨 Creating the Dashboard

### Step 1: Create Dashboard

1. **Dashboards** → **+ Dashboard**
2. **Title:** "Badr Interactive - Stock Dashboard"
3. **Description:** "Health logistics stock monitoring"

### Step 2: Add Charts

1. Click **"Edit Dashboard"**
2. **Add Chart** → Select created charts
3. **Arrange Layout:**

```
┌─────────────────────────────────────────────────────────┐
│  [Filter Bar]                                           │
│  Date Range | Province | Material Type | Entity Tag    │
└─────────────────────────────────────────────────────────┘

┌──────────────────┐ ┌──────────────────┐ ┌──────────────────┐
│  Jumlah Stok     │ │  Vaccine vs      │ │  Active Entities │
│  (Big Number)    │ │  Non-Vaccine     │ │  (Big Number)    │
│                  │ │  (Pie Chart)     │ │                  │
└──────────────────┘ └──────────────────┘ └──────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Stok per Tag Entitas (Bar Chart)                        │
│                                                          │
└──────────────────────────────────────────────────────────┘

┌─────────────────────────────────────┐ ┌──────────────────┐
│  Stok per Material (Bar Chart)      │ │  Top 10 Entities │
│                                     │ │  (Table)         │
│                                     │ │                  │
└─────────────────────────────────────┘ └──────────────────┘

┌──────────────────────────────────────────────────────────┐
│  Stock Trend Over Time (Line Chart)                      │
│                                                          │
└──────────────────────────────────────────────────────────┘
```

### Step 3: Add Filters

1. **Add Filter** → **Native Filter**
2. **Configure Filters:**

#### Filter 1: Date Range
- **Filter Type:** Date Range
- **Target Charts:** All charts
- **Default:** Last 30 days
- **Column:** `date_key` (need to convert)

#### Filter 2: Province
- **Filter Type:** Select
- **Dataset Query:**
  ```sql
  SELECT DISTINCT province_id, province_name 
  FROM dim_location 
  ORDER BY province_name
  ```
- **Target Charts:** All location-based charts

#### Filter 3: Material Type
- **Filter Type:** Select
- **Options:** Vaccine, Non-Vaccine
- **Target Charts:** Material-related charts

#### Filter 4: Entity Tag
- **Filter Type:** Multi-select
- **Dataset Query:**
  ```sql
  SELECT DISTINCT tag_key, tag_name 
  FROM dim_entity_tag 
  ORDER BY tag_name
  ```
- **Target Charts:** Entity tag chart

### Step 4: Save & Publish

1. **Save Dashboard**
2. **Set Permissions:**
   - Make public if needed (Settings → Public)
   - Or restrict to specific roles
3. **Publish Dashboard**

---

## 🔧 Advanced Configuration

### Enable Caching (Redis)

In `superset_config.py`:

```python
from datetime import timedelta

CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': timedelta(hours=1),
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 0,
    'CACHE_REDIS_URL': 'redis://redis:6379/0'
}

DATA_CACHE_CONFIG = CACHE_CONFIG
```

### Custom ClickHouse Dialect

If you encounter SQL syntax issues, register custom ClickHouse dialect:

```python
# In superset_config.py
from clickhouse_connect.dbapi import connect as clickhouse_connect

def get_clickhouse_engine():
    from sqlalchemy import create_engine
    return create_engine(
        'clickhouse://default:@localhost:9000/datamart_badr_interactive',
        pool_pre_ping=True
    )
```

### Dashboard JSON Export

To backup or share dashboard:

1. **Dashboard** → **Export Dashboard**
2. Save as JSON file
3. Import later: **Dashboards** → **Import**

---

## 📈 Dashboard Usage Tips

### For End Users

1. **Explore Data:**
   - Use **SQL Lab** for ad-hoc queries
   - Use **Charts** → **Explore** for no-code analysis

2. **Filter Data:**
   - Use filter bar at top
   - Filters apply to all linked charts

3. **Export Data:**
   - Click chart menu (⋮) → **Download as CSV**

4. **Share Dashboard:**
   - Click **Share** → Copy link
   - Or export as PDF/PNG

### For Developers

1. **Embed Dashboard:**
   ```html
   <iframe 
     src="http://localhost:8088/superset/dashboard/1/"
     width="100%" 
     height="800"
     frameborder="0">
   </iframe>
   ```

2. **API Access:**
   ```bash
   # Get dashboard data via API
   curl -X GET "http://localhost:8088/api/v1/dashboard/1" \
     -H "Authorization: Bearer YOUR_TOKEN"
   ```

---

## 🐛 Troubleshooting

### Issue: Cannot connect to ClickHouse

**Solution:**
```bash
# Check ClickHouse is running
docker ps | grep clickhouse

# Test connection
curl http://localhost:8123/

# Check logs
docker logs clickhouse-badr
```

### Issue: SQL syntax errors in Superset

**Solution:**
- ClickHouse uses different SQL syntax than MySQL
- Use `toYYYYMMDD()` instead of `DATE_FORMAT()`
- Use `count()` instead of `COUNT(*)`
- Refer to ClickHouse SQL documentation

### Issue: Slow query performance

**Solution:**
1. Check query execution plan:
   ```sql
   EXPLAIN SELECT ... FROM fact_stock
   ```

2. Verify indexes (ORDER BY keys) are being used

3. Add caching (Redis)

4. Optimize queries to leverage columnar storage

---

## 📚 Resources

- **Superset Docs:** https://superset.apache.org/
- **ClickHouse Docs:** https://clickhouse.com/docs/
- **ClickHouse Superset Integration:** https://clickhouse.com/docs/integrations/superset
- **SQL Lab Guide:** https://superset.apache.org/docs/using-superset/sql-lab

---

**Last Updated:** April 7, 2026  
**Author:** Data Engineer Candidate
