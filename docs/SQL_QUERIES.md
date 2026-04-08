# SQL Queries Documentation

## Overview

This document provides comprehensive documentation for all SQL queries used in the Badr Interactive Data Mart dashboard. Each query includes purpose, usage, parameters, and optimization notes.

---

## Table of Contents

1. [Dashboard Queries](#dashboard-queries)
   - [Query 1: Total Stock (Jumlah Stok)](#query-1-total-stock-jumlah-stok)
   - [Query 2: Stock by Entity Tag (Stok per Tag Entitas)](#query-2-stock-by-entity-tag-stok-per-tag-entitas)
   - [Query 3: Stock by Material (Stok per Material)](#query-3-stock-by-material-stok-per-material)
   - [Query 4: Stock by Location (Stok per Lokasi)](#query-4-stock-by-location-stok-per-lokasi)
   - [Query 5: Stock by Activity (Stok per Kegiatan)](#query-5-stock-by-activity-stok-per-kegiatan)
   - [Query 6: Stock Trend Over Time](#query-6-stock-trend-over-time)
   - [Query 7: Stock Summary by Entity](#query-7-stock-summary-by-entity)
   - [Query 8: Vaccine vs Non-Vaccine Summary](#query-8-vaccine-vs-non-vaccine-summary)
   - [Query 9: Top 10 Materials by Stock](#query-9-top-10-materials-by-stock)
   - [Query 10: Top 10 Entities by Stock](#query-10-top-10-entities-by-stock)

2. [Filter Queries](#filter-queries)
   - [Filter 1: Get Materials](#filter-1-get-materials-for-dropdown)
   - [Filter 2: Get Entity Tags](#filter-2-get-entity-tags-for-dropdown)
   - [Filter 3: Get Provinces](#filter-3-get-provinces-for-dropdown)
   - [Filter 4: Get Regencies](#filter-4-get-regenciescities-for-dropdown)
   - [Filter 5: Get Activities](#filter-5-get-activities-for-dropdown)
   - [Filter 6: Get Date Range](#filter-6-get-date-range-min-and-max-dates)
   - [Filter 7: Get Material Types](#filter-7-get-material-types-vaccinenon-vaccine)
   - [Filter 8: Get Entity Types](#filter-8-get-entity-types-for-dropdown)
   - [Filter 9: Hierarchical Location](#filter-9-hierarchical-location-province--regency--entity)
   - [Filter 10: Get Entities by Location](#filter-10-get-entities-by-location-for-cascading-dropdown)
   - [Filter 11: Get Years Available](#filter-11-get-years-available-in-data)
   - [Filter 12: Get Material by Category](#filter-12-get-material-by-category-cascading)

3. [Query Optimization](#query-optimization)
4. [Query Performance Testing](#query-performance-testing)

---

## Dashboard Queries

### Query 1: Total Stock (Jumlah Stok)

**Purpose:** Calculate total stock quantity across all materials with applied filters

**Dashboard Component:** "Jumlah Stok" card/widget

**SQL:**
```sql
SELECT 
    COALESCE(SUM(fs.stock_quantity), 0) AS total_stock,
    COUNT(DISTINCT fs.material_key) AS material_count,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    COUNT(DISTINCT fs.batch_id) AS batch_count
FROM fact_stock fs
WHERE 1=1
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter (Vaccine/Non-Vaccine)
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Specific material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key);
```

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| date_from | INT | Yes | Start date key (YYYYMMDD format) |
| date_to | INT | Yes | End date key (YYYYMMDD format) |
| material_type | VARCHAR | No | 'Vaccine' or 'Non-Vaccine' |
| material_key | INT | No | Specific material ID |
| tag_key | INT | No | Entity tag ID |
| province_id | VARCHAR | No | Province ID |
| regency_id | VARCHAR | No | Regency/City ID |
| activity_key | INT | No | Activity ID |

**Returns:**
- `total_stock`: Total quantity (DECIMAL)
- `material_count`: Number of unique materials (INT)
- `entity_count`: Number of unique entities (INT)
- `batch_count`: Number of unique batches (INT)

**Example Output:**
```
total_stock  | material_count | entity_count | batch_count
-------------|----------------|--------------|------------
150000.50    | 45             | 234          | 567
```

**Performance Notes:**
- Uses indexes on `date_key`, `material_key`, `entity_key`
- COALESCE handles empty result sets
- Query time: < 500ms with proper indexes

**Usage Example (Python):**
```python
query = text("""
    SELECT COALESCE(SUM(fs.stock_quantity), 0) AS total_stock
    FROM fact_stock fs
    WHERE fs.date_key BETWEEN :date_from AND :date_to
""")

result = conn.execute(query, {
    'date_from': 20260101,
    'date_to': 20260407
})

total_stock = result.scalar()
```

---

### Query 2: Stock by Entity Tag (Stok per Tag Entitas)

**Purpose:** Show stock distribution by entity type (Puskesmas, Dinkes, etc.)

**Dashboard Component:** Bar chart or pie chart showing stock per entity tag

**SQL:**
```sql
SELECT 
    det.tag_key,
    det.tag_name,
    det.tag_category,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock,
    AVG(fs.stock_quantity) AS avg_stock_per_entity,
    MIN(fs.stock_quantity) AS min_stock,
    MAX(fs.stock_quantity) AS max_stock,
    ROUND(
        SUM(fs.stock_quantity) * 100.0 / (
            SELECT SUM(stock_quantity) 
            FROM fact_stock 
            WHERE date_key BETWEEN :date_from AND :date_to
        ), 
        2
    ) AS percentage_of_total
FROM fact_stock fs
INNER JOIN dim_entity_tag det ON fs.tag_key = det.tag_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    AND (:entity_type IS NULL OR de.entity_type = :entity_type)
GROUP BY det.tag_key, det.tag_name, det.tag_category
ORDER BY total_stock DESC;
```

**Parameters:** Same as Query 1, plus:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| entity_type | VARCHAR | No | Entity type description |

**Returns:**

| Column | Type | Description |
|--------|------|-------------|
| tag_key | INT | Tag ID |
| tag_name | VARCHAR | Tag name |
| tag_category | VARCHAR | Tag category |
| entity_count | INT | Number of entities with this tag |
| total_stock | DECIMAL | Total stock for this tag |
| avg_stock_per_entity | DECIMAL | Average stock per entity |
| min_stock | DECIMAL | Minimum stock |
| max_stock | DECIMAL | Maximum stock |
| percentage_of_total | DECIMAL | Percentage of total stock |

**Example Output:**
```
tag_key | tag_name               | tag_category          | entity_count | total_stock  | percentage
--------|------------------------|-----------------------|--------------|--------------|-----------
3       | Puskesmas              | Puskesmas             | 150          | 75000.00     | 50.00
1       | Dinkes Provinsi        | Dinas Kesehatan Prov. | 34           | 45000.00     | 30.00
2       | Dinkes Kabupaten       | Dinas Kesehatan Kab.  | 50           | 30000.00     | 20.00
```

**Performance Notes:**
- Subquery for percentage calculation may be slow
- Consider caching total_stock value
- Query time: < 1 second

**Optimization Tip:**
```sql
-- Alternative: Calculate percentage in application layer
-- Remove subquery and calculate in Python/JavaScript
```

---

### Query 3: Stock by Material (Stok per Material)

**Purpose:** Show stock distribution for each material

**Dashboard Component:** Bar chart or table showing stock per material

**SQL:**
```sql
SELECT 
    dm.material_key,
    dm.material_code,
    dm.material_name,
    dm.material_type,
    dm.category,
    dm.is_vaccine,
    dm.unit,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock,
    AVG(fs.stock_quantity) AS avg_stock_per_entity,
    MIN(fs.stock_quantity) AS min_stock,
    MAX(fs.stock_quantity) AS max_stock,
    ROUND(
        SUM(fs.stock_quantity) * 100.0 / (
            SELECT SUM(stock_quantity) 
            FROM fact_stock 
            WHERE date_key BETWEEN :date_from AND :date_to
        ), 
        2
    ) AS percentage_of_total
FROM fact_stock fs
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.material_key, dm.material_code, dm.material_name, 
         dm.material_type, dm.category, dm.is_vaccine, dm.unit
ORDER BY total_stock DESC;
```

**Returns:** Similar to Query 2, with material-specific fields

**Example Output:**
```
material_key | material_name | category    | entity_count | total_stock  | percentage
-------------|---------------|-------------|--------------|--------------|-----------
15           | PCV           | Vaccine     | 120          | 50000.00     | 33.33
22           | Masker        | Non-Vaccine | 200          | 40000.00     | 26.67
8            | BCG           | Vaccine     | 95           | 35000.00     | 23.33
```

**Use Case:** Identify which materials have the highest stock levels

---

### Query 4: Stock by Location (Stok per Lokasi)

**Purpose:** Show stock distribution by geographic location

**Dashboard Component:** Map visualization or hierarchical table

**SQL:**
```sql
SELECT 
    dl.province_id,
    dl.province_name,
    dl.regency_id,
    dl.regency_name,
    dl.full_location,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    COUNT(DISTINCT fs.material_key) AS material_count,
    SUM(fs.stock_quantity) AS total_stock,
    AVG(fs.stock_quantity) AS avg_stock_per_entity
FROM fact_stock fs
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dl.location_key, dl.province_id, dl.province_name, 
         dl.regency_id, dl.regency_name, dl.full_location
ORDER BY total_stock DESC;
```

**Use Case:** Geographic analysis of stock distribution

**Visualization:** Can be used with map libraries (Leaflet, Google Maps)

---

### Query 5: Stock by Activity (Stok per Kegiatan)

**Purpose:** Show stock distribution by activity/program type

**Dashboard Component:** Bar chart showing stock per activity

**SQL:**
```sql
SELECT 
    da.activity_key,
    da.activity_code,
    da.activity_name,
    da.activity_type,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    COUNT(DISTINCT fs.material_key) AS material_count,
    SUM(fs.stock_quantity) AS total_stock,
    AVG(fs.stock_quantity) AS avg_stock_per_activity
FROM fact_stock fs
INNER JOIN dim_activity da ON fs.activity_key = da.activity_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND da.activity_key IS NOT NULL
GROUP BY da.activity_key, da.activity_code, da.activity_name, da.activity_type
ORDER BY total_stock DESC;
```

**Note:** Only includes records with activity associations

---

### Query 6: Stock Trend Over Time

**Purpose:** Show stock trend over time for time-series visualization

**Dashboard Component:** Line chart or area chart showing stock trends

**SQL:**
```sql
SELECT 
    dd.full_date,
    dd.date_key,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.day,
    SUM(fs.stock_quantity) AS total_stock,
    COUNT(DISTINCT fs.material_key) AS material_count,
    COUNT(DISTINCT fs.entity_key) AS entity_count
FROM fact_stock fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dd.date_key, dd.full_date, dd.year, dd.quarter, dd.month, dd.month_name, dd.day
ORDER BY dd.full_date ASC;
```

**Use Case:** 
- Identify stock trends over time
- Seasonal pattern analysis
- Forecasting

**Visualization:** Line chart with date on X-axis, stock quantity on Y-axis

---

### Query 7: Stock Summary by Entity

**Purpose:** Show detailed stock per entity

**Dashboard Component:** Detailed table with entity-level breakdown

**SQL:**
```sql
SELECT 
    de.entity_key,
    de.entity_id,
    de.entity_code,
    de.entity_name,
    de.entity_type,
    de.is_puskesmas,
    dl.province_name,
    dl.regency_name,
    dl.full_location,
    COUNT(DISTINCT fs.material_key) AS material_count,
    COUNT(DISTINCT fs.batch_id) AS batch_count,
    SUM(fs.stock_quantity) AS total_stock,
    SUM(fs.allocated) AS total_allocated,
    SUM(fs.in_transit) AS total_in_transit,
    MIN(fs.stock_quantity) AS min_stock,
    MAX(fs.stock_quantity) AS max_stock,
    AVG(fs.stock_quantity) AS avg_stock
FROM fact_stock fs
INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    AND de.is_active = TRUE
GROUP BY de.entity_key, de.entity_id, de.entity_code, de.entity_name,
         de.entity_type, de.is_puskesmas, dl.province_name, 
         dl.regency_name, dl.full_location
ORDER BY total_stock DESC;
```

**Use Case:** Entity-level detailed analysis

---

### Query 8: Vaccine vs Non-Vaccine Summary

**Purpose:** Compare vaccine and non-vaccine stocks

**Dashboard Component:** Pie chart or comparison cards

**SQL:**
```sql
SELECT 
    dm.category,
    dm.is_vaccine,
    COUNT(DISTINCT dm.material_key) AS material_count,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    COUNT(DISTINCT fs.batch_id) AS batch_count,
    SUM(fs.stock_quantity) AS total_stock,
    AVG(fs.stock_quantity) AS avg_stock,
    MIN(fs.stock_quantity) AS min_stock,
    MAX(fs.stock_quantity) AS max_stock,
    ROUND(
        SUM(fs.stock_quantity) * 100.0 / (
            SELECT SUM(stock_quantity) 
            FROM fact_stock 
            WHERE date_key BETWEEN :date_from AND :date_to
        ), 
        2
    ) AS percentage_of_total
FROM fact_stock fs
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.category, dm.is_vaccine
ORDER BY total_stock DESC;
```

**Expected Output:** 2 rows (Vaccine and Non-Vaccine)

---

### Query 9: Top 10 Materials by Stock

**Purpose:** Show top 10 materials with highest stock

**Dashboard Component:** Top 10 list or ranking table

**SQL:**
```sql
SELECT 
    dm.material_key,
    dm.material_code,
    dm.material_name,
    dm.category,
    dm.is_vaccine,
    dm.unit,
    COUNT(DISTINCT fs.entity_key) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock,
    SUM(fs.allocated) AS total_allocated,
    SUM(fs.in_transit) AS total_in_transit
FROM fact_stock fs
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.material_key, dm.material_code, dm.material_name,
         dm.category, dm.is_vaccine, dm.unit
ORDER BY total_stock DESC
LIMIT 10;
```

**Use Case:** Quick view of highest-stock materials

---

### Query 10: Top 10 Entities by Stock

**Purpose:** Show top 10 entities with highest stock

**Dashboard Component:** Top 10 list or ranking table

**SQL:**
```sql
SELECT 
    de.entity_key,
    de.entity_id,
    de.entity_code,
    de.entity_name,
    de.entity_type,
    dl.province_name,
    dl.regency_name,
    dl.full_location,
    COUNT(DISTINCT fs.material_key) AS material_count,
    SUM(fs.stock_quantity) AS total_stock,
    SUM(fs.allocated) AS total_allocated,
    SUM(fs.in_transit) AS total_in_transit
FROM fact_stock fs
INNER JOIN dim_entity de ON fs.entity_key = de.entity_key
INNER JOIN dim_location dl ON fs.location_key = dl.location_key
INNER JOIN dim_material dm ON fs.material_key = dm.material_key
WHERE 1=1
    AND fs.date_key BETWEEN :date_from AND :date_to
    AND (:material_type IS NULL OR dm.category = :material_type)
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    AND de.is_active = TRUE
GROUP BY de.entity_key, de.entity_id, de.entity_code, de.entity_name,
         de.entity_type, dl.province_name, dl.regency_name, dl.full_location
ORDER BY total_stock DESC
LIMIT 10;
```

---

## Filter Queries

### Filter 1: Get Materials for Dropdown

**Purpose:** Populate material filter dropdown

**SQL:**
```sql
SELECT DISTINCT
    dm.material_key,
    dm.material_code,
    dm.material_name,
    dm.category,
    dm.is_vaccine,
    dm.unit
FROM dim_material dm
INNER JOIN fact_stock fs ON dm.material_key = fs.material_key
WHERE dm.is_active = TRUE
ORDER BY dm.material_name;
```

**Use Case:** Dashboard filter dropdown

---

### Filter 2: Get Entity Tags for Dropdown

**Purpose:** Populate entity tag filter dropdown

**SQL:**
```sql
SELECT DISTINCT
    det.tag_key,
    det.tag_name,
    det.tag_category
FROM dim_entity_tag det
INNER JOIN fact_stock fs ON det.tag_key = fs.tag_key
WHERE det.is_active = TRUE
ORDER BY det.tag_name;
```

---

### Filter 3: Get Provinces for Dropdown

**Purpose:** Populate province filter dropdown

**SQL:**
```sql
SELECT DISTINCT
    dl.province_id,
    dl.province_name
FROM dim_location dl
INNER JOIN fact_stock fs ON dl.location_key = fs.location_key
WHERE dl.province_id IS NOT NULL
ORDER BY dl.province_name;
```

---

### Filter 4: Get Regencies/Cities for Dropdown

**Purpose:** Populate regency/city filter dropdown (can be filtered by province)

**SQL:**
```sql
SELECT DISTINCT
    dl.regency_id,
    dl.regency_name,
    dl.province_id,
    dl.province_name
FROM dim_location dl
INNER JOIN fact_stock fs ON dl.location_key = fs.location_key
WHERE dl.regency_id IS NOT NULL
    AND (:province_id IS NULL OR dl.province_id = :province_id)
ORDER BY dl.province_name, dl.regency_name;
```

**Cascading Behavior:** When province is selected, only show regencies in that province

---

### Filter 5: Get Activities for Dropdown

**Purpose:** Populate activity filter dropdown

**SQL:**
```sql
SELECT DISTINCT
    da.activity_key,
    da.activity_code,
    da.activity_name,
    da.activity_type
FROM dim_activity da
INNER JOIN fact_stock fs ON da.activity_key = fs.activity_key
WHERE da.is_active = TRUE
    AND da.activity_key IS NOT NULL
ORDER BY da.activity_name;
```

---

### Filter 6: Get Date Range (Min and Max dates)

**Purpose:** Determine available date range for date picker

**SQL:**
```sql
SELECT 
    MIN(dd.full_date) AS min_date,
    MAX(dd.full_date) AS max_date,
    MIN(fs.date_key) AS min_date_key,
    MAX(fs.date_key) AS max_date_key
FROM fact_stock fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key;
```

**Use Case:** Set default date range in date picker

---

### Filter 7: Get Material Types (Vaccine/Non-Vaccine)

**Purpose:** Populate material type filter

**SQL:**
```sql
SELECT DISTINCT
    dm.category,
    dm.is_vaccine,
    COUNT(DISTINCT dm.material_key) AS material_count,
    SUM(fs.stock_quantity) AS total_stock
FROM dim_material dm
INNER JOIN fact_stock fs ON dm.material_key = fs.material_key
WHERE dm.is_active = TRUE
GROUP BY dm.category, dm.is_vaccine
ORDER BY dm.category;
```

---

### Filter 8: Get Entity Types for Dropdown

**Purpose:** Populate entity type filter

**SQL:**
```sql
SELECT DISTINCT
    de.entity_type,
    COUNT(DISTINCT de.entity_key) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock
FROM dim_entity de
INNER JOIN fact_stock fs ON de.entity_key = fs.entity_key
WHERE de.is_active = TRUE
GROUP BY de.entity_type
ORDER BY de.entity_type;
```

---

### Filter 9: Hierarchical Location (Province → Regency → Entity)

**Purpose:** Get all locations with hierarchy for tree view

**SQL:**
```sql
SELECT 
    dl.province_id,
    dl.province_name,
    dl.regency_id,
    dl.regency_name,
    dl.full_location
FROM dim_location dl
WHERE dl.province_id IS NOT NULL
ORDER BY dl.province_name, dl.regency_name;
```

---

### Filter 10: Get Entities by Location (Cascading)

**Purpose:** Populate entity dropdown based on selected location

**SQL:**
```sql
SELECT DISTINCT
    de.entity_key,
    de.entity_id,
    de.entity_code,
    de.entity_name,
    de.entity_type,
    dl.province_id,
    dl.province_name,
    dl.regency_id,
    dl.regency_name
FROM dim_entity de
INNER JOIN dim_location dl ON de.province_id = dl.province_id 
    AND de.regency_id = dl.regency_id
INNER JOIN fact_stock fs ON de.entity_key = fs.entity_key
WHERE de.is_active = TRUE
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
ORDER BY de.entity_name;
```

---

### Filter 11: Get Years Available in Data

**Purpose:** Populate year filter

**SQL:**
```sql
SELECT DISTINCT
    dd.year,
    COUNT(DISTINCT fs.stock_key) AS stock_records,
    SUM(fs.stock_quantity) AS total_stock
FROM fact_stock fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year
ORDER BY dd.year DESC;
```

---

### Filter 12: Get Material by Category (Cascading)

**Purpose:** Populate material dropdown filtered by category

**SQL:**
```sql
SELECT DISTINCT
    dm.material_key,
    dm.material_code,
    dm.material_name,
    dm.category,
    dm.is_vaccine,
    dm.unit
FROM dim_material dm
INNER JOIN fact_stock fs ON dm.material_key = fs.material_key
WHERE dm.is_active = TRUE
    AND (:category IS NULL OR dm.category = :category)
ORDER BY dm.material_name;
```

---

## Query Optimization

### General Optimization Techniques

1. **Use EXPLAIN to Analyze Queries**
   ```sql
   EXPLAIN SELECT ... FROM fact_stock fs WHERE ...
   ```
   - Check if indexes are being used
   - Identify full table scans
   - Optimize based on execution plan

2. **Avoid SELECT ***
   - Only select required columns
   - Reduces I/O and memory usage

3. **Use Indexed Columns in WHERE Clauses**
   - `date_key`, `material_key`, `entity_key` are indexed
   - Avoid functions on indexed columns (e.g., `YEAR(full_date)`)

4. **Limit Result Sets**
   - Use `LIMIT` for top-N queries
   - Use pagination for large result sets

5. **Avoid Subqueries When Possible**
   - Replace with JOINs or application-layer calculations
   - Subqueries in SELECT can be slow

### Index Usage

**Check Index Usage:**
```sql
SHOW INDEX FROM fact_stock;
```

**Query Performance with EXPLAIN:**
```sql
EXPLAIN SELECT 
    SUM(fs.stock_quantity) AS total_stock
FROM fact_stock fs
WHERE fs.date_key BETWEEN 20260101 AND 20260407;
```

**Expected Output:**
- `type`: range (using index)
- `key`: idx_fact_stock_date_key
- `rows`: Estimated rows scanned

### Caching Strategies

1. **Application-Level Caching**
   - Cache filter dropdown values (rarely change)
   - Cache total_stock value for reuse in percentage calculations

2. **Query Result Caching**
   - Cache dashboard query results for short period (5-15 minutes)
   - Invalidate cache when data is refreshed

3. **Materialized Views**
   - Pre-aggregate common metrics
   - Update on ETL schedule

---

## Query Performance Testing

### Test Methodology

1. **Run EXPLAIN on Each Query**
   ```sql
   EXPLAIN <query>;
   ```

2. **Measure Execution Time**
   ```sql
   SET profiling = 1;
   <query>;
   SHOW PROFILES;
   ```

3. **Check Index Usage**
   - Ensure queries use indexes
   - No full table scans on fact_stock

### Expected Performance

| Query Type | Expected Time | Indexes Used |
|------------|---------------|--------------|
| Total Stock | < 500ms | date_key, material_key |
| Stock by Tag | < 1s | tag_key, date_key |
| Stock by Material | < 1s | material_key, date_key |
| Stock Trend | < 1s | date_key |
| Filter Dropdowns | < 200ms | Various dimension indexes |

### Troubleshooting Slow Queries

**Symptom:** Query takes > 2 seconds

**Steps:**
1. Run `EXPLAIN` to check execution plan
2. Verify indexes exist on filtered columns
3. Check if full table scan is occurring
4. Add missing indexes if needed
5. Consider query rewrite or summary tables

**Example:**
```sql
-- Slow query
SELECT SUM(stock_quantity) FROM fact_stock WHERE date_key BETWEEN 20260101 AND 20260407;

-- Check execution plan
EXPLAIN SELECT SUM(stock_quantity) FROM fact_stock WHERE date_key BETWEEN 20260101 AND 20260407;

-- If not using index, verify it exists
SHOW INDEX FROM fact_stock WHERE Key_name = 'idx_fact_stock_date_key';

-- If missing, create it
CREATE INDEX idx_fact_stock_date_key ON fact_stock(date_key);
```

---

## Parameter Binding Examples

### Python (SQLAlchemy)

```python
from sqlalchemy import text

query = text("""
    SELECT SUM(fs.stock_quantity) AS total_stock
    FROM fact_stock fs
    WHERE fs.date_key BETWEEN :date_from AND :date_to
        AND (:material_key IS NULL OR fs.material_key = :material_key)
""")

params = {
    'date_from': 20260101,
    'date_to': 20260407,
    'material_key': None  # or specific material_key
}

result = conn.execute(query, params)
total_stock = result.scalar()
```

### JavaScript (Node.js with mysql2)

```javascript
const query = `
    SELECT SUM(fs.stock_quantity) AS total_stock
    FROM fact_stock fs
    WHERE fs.date_key BETWEEN ? AND ?
        AND (? IS NULL OR fs.material_key = ?)
`;

const params = [20260101, 20260407, null, null];

const [rows] = await connection.promise().query(query, params);
const totalStock = rows[0].total_stock;
```

---

**Last Updated:** April 7, 2026  
**Author:** Data Engineer Candidate  
**Version:** 1.0
