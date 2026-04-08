# Data Mart Design Documentation

## Overview

This document describes the dimensional model design for the Badr Interactive Data Mart. The design follows the **Star Schema** pattern, which is optimized for OLAP (Online Analytical Processing) queries and dashboard visualizations.

---

## Table of Contents

1. [Design Principles](#design-principles)
2. [Star Schema Architecture](#star-schema-architecture)
3. [Dimension Tables](#dimension-tables)
4. [Fact Table](#fact-table)
5. [Relationships](#relationships)
6. [Indexing Strategy](#indexing-strategy)
7. [Design Decisions](#design-decisions)
8. [Scalability Considerations](#scalability-considerations)

---

## Design Principles

### 1. Dimensional Modeling

The data mart uses **dimensional modeling** techniques pioneered by Ralph Kimball:

- **Fact Tables:** Contain measurable metrics (stock quantities)
- **Dimension Tables:** Contain descriptive attributes for filtering and grouping
- **Star Schema:** Single fact table surrounded by dimension tables

### 2. Grain Definition

**Fact Table Grain:** One row per stock record per batch per entity per activity per day

This grain allows:
- Aggregation by any dimension combination
- Time-series analysis at daily granularity
- Drill-down from summary to detail

### 3. Surrogate Keys

All dimension tables use **surrogate keys** (system-generated integers) instead of source system IDs:

**Benefits:**
- Decouples data mart from source system changes
- Handles Type 2 slowly changing dimensions (future enhancement)
- Simplifies joins with integer keys
- Enables integration of multiple source systems

### 4. Denormalization

Dimension tables are intentionally **denormalized** to:
- Reduce join complexity
- Improve query performance
- Simplify dashboard query writing
- Make the schema more intuitive for end users

---

## Star Schema Architecture

### Complete Schema Diagram

```
                                ┌─────────────────┐
                                │   dim_date      │
                                │─────────────────│
                                │ date_key (PK)   │
                                │ full_date       │
                                │ year            │
                                │ quarter         │
                                │ month           │
                                │ day             │
                                │ day_of_week     │
                                │ is_weekend      │
                                └────────┬────────┘
                                         │
                                         │ date_key (FK)
                                         │
┌──────────────────┐             ┌───────▼────────┐            ┌─────────────────┐
│  dim_material    │             │  fact_stock    │            │  dim_entity     │
│──────────────────│             │────────────────│            │─────────────────│
│ material_key(PK) │◄────────────│ material_key   │────────────► entity_key(PK)  │
│ material_id      │   (FK)      │ entity_key     │    (FK)    │ entity_id       │
│ material_name    │             │ date_key       │            │ entity_name     │
│ category         │             │ activity_key   │            │ entity_type     │
│ is_vaccine       │             │ tag_key        │            │ province_id     │
│ unit             │             │ location_key   │            │ regency_id      │
└──────────────────┘             │ batch_id       │            └────────┬────────┘
                                 │ stock_quantity │                     │
          ┌────────────────────►│ allocated        │                     │
          │                     │ in_transit       │          ┌──────────▼──────────┐
          │                     │ open_vial        │          │  dim_location       │
          │                     │ expiry_date      │          │─────────────────────│
          │                     │ price            │          │ location_key (PK)   │
          │                     │ total_price      │          │ province_id         │
          │                     └───────┬──────────┘          │ province_name       │
          │                             │                     │ regency_id          │
          │                             │                     │ regency_name        │
          │                    ┌────────▼────────┐            │ full_location       │
          │                    │ dim_activity    │            └─────────────────────┘
          │                    │─────────────────│
          │                    │ activity_key(PK)│
          │                    │ activity_id     │
          │                    │ activity_name   │
          │                    │ activity_type   │
          │                    └─────────────────┘
          │
          │                           ┌──────────────────┐
          │                           │ dim_entity_tag   │
          │                           │──────────────────│
          └───────────────────────────│ tag_key (PK)     │
                                      │ tag_id           │
                                      │ tag_name         │
                                      │ tag_category     │
                                      └──────────────────┘
```

### Schema Components

| Component | Type | Purpose |
|-----------|------|---------|
| dim_date | Dimension | Time-based analysis and filtering |
| dim_material | Dimension | Material/vaccine classification |
| dim_entity | Dimension | Healthcare facility information |
| dim_location | Dimension | Geographic hierarchy (Province → Regency) |
| dim_activity | Dimension | Program/activity types |
| dim_entity_tag | Dimension | Entity classification (Puskesmas, Dinkes, etc.) |
| fact_stock | Fact Table | Stock quantities with dimensional references |

---

## Dimension Tables

### 1. dim_date

**Purpose:** Time dimension for date-based filtering and aggregation

**Grain:** One row per day

**Type:** Role-playing dimension (can be used for stock date, expiry date, etc.)

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| date_key | INT | PRIMARY KEY | Surrogate key in YYYYMMDD format (e.g., 20260407) |
| full_date | DATE | NOT NULL | Complete date value |
| year | INT | NOT NULL | Year (e.g., 2026) |
| quarter | INT | NOT NULL | Quarter (1-4) |
| month | INT | NOT NULL | Month (1-12) |
| month_name | VARCHAR(20) | NOT NULL | Full month name (January, February, etc.) |
| month_short | VARCHAR(3) | NOT NULL | Abbreviated month (Jan, Feb, etc.) |
| day | INT | NOT NULL | Day of month (1-31) |
| day_of_week | INT | NOT NULL | Day of week (1=Monday, 7=Sunday) |
| day_name | VARCHAR(20) | NOT NULL | Full day name (Monday, Tuesday, etc.) |
| day_short | VARCHAR(3) | NOT NULL | Abbreviated day (Mon, Tue, etc.) |
| is_weekend | BOOLEAN | DEFAULT FALSE | Weekend flag (TRUE for Saturday/Sunday) |
| is_holiday | BOOLEAN | DEFAULT FALSE | Holiday flag (can be populated later) |
| week_of_year | INT | NULLABLE | ISO week number (1-53) |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Record update time |

**Date Range:** 2020-01-01 to 2030-12-31 (~4,018 records)

**Indexes:**
- PRIMARY KEY (date_key)
- INDEX idx_full_date (full_date)
- INDEX idx_year (year)
- INDEX idx_month (month)
- INDEX idx_quarter (quarter)
- INDEX idx_year_month (year, month) - Composite
- INDEX idx_year_quarter (year, quarter) - Composite

**Population Method:** Stored procedure `populate_date_dimension()` that generates records programmatically

**Example Records:**
```
date_key   | full_date  | year | quarter | month | month_name | day | day_of_week | is_weekend
-----------|------------|------|---------|-------|------------|-----|-------------|-----------
20260407   | 2026-04-07 | 2026 | 2       | 4     | April      | 7   | 2           | FALSE
20260408   | 2026-04-08 | 2026 | 2       | 4     | April      | 8   | 3           | FALSE
```

**Use Cases:**
- Filter stock data by date range
- Aggregate stock by month/quarter/year
- Analyze trends over time
- Identify weekend vs weekday patterns

---

### 2. dim_material

**Purpose:** Material master dimension for vaccine and non-vaccine items

**Grain:** One row per material

**Source:** `master_materials` table

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| material_key | INT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| material_id | INT | NOT NULL, UNIQUE | Source system material ID |
| material_code | VARCHAR(50) | NULLABLE | Material code from source |
| material_name | VARCHAR(255) | NOT NULL | Material name |
| material_type | VARCHAR(100) | NULLABLE | Material type classification |
| category | VARCHAR(50) | NULLABLE | Category: 'Vaccine' or 'Non-Vaccine' |
| is_vaccine | BOOLEAN | DEFAULT FALSE | Vaccine flag (TRUE=vaccine) |
| unit_of_distribution | VARCHAR(50) | NULLABLE | Unit of distribution |
| unit | VARCHAR(50) | NULLABLE | Base unit of measurement |
| pieces_per_unit | DECIMAL(10,2) | DEFAULT 1 | Pieces per distribution unit |
| temperature_sensitive | TINYINT | NULLABLE | Temperature sensitive flag |
| temperature_min | FLOAT | NULLABLE | Minimum storage temperature |
| temperature_max | FLOAT | NULLABLE | Maximum storage temperature |
| is_active | BOOLEAN | DEFAULT TRUE | Active status flag |
| source_created_at | DATETIME | NULLABLE | Original created timestamp |
| source_updated_at | DATETIME | NULLABLE | Original updated timestamp |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Data mart load timestamp |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Data mart update timestamp |

**Derived Fields:**
- `category`: Derived from `is_vaccine` flag
  - `is_vaccine = 1` → `category = 'Vaccine'`
  - `is_vaccine = 0` → `category = 'Non-Vaccine'`

**Indexes:**
- PRIMARY KEY (material_key)
- UNIQUE KEY uk_material_id (material_id)
- INDEX idx_material_code (material_code)
- INDEX idx_material_name (material_name)
- INDEX idx_category (category)
- INDEX idx_is_vaccine (is_vaccine)
- INDEX idx_is_active (is_active)
- INDEX idx_category_is_vaccine (category, is_vaccine) - Composite

**Use Cases:**
- Filter by vaccine vs non-vaccine
- Group stock by material type
- Search for specific materials
- Analyze material distribution

---

### 3. dim_entity

**Purpose:** Healthcare facility dimension

**Grain:** One row per entity (facility)

**Source:** `entities` table

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| entity_key | INT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| entity_id | INT | NOT NULL, UNIQUE | Source system entity ID |
| entity_code | VARCHAR(50) | NULLABLE | Entity code |
| entity_name | VARCHAR(255) | NOT NULL | Entity/facility name |
| entity_type | VARCHAR(100) | NULLABLE | Entity type description |
| type | TINYINT | NULLABLE | Entity type code from source |
| status | TINYINT | DEFAULT 1 | Entity status from source |
| regency_id | VARCHAR(50) | NULLABLE | Regency/City ID |
| province_id | VARCHAR(50) | NULLABLE | Province ID |
| address | TEXT | NULLABLE | Entity address |
| lat | VARCHAR(50) | NULLABLE | Latitude coordinate |
| lng | VARCHAR(50) | NULLABLE | Longitude coordinate |
| postal_code | VARCHAR(20) | NULLABLE | Postal code |
| country | VARCHAR(100) | DEFAULT 'Indonesia' | Country |
| is_puskesmas | TINYINT | DEFAULT 0 | Puskesmas flag |
| is_active | BOOLEAN | DEFAULT TRUE | Active status flag |
| source_created_at | DATETIME | NULLABLE | Original created timestamp |
| source_updated_at | DATETIME | NULLABLE | Original updated timestamp |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Data mart load timestamp |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Data mart update timestamp |

**Derived Fields:**
- `entity_type`: Mapped from `type` code
  - 1 → 'Dinas Kesehatan Provinsi'
  - 2 → 'Dinas Kesehatan Kabupaten/Kota'
  - 3 → 'Puskesmas'
  - 4 → 'Rumah Sakit'
  - 5 → 'Klinik'
  - 6 → 'Apotek'
  - 7 → 'Laboratorium'
  - Other → 'Lainnya'

**Indexes:**
- PRIMARY KEY (entity_key)
- UNIQUE KEY uk_entity_id (entity_id)
- INDEX idx_entity_code (entity_code)
- INDEX idx_entity_name (entity_name)
- INDEX idx_entity_type (entity_type)
- INDEX idx_regency_id (regency_id)
- INDEX idx_province_id (province_id)
- INDEX idx_is_active (is_active)
- INDEX idx_entity_type_is_active (entity_type, is_active) - Composite

**Use Cases:**
- Filter stock by facility type (Puskesmas, Dinkes, etc.)
- Analyze stock by specific entity
- Geographic analysis by province/regency

---

### 4. dim_location

**Purpose:** Geographic hierarchy dimension (Province → Regency/City)

**Grain:** One row per unique province-regency combination

**Source:** `provinces` + `regencies` tables (merged)

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| location_key | INT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| province_id | VARCHAR(50) | NULLABLE | Province ID |
| province_code | VARCHAR(50) | NULLABLE | Province code |
| province_name | VARCHAR(255) | NULLABLE | Province name |
| regency_id | VARCHAR(50) | NULLABLE | Regency/City ID |
| regency_code | VARCHAR(50) | NULLABLE | Regency code |
| regency_name | VARCHAR(255) | NULLABLE | Regency/City name |
| full_location | VARCHAR(500) | NULLABLE | Combined location string |
| lat | VARCHAR(50) | NULLABLE | Latitude coordinate |
| lng | VARCHAR(50) | NULLABLE | Longitude coordinate |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Data mart load timestamp |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Data mart update timestamp |

**Derived Fields:**
- `full_location`: Concatenated "Province - Regency" string
  - Example: "JAWA BARAT - BANDUNG"

**Unique Constraint:**
- UNIQUE KEY uk_location (province_id, regency_id)

**Indexes:**
- PRIMARY KEY (location_key)
- UNIQUE KEY uk_location (province_id, regency_id)
- INDEX idx_province_id (province_id)
- INDEX idx_province_name (province_name)
- INDEX idx_regency_id (regency_id)
- INDEX idx_regency_name (regency_name)

**Use Cases:**
- Filter stock by province
- Filter stock by regency/city
- Hierarchical geographic analysis
- Cascading dropdowns (Province → Regency)

---

### 5. dim_activity

**Purpose:** Activity/program dimension

**Grain:** One row per activity

**Source:** `master_activities` table

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| activity_key | INT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| activity_id | INT | NOT NULL, UNIQUE | Source system activity ID |
| activity_code | VARCHAR(50) | NULLABLE | Activity code |
| activity_name | VARCHAR(255) | NOT NULL | Activity name |
| activity_type | VARCHAR(100) | NULLABLE | Activity type |
| is_ordered_sales | TINYINT | DEFAULT 1 | Ordered sales flag |
| is_ordered_purchase | TINYINT | DEFAULT 1 | Ordered purchase flag |
| is_patient_id | TINYINT | NULLABLE | Patient ID required flag |
| is_active | BOOLEAN | DEFAULT TRUE | Active status flag |
| source_created_at | DATETIME | NULLABLE | Original created timestamp |
| source_updated_at | DATETIME | NULLABLE | Original updated timestamp |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Data mart load timestamp |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Data mart update timestamp |

**Indexes:**
- PRIMARY KEY (activity_key)
- UNIQUE KEY uk_activity_id (activity_id)
- INDEX idx_activity_code (activity_code)
- INDEX idx_activity_name (activity_name)
- INDEX idx_activity_type (activity_type)
- INDEX idx_is_active (is_active)

**Use Cases:**
- Filter stock by activity/program
- Analyze stock distribution by activity
- Activity-based reporting

---

### 6. dim_entity_tag

**Purpose:** Entity tag/classification dimension

**Grain:** One row per entity tag

**Source:** `entity_tags` table

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| tag_key | INT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| tag_id | INT | NOT NULL, UNIQUE | Source system tag ID |
| tag_code | VARCHAR(50) | NULLABLE | Tag code |
| tag_name | VARCHAR(255) | NOT NULL | Tag name/title |
| tag_category | VARCHAR(100) | NULLABLE | Tag category |
| is_active | BOOLEAN | DEFAULT TRUE | Active status flag |
| source_created_at | DATETIME | NULLABLE | Original created timestamp |
| source_updated_at | DATETIME | NULLABLE | Original updated timestamp |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Data mart load timestamp |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Data mart update timestamp |

**Derived Fields:**
- `tag_category`: Categorized from `tag_name`
  - Contains 'puskesmas' → 'Puskesmas'
  - Contains 'dinkes provinsi' → 'Dinas Kesehatan Provinsi'
  - Contains 'dinkes kab' → 'Dinas Kesehatan Kabupaten'
  - Contains 'rumah sakit' or 'rs' → 'Rumah Sakit'
  - Contains 'klinik' → 'Klinik'
  - Other → 'Lainnya'

**Indexes:**
- PRIMARY KEY (tag_key)
- UNIQUE KEY uk_tag_id (tag_id)
- INDEX idx_tag_code (tag_code)
- INDEX idx_tag_name (tag_name)
- INDEX idx_tag_category (tag_category)
- INDEX idx_is_active (is_active)

**Use Cases:**
- Filter stock by entity tag (Puskesmas, Dinkes, etc.)
- "Stok per Tag Entitas" dashboard component
- Entity type analysis

---

## Fact Table

### fact_stock

**Purpose:** Central fact table storing stock quantities with all dimensional references

**Grain:** One row per stock record (per batch per entity per activity per day)

**Source:** `stocks` + `batches` + `entity_has_master_materials` (merged)

| Column | Data Type | Constraints | Description |
|--------|-----------|-------------|-------------|
| stock_key | BIGINT | PRIMARY KEY, AUTO_INCREMENT | Surrogate key |
| material_key | INT | NOT NULL, FK | Foreign key to dim_material |
| entity_key | INT | NOT NULL, FK | Foreign key to dim_entity |
| date_key | INT | NOT NULL, FK | Foreign key to dim_date |
| activity_key | INT | NULLABLE, FK | Foreign key to dim_activity |
| tag_key | INT | NULLABLE, FK | Foreign key to dim_entity_tag |
| location_key | INT | NULLABLE, FK | Foreign key to dim_location |
| batch_id | BIGINT | NULLABLE | Source batch ID |
| batch_number | VARCHAR(100) | NULLABLE | Batch number/code |
| stock_quantity | DECIMAL(15,2) | NOT NULL | Stock quantity (main metric) |
| unit | VARCHAR(50) | NULLABLE | Unit of measurement |
| allocated | DECIMAL(15,2) | DEFAULT 0 | Allocated stock quantity |
| in_transit | DECIMAL(15,2) | DEFAULT 0 | Stock in transit quantity |
| open_vial | INT | DEFAULT 0 | Open vial count |
| extermination_discard_qty | DECIMAL(15,2) | DEFAULT 0 | Discarded quantity |
| extermination_received_qty | DECIMAL(15,2) | DEFAULT 0 | Received extermination qty |
| extermination_qty | DECIMAL(15,2) | DEFAULT 0 | Extermination qty |
| extermination_shipped_qty | DECIMAL(15,2) | DEFAULT 0 | Shipped extermination qty |
| expiry_date | DATE | NULLABLE | Batch expiry date |
| budget_source | INT | NULLABLE | Budget source |
| year | INT | NULLABLE | Stock year |
| price | DECIMAL(15,2) | NULLABLE | Unit price |
| total_price | DECIMAL(15,2) | NULLABLE | Total price (qty × price) |
| source_stock_id | BIGINT | NULLABLE | Source stock record ID |
| source_batch_id | BIGINT | NULLABLE | Source batch record ID |
| source_entity_material_id | BIGINT | NULLABLE | Source entity-material mapping ID |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Record creation time |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP ON UPDATE | Record update time |

**Measures (Metrics):**
- `stock_quantity`: Primary measure (remaining stock)
- `allocated`: Allocated but not yet used stock
- `in_transit`: Stock being transported
- `open_vial`: Opened vials count
- Extermination quantities: Various extermination metrics
- `price`, `total_price`: Financial metrics

**Foreign Keys:**
```sql
CONSTRAINT fk_fact_stock_material FOREIGN KEY (material_key) REFERENCES dim_material(material_key)
CONSTRAINT fk_fact_stock_entity FOREIGN KEY (entity_key) REFERENCES dim_entity(entity_key)
CONSTRAINT fk_fact_stock_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
CONSTRAINT fk_fact_stock_activity FOREIGN KEY (activity_key) REFERENCES dim_activity(activity_key)
CONSTRAINT fk_fact_stock_tag FOREIGN KEY (tag_key) REFERENCES dim_entity_tag(tag_key)
CONSTRAINT fk_fact_stock_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
```

**Indexes:**

*Single Column Indexes:*
- PRIMARY KEY (stock_key)
- INDEX idx_fact_stock_material_key (material_key)
- INDEX idx_fact_stock_entity_key (entity_key)
- INDEX idx_fact_stock_date_key (date_key)
- INDEX idx_fact_stock_activity_key (activity_key)
- INDEX idx_fact_stock_tag_key (tag_key)
- INDEX idx_fact_stock_location_key (location_key)
- INDEX idx_fact_stock_batch (batch_id)
- INDEX idx_fact_stock_year (year)

*Composite Indexes:*
- INDEX idx_fact_stock_date_material (date_key, material_key)
- INDEX idx_fact_stock_date_entity (date_key, entity_key)
- INDEX idx_fact_stock_date_location (date_key, location_key)
- INDEX idx_fact_stock_material_entity (material_key, entity_key)
- INDEX idx_fact_stock_tag_entity (tag_key, entity_key)

**Data Types Rationale:**
- `DECIMAL(15,2)`: Supports values up to 999,999,999,999.99 (sufficient for stock quantities and prices)
- `BIGINT` for stock_key: Supports billions of stock records
- `VARCHAR` lengths: Based on source data patterns with buffer

**NULL Handling:**
- Required fields (NOT NULL): `material_key`, `entity_key`, `date_key`, `stock_quantity`
- Optional fields (NULLABLE): `activity_key`, `tag_key`, `location_key` (not all stock records have these)

---

## Relationships

### Entity-Relationship Diagram

```
dim_date (1) ──────┐
                    │
dim_material (1) ───┤
                    │
dim_entity (1) ─────┤
                    ├─── (N) fact_stock
dim_location (1) ───┤
                    │
dim_activity (1) ───┤
                    │
dim_entity_tag (1) ─┘
```

**Relationship Type:** One-to-Many (1:N)

- One dimension record → Many fact records
- Each fact record → Exactly one dimension record (per dimension)

### Cardinality

| Dimension | Fact Table | Cardinality | Explanation |
|-----------|------------|-------------|-------------|
| dim_date | fact_stock | 1:N | One date → Many stock records |
| dim_material | fact_stock | 1:N | One material → Many stock records |
| dim_entity | fact_stock | 1:N | One entity → Many stock records |
| dim_location | fact_stock | 1:N | One location → Many stock records |
| dim_activity | fact_stock | 1:N | One activity → Many stock records |
| dim_entity_tag | fact_stock | 1:N | One tag → Many stock records |

---

## Indexing Strategy

### Index Types Used

1. **PRIMARY KEY:** Unique identifier for each table
2. **UNIQUE KEY:** Ensures uniqueness of business keys
3. **Regular INDEX:** Improves query performance
4. **COMPOSITE INDEX:** Optimizes multi-column filtering

### Index Design Principles

1. **Foreign Keys:** All FKs in fact_stock are indexed
2. **Filter Columns:** Columns used in WHERE clauses are indexed
3. **Join Columns:** Columns used in JOINs are indexed
4. **Composite Patterns:** Frequently combined filters have composite indexes

### Index Priority

**High Priority (Must Have):**
- All foreign keys in fact_stock
- Unique business keys (material_id, entity_id, etc.)
- Date columns for time-series queries

**Medium Priority (Should Have):**
- Name columns for search functionality
- Category/type columns for filtering
- Composite indexes for common query patterns

**Low Priority (Nice to Have):**
- Timestamp columns
- Description fields
- Low-cardinality columns

---

## Design Decisions

### 1. Why Star Schema (not Snowflake)?

**Decision:** Use Star Schema with denormalized dimensions

**Rationale:**
- **Simpler Queries:** Fewer joins required for dashboard queries
- **Better Performance:** Denormalization reduces join overhead
- **User-Friendly:** Easier for business users to understand
- **OLAP Optimized:** Designed for read-heavy analytical workloads

**Trade-off:**
- Some data redundancy (acceptable for analytical workloads)
- Slightly more storage (negligible with modern storage costs)

### 2. Why Surrogate Keys?

**Decision:** Use system-generated surrogate keys for all dimensions

**Rationale:**
- **Source Independence:** Decouples data mart from source system changes
- **Type 2 SCD Support:** Enables future implementation of slowly changing dimensions
- **Integration Flexibility:** Allows merging multiple source systems
- **Performance:** Integer joins are faster than string joins
- **Historical Tracking:** Can track changes even if source IDs change

### 3. Why Separate dim_location?

**Decision:** Create separate location dimension instead of embedding in dim_entity

**Rationale:**
- **Reusability:** Location can be used for other purposes (e.g., manufacturer location)
- **Normalization:** Avoids repeating province/regency data for each entity
- **Performance:** Smaller dim_entity table
- **Flexibility:** Can add location-specific attributes easily

### 4. Why Optional FKs in Fact Table?

**Decision:** Make `activity_key`, `tag_key`, and `location_key` NULLABLE

**Rationale:**
- Not all stock records have activity associations
- Some entities may not have tags
- Location is derived from entity, may not always be available
- Allows loading partial data without failures

**Impact:**
- Queries must handle NULL values (using `IS NULL` or `COALESCE`)
- Slightly more complex validation

### 5. Why Store Both Source IDs and Surrogate Keys?

**Decision:** Keep source system IDs alongside surrogate keys in dimensions

**Rationale:**
- **Traceability:** Easy to trace back to source records
- **Debugging:** Simplifies data validation and debugging
- **Incremental Updates:** Can identify new/changed records
- **Minimal Overhead:** Storage cost is negligible

### 6. Why Derived Fields?

**Decision:** Create derived fields like `category`, `entity_type`, `tag_category`

**Rationale:**
- **Query Simplicity:** Dashboard queries don't need CASE statements
- **Performance:** Pre-computed values are faster than runtime calculations
- **Consistency:** Ensures consistent categorization across all queries
- **Business Logic:** Encodes business rules in the data model

---

## Scalability Considerations

### Current Design Capacity

| Metric | Estimate | Capacity |
|--------|----------|----------|
| Fact table rows | ~100K - 1M | Up to 10M (with current indexes) |
| Dimension rows | < 10K each | Up to 1M each |
| Query response time | < 2 seconds | < 5 seconds at full capacity |
| Daily load time | < 10 minutes | < 30 minutes at full capacity |

### Future Enhancements for Scale

#### 1. Partitioning

**When:** Fact table exceeds 10M rows

**Approach:** Partition `fact_stock` by `date_key` (monthly or quarterly)

```sql
ALTER TABLE fact_stock 
PARTITION BY RANGE (date_key) (
    PARTITION p202401 VALUES LESS THAN (20240200),
    PARTITION p202402 VALUES LESS THAN (20240300),
    ...
    PARTITION pmax VALUES LESS THAN MAXVALUE
);
```

**Benefits:**
- Faster queries with date range filters (partition pruning)
- Easier data archival (drop old partitions)
- Improved maintenance operations

#### 2. Materialized Views / Summary Tables

**When:** Dashboard queries become slow

**Approach:** Pre-aggregate common metrics

```sql
CREATE TABLE stock_summary_daily AS
SELECT 
    date_key,
    material_key,
    entity_key,
    SUM(stock_quantity) AS total_stock,
    COUNT(*) AS record_count
FROM fact_stock
GROUP BY date_key, material_key, entity_key;
```

**Benefits:**
- Instant query response for common aggregations
- Reduced load on fact table
- Simplified dashboard queries

#### 3. Columnar Storage

**When:** Analytical queries require full table scans

**Approach:** Migrate to columnar database (ClickHouse, Amazon Redshift)

**Benefits:**
- 10-100x faster analytical queries
- Better compression
- Optimized for OLAP workloads

#### 4. Incremental ETL

**When:** Full ETL takes too long

**Approach:** Extract and load only new/changed records based on `updated_at`

**Benefits:**
- Faster ETL execution
- Reduced load on source database
- Near real-time data freshness

### Monitoring Thresholds

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| Fact table size | 5M rows | 10M rows | Implement partitioning |
| Query time | 2 seconds | 5 seconds | Add indexes or summary tables |
| ETL duration | 10 minutes | 30 minutes | Switch to incremental ETL |
| Storage usage | 70% | 90% | Archive old data or scale storage |

---

## Data Quality Considerations

### Constraints

**NOT NULL Constraints:**
- Primary keys (all tables)
- Foreign keys in fact_stock (material_key, entity_key, date_key)
- Stock quantity (fact_stock)

**UNIQUE Constraints:**
- Business keys in dimension tables
- Location combination (province_id, regency_id)

**CHECK Constraints:**
- Not implemented in MySQL (limited support)
- Enforced in ETL logic instead

### Referential Integrity

**Enforced By:**
- Foreign key constraints in fact_stock
- ETL validation checks
- Post-load validation queries

**Orphaned Records:**
- Dropped during ETL if dimension mapping fails
- Logged for investigation

---

## Security Considerations

### Access Control

**Recommended Permissions:**
- ETL user: SELECT, INSERT, UPDATE on data mart tables
- Dashboard user: SELECT only on data mart tables
- No direct access to source database from dashboard

### Data Sensitivity

**Current Data:**
- Stock quantities (business-sensitive)
- Facility information (public)
- No PII (Personal Identifiable Information)

**Future Considerations:**
- If PII is added, implement row-level security
- Consider data masking for sensitive fields
- Implement audit logging

---

**Last Updated:** April 7, 2026  
**Author:** Data Engineer Candidate  
**Version:** 1.0
