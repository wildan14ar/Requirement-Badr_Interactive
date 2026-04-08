# ETL Documentation

## Overview

This document describes the Extract, Transform, Load (ETL) process for the Badr Interactive Data Mart project. The ETL pipeline moves data from the operational database (`recruitment_dev`) to the analytical data mart (`datamart_badr_interactive`).

---

## Table of Contents

1. [Architecture](#architecture)
2. [Source Data](#source-data)
3. [ETL Process](#etl-process)
4. [Extract Phase](#extract-phase)
5. [Transform Phase](#transform-phase)
6. [Load Phase](#load-phase)
7. [Execution](#execution)
8. [Monitoring & Logging](#monitoring--logging)
9. [Error Handling](#error-handling)
10. [Validation](#validation)

---

## Architecture

### ETL Pipeline Components

```
┌─────────────────────────────────────────────────────────────┐
│                    Source Database (OLTP)                    │
│   MySQL: recruitment_dev @ 10.10.0.30:3306                 │
│                                                              │
│   Tables:                                                    │
│   • stocks                  • entity_has_master_materials   │
│   • batches                 • entity_master_material_act.   │
│   • master_materials        • master_activities             │
│   • entities                • entity_entity_tags            │
│   • entity_tags             • provinces & regencies         │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ PyMySQL + SQLAlchemy
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                  Extract Phase (extract.py)                  │
│                                                              │
│   • Connect to source database                              │
│   • Query each table with appropriate filters               │
│   • Load into Pandas DataFrames                             │
│   • Handle NULL values and data types                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Dictionary of DataFrames
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                Transform Phase (transform.py)                │
│                                                              │
│   • Clean and standardize data                              │
│   • Join related tables                                     │
│   • Create derived fields (category, flags)                 │
│   • Map source IDs to business keys                         │
│   • Prepare dimension and fact tables                       │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   │ Transformed DataFrames
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                  Load Phase (load.py)                        │
│                                                              │
│   • Connect to data mart database                           │
│   • Load dimension tables (with upsert logic)               │
│   • Map surrogate keys for fact table                       │
│   • Load fact table with foreign keys                       │
│   • Validate loaded data                                    │
└──────────────────┬──────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────┐
│                  Data Mart (OLAP)                            │
│   MySQL: datamart_badr_interactive                          │
│                                                              │
│   Star Schema:                                               │
│   • dim_date, dim_material, dim_entity                      │
│   • dim_location, dim_activity, dim_entity_tag              │
│   • fact_stock (central fact table)                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Source Data

### Database Information

| Parameter | Value |
|-----------|-------|
| Database Type | MySQL 8.0+ |
| Host | 10.10.0.30 |
| Port | 3306 |
| Database Name | recruitment_dev |
| Username | devel |
| Password | recruitment2024 |
| Access | VPN required (OpenVPN) |

### Source Tables

#### 1. stocks
**Purpose:** Actual stock quantities per batch per entity

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Primary key |
| batch_id | BIGINT | Foreign key to batches |
| qty | FLOAT | Stock quantity |
| entity_has_material_id | BIGINT | FK to entity-material mapping |
| activity_id | INT | FK to activities |
| allocated | FLOAT | Allocated stock |
| in_transit | FLOAT | Stock in transit |
| createdAt | DATETIME | Record creation time |
| updatedAt | DATETIME | Record update time |

**Extraction Filter:** `deleted_at IS NULL`

#### 2. batches
**Purpose:** Batch information for materials

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Primary key |
| code | VARCHAR(255) | Batch number/code |
| expired_date | DATETIME | Expiry date |
| production_date | DATETIME | Production date |
| manufacture_id | INT | Manufacturer ID |
| status | BOOLEAN | Active status |

**Extraction Filter:** `deleted_at IS NULL`

#### 3. master_materials
**Purpose:** Material master data (vaccines and non-vaccines)

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| name | VARCHAR(255) | Material name |
| code | VARCHAR(255) | Material code |
| is_vaccine | TINYINT | Vaccine flag (1=vaccine) |
| unit | VARCHAR(255) | Unit of measurement |
| status | TINYINT | Active status |

**Extraction Filter:** `deleted_at IS NULL`

#### 4. entities
**Purpose:** Healthcare facilities

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| name | VARCHAR(255) | Entity name |
| code | VARCHAR(255) | Entity code |
| type | TINYINT | Entity type |
| province_id | VARCHAR(255) | Province FK |
| regency_id | VARCHAR(255) | Regency FK |
| status | TINYINT | Active status |

**Extraction Filter:** `deleted_at IS NULL`

#### 5. entity_tags
**Purpose:** Entity classification tags

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| title | VARCHAR(255) | Tag name/title |
| deleted_at | DATETIME | Soft delete timestamp |

**Extraction Filter:** `deleted_at IS NULL`

#### 6. entity_entity_tags
**Purpose:** Mapping between entities and tags (pivot table)

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| entity_id | INT | Entity FK |
| entity_tag_id | INT | Tag FK |

**Extraction Filter:** None (no soft delete)

#### 7. entity_has_master_materials
**Purpose:** Mapping between entities and materials

| Column | Type | Description |
|--------|------|-------------|
| id | BIGINT | Primary key |
| entity_id | INT | Entity FK |
| master_material_id | INT | Material FK |
| on_hand_stock | INT | Current stock |
| min | FLOAT | Minimum stock level |
| max | FLOAT | Maximum stock level |

**Extraction Filter:** `deleted_at IS NULL`

#### 8. master_activities
**Purpose:** Activity/program types

| Column | Type | Description |
|--------|------|-------------|
| id | INT | Primary key |
| name | VARCHAR(255) | Activity name |
| code | VARCHAR(255) | Activity code |
| deleted_at | DATETIME | Soft delete timestamp |

**Extraction Filter:** `deleted_at IS NULL`

#### 9. provinces
**Purpose:** Province master data

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Primary key |
| name | VARCHAR(255) | Province name |

**Extraction Filter:** `deleted_at IS NULL`

#### 10. regencies
**Purpose:** Regency/City master data

| Column | Type | Description |
|--------|------|-------------|
| id | VARCHAR(255) | Primary key |
| name | VARCHAR(255) | Regency name |
| province_id | VARCHAR(255) | Province FK |

**Extraction Filter:** `deleted_at IS NULL`

---

## ETL Process

### Execution Flow

```
main.py (Orchestrator)
  ├── extract.py (DataExtractor)
  │   ├── extract_stocks()
  │   ├── extract_batches()
  │   ├── extract_master_materials()
  │   ├── extract_entities()
  │   ├── extract_entity_tags()
  │   ├── extract_entity_entity_tags()
  │   ├── extract_entity_has_master_materials()
  │   ├── extract_master_activities()
  │   ├── extract_provinces()
  │   └── extract_regencies()
  │
  ├── transform.py (DataTransformer)
  │   ├── transform_dim_material()
  │   ├── transform_dim_entity()
  │   ├── transform_dim_location()
  │   ├── transform_dim_activity()
  │   ├── transform_dim_entity_tag()
  │   ├── create_entity_tag_mapping()
  │   └── transform_fact_stock()
  │
  └── load.py (DataLoader)
      ├── load_dim_location()
      ├── load_dim_material()
      ├── load_dim_entity()
      ├── load_dim_activity()
      ├── load_dim_entity_tag()
      └── load_fact_stock_with_keys()
```

### Data Flow Diagram

```
Source Tables                    Transformed Dimensions
─────────────                    ──────────────────────
master_materials      ──────►    dim_material
  • id                             • material_key (surrogate)
  • name                           • material_id (source)
  • code                           • material_name
  • is_vaccine                     • category (derived)
                                   • is_vaccine

entities + provinces + regencies ──► dim_entity + dim_location
  • entity info                    • entity_key (surrogate)
  • province_id                    • entity_name
  • regency_id                     • province_name, regency_name

entity_tags + entity_entity_tags ──► dim_entity_tag
  • tag title                      • tag_key (surrogate)
  • entity-tag mapping             • tag_name

master_activities       ──────►    dim_activity
  • id                             • activity_key (surrogate)
  • name                           • activity_name
  • code                           • activity_code

stocks + batches + mappings   ──►  fact_stock
  • qty                            • stock_key (surrogate)
  • batch_id                       • material_key, entity_key
  • entity_has_material_id         • date_key, activity_key
  • activity_id                    • stock_quantity
                                   • batch_number
```

---

## Extract Phase

### Implementation: `scripts/etl/extract.py`

**Class:** `DataExtractor`

### Key Methods

#### `extract_all()`
Extracts all required tables from source database.

**Returns:** Dictionary with table names as keys and DataFrames as values

**Process:**
1. Connect to source database using SQLAlchemy + PyMySQL
2. Execute SELECT queries for each table
3. Apply filters (exclude soft-deleted records)
4. Load results into Pandas DataFrames
5. Store in `self.extracted_data` dictionary

### Extraction Queries

Each table has a dedicated extraction method that:
- Selects only required columns
- Applies `WHERE deleted_at IS NULL` filter for soft-deleted records
- Handles potential NULL values
- Logs extraction statistics

### Error Handling

- Connection failures: Retries with backoff
- Query failures: Logs error and raises exception
- NULL handling: Fills with appropriate defaults

### Performance Considerations

- Uses connection pooling (SQLAlchemy)
- Reads data in chunks if needed
- Logs row counts for validation

---

## Transform Phase

### Implementation: `scripts/etl/transform.py`

**Class:** `DataTransformer`

### Transformations by Dimension

#### 1. dim_material Transformation

**Input:** `master_materials` DataFrame

**Transformations:**
- Create `category` field: 'Vaccine' if `is_vaccine=1`, else 'Non-Vaccine'
- Convert `is_vaccine` to boolean
- Create `is_active` flag from `status` field
- Handle NULL values (fill with defaults)
- Select and rename columns to match dimension schema

**Output Columns:**
```
material_id, material_code, material_name, category, is_vaccine,
unit, pieces_per_unit, temperature_sensitive, temperature_min,
temperature_max, is_active, source_created_at, source_updated_at
```

#### 2. dim_entity Transformation

**Input:** `entities` DataFrame

**Transformations:**
- Map `type` code to descriptive `entity_type`:
  - 1 → 'Dinas Kesehatan Provinsi'
  - 2 → 'Dinas Kesehatan Kabupaten/Kota'
  - 3 → 'Puskesmas'
  - 4 → 'Rumah Sakit'
  - etc.
- Create `is_active` flag from `status`
- Handle NULL values in location fields

**Output Columns:**
```
entity_id, entity_code, entity_name, entity_type, type, status,
province_id, regency_id, address, lat, lng, postal_code, country,
is_puskesmas, is_active, source_created_at, source_updated_at
```

#### 3. dim_location Transformation

**Input:** `provinces` + `regencies` DataFrames

**Transformations:**
- Merge provinces with regencies on `province_id`
- Create `full_location` string: "Province - Regency"
- Remove duplicates
- Handle NULL values

**Output Columns:**
```
province_id, province_name, lat, regency_id, regency_name, lng,
full_location
```

#### 4. dim_activity Transformation

**Input:** `master_activities` DataFrame

**Transformations:**
- Set `is_active = TRUE` for all records
- Handle NULL values in name and code

**Output Columns:**
```
activity_id, activity_code, activity_name, is_ordered_sales,
is_ordered_purchase, is_patient_id, is_active,
source_created_at, source_updated_at
```

#### 5. dim_entity_tag Transformation

**Input:** `entity_tags` DataFrame

**Transformations:**
- Categorize tags based on name:
  - Contains 'puskesmas' → 'Puskesmas'
  - Contains 'dinkes provinsi' → 'Dinas Kesehatan Provinsi'
  - Contains 'dinkes kab' → 'Dinas Kesehatan Kabupaten'
  - Contains 'rumah sakit' or 'rs' → 'Rumah Sakit'
  - etc.
- Set `is_active = TRUE`

**Output Columns:**
```
tag_id, tag_name, tag_category, is_active,
source_created_at, source_updated_at
```

#### 6. fact_stock Transformation

**Input:** `stocks` + `batches` + `entity_has_master_materials` DataFrames

**Transformations:**
- Merge stocks with batches on `batch_id`
- Merge with entity-material mapping to get `entity_id` and `material_id`
- Extract `date_key` from `created_at` (format: YYYYMMDD)
- Calculate `total_price` if missing: `qty * price`
- Handle NULL values (fill with 0 for quantities)
- Prepare foreign key references for dimension mapping

**Output Columns:**
```
stock_id, batch_id, batch_number, stock_quantity, unit, allocated,
in_transit, open_vial, extermination_discard_qty,
extermination_received_qty, extermination_qty, extermination_shipped_qty,
expiry_date, budget_source, stock_year, price, total_price,
entity_id, material_id, activity_id, date_key, source_created_at
```

#### 7. Entity-Tag Mapping

**Input:** `entity_entity_tags` DataFrame

**Transformations:**
- Group by `entity_id` and get first `tag_id`
- One entity can have multiple tags; use primary tag for simplicity

**Output:**
```
entity_id, tag_id
```

---

## Load Phase

### Implementation: `scripts/etl/load.py`

**Class:** `DataLoader`

### Loading Strategy

#### Dimension Tables

**Method:** Upsert (INSERT if not exists)

**Process:**
1. Query existing records from target table
2. Compare with source data using unique key (e.g., `material_id`)
3. Filter out records that already exist
4. INSERT new records using `to_sql(..., if_exists='append')`
5. Log number of new vs existing records

**Example:**
```python
# Get existing material_ids
existing = pd.read_sql("SELECT material_id FROM dim_material", conn)

# Filter new records
df_new = df[~df['material_id'].isin(existing['material_id'])]

# Insert new records
df_new.to_sql('dim_material', conn, if_exists='append', index=False)
```

#### Special Case: dim_location

**Unique Key:** Composite (`province_id`, `regency_id`)

**Process:**
1. Query existing locations
2. Merge to identify new combinations
3. INSERT only new combinations

#### Fact Table: fact_stock

**Method:** Direct INSERT with foreign key mapping

**Process:**
1. Query all dimension tables to get surrogate key mappings:
   - `material_id` → `material_key`
   - `entity_id` → `entity_key`
   - `activity_id` → `activity_key`
   - `tag_id` → `tag_key`
   - (`province_id`, `regency_id`) → `location_key`
2. Merge fact stock data with dimension mappings
3. Replace source IDs with surrogate keys
4. Remove records with missing required keys (`material_key`, `entity_key`, `date_key`)
5. Fill NULL optional keys (`activity_key`, `tag_key`, `location_key`)
6. INSERT into `fact_stock`

**Foreign Key Mapping Example:**
```python
# Map material keys
fact_stock = fact_stock.merge(material_map, on='material_id', how='left')

# Map entity keys
fact_stock = fact_stock.merge(entity_map, on='entity_id', how='left')

# Map activity keys
fact_stock = fact_stock.merge(activity_map, on='activity_id', how='left')

# Map tag keys (via entity-tag mapping)
fact_stock = fact_stock.merge(entity_tag_mapping, on='entity_id', how='left')
fact_stock = fact_stock.merge(tag_map, on='tag_id', how='left')

# Map location keys
fact_stock = fact_stock.merge(location_map, on=['province_id', 'regency_id'], how='left')
```

### Validation

After loading, the `validate_load()` method:
1. Queries COUNT(*) from each table
2. Compares with expected counts
3. Logs results for verification

---

## Execution

### Full Pipeline

```bash
# Run complete ETL pipeline
python scripts/etl/main.py

# Run with verbose logging
python scripts/etl/main.py --verbose
```

### Individual Phases

```bash
# Run only extraction
python scripts/etl/main.py --phase extract

# Run extraction + transformation
python scripts/etl/main.py --phase transform

# Run full pipeline (extract + transform + load)
python scripts/etl/main.py --phase all
```

### Programmatic Usage

```python
from scripts.etl.main import ETLPipeline

# Initialize pipeline
pipeline = ETLPipeline(verbose=True)

# Run full pipeline
result = pipeline.run_full_pipeline()

# Check results
if result['status'] == 'success':
    print(f"Loaded {result['load_stats']['fact_stock']} stock records")
```

---

## Monitoring & Logging

### Log Files

| File | Purpose |
|------|---------|
| `etl_extract.log` | Extraction phase logs |
| `etl_transform.log` | Transformation phase logs |
| `etl_load.log` | Loading phase logs |
| `etl_pipeline.log` | Full pipeline logs |

### Log Format

```
2026-04-07 10:30:15,123 - __main__ - INFO - ============================================================
2026-04-07 10:30:15,124 - __main__ - INFO - PHASE 1: EXTRACTION
2026-04-07 10:30:15,125 - __main__ - INFO - ============================================================
2026-04-07 10:30:15,234 - extract - INFO - Extracting stocks table
2026-04-07 10:30:16,456 - extract - INFO - Extracted 50,000 stock records
```

### Key Metrics Logged

- Number of rows extracted per table
- Number of new vs existing records per dimension
- Number of records loaded into fact table
- Execution time per phase
- Total execution time
- Errors and warnings

---

## Error Handling

### Connection Errors

**Scenario:** Cannot connect to source/database

**Handling:**
- Log error with connection details
- Raise exception with clear message
- Pipeline stops immediately

### Data Errors

**Scenario:** NULL values, type mismatches

**Handling:**
- Fill NULLs with appropriate defaults
- Log warnings for data quality issues
- Continue processing

### Foreign Key Violations

**Scenario:** Missing dimension records

**Handling:**
- Drop fact records with missing required keys
- Log count of dropped records
- Continue with valid records

### Pipeline Failures

**Scenario:** Any unhandled exception

**Handling:**
- Log full stack trace
- Clean up resources (close connections)
- Return failure status with error message

---

## Validation

### Pre-Load Validation

1. **Check extracted data:**
   - Verify row counts match expectations
   - Check for NULL values in required fields
   - Validate data types

2. **Check transformed data:**
   - Verify dimension counts
   - Check foreign key mappings
   - Validate derived fields

### Post-Load Validation

1. **Row count validation:**
   ```sql
   SELECT COUNT(*) FROM dim_material;
   SELECT COUNT(*) FROM dim_entity;
   SELECT COUNT(*) FROM fact_stock;
   ```

2. **Aggregation validation:**
   ```sql
   -- Compare total stock between source and data mart
   SELECT SUM(qty) FROM recruitment_dev.stocks;
   SELECT SUM(stock_quantity) FROM datamart_badr_interactive.fact_stock;
   ```

3. **Referential integrity:**
   ```sql
   -- Check for orphaned fact records
   SELECT COUNT(*) FROM fact_stock fs
   LEFT JOIN dim_material dm ON fs.material_key = dm.material_key
   WHERE dm.material_key IS NULL;
   ```

### Expected Results

| Table | Expected Count | Validation |
|-------|---------------|------------|
| dim_date | ~4,018 (2020-2030) | Fixed based on date range |
| dim_material | All active materials | Match source count |
| dim_entity | All active entities | Match source count |
| dim_location | All province-regency combos | Match source count |
| dim_activity | All active activities | Match source count |
| dim_entity_tag | All active tags | Match source count |
| fact_stock | All stock records | Match source count (minus orphans) |

---

## Performance Optimization

### Extraction

- Use selective column lists (avoid `SELECT *`)
- Apply filters to reduce data volume
- Use connection pooling

### Transformation

- Use Pandas vectorized operations
- Avoid row-by-row processing when possible
- Merge operations to reduce passes through data

### Loading

- Batch INSERT operations (chunksize=1000)
- Use upsert logic to avoid duplicates
- Load dimensions before fact table

### Future Enhancements

- **Incremental extraction:** Only extract new/changed records based on `updated_at`
- **Parallel processing:** Extract tables in parallel
- **Partitioning:** Partition fact table by date for faster queries
- **Index optimization:** Add indexes based on query patterns

---

## Troubleshooting

### Issue: Connection timeout to source database

**Solution:**
- Verify VPN connection is active
- Check network connectivity: `ping 10.10.0.30`
- Verify credentials in `.env` file

### Issue: Foreign key constraint violation during load

**Solution:**
- Check that all dimension tables are loaded before fact table
- Verify dimension mappings are correct
- Review logs for dropped records

### Issue: Memory error during transformation

**Solution:**
- Process data in smaller chunks
- Use `chunksize` parameter in `read_sql`
- Increase available memory or use 64-bit Python

### Issue: Slow performance

**Solution:**
- Check network latency to database
- Optimize queries to select only needed columns
- Add indexes to data mart tables
- Consider incremental loading

---

**Last Updated:** April 7, 2026  
**Author:** Data Engineer Candidate  
**Version:** 1.0
