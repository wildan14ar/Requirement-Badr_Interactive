-- =====================================================
-- Data Mart Dimension Tables DDL - ClickHouse
-- Database: datamart_badr_interactive
-- Purpose: Create all dimension tables for star schema
-- Date: 2026-04-07
-- =====================================================

-- Create database
CREATE DATABASE IF NOT EXISTS datamart_badr_interactive;

USE datamart_badr_interactive;

-- =====================================================
-- Dimension: Date
-- =====================================================
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date
(
    date_key Int32 COMMENT 'YYYYMMDD format',
    full_date Date COMMENT 'Complete date',
    year Int16 COMMENT 'Year (e.g., 2024)',
    quarter Int8 COMMENT 'Quarter (1-4)',
    month Int8 COMMENT 'Month (1-12)',
    month_name String COMMENT 'Month name (January, etc.)',
    month_short String COMMENT 'Month short name (Jan, Feb, etc.)',
    day Int8 COMMENT 'Day of month (1-31)',
    day_of_week Int8 COMMENT 'Day of week (1=Monday, 7=Sunday)',
    day_name String COMMENT 'Day name (Monday, Tuesday, etc.)',
    day_short String COMMENT 'Day short name (Mon, Tue, etc.)',
    is_weekend UInt8 COMMENT 'Weekend flag',
    is_holiday UInt8 COMMENT 'Holiday flag',
    week_of_year Int8 COMMENT 'ISO week number',
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (date_key, year, quarter, month)
PRIMARY KEY date_key
COMMENT 'Date dimension for time-based analysis';

-- =====================================================
-- Dimension: Material
-- =====================================================
DROP TABLE IF EXISTS dim_material;

CREATE TABLE dim_material
(
    material_key Int64 COMMENT 'Surrogate key',
    material_id Int64 COMMENT 'Source system material ID',
    material_code String COMMENT 'Material code from source',
    material_name String COMMENT 'Material name',
    material_type String COMMENT 'Material type/category',
    category String COMMENT 'Category: vaccine or non-vaccine',
    is_vaccine UInt8 COMMENT 'Vaccine flag',
    unit_of_distribution String COMMENT 'Unit of distribution',
    unit String COMMENT 'Base unit',
    pieces_per_unit Float64 DEFAULT 1 COMMENT 'Pieces per unit',
    temperature_sensitive Int8 COMMENT 'Temperature sensitive flag',
    temperature_min Float64 COMMENT 'Minimum temperature',
    temperature_max Float64 COMMENT 'Maximum temperature',
    is_active UInt8 DEFAULT 1 COMMENT 'Active flag',
    source_created_at Nullable(DateTime) COMMENT 'Original created timestamp',
    source_updated_at Nullable(DateTime) COMMENT 'Original updated timestamp',
    created_at DateTime DEFAULT now() COMMENT 'Data mart load timestamp',
    updated_at DateTime DEFAULT now() COMMENT 'Data mart update timestamp'
)
ENGINE = MergeTree()
ORDER BY (material_id, material_key)
PRIMARY KEY material_key
COMMENT 'Material dimension - vaccine and non-vaccine items';

-- =====================================================
-- Dimension: Entity
-- =====================================================
DROP TABLE IF EXISTS dim_entity;

CREATE TABLE dim_entity
(
    entity_key Int64 COMMENT 'Surrogate key',
    entity_id Int64 COMMENT 'Source system entity ID',
    entity_code String COMMENT 'Entity code',
    entity_name String COMMENT 'Entity name (facility name)',
    entity_type String COMMENT 'Entity type classification',
    type Int8 COMMENT 'Entity type code',
    status Int8 DEFAULT 1 COMMENT 'Entity status',
    regency_id String COMMENT 'Regency/City ID',
    province_id String COMMENT 'Province ID',
    address String COMMENT 'Entity address',
    lat String COMMENT 'Latitude',
    lng String COMMENT 'Longitude',
    postal_code String COMMENT 'Postal code',
    country String DEFAULT 'Indonesia' COMMENT 'Country',
    is_puskesmas Int8 DEFAULT 0 COMMENT 'Puskesmas flag',
    is_active UInt8 DEFAULT 1 COMMENT 'Active flag',
    source_created_at Nullable(DateTime) COMMENT 'Original created timestamp',
    source_updated_at Nullable(DateTime) COMMENT 'Original updated timestamp',
    created_at DateTime DEFAULT now() COMMENT 'Data mart load timestamp',
    updated_at DateTime DEFAULT now() COMMENT 'Data mart update timestamp'
)
ENGINE = MergeTree()
ORDER BY (entity_id, entity_key, province_id, regency_id)
PRIMARY KEY entity_key
COMMENT 'Entity dimension - healthcare facilities';

-- =====================================================
-- Dimension: Location (Geographic Hierarchy)
-- =====================================================
DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location
(
    location_key Int64 COMMENT 'Surrogate key',
    province_id String COMMENT 'Province ID',
    province_code String COMMENT 'Province code',
    province_name String COMMENT 'Province name',
    regency_id String COMMENT 'Regency/City ID',
    regency_code String COMMENT 'Regency code',
    regency_name String COMMENT 'Regency/City name',
    full_location String COMMENT 'Full location string (Province - Regency)',
    lat String COMMENT 'Latitude',
    lng String COMMENT 'Longitude',
    created_at DateTime DEFAULT now() COMMENT 'Data mart load timestamp',
    updated_at DateTime DEFAULT now() COMMENT 'Data mart update timestamp'
)
ENGINE = MergeTree()
ORDER BY (province_id, regency_id, location_key)
PRIMARY KEY location_key
COMMENT 'Location dimension - geographic hierarchy (Province → Regency/City)';

-- =====================================================
-- Dimension: Activity
-- =====================================================
DROP TABLE IF EXISTS dim_activity;

CREATE TABLE dim_activity
(
    activity_key Int64 COMMENT 'Surrogate key',
    activity_id Int64 COMMENT 'Source system activity ID',
    activity_code String COMMENT 'Activity code',
    activity_name String COMMENT 'Activity name',
    activity_type String COMMENT 'Activity type',
    is_ordered_sales Int8 DEFAULT 1 COMMENT 'Ordered sales flag',
    is_ordered_purchase Int8 DEFAULT 1 COMMENT 'Ordered purchase flag',
    is_active UInt8 DEFAULT 1 COMMENT 'Active flag',
    source_created_at Nullable(DateTime) COMMENT 'Original created timestamp',
    source_updated_at Nullable(DateTime) COMMENT 'Original updated timestamp',
    created_at DateTime DEFAULT now() COMMENT 'Data mart load timestamp',
    updated_at DateTime DEFAULT now() COMMENT 'Data mart update timestamp'
)
ENGINE = MergeTree()
ORDER BY (activity_id, activity_key)
PRIMARY KEY activity_key
COMMENT 'Activity dimension - program/activity types';

-- =====================================================
-- Dimension: Entity Tag
-- =====================================================
DROP TABLE IF EXISTS dim_entity_tag;

CREATE TABLE dim_entity_tag
(
    tag_key Int64 COMMENT 'Surrogate key',
    tag_id Int64 COMMENT 'Source system tag ID',
    tag_code String COMMENT 'Tag code',
    tag_name String COMMENT 'Tag name (e.g., Puskesmas, Dinkes Provinsi)',
    tag_category String COMMENT 'Tag category',
    is_active UInt8 DEFAULT 1 COMMENT 'Active flag',
    source_created_at Nullable(DateTime) COMMENT 'Original created timestamp',
    source_updated_at Nullable(DateTime) COMMENT 'Original updated timestamp',
    created_at DateTime DEFAULT now() COMMENT 'Data mart load timestamp',
    updated_at DateTime DEFAULT now() COMMENT 'Data mart update timestamp'
)
ENGINE = MergeTree()
ORDER BY (tag_id, tag_key)
PRIMARY KEY tag_key
COMMENT 'Entity tag dimension - entity classification (Puskesmas, Dinkes, etc.)';

-- =====================================================
-- Verify tables created
-- =====================================================
SHOW TABLES FROM datamart_badr_interactive LIKE 'dim_%';

SELECT 'Dimension tables created successfully!' AS status;
