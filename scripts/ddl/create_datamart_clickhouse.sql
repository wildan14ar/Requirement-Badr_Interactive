CREATE DATABASE IF NOT EXISTS datamart_badr_interactive;

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_date
(
    date_key UInt32,
    full_date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    month_name LowCardinality(String),
    month_short LowCardinality(String),
    day UInt8,
    day_name LowCardinality(String),
    day_short LowCardinality(String),
    week_of_year UInt8,
    is_weekend UInt8,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (date_key)
PARTITION BY toYYYYMM(full_date);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_location
(
    province_id String,
    province_name LowCardinality(String),
    regency_id String,
    regency_name LowCardinality(String),
    full_location LowCardinality(String),
    lat Nullable(String),
    lng Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (province_id, regency_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_material
(
    material_id UInt32,
    material_code Nullable(String),
    material_name LowCardinality(String),
    category LowCardinality(String),
    is_vaccine UInt8,
    unit Nullable(String),
    unit_of_distribution Nullable(String),
    pieces_per_unit Float64,
    temperature_sensitive UInt8,
    temperature_min Nullable(Float64),
    temperature_max Nullable(Float64),
    status UInt8,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (material_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_entity
(
    entity_id UInt32,
    entity_code Nullable(String),
    entity_name LowCardinality(String),
    entity_type LowCardinality(String),
    type_code Nullable(UInt8),
    status UInt8,
    is_puskesmas UInt8,
    province_id Nullable(String),
    regency_id Nullable(String),
    postal_code Nullable(String),
    country Nullable(String),
    lat Nullable(String),
    lng Nullable(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (entity_id)
PARTITION BY intDiv(entity_id, 100000);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_activity
(
    activity_id UInt32,
    activity_code Nullable(String),
    activity_name LowCardinality(String),
    is_ordered_sales UInt8,
    is_ordered_purchase UInt8,
    is_patient_id UInt8,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (activity_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.dim_entity_tag
(
    tag_id UInt32,
    tag_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (tag_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.bridge_entity_tag
(
    entity_id UInt32,
    tag_id UInt32,
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (entity_id, tag_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.bridge_entity_activity
(
    entity_id UInt32,
    activity_id UInt32,
    join_date Nullable(Date),
    end_date Nullable(Date),
    created_at DateTime,
    updated_at DateTime
)
ENGINE = MergeTree
ORDER BY (entity_id, activity_id);

CREATE TABLE IF NOT EXISTS datamart_badr_interactive.fact_stock_snapshot
(
    stock_id UInt64,
    stock_date Date,
    date_key UInt32,
    batch_id Nullable(UInt64),
    batch_code Nullable(String),
    batch_expired_date Nullable(Date),
    entity_id Nullable(UInt32),
    material_id Nullable(UInt32),
    activity_id Nullable(UInt32),
    province_id Nullable(String),
    regency_id Nullable(String),
    stock_quantity Float64,
    allocated Float64,
    in_transit Float64,
    open_vial Float64,
    extermination_discard_qty Float64,
    extermination_received_qty Float64,
    extermination_qty Float64,
    extermination_shipped_qty Float64,
    budget_source Nullable(Int32),
    stock_year Nullable(Int32),
    price Nullable(Float64),
    total_price Nullable(Float64),
    source_created_at DateTime,
    source_updated_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(stock_date)
ORDER BY (stock_date, province_id, regency_id, entity_id, material_id, activity_id, stock_id);
