-- =====================================================
-- Data Mart Fact Table DDL - ClickHouse
-- Database: datamart_badr_interactive
-- Purpose: Create central fact table for stock data
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Fact Table: Stock
-- =====================================================
DROP TABLE IF EXISTS fact_stock;

CREATE TABLE fact_stock
(
    stock_key Int64 COMMENT 'Surrogate key',
    
    -- Foreign Keys to Dimensions
    material_key Int64 COMMENT 'FK to dim_material',
    entity_key Int64 COMMENT 'FK to dim_entity',
    date_key Int32 COMMENT 'FK to dim_date (stock date)',
    activity_key Nullable(Int64) COMMENT 'FK to dim_activity (optional)',
    tag_key Nullable(Int64) COMMENT 'FK to dim_entity_tag (optional)',
    location_key Nullable(Int64) COMMENT 'FK to dim_location (optional)',
    
    -- Stock Details
    batch_id Nullable(Int64) COMMENT 'Source batch ID',
    batch_number String COMMENT 'Batch number/code',
    stock_quantity Float64 COMMENT 'Stock quantity (qty from source)',
    unit String COMMENT 'Unit of measurement',
    allocated Float64 DEFAULT 0 COMMENT 'Allocated stock',
    in_transit Float64 DEFAULT 0 COMMENT 'Stock in transit',
    open_vial Int32 DEFAULT 0 COMMENT 'Open vial count',
    
    -- Extermination Data
    extermination_discard_qty Float64 DEFAULT 0 COMMENT 'Discarded quantity',
    extermination_received_qty Float64 DEFAULT 0 COMMENT 'Received extermination qty',
    extermination_qty Float64 DEFAULT 0 COMMENT 'Extermination qty',
    extermination_shipped_qty Float64 DEFAULT 0 COMMENT 'Shipped extermination qty',
    
    -- Additional Details
    expiry_date Nullable(Date) COMMENT 'Batch expiry date',
    budget_source Nullable(Int32) COMMENT 'Budget source',
    year Nullable(Int16) COMMENT 'Stock year',
    price Nullable(Float64) COMMENT 'Unit price',
    total_price Nullable(Float64) COMMENT 'Total price (qty * price)',
    
    -- Source References
    source_stock_id Nullable(Int64) COMMENT 'Source stock record ID',
    source_batch_id Nullable(Int64) COMMENT 'Source batch record ID',
    source_entity_material_id Nullable(Int64) COMMENT 'Source entity-material mapping ID',
    
    -- Metadata
    created_at DateTime DEFAULT now() COMMENT 'Record creation time',
    updated_at DateTime DEFAULT now() COMMENT 'Record update time'
)
ENGINE = MergeTree()
ORDER BY (date_key, material_key, entity_key, stock_key)
PRIMARY KEY (date_key, material_key, entity_key)
PARTITION BY toYYYYMM(full_date)
COMMENT 'Fact table - Stock quantities with dimensional references';

-- Note: For partitioning by date, we need to add full_date column
-- Alternative without full_date: PARTITION BY toInt64(date_key / 100)  -- Monthly partition

-- =====================================================
-- Verify table created
-- =====================================================
SHOW TABLES FROM datamart_badr_interactive LIKE 'fact_stock';

DESCRIBE TABLE fact_stock;

SELECT 'Fact table created successfully!' AS status;
