-- =====================================================
-- Data Mart Fact Table DDL
-- Database: datamart_badr_interactive
-- Purpose: Create central fact table for stock data
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Fact Table: Stock
-- =====================================================
DROP TABLE IF EXISTS fact_stock;

CREATE TABLE fact_stock (
    stock_key BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    
    -- Foreign Keys to Dimensions
    material_key INT NOT NULL COMMENT 'FK to dim_material',
    entity_key INT NOT NULL COMMENT 'FK to dim_entity',
    date_key INT NOT NULL COMMENT 'FK to dim_date (stock date)',
    activity_key INT COMMENT 'FK to dim_activity (optional)',
    tag_key INT COMMENT 'FK to dim_entity_tag (optional)',
    location_key INT COMMENT 'FK to dim_location (optional)',
    
    -- Stock Details
    batch_id BIGINT COMMENT 'Source batch ID',
    batch_number VARCHAR(100) COMMENT 'Batch number/code',
    stock_quantity DECIMAL(15,2) NOT NULL COMMENT 'Stock quantity (qty from source)',
    unit VARCHAR(50) COMMENT 'Unit of measurement',
    allocated DECIMAL(15,2) DEFAULT 0 COMMENT 'Allocated stock',
    in_transit DECIMAL(15,2) DEFAULT 0 COMMENT 'Stock in transit',
    open_vial INT DEFAULT 0 COMMENT 'Open vial count',
    
    -- Extermination Data
    extermination_discard_qty DECIMAL(15,2) DEFAULT 0 COMMENT 'Discarded quantity',
    extermination_received_qty DECIMAL(15,2) DEFAULT 0 COMMENT 'Received extermination qty',
    extermination_qty DECIMAL(15,2) DEFAULT 0 COMMENT 'Extermination qty',
    extermination_shipped_qty DECIMAL(15,2) DEFAULT 0 COMMENT 'Shipped extermination qty',
    
    -- Additional Details
    expiry_date DATE COMMENT 'Batch expiry date',
    budget_source INT COMMENT 'Budget source',
    year INT COMMENT 'Stock year',
    price DECIMAL(15,2) COMMENT 'Unit price',
    total_price DECIMAL(15,2) COMMENT 'Total price (qty * price)',
    
    -- Source References
    source_stock_id BIGINT COMMENT 'Source stock record ID',
    source_batch_id BIGINT COMMENT 'Source batch record ID',
    source_entity_material_id BIGINT COMMENT 'Source entity-material mapping ID',
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Record creation time',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Record update time',
    
    -- Foreign Key Constraints
    CONSTRAINT fk_fact_stock_material FOREIGN KEY (material_key) REFERENCES dim_material(material_key),
    CONSTRAINT fk_fact_stock_entity FOREIGN KEY (entity_key) REFERENCES dim_entity(entity_key),
    CONSTRAINT fk_fact_stock_date FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fk_fact_stock_activity FOREIGN KEY (activity_key) REFERENCES dim_activity(activity_key),
    CONSTRAINT fk_fact_stock_tag FOREIGN KEY (tag_key) REFERENCES dim_entity_tag(tag_key),
    CONSTRAINT fk_fact_stock_location FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
    
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Fact table - Stock quantities with dimensional references';

-- =====================================================
-- Verify table created
-- =====================================================
SHOW TABLES LIKE 'fact_stock';

DESCRIBE fact_stock;

SELECT 'Fact table created successfully!' AS status;
