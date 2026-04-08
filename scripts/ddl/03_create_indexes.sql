-- =====================================================
-- Data Mart Index Creation
-- Database: datamart_badr_interactive
-- Purpose: Create indexes for query optimization
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Indexes for fact_stock (Most Critical for Performance)
-- =====================================================

-- Single column indexes for foreign keys
CREATE INDEX idx_fact_stock_material_key ON fact_stock(material_key) 
COMMENT 'Index for material filtering';

CREATE INDEX idx_fact_stock_entity_key ON fact_stock(entity_key) 
COMMENT 'Index for entity filtering';

CREATE INDEX idx_fact_stock_date_key ON fact_stock(date_key) 
COMMENT 'Index for date range filtering';

CREATE INDEX idx_fact_stock_activity_key ON fact_stock(activity_key) 
COMMENT 'Index for activity filtering';

CREATE INDEX idx_fact_stock_tag_key ON fact_stock(tag_key) 
COMMENT 'Index for entity tag filtering';

CREATE INDEX idx_fact_stock_location_key ON fact_stock(location_key) 
COMMENT 'Index for location filtering';

-- Composite indexes for common query patterns
CREATE INDEX idx_fact_stock_date_material ON fact_stock(date_key, material_key) 
COMMENT 'Composite index for date + material queries';

CREATE INDEX idx_fact_stock_date_entity ON fact_stock(date_key, entity_key) 
COMMENT 'Composite index for date + entity queries';

CREATE INDEX idx_fact_stock_date_location ON fact_stock(date_key, location_key) 
COMMENT 'Composite index for date + location queries';

CREATE INDEX idx_fact_stock_material_entity ON fact_stock(material_key, entity_key) 
COMMENT 'Composite index for material + entity aggregation';

CREATE INDEX idx_fact_stock_tag_entity ON fact_stock(tag_key, entity_key) 
COMMENT 'Composite index for tag + entity queries';

CREATE INDEX idx_fact_stock_batch ON fact_stock(batch_id) 
COMMENT 'Index for batch lookups';

-- Index for year-based queries
CREATE INDEX idx_fact_stock_year ON fact_stock(year) 
COMMENT 'Index for year-based filtering';

-- =====================================================
-- Additional Indexes for Dimension Tables
-- =====================================================

-- dim_material indexes
CREATE INDEX idx_dim_material_category ON dim_material(category, is_vaccine) 
COMMENT 'Composite index for material category filtering';

CREATE INDEX idx_dim_material_name_search ON dim_material(material_name) 
COMMENT 'Index for material name search';

-- dim_entity indexes
CREATE INDEX idx_dim_entity_type ON dim_entity(entity_type, is_active) 
COMMENT 'Composite index for entity type filtering';

CREATE INDEX idx_dim_entity_name_search ON dim_entity(entity_name) 
COMMENT 'Index for entity name search';

CREATE INDEX idx_dim_entity_province ON dim_entity(province_id) 
COMMENT 'Index for province filtering';

CREATE INDEX idx_dim_entity_regency ON dim_entity(regency_id) 
COMMENT 'Index for regency filtering';

-- dim_location indexes
CREATE INDEX idx_dim_location_province_name ON dim_location(province_name) 
COMMENT 'Index for province name filtering';

CREATE INDEX idx_dim_location_regency_name ON dim_location(regency_name) 
COMMENT 'Index for regency name filtering';

-- dim_entity_tag indexes
CREATE INDEX idx_dim_entity_tag_name ON dim_entity_tag(tag_name) 
COMMENT 'Index for tag name filtering';

CREATE INDEX idx_dim_entity_tag_category ON dim_entity_tag(tag_category) 
COMMENT 'Index for tag category filtering';

-- dim_activity indexes
CREATE INDEX idx_dim_activity_name ON dim_activity(activity_name) 
COMMENT 'Index for activity name filtering';

-- dim_date indexes (already has indexes on creation)
-- Additional composite index for common date patterns
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month) 
COMMENT 'Composite index for year-month filtering';

CREATE INDEX idx_dim_date_year_quarter ON dim_date(year, quarter) 
COMMENT 'Composite index for year-quarter filtering';

-- =====================================================
-- Verify indexes created
-- =====================================================
SELECT 
    TABLE_NAME,
    INDEX_NAME,
    GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLUMNS,
    NON_UNIQUE,
    INDEX_TYPE
FROM INFORMATION_SCHEMA.STATISTICS
WHERE TABLE_SCHEMA = 'datamart_badr_interactive'
    AND TABLE_NAME IN ('fact_stock', 'dim_material', 'dim_entity', 'dim_location', 'dim_activity', 'dim_entity_tag', 'dim_date')
GROUP BY TABLE_NAME, INDEX_NAME, NON_UNIQUE, INDEX_TYPE
ORDER BY TABLE_NAME, INDEX_NAME;

SELECT 'Indexes created successfully!' AS status;
