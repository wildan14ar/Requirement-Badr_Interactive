-- =====================================================
-- Data Mart Dimension Tables DDL
-- Database: datamart_badr_interactive
-- Purpose: Create all dimension tables for star schema
-- Date: 2026-04-07
-- =====================================================

-- Create database
CREATE DATABASE IF NOT EXISTS datamart_badr_interactive 
DEFAULT CHARACTER SET utf8mb4 
DEFAULT COLLATE utf8mb4_unicode_ci;

USE datamart_badr_interactive;

-- =====================================================
-- Dimension: Date
-- =====================================================
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY COMMENT 'YYYYMMDD format',
    full_date DATE NOT NULL COMMENT 'Complete date',
    year INT NOT NULL COMMENT 'Year (e.g., 2024)',
    quarter INT NOT NULL COMMENT 'Quarter (1-4)',
    month INT NOT NULL COMMENT 'Month (1-12)',
    month_name VARCHAR(20) NOT NULL COMMENT 'Month name (January, etc.)',
    month_short VARCHAR(3) NOT NULL COMMENT 'Month short name (Jan, Feb, etc.)',
    day INT NOT NULL COMMENT 'Day of month (1-31)',
    day_of_week INT NOT NULL COMMENT 'Day of week (1=Monday, 7=Sunday)',
    day_name VARCHAR(20) NOT NULL COMMENT 'Day name (Monday, Tuesday, etc.)',
    day_short VARCHAR(3) NOT NULL COMMENT 'Day short name (Mon, Tue, etc.)',
    is_weekend BOOLEAN DEFAULT FALSE COMMENT 'Weekend flag',
    is_holiday BOOLEAN DEFAULT FALSE COMMENT 'Holiday flag (can be populated later)',
    week_of_year INT COMMENT 'ISO week number',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_full_date (full_date),
    INDEX idx_year (year),
    INDEX idx_month (month),
    INDEX idx_quarter (quarter)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Date dimension for time-based analysis';

-- =====================================================
-- Dimension: Material
-- =====================================================
DROP TABLE IF EXISTS dim_material;

CREATE TABLE dim_material (
    material_key INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    material_id INT NOT NULL COMMENT 'Source system material ID',
    material_code VARCHAR(50) COMMENT 'Material code from source',
    material_name VARCHAR(255) NOT NULL COMMENT 'Material name',
    material_type VARCHAR(100) COMMENT 'Material type/category',
    category VARCHAR(50) COMMENT 'Category: vaccine or non-vaccine',
    is_vaccine BOOLEAN DEFAULT FALSE COMMENT 'Vaccine flag (1=vaccine, 0=non-vaccine)',
    unit_of_distribution VARCHAR(50) COMMENT 'Unit of distribution',
    unit VARCHAR(50) COMMENT 'Base unit',
    pieces_per_unit DECIMAL(10,2) DEFAULT 1 COMMENT 'Pieces per unit',
    temperature_sensitive TINYINT COMMENT 'Temperature sensitive flag',
    temperature_min FLOAT COMMENT 'Minimum temperature',
    temperature_max FLOAT COMMENT 'Maximum temperature',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Active flag',
    source_created_at DATETIME COMMENT 'Original created timestamp',
    source_updated_at DATETIME COMMENT 'Original updated timestamp',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data mart load timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data mart update timestamp',
    UNIQUE KEY uk_material_id (material_id),
    INDEX idx_material_code (material_code),
    INDEX idx_material_name (material_name),
    INDEX idx_category (category),
    INDEX idx_is_vaccine (is_vaccine),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Material dimension - vaccine and non-vaccine items';

-- =====================================================
-- Dimension: Entity
-- =====================================================
DROP TABLE IF EXISTS dim_entity;

CREATE TABLE dim_entity (
    entity_key INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    entity_id INT NOT NULL COMMENT 'Source system entity ID',
    entity_code VARCHAR(50) COMMENT 'Entity code',
    entity_name VARCHAR(255) NOT NULL COMMENT 'Entity name (facility name)',
    entity_type VARCHAR(100) COMMENT 'Entity type classification',
    type TINYINT COMMENT 'Entity type code',
    status TINYINT DEFAULT 1 COMMENT 'Entity status',
    regency_id VARCHAR(50) COMMENT 'Regency/City ID',
    province_id VARCHAR(50) COMMENT 'Province ID',
    address TEXT COMMENT 'Entity address',
    lat VARCHAR(50) COMMENT 'Latitude',
    lng VARCHAR(50) COMMENT 'Longitude',
    postal_code VARCHAR(20) COMMENT 'Postal code',
    country VARCHAR(100) DEFAULT 'Indonesia' COMMENT 'Country',
    is_puskesmas TINYINT DEFAULT 0 COMMENT 'Puskesmas flag',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Active flag',
    source_created_at DATETIME COMMENT 'Original created timestamp',
    source_updated_at DATETIME COMMENT 'Original updated timestamp',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data mart load timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data mart update timestamp',
    UNIQUE KEY uk_entity_id (entity_id),
    INDEX idx_entity_code (entity_code),
    INDEX idx_entity_name (entity_name),
    INDEX idx_entity_type (entity_type),
    INDEX idx_regency_id (regency_id),
    INDEX idx_province_id (province_id),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Entity dimension - healthcare facilities';

-- =====================================================
-- Dimension: Location (Geographic Hierarchy)
-- =====================================================
DROP TABLE IF EXISTS dim_location;

CREATE TABLE dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    province_id VARCHAR(50) COMMENT 'Province ID',
    province_code VARCHAR(50) COMMENT 'Province code',
    province_name VARCHAR(255) COMMENT 'Province name',
    regency_id VARCHAR(50) COMMENT 'Regency/City ID',
    regency_code VARCHAR(50) COMMENT 'Regency code',
    regency_name VARCHAR(255) COMMENT 'Regency/City name',
    full_location VARCHAR(500) COMMENT 'Full location string (Province - Regency)',
    lat VARCHAR(50) COMMENT 'Latitude',
    lng VARCHAR(50) COMMENT 'Longitude',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data mart load timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data mart update timestamp',
    UNIQUE KEY uk_location (province_id, regency_id),
    INDEX idx_province_id (province_id),
    INDEX idx_province_name (province_name),
    INDEX idx_regency_id (regency_id),
    INDEX idx_regency_name (regency_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Location dimension - geographic hierarchy (Province → Regency/City)';

-- =====================================================
-- Dimension: Activity
-- =====================================================
DROP TABLE IF EXISTS dim_activity;

CREATE TABLE dim_activity (
    activity_key INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    activity_id INT NOT NULL COMMENT 'Source system activity ID',
    activity_code VARCHAR(50) COMMENT 'Activity code',
    activity_name VARCHAR(255) NOT NULL COMMENT 'Activity name',
    activity_type VARCHAR(100) COMMENT 'Activity type',
    is_ordered_sales TINYINT DEFAULT 1 COMMENT 'Ordered sales flag',
    is_ordered_purchase TINYINT DEFAULT 1 COMMENT 'Ordered purchase flag',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Active flag',
    source_created_at DATETIME COMMENT 'Original created timestamp',
    source_updated_at DATETIME COMMENT 'Original updated timestamp',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data mart load timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data mart update timestamp',
    UNIQUE KEY uk_activity_id (activity_id),
    INDEX idx_activity_code (activity_code),
    INDEX idx_activity_name (activity_name),
    INDEX idx_activity_type (activity_type),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Activity dimension - program/activity types';

-- =====================================================
-- Dimension: Entity Tag
-- =====================================================
DROP TABLE IF EXISTS dim_entity_tag;

CREATE TABLE dim_entity_tag (
    tag_key INT AUTO_INCREMENT PRIMARY KEY COMMENT 'Surrogate key',
    tag_id INT NOT NULL COMMENT 'Source system tag ID',
    tag_code VARCHAR(50) COMMENT 'Tag code',
    tag_name VARCHAR(255) NOT NULL COMMENT 'Tag name (e.g., Puskesmas, Dinkes Provinsi)',
    tag_category VARCHAR(100) COMMENT 'Tag category',
    is_active BOOLEAN DEFAULT TRUE COMMENT 'Active flag',
    source_created_at DATETIME COMMENT 'Original created timestamp',
    source_updated_at DATETIME COMMENT 'Original updated timestamp',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Data mart load timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Data mart update timestamp',
    UNIQUE KEY uk_tag_id (tag_id),
    INDEX idx_tag_code (tag_code),
    INDEX idx_tag_name (tag_name),
    INDEX idx_tag_category (tag_category),
    INDEX idx_is_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci 
COMMENT='Entity tag dimension - entity classification (Puskesmas, Dinkes, etc.)';

-- =====================================================
-- Verify tables created
-- =====================================================
SHOW TABLES LIKE 'dim_%';

SELECT 'Dimension tables created successfully!' AS status;
