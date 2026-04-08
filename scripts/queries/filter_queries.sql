-- =====================================================
-- Filter Dropdown Queries
-- Database: datamart_badr_interactive
-- Purpose: Queries to populate filter dropdowns in dashboard
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Filter 1: Get Materials for Dropdown
-- =====================================================

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


-- =====================================================
-- Filter 2: Get Entity Tags for Dropdown
-- =====================================================

SELECT DISTINCT
    det.tag_key,
    det.tag_name,
    det.tag_category
FROM dim_entity_tag det
INNER JOIN fact_stock fs ON det.tag_key = fs.tag_key
WHERE det.is_active = TRUE
ORDER BY det.tag_name;


-- =====================================================
-- Filter 3: Get Provinces for Dropdown
-- =====================================================

SELECT DISTINCT
    dl.province_id,
    dl.province_name
FROM dim_location dl
INNER JOIN fact_stock fs ON dl.location_key = fs.location_key
WHERE dl.province_id IS NOT NULL
ORDER BY dl.province_name;


-- =====================================================
-- Filter 4: Get Regencies/Cities for Dropdown (filtered by province)
-- =====================================================

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


-- =====================================================
-- Filter 5: Get Activities for Dropdown
-- =====================================================

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


-- =====================================================
-- Filter 6: Get Date Range (Min and Max dates in data)
-- =====================================================

SELECT 
    MIN(dd.full_date) AS min_date,
    MAX(dd.full_date) AS max_date,
    MIN(fs.date_key) AS min_date_key,
    MAX(fs.date_key) AS max_date_key
FROM fact_stock fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key;


-- =====================================================
-- Filter 7: Get Material Types (Vaccine/Non-Vaccine)
-- =====================================================

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


-- =====================================================
-- Filter 8: Get Entity Types for Dropdown
-- =====================================================

SELECT DISTINCT
    de.entity_type,
    COUNT(DISTINCT de.entity_key) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock
FROM dim_entity de
INNER JOIN fact_stock fs ON de.entity_key = fs.entity_key
WHERE de.is_active = TRUE
GROUP BY de.entity_type
ORDER BY de.entity_type;


-- =====================================================
-- Filter 9: Hierarchical Location (Province → Regency → Entity)
-- =====================================================

-- Get all locations with hierarchy
SELECT 
    dl.province_id,
    dl.province_name,
    dl.regency_id,
    dl.regency_name,
    dl.full_location
FROM dim_location dl
WHERE dl.province_id IS NOT NULL
ORDER BY dl.province_name, dl.regency_name;


-- =====================================================
-- Filter 10: Get Entities by Location (for cascading dropdown)
-- =====================================================

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


-- =====================================================
-- Filter 11: Get Years Available in Data
-- =====================================================

SELECT DISTINCT
    dd.year,
    COUNT(DISTINCT fs.stock_key) AS stock_records,
    SUM(fs.stock_quantity) AS total_stock
FROM fact_stock fs
INNER JOIN dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year
ORDER BY dd.year DESC;


-- =====================================================
-- Filter 12: Get Material by Category (Cascading)
-- =====================================================

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
