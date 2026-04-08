-- =====================================================
-- Dashboard Queries
-- Database: datamart_badr_interactive
-- Purpose: SQL queries for dashboard visualization
-- Date: 2026-04-07
-- =====================================================

USE datamart_badr_interactive;

-- =====================================================
-- Query 1: Total Stock (Jumlah Stok)
-- Purpose: Calculate total stock quantity with filters
-- =====================================================

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


-- =====================================================
-- Query 2: Stock by Entity Tag (Stok per Tag Entitas)
-- Purpose: Show stock distribution by entity type
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material name filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    -- Entity type filter
    AND (:entity_type IS NULL OR de.entity_type = :entity_type)
GROUP BY det.tag_key, det.tag_name, det.tag_category
ORDER BY total_stock DESC;


-- =====================================================
-- Query 3: Stock by Material (Stok per Material)
-- Purpose: Show stock distribution for each material
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material name filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.material_key, dm.material_code, dm.material_name, 
         dm.material_type, dm.category, dm.is_vaccine, dm.unit
ORDER BY total_stock DESC;


-- =====================================================
-- Query 4: Stock by Location (Stok per Lokasi)
-- Purpose: Show stock distribution by geographic location
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dl.location_key, dl.province_id, dl.province_name, 
         dl.regency_id, dl.regency_name, dl.full_location
ORDER BY total_stock DESC;


-- =====================================================
-- Query 5: Stock by Activity (Stok per Kegiatan)
-- Purpose: Show stock distribution by activity type
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    AND da.activity_key IS NOT NULL
GROUP BY da.activity_key, da.activity_code, da.activity_name, da.activity_type
ORDER BY total_stock DESC;


-- =====================================================
-- Query 6: Stock Trend Over Time (Time Series)
-- Purpose: Show stock trend over time for charts
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dd.date_key, dd.full_date, dd.year, dd.quarter, dd.month, dd.month_name, dd.day
ORDER BY dd.full_date ASC;


-- =====================================================
-- Query 7: Stock Summary by Entity (Detail per Entity)
-- Purpose: Show detailed stock per entity
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    AND de.is_active = TRUE
GROUP BY de.entity_key, de.entity_id, de.entity_code, de.entity_name,
         de.entity_type, de.is_puskesmas, dl.province_name, 
         dl.regency_name, dl.full_location
ORDER BY total_stock DESC;


-- =====================================================
-- Query 8: Vaccine vs Non-Vaccine Summary
-- Purpose: Compare vaccine and non-vaccine stocks
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.category, dm.is_vaccine
ORDER BY total_stock DESC;


-- =====================================================
-- Query 9: Top 10 Materials by Stock
-- Purpose: Show top 10 materials with highest stock
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
GROUP BY dm.material_key, dm.material_code, dm.material_name,
         dm.category, dm.is_vaccine, dm.unit
ORDER BY total_stock DESC
LIMIT 10;


-- =====================================================
-- Query 10: Top 10 Entities by Stock
-- Purpose: Show top 10 entities with highest stock
-- =====================================================

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
    -- Date range filter
    AND fs.date_key BETWEEN :date_from AND :date_to
    -- Material type filter
    AND (:material_type IS NULL OR dm.category = :material_type)
    -- Material filter
    AND (:material_key IS NULL OR fs.material_key = :material_key)
    -- Province filter
    AND (:province_id IS NULL OR dl.province_id = :province_id)
    -- Regency/City filter
    AND (:regency_id IS NULL OR dl.regency_id = :regency_id)
    -- Entity tag filter
    AND (:tag_key IS NULL OR fs.tag_key = :tag_key)
    -- Activity filter
    AND (:activity_key IS NULL OR fs.activity_key = :activity_key)
    AND de.is_active = TRUE
GROUP BY de.entity_key, de.entity_id, de.entity_code, de.entity_name,
         de.entity_type, dl.province_name, dl.regency_name, dl.full_location
ORDER BY total_stock DESC
LIMIT 10;
