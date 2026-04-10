-- Total stock card
SELECT
    COALESCE(SUM(fs.stock_quantity), 0) AS total_stock
FROM fact_stock_snapshot fs
LEFT JOIN dim_material dm ON fs.material_id = dm.material_id
LEFT JOIN dim_entity de ON fs.entity_id = de.entity_id
WHERE fs.stock_date BETWEEN :date_from AND :date_to
  AND (:activity_id IS NULL OR fs.activity_id = :activity_id)
  AND (:material_type IS NULL OR dm.category = :material_type)
  AND (:material_id IS NULL OR fs.material_id = :material_id)
  AND (
      :tag_id IS NULL
      OR EXISTS (
          SELECT 1
          FROM bridge_entity_tag bet_filter
          WHERE bet_filter.entity_id = fs.entity_id
            AND bet_filter.tag_id = :tag_id
      )
  )
  AND (:province_id IS NULL OR de.province_id = :province_id)
  AND (:regency_id IS NULL OR de.regency_id = :regency_id)
  AND (:entity_type IS NULL OR de.entity_type = :entity_type)
  AND (:information_type IS NULL OR :information_type = 'Sisa Stok');

-- Stock per entity tag
SELECT
    det.tag_id,
    det.tag_name,
    COUNT(DISTINCT fs.entity_id) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock
FROM fact_stock_snapshot fs
JOIN bridge_entity_tag bet ON fs.entity_id = bet.entity_id
JOIN dim_entity_tag det ON bet.tag_id = det.tag_id
LEFT JOIN dim_material dm ON fs.material_id = dm.material_id
LEFT JOIN dim_entity de ON fs.entity_id = de.entity_id
WHERE fs.stock_date BETWEEN :date_from AND :date_to
  AND (:activity_id IS NULL OR fs.activity_id = :activity_id)
  AND (:material_type IS NULL OR dm.category = :material_type)
  AND (:material_id IS NULL OR fs.material_id = :material_id)
  AND (:tag_id IS NULL OR bet.tag_id = :tag_id)
  AND (:province_id IS NULL OR de.province_id = :province_id)
  AND (:regency_id IS NULL OR de.regency_id = :regency_id)
  AND (:entity_type IS NULL OR de.entity_type = :entity_type)
GROUP BY det.tag_id, det.tag_name
ORDER BY total_stock DESC, det.tag_name;

-- Stock per material
SELECT
    dm.material_id,
    dm.material_name,
    dm.category,
    dm.is_vaccine,
    COUNT(DISTINCT fs.entity_id) AS entity_count,
    SUM(fs.stock_quantity) AS total_stock
FROM fact_stock_snapshot fs
JOIN dim_material dm ON fs.material_id = dm.material_id
LEFT JOIN dim_entity de ON fs.entity_id = de.entity_id
WHERE fs.stock_date BETWEEN :date_from AND :date_to
  AND (:activity_id IS NULL OR fs.activity_id = :activity_id)
  AND (:material_type IS NULL OR dm.category = :material_type)
  AND (:material_id IS NULL OR fs.material_id = :material_id)
  AND (
      :tag_id IS NULL
      OR EXISTS (
          SELECT 1
          FROM bridge_entity_tag bet_filter
          WHERE bet_filter.entity_id = fs.entity_id
            AND bet_filter.tag_id = :tag_id
      )
  )
  AND (:province_id IS NULL OR de.province_id = :province_id)
  AND (:regency_id IS NULL OR de.regency_id = :regency_id)
  AND (:entity_type IS NULL OR de.entity_type = :entity_type)
GROUP BY dm.material_id, dm.material_name, dm.category, dm.is_vaccine
ORDER BY total_stock DESC, dm.material_name;
