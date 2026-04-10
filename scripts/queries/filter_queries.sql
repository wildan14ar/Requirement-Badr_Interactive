-- Material dropdown
SELECT
    material_id,
    material_name,
    category,
    is_vaccine
FROM dim_material
ORDER BY material_name;

-- Activity dropdown
SELECT
    activity_id,
    activity_name
FROM dim_activity
ORDER BY activity_name;

-- Entity tag dropdown
SELECT
    tag_id,
    tag_name
FROM dim_entity_tag
ORDER BY tag_name;

-- Province dropdown
SELECT DISTINCT
    province_id,
    province_name
FROM dim_location
ORDER BY province_name;

-- Regency dropdown filtered by province
SELECT DISTINCT
    regency_id,
    regency_name
FROM dim_location
WHERE province_id = :province_id
ORDER BY regency_name;

-- Entity type dropdown
SELECT DISTINCT
    entity_type
FROM dim_entity
ORDER BY entity_type;
