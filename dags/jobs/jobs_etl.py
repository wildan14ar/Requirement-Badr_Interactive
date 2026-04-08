"""
Stock ETL Jobs - PySpark
=========================
Extract from MySQL → Transform → Load to ClickHouse

Uses configuration-driven approach with ETL_JOBS array.
Each job defines: name, sql, target_table, method, conflict_columns

Usage:
    python jobs_etl.py \
        --source_type mysql \
        --source_host 10.10.0.30 \
        --source_port 3306 \
        --source_user devel \
        --source_password recruitment2024 \
        --source_db recruitment_dev \
        --target_type clickhouse \
        --target_host localhost \
        --target_port 8123 \
        --target_user default \
        --target_password "" \
        --target_db datamart_badr_interactive
"""

import argparse
import sys
import os
from datetime import date, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# ETL JOBS CONFIGURATION - ORDERED BY PHASE & DEPENDENCY
# =============================================================================
# Each job defines:
#   - name: Job identifier
#   - sql: SQL query to extract & transform data
#   - target_table: Target table in ClickHouse
#   - method: upsert | insert | overwrite
#   - conflict_columns: Columns for conflict resolution (upsert only)
# =============================================================================

ETL_JOBS = [
    # =========================================================================
    # PHASE 1: DIMENSION TABLES (No Dependencies)
    # =========================================================================

    # dim_date - Generated programmatically
    {
        "name": "dim_date",
        "execution_engine": "python",
        "target_table": "dim_date",
        "method": "overwrite",
    },

    # dim_location - Merged from provinces + regencies
    {
        "name": "dim_location",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY p.id, r.id) AS location_key,
                p.id AS province_id,
                '' AS province_code,
                p.name AS province_name,
                r.id AS regency_id,
                '' AS regency_code,
                r.name AS regency_name,
                CONCAT(p.name, ' - ', r.name) AS full_location,
                r.lat,
                r.lng,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM regencies r
            LEFT JOIN provinces p ON r.province_id = p.id
        """,
        "target_table": "dim_location",
        "method": "overwrite",
    },

    # dim_material - From master_materials
    {
        "name": "dim_material",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY id) AS material_key,
                id AS material_id,
                code AS material_code,
                COALESCE(name, 'Unknown') AS material_name,
                '' AS material_type,
                CASE WHEN is_vaccine = 1 THEN 'Vaccine' ELSE 'Non-Vaccine' END AS category,
                CASE WHEN is_vaccine = 1 THEN TRUE ELSE FALSE END AS is_vaccine,
                unit_of_distribution,
                unit,
                pieces_per_unit,
                temperature_sensitive,
                temperature_min,
                temperature_max,
                CASE WHEN status = 1 THEN TRUE ELSE FALSE END AS is_active,
                created_at AS source_created_at,
                updated_at AS source_updated_at,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM master_materials
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_material",
        "method": "overwrite",
    },

    # dim_entity - From entities
    {
        "name": "dim_entity",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY id) AS entity_key,
                id AS entity_id,
                code AS entity_code,
                COALESCE(name, 'Unknown Facility') AS entity_name,
                CASE type
                    WHEN 1 THEN 'Dinas Kesehatan Provinsi'
                    WHEN 2 THEN 'Dinas Kesehatan Kabupaten/Kota'
                    WHEN 3 THEN 'Puskesmas'
                    WHEN 4 THEN 'Rumah Sakit'
                    WHEN 5 THEN 'Klinik'
                    WHEN 6 THEN 'Apotek'
                    WHEN 7 THEN 'Laboratorium'
                    ELSE 'Lainnya'
                END AS entity_type,
                type,
                status,
                COALESCE(province_id, '') AS province_id,
                COALESCE(regency_id, '') AS regency_id,
                address,
                lat,
                lng,
                postal_code,
                COALESCE(country, 'Indonesia') AS country,
                is_puskesmas,
                CASE WHEN status = 1 THEN TRUE ELSE FALSE END AS is_active,
                created_at AS source_created_at,
                updated_at AS source_updated_at,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM entities
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_entity",
        "method": "overwrite",
    },

    # dim_activity - From master_activities
    {
        "name": "dim_activity",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY id) AS activity_key,
                id AS activity_id,
                code AS activity_code,
                COALESCE(name, 'Unknown Activity') AS activity_name,
                '' AS activity_type,
                is_ordered_sales,
                is_ordered_purchase,
                TRUE AS is_active,
                created_at AS source_created_at,
                updated_at AS source_updated_at,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM master_activities
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_activity",
        "method": "overwrite",
    },

    # dim_entity_tag - From entity_tags
    {
        "name": "dim_entity_tag",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY id) AS tag_key,
                id AS tag_id,
                '' AS tag_code,
                COALESCE(title, 'Unknown Tag') AS tag_name,
                '' AS tag_category,
                TRUE AS is_active,
                created_at AS source_created_at,
                updated_at AS source_updated_at,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM entity_tags
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_entity_tag",
        "method": "overwrite",
    },

    # =========================================================================
    # PHASE 2: FACT TABLE (Depends on all dimensions)
    # =========================================================================

    # fact_stock - Main fact table with dimension key mappings
    {
        "name": "fact_stock",
        "sql": """
            SELECT
                ROW_NUMBER() OVER (ORDER BY s.id) AS stock_key,
                COALESCE(dm.material_key, 0) AS material_key,
                COALESCE(de.entity_key, 0) AS entity_key,
                CAST(DATE_FORMAT(s.createdAt, 'yyyyMMdd') AS INT) AS date_key,
                da.activity_key,
                NULL AS tag_key,
                dl.location_key,
                s.batch_id,
                COALESCE(b.code, '') AS batch_number,
                COALESCE(s.qty, 0) AS stock_quantity,
                '' AS unit,
                COALESCE(s.allocated, 0) AS allocated,
                COALESCE(s.in_transit, 0) AS in_transit,
                COALESCE(s.open_vial, 0) AS open_vial,
                COALESCE(s.extermination_discard_qty, 0) AS extermination_discard_qty,
                COALESCE(s.extermination_received_qty, 0) AS extermination_received_qty,
                COALESCE(s.extermination_qty, 0) AS extermination_qty,
                COALESCE(s.extermination_shipped_qty, 0) AS extermination_shipped_qty,
                b.expired_date AS expiry_date,
                s.budget_source,
                s.year,
                s.price,
                s.total_price,
                s.id AS source_stock_id,
                s.batch_id AS source_batch_id,
                em.id AS source_entity_material_id,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM stocks s
            LEFT JOIN batches b ON s.batch_id = b.id
            LEFT JOIN entity_has_master_materials em ON s.entity_has_material_id = em.id
            LEFT JOIN dim_material dm ON em.master_material_id = dm.material_id
            LEFT JOIN dim_entity de ON em.entity_id = de.entity_id
            LEFT JOIN dim_activity da ON s.activity_id = da.activity_id
            LEFT JOIN dim_location dl ON de.province_id = dl.province_id 
                AND de.regency_id = dl.regency_id
            WHERE s.deleted_at IS NULL
        """,
        "target_table": "fact_stock",
        "method": "overwrite",
    },
]


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_jdbc_url(db_type, host, port, database, extra_params=None):
    """Build JDBC URL for different database types"""
    if db_type == "mysql":
        params = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
        if extra_params:
            params += f"&{extra_params}"
        return f"jdbc:mysql://{host}:{port}/{database}?{params}"
    elif db_type == "clickhouse":
        return f"jdbc:clickhouse://{host}:{port}/{database}"
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def jdbc_read(spark, jdbc_url, table, properties):
    """Read data from JDBC source"""
    return spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", f"({table}) AS temp") \
        .options(**properties) \
        .load()


def write_to_clickhouse(df, jdbc_url, table, properties, mode="overwrite"):
    """Write DataFrame to ClickHouse"""
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table) \
        .options(**properties) \
        .mode(mode) \
        .save()


def create_dim_date(spark):
    """Generate date dimension programmatically (2020-2030)"""
    dates = []
    start = date(2020, 1, 1)
    end = date(2030, 12, 31)
    delta = timedelta(days=1)
    
    curr = start
    while curr <= end:
        dates.append({
            "date_key": int(curr.strftime("%Y%m%d")),
            "full_date": curr,
            "year": curr.year,
            "quarter": (curr.month - 1) // 3 + 1,
            "month": curr.month,
            "month_name": curr.strftime("%B"),
            "month_short": curr.strftime("%b"),
            "day": curr.day,
            "day_of_week": curr.isoweekday(),
            "day_name": curr.strftime("%A"),
            "day_short": curr.strftime("%a"),
            "is_weekend": curr.isoweekday() >= 6,
            "is_holiday": False,
            "week_of_year": curr.isocalendar()[1],
            "created_at": date.today(),
            "updated_at": date.today(),
        })
        curr += delta
    
    return spark.createDataFrame(dates)


# =============================================================================
# MAIN ETL EXECUTION
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Stock ETL Pipeline")
    parser.add_argument("--source_type", required=True, choices=["mysql"])
    parser.add_argument("--source_host", required=True)
    parser.add_argument("--source_port", required=True)
    parser.add_argument("--source_user", required=True)
    parser.add_argument("--source_password", required=True)
    parser.add_argument("--source_db", required=True)
    
    parser.add_argument("--target_type", required=True, choices=["clickhouse"])
    parser.add_argument("--target_host", required=True)
    parser.add_argument("--target_port", required=True)
    parser.add_argument("--target_user", required=True)
    parser.add_argument("--target_password", required=True)
    parser.add_argument("--target_db", required=True)
    
    args = parser.parse_args()

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("stock_etl") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33,com.clickhouse:clickhouse-jdbc:0.4.6") \
        .getOrCreate()

    print("=" * 80)
    print("🚀 STARTING STOCK ETL PIPELINE")
    print("=" * 80)
    print(f"Source: {args.source_type}://{args.source_host}:{args.source_port}/{args.source_db}")
    print(f"Target: {args.target_type}://{args.target_host}:{args.target_port}/{args.target_db}")
    print(f"Jobs: {len(ETL_JOBS)}")
    print("=" * 80)

    # =========================================================================
    # SETUP CONNECTIONS
    # =========================================================================
    source_jdbc_url = build_jdbc_url(
        args.source_type, args.source_host, args.source_port, args.source_db
    )
    
    source_properties = {
        "user": args.source_user,
        "password": args.source_password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    
    target_jdbc_url = build_jdbc_url(
        args.target_type, args.target_host, args.target_port, args.target_db
    )
    
    target_properties = {
        "user": args.target_user,
        "password": args.target_password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }

    # =========================================================================
    # EXECUTE ETL JOBS
    # =========================================================================
    for idx, job in enumerate(ETL_JOBS, 1):
        print(f"\n[{idx}/{len(ETL_JOBS)}] 📦 Job: {job['name']}")
        print(f"  Target: {job['target_table']}")
        print(f"  Method: {job['method']}")
        
        try:
            # Special handling for dim_date (generated in Python)
            if job.get("execution_engine") == "python" and job["name"] == "dim_date":
                print("  ⚙️  Generating date dimension...")
                df = create_dim_date(spark)
            else:
                # Execute SQL query on source database
                print("  📥 Extracting from source...")
                df = jdbc_read(spark, source_jdbc_url, job["sql"], source_properties)
                print(f"  ✅ Extracted {df.count()} rows")
            
            # Load to target database
            print(f"  📤 Loading to {job['target_table']}...")
            write_to_clickhouse(df, target_jdbc_url, job["target_table"], target_properties, mode=job["method"])
            print(f"  ✅ Loaded {df.count()} rows to {job['target_table']}")
            
        except Exception as e:
            print(f"  ❌ FAILED: {str(e)}")
            raise

    # =========================================================================
    # COMPLETION
    # =========================================================================
    print("\n" + "=" * 80)
    print("✅ ETL PIPELINE COMPLETED SUCCESSFULLY")
    print(f"✅ {len(ETL_JOBS)} jobs completed")
    print("=" * 80)
    
    spark.stop()


if __name__ == "__main__":
    main()
