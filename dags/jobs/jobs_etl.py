"""
Stock ETL Jobs - PySpark
=========================

Extract from MySQL `recruitment_dev`, transform into a ClickHouse datamart,
and load natural-key dimensions, bridge tables, and the stock fact table.
"""

import argparse
from datetime import date, datetime, timedelta

from pyspark.sql import SparkSession


ETL_JOBS = [
    {
        "name": "dim_date",
        "execution_engine": "python",
        "target_table": "dim_date",
        "method": "overwrite",
    },
    {
        "name": "dim_location",
        "sql": """
            SELECT DISTINCT
                p.id AS province_id,
                COALESCE(p.name, 'Unknown Province') AS province_name,
                r.id AS regency_id,
                COALESCE(r.name, 'Unknown Regency') AS regency_name,
                CONCAT(COALESCE(p.name, 'Unknown Province'), ' - ', COALESCE(r.name, 'Unknown Regency')) AS full_location,
                r.lat,
                r.lng,
                CURRENT_TIMESTAMP() AS created_at,
                CURRENT_TIMESTAMP() AS updated_at
            FROM regencies r
            LEFT JOIN provinces p ON r.province_id = p.id
            WHERE r.deleted_at IS NULL
        """,
        "target_table": "dim_location",
        "method": "overwrite",
    },
    {
        "name": "dim_material",
        "sql": """
            SELECT
                id AS material_id,
                code AS material_code,
                COALESCE(name, 'Unknown Material') AS material_name,
                CASE WHEN is_vaccine = 1 THEN 'Vaccine' ELSE 'Non-Vaccine' END AS category,
                CASE WHEN is_vaccine = 1 THEN 1 ELSE 0 END AS is_vaccine,
                unit,
                unit_of_distribution,
                pieces_per_unit,
                CASE WHEN temperature_sensitive = 1 THEN 1 ELSE 0 END AS temperature_sensitive,
                temperature_min,
                temperature_max,
                CASE WHEN status = 1 THEN 1 ELSE 0 END AS status,
                created_at,
                updated_at
            FROM master_materials
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_material",
        "method": "overwrite",
    },
    {
        "name": "dim_entity",
        "sql": """
            SELECT
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
                type AS type_code,
                CASE WHEN status = 1 THEN 1 ELSE 0 END AS status,
                COALESCE(is_puskesmas, 0) AS is_puskesmas,
                province_id,
                regency_id,
                postal_code,
                COALESCE(country, 'Indonesia') AS country,
                lat,
                lng,
                created_at,
                updated_at
            FROM entities
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_entity",
        "method": "overwrite",
    },
    {
        "name": "dim_activity",
        "sql": """
            SELECT
                id AS activity_id,
                code AS activity_code,
                COALESCE(name, 'Unknown Activity') AS activity_name,
                COALESCE(is_ordered_sales, 0) AS is_ordered_sales,
                COALESCE(is_ordered_purchase, 0) AS is_ordered_purchase,
                COALESCE(is_patient_id, 0) AS is_patient_id,
                created_at,
                updated_at
            FROM master_activities
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_activity",
        "method": "overwrite",
    },
    {
        "name": "dim_entity_tag",
        "sql": """
            SELECT
                id AS tag_id,
                title AS tag_name,
                created_at,
                updated_at
            FROM entity_tags
            WHERE deleted_at IS NULL
        """,
        "target_table": "dim_entity_tag",
        "method": "overwrite",
    },
    {
        "name": "bridge_entity_tag",
        "sql": """
            SELECT
                entity_id,
                entity_tag_id AS tag_id,
                MIN(created_at) AS created_at,
                MAX(updated_at) AS updated_at
            FROM entity_entity_tags
            GROUP BY entity_id, entity_tag_id
        """,
        "target_table": "bridge_entity_tag",
        "method": "overwrite",
    },
    {
        "name": "bridge_entity_activity",
        "sql": """
            SELECT
                entity_id,
                activity_id,
                join_date,
                end_date,
                MIN(created_at) AS created_at,
                MAX(updated_at) AS updated_at
            FROM entity_activity_date
            WHERE deleted_at IS NULL
            GROUP BY entity_id, activity_id, join_date, end_date
        """,
        "target_table": "bridge_entity_activity",
        "method": "overwrite",
    },
    {
        "name": "fact_stock_snapshot",
        "sql": """
            SELECT
                s.id AS stock_id,
                DATE(s.createdAt) AS stock_date,
                CAST(DATE_FORMAT(s.createdAt, '%Y%m%d') AS UNSIGNED) AS date_key,
                s.batch_id,
                b.code AS batch_code,
                DATE(b.expired_date) AS batch_expired_date,
                em.entity_id,
                em.master_material_id AS material_id,
                s.activity_id,
                e.province_id,
                e.regency_id,
                COALESCE(s.qty, 0) AS stock_quantity,
                COALESCE(s.allocated, 0) AS allocated,
                COALESCE(s.in_transit, 0) AS in_transit,
                COALESCE(s.open_vial, 0) AS open_vial,
                COALESCE(s.extermination_discard_qty, 0) AS extermination_discard_qty,
                COALESCE(s.extermination_received_qty, 0) AS extermination_received_qty,
                COALESCE(s.extermination_qty, 0) AS extermination_qty,
                COALESCE(s.extermination_shipped_qty, 0) AS extermination_shipped_qty,
                s.budget_source,
                s.year AS stock_year,
                s.price,
                s.total_price,
                s.createdAt AS source_created_at,
                s.updatedAt AS source_updated_at
            FROM stocks s
            LEFT JOIN batches b ON s.batch_id = b.id
            LEFT JOIN entity_has_master_materials em ON s.entity_has_material_id = em.id
            LEFT JOIN entities e ON em.entity_id = e.id
        """,
        "target_table": "fact_stock_snapshot",
        "method": "overwrite",
    },
]


def build_jdbc_url(db_type, host, port, database):
    """Build JDBC URL for the requested database type."""
    if db_type == "mysql":
        params = "useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
        return f"jdbc:mysql://{host}:{port}/{database}?{params}"
    if db_type == "clickhouse":
        return f"jdbc:clickhouse://{host}:{port}/{database}"
    raise ValueError(f"Unsupported database type: {db_type}")


def jdbc_read(spark, jdbc_url, sql, properties):
    """Read data from JDBC source using an inline SQL query."""
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"({sql}) AS source_data")
        .options(**properties)
        .load()
    )


def write_to_clickhouse(df, jdbc_url, table, properties, mode="overwrite"):
    """Write a DataFrame to ClickHouse via JDBC."""
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("truncate", "true")
        .options(**properties)
        .mode(mode)
        .save()
    )


def create_dim_date(spark):
    """Generate the date dimension from 2020 through 2030."""
    rows = []
    current_day = date(2020, 1, 1)
    end_day = date(2030, 12, 31)
    while current_day <= end_day:
        rows.append(
            {
                "date_key": int(current_day.strftime("%Y%m%d")),
                "full_date": current_day,
                "year": current_day.year,
                "quarter": (current_day.month - 1) // 3 + 1,
                "month": current_day.month,
                "month_name": current_day.strftime("%B"),
                "month_short": current_day.strftime("%b"),
                "day": current_day.day,
                "day_name": current_day.strftime("%A"),
                "day_short": current_day.strftime("%a"),
                "week_of_year": current_day.isocalendar()[1],
                "is_weekend": 1 if current_day.isoweekday() >= 6 else 0,
                "created_at": datetime(current_day.year, current_day.month, current_day.day),
                "updated_at": datetime(current_day.year, current_day.month, current_day.day),
            }
        )
        current_day += timedelta(days=1)

    return spark.createDataFrame(rows)


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

    spark = (
        SparkSession.builder.appName("stock_etl")
        .config(
            "spark.jars.packages",
            "mysql:mysql-connector-java:8.0.33,com.clickhouse:clickhouse-jdbc:0.4.6",
        )
        .getOrCreate()
    )

    print("=" * 80)
    print("Starting stock ETL pipeline")
    print("=" * 80)
    print(f"Source: {args.source_type}://{args.source_host}:{args.source_port}/{args.source_db}")
    print(f"Target: {args.target_type}://{args.target_host}:{args.target_port}/{args.target_db}")
    print(f"Jobs: {len(ETL_JOBS)}")
    print("=" * 80)

    source_jdbc_url = build_jdbc_url(
        args.source_type, args.source_host, args.source_port, args.source_db
    )
    target_jdbc_url = build_jdbc_url(
        args.target_type, args.target_host, args.target_port, args.target_db
    )

    source_properties = {
        "user": args.source_user,
        "password": args.source_password,
        "driver": "com.mysql.cj.jdbc.Driver",
    }
    target_properties = {
        "user": args.target_user,
        "password": args.target_password,
        "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    }

    for index, job in enumerate(ETL_JOBS, start=1):
        print(f"\n[{index}/{len(ETL_JOBS)}] Job: {job['name']}")
        print(f"  Target: {job['target_table']}")
        print(f"  Method: {job['method']}")

        if job.get("execution_engine") == "python":
            df = create_dim_date(spark)
        else:
            df = jdbc_read(spark, source_jdbc_url, job["sql"], source_properties)

        row_count = df.count()
        print(f"  Extracted rows: {row_count}")
        write_to_clickhouse(
            df,
            target_jdbc_url,
            job["target_table"],
            target_properties,
            mode=job["method"],
        )
        print(f"  Loaded rows: {row_count}")

    print("\n" + "=" * 80)
    print("ETL pipeline completed successfully")
    print(f"Completed jobs: {len(ETL_JOBS)}")
    print("=" * 80)
    spark.stop()


if __name__ == "__main__":
    main()
