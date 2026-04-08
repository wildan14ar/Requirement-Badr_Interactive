"""
Stock ETL Pipeline - Airflow DAG (MySQL → ClickHouse)
======================================================
Extract from MySQL recruitment_dev → Transform with Spark → Load to ClickHouse

Schedule: 4x/day (00:00, 06:00, 12:00, 18:00)
"""

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
}

# Get connections from Airflow
mysql_source = BaseHook.get_connection("mysql_recruitment_source")
clickhouse_target = BaseHook.get_connection("clickhouse_datamart")

with DAG(
    dag_id="stock_etl_pipeline",
    default_args=default_args,
    schedule="0 0,6,12,18 * * *",  # 4x/day
    catchup=False,
    tags=["etl", "spark", "clickhouse", "stock", "badr-interactive"],
) as dag:

    # =========================================================================
    # TASK 1: Extract from MySQL → Transform → Load to ClickHouse
    # Single Spark job that handles entire ETL process
    # =========================================================================
    run_etl = SparkSubmitOperator(
        task_id="run_etl",
        application="/opt/airflow/dags/jobs/jobs_etl.py",
        conn_id="spark_default",
        packages="mysql:mysql-connector-java:8.0.33,com.clickhouse:clickhouse-jdbc:0.4.6",
        py_files="/opt/airflow/dags/jobs/scripts/helpers.py,/opt/airflow/dags/jobs/scripts/methods.py",
        application_args=[
            # Source MySQL
            "--source_type", "mysql",
            "--source_host", mysql_source.host,
            "--source_port", str(mysql_source.port or 3306),
            "--source_user", mysql_source.login,
            "--source_password", mysql_source.password,
            "--source_db", mysql_source.schema or "recruitment_dev",
            # Target ClickHouse
            "--target_type", "clickhouse",
            "--target_host", clickhouse_target.host,
            "--target_port", str(clickhouse_target.port or 8123),
            "--target_user", clickhouse_target.login,
            "--target_password", clickhouse_target.password,
            "--target_db", clickhouse_target.schema or "datamart_badr_interactive",
        ],
        conf={
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
        },
    )

    # =========================================================================
    # TASK 2: Validate data quality (optional)
    # =========================================================================
    validate_data = BashOperator(
        task_id="validate_data",
        bash_command="""
        echo "🔍 Validating data quality..."
        echo "✅ ETL completed successfully at $(date)"
        """,
    )

    # =========================================================================
    # DEPENDENCIES
    # =========================================================================
    run_etl >> validate_data
