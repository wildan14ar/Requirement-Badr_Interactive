from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import os
import subprocess
import sys

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
}

VENV_PATH = "/opt/airflow/dms_adhi"
REQUIREMENTS_FILE = "/opt/airflow/dags/dms_adhi.txt"


def run_python_in_venv(venv_path, script_path, env_vars=None):
    if not os.path.exists(venv_path):
        raise FileNotFoundError(f"Venv tidak ditemukan di {venv_path}")

    python_bin = os.path.join(venv_path, "bin", "python")
    environment = os.environ.copy()
    if env_vars:
        for k, v in env_vars.items():
            environment[k] = v

    process = subprocess.Popen(
        [python_bin, script_path],
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=environment,
    )
    process.wait()
    if process.returncode != 0:
        raise Exception(
            f"Script gagal dijalankan dengan kode return {process.returncode}"
        )


dms_adhi_mysql_source = BaseHook.get_connection("dms_adhi_mysql_source")
dms_adhi_postgres_staging = BaseHook.get_connection("dms_adhi_postgres_staging")
dms_adhi_postgres_prod = BaseHook.get_connection("dms_adhi_postgres_prod")

with DAG(
    dag_id="Dags_DMS_ADHI",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["etl", "spark", "postgres", "adhi", "data"],
) as dag:

    source_to_raw = BashOperator(
        task_id="source_to_raw",
        bash_command=f"pgloader mysql://{dms_adhi_mysql_source.login}:{dms_adhi_mysql_source.password}@{dms_adhi_mysql_source.host}:{dms_adhi_mysql_source.port or 3306}/{dms_adhi_mysql_source.schema} postgresql://{dms_adhi_postgres_staging.login}:{dms_adhi_postgres_staging.password}@{dms_adhi_postgres_staging.host}:{dms_adhi_postgres_staging.port or 5432}/{dms_adhi_postgres_staging.schema}",
    )

    raw_to_stagging = SparkSubmitOperator(
        task_id="raw_to_stagging",
        application="/opt/airflow/dags/jobs/dms_adhi_raw_to_stagging.py",
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.3",
        py_files="dags/jobs/scripts/helpers.py,dags/jobs/scripts/methods.py",
        application_args=[
            "--source_type",
            "postgres",
            "--source_host",
            dms_adhi_postgres_staging.host,
            "--source_port",
            str(dms_adhi_postgres_staging.port or 5432),
            "--source_user",
            dms_adhi_postgres_staging.login,
            "--source_password",
            dms_adhi_postgres_staging.password,
            "--source_db",
            dms_adhi_postgres_staging.schema,
            "--target_type",
            "postgres",
            "--target_host",
            dms_adhi_postgres_staging.host,
            "--target_port",
            str(dms_adhi_postgres_staging.port or 5432),
            "--target_user",
            dms_adhi_postgres_staging.login,
            "--target_password",
            dms_adhi_postgres_staging.password,
            "--target_db",
            dms_adhi_postgres_staging.schema,
        ],
    )

    create_venv = BashOperator(
        task_id="create_venv",
        bash_command=f"""
        if [ ! -d "{VENV_PATH}" ]; then
            python3 -m venv {VENV_PATH}
        fi
        {VENV_PATH}/bin/pip install --upgrade pip
        {VENV_PATH}/bin/pip install -r {REQUIREMENTS_FILE}
        """,
    )

    generate_ulid = PythonOperator(
        task_id="generate_ulid",
        python_callable=run_python_in_venv,
        op_args=[VENV_PATH, "/opt/airflow/dags/jobs/dms_adhi_generate_ulid.py"],
        op_kwargs={
            "env_vars": {
                "host": dms_adhi_postgres_staging.host,
                "port": str(dms_adhi_postgres_staging.port or 5432),
                "user": dms_adhi_postgres_staging.login,
                "password": dms_adhi_postgres_staging.password,
                "db": dms_adhi_postgres_staging.schema,
                "schema": "public",
            },
        },
    )

    stagging_to_prod = SparkSubmitOperator(
        task_id="stagging_to_prod",
        application="dags/jobs/dms_adhi_stagging_to_prod.py",
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.3",
        py_files="dags/jobs/scripts/helpers.py,dags/jobs/scripts/methods.py",
        conf={
            "spark.executor.memory": "1g",
            "spark.executor.cores": "2",
            "spark.cores.max": "2",
            "spark.driver.memory": "1g",
        },
        application_args=[
            "--source_type",
            "postgres",
            "--source_host",
            dms_adhi_postgres_staging.host,
            "--source_port",
            str(dms_adhi_postgres_staging.port or 5432),
            "--source_user",
            dms_adhi_postgres_staging.login,
            "--source_password",
            dms_adhi_postgres_staging.password,
            "--source_db",
            dms_adhi_postgres_staging.schema,
            "--target_type",
            "postgres",
            "--target_host",
            dms_adhi_postgres_prod.host,
            "--target_port",
            str(dms_adhi_postgres_prod.port or 5432),
            "--target_user",
            dms_adhi_postgres_prod.login,
            "--target_password",
            dms_adhi_postgres_prod.password,
            "--target_db",
            dms_adhi_postgres_prod.schema,
            # Performance tuning - adjust based on your environment
            "--batch_size",
            "10000",  # Rows per batch (increase for faster writes, decrease for memory issues)
            "--workers",
            "4",  # Parallel workers (increase for multi-core systems)
        ],
    )

    # inject_document_types = PythonOperator(
    #     task_id="inject_document_types",
    #     python_callable=run_python_in_venv,
    #     op_args=[VENV_PATH, "/opt/airflow/dags/jobs/inject_document_types.py"],
    #     op_kwargs={
    #         "env_vars": {
    #             "PROD_HOST": dms_adhi_postgres_prod.host,
    #             "PROD_PORT": str(dms_adhi_postgres_prod.port or 5432),
    #             "PROD_USER": dms_adhi_postgres_prod.login,
    #             "PROD_PASSWORD": dms_adhi_postgres_prod.password,
    #             "PROD_DB": dms_adhi_postgres_prod.schema,
    #         },
    #     },
    # )

    (source_to_raw >> [raw_to_stagging, create_venv] >> generate_ulid >> stagging_to_prod)
