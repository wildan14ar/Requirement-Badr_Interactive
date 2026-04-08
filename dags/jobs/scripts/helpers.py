# helpers.py
from typing import Dict, Any, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


# ============================================================================
# URL BUILDERS
# ============================================================================


def build_jdbc_url(db_type: str, host: str, port: str, database: str) -> str:
    """Build JDBC URL for Spark."""
    db_type = db_type.lower()
    if db_type == "postgres":
        return f"jdbc:postgresql://{host}:{port}/{database}"
    if db_type == "mssql":
        return (
            f"jdbc:sqlserver://{host}:{port};"
            f"databaseName={database};encrypt=true;trustServerCertificate=true;"
        )
    if db_type in ("mysql", "mariadb"):
        return f"jdbc:{db_type}://{host}:{port}/{database}"
    if db_type == "oracle":
        return f"jdbc:oracle:thin:@{host}:{port}/{database}"
    raise ValueError(f"Unsupported db_type: {db_type}")


def build_sqlalchemy_url(
    db_type: str, host: str, port: str, database: str, user: str, password: str
) -> str:
    """Build SQLAlchemy connection URL for multiple database types."""
    db_type = db_type.lower()
    if db_type == "postgres":
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    if db_type == "mysql":
        return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    if db_type == "mariadb":
        return f"mariadb+pymysql://{user}:{password}@{host}:{port}/{database}"
    if db_type == "mssql":
        return f"mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    if db_type == "oracle":
        return f"oracle+cx_oracle://{user}:{password}@{host}:{port}/{database}"
    raise ValueError(f"Unsupported db_type: {db_type}")


def get_sqlalchemy_engine(cfg: Dict[str, str], pool_size: int = 5) -> Engine:
    """Create SQLAlchemy engine from config with connection pooling."""
    # Extract db_type from jdbc_url
    jdbc_url = cfg["jdbc_url"]
    if "postgresql" in jdbc_url:
        db_type = "postgres"
    elif "sqlserver" in jdbc_url:
        db_type = "mssql"
    elif "mysql" in jdbc_url:
        db_type = "mysql"
    elif "mariadb" in jdbc_url:
        db_type = "mariadb"
    elif "oracle" in jdbc_url:
        db_type = "oracle"
    else:
        raise ValueError(f"Cannot determine db_type from jdbc_url: {jdbc_url}")

    # Parse host/port/database from jdbc_url
    if db_type == "postgres":
        # jdbc:postgresql://host:port/database
        parts = jdbc_url.replace("jdbc:postgresql://", "").split("/")
        host_port = parts[0].split(":")
        host, port = host_port[0], host_port[1]
        database = parts[1]
    elif db_type == "mssql":
        # jdbc:sqlserver://host:port;databaseName=database;...
        url_part = jdbc_url.replace("jdbc:sqlserver://", "")
        host_port = url_part.split(";")[0].split(":")
        host, port = host_port[0], host_port[1]
        for param in url_part.split(";"):
            if param.startswith("databaseName="):
                database = param.split("=")[1]
                break
    elif db_type in ("mysql", "mariadb"):
        # jdbc:mysql://host:port/database or jdbc:mariadb://host:port/database
        parts = jdbc_url.split("://")[1].split("/")
        host_port = parts[0].split(":")
        host, port = host_port[0], host_port[1]
        database = parts[1]
    elif db_type == "oracle":
        # jdbc:oracle:thin:@host:port/database
        parts = jdbc_url.replace("jdbc:oracle:thin:@", "").split("/")
        host_port = parts[0].split(":")
        host, port = host_port[0], host_port[1]
        database = parts[1]

    url = build_sqlalchemy_url(
        db_type, host, port, database, cfg["user"], cfg["password"]
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=pool_size * 2,
        pool_recycle=3600,
    )


def get_db_type(cfg: Dict[str, str]) -> str:
    """Get database type from config."""
    jdbc_url = cfg["jdbc_url"]
    if "postgresql" in jdbc_url:
        return "postgres"
    elif "sqlserver" in jdbc_url:
        return "mssql"
    elif "mysql" in jdbc_url:
        return "mysql"
    elif "mariadb" in jdbc_url:
        return "mariadb"
    elif "oracle" in jdbc_url:
        return "oracle"
    raise ValueError(f"Cannot determine db_type from jdbc_url: {jdbc_url}")


# ============================================================================
# SPARK JDBC FUNCTIONS
# ============================================================================


def jdbc_read(spark: SparkSession, cfg: Dict[str, str], sql: str) -> DataFrame:
    """Read data using Spark JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", cfg["jdbc_url"])
        .option("dbtable", f"({sql}) t")
        .option("user", cfg["user"])
        .option("password", cfg["password"])
        .option("driver", cfg["driver"])
        .option("fetchsize", "10000")
        .load()
    )


def jdbc_write(
    df: DataFrame,
    cfg: Dict[str, str],
    mode: str,
    batch_size: int = 10000,
    num_partitions: int = 4,
):
    """Write data using Spark JDBC with optimized settings."""
    (
        df.repartition(num_partitions)
        .write.format("jdbc")
        .option("url", cfg["jdbc_url"])
        .option("dbtable", cfg["table"])
        .option("user", cfg["user"])
        .option("password", cfg["password"])
        .option("driver", cfg["driver"])
        .option("batchsize", batch_size)
        .option("isolationLevel", "READ_COMMITTED")
        .mode(mode)
        .save()
    )


def jdbc_truncate(spark: SparkSession, cfg: Dict[str, str]):
    """Execute TRUNCATE TABLE on target database."""
    table = cfg["table"]
    engine = get_sqlalchemy_engine(cfg)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
    engine.dispose()


# ============================================================================
# SQL BUILDERS FOR UPSERT
# ============================================================================


def build_upsert_sql(
    db_type: str, table: str, columns: List[str], conflict_cols: List[str] = None
) -> str:
    """
    Build INSERT or UPSERT SQL based on database type and conflict columns.

    Supports:
    - PostgreSQL: INSERT ... ON CONFLICT ... DO UPDATE
    - MySQL/MariaDB: INSERT ... ON DUPLICATE KEY UPDATE
    - MSSQL: MERGE statement
    - Oracle: MERGE statement
    """
    quoted_cols = [f'"{c}"' for c in columns]
    col_list = ", ".join(quoted_cols)

    db_type = db_type.lower()

    # Simple INSERT (no conflict handling)
    if not conflict_cols:
        if db_type in ("mysql", "mariadb"):
            # MySQL/MariaDB use backticks
            backtick_cols = [f"`{c}`" for c in columns]
            col_list_bt = ", ".join(backtick_cols)
            placeholders = ", ".join([":" + c for c in columns])
            return f"INSERT INTO {table} ({col_list_bt}) VALUES ({placeholders})"
        else:
            placeholders = ", ".join([":" + c for c in columns])
            return f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

    # UPSERT based on database type
    if db_type == "postgres":
        return _build_postgres_upsert(table, columns, conflict_cols)
    elif db_type in ("mysql", "mariadb"):
        return _build_mysql_upsert(table, columns, conflict_cols)
    elif db_type == "mssql":
        return _build_mssql_merge(table, columns, conflict_cols)
    elif db_type == "oracle":
        return _build_oracle_merge(table, columns, conflict_cols)
    else:
        raise ValueError(f"Unsupported db_type for upsert: {db_type}")


def _build_postgres_upsert(
    table: str, columns: List[str], conflict_cols: List[str]
) -> str:
    """PostgreSQL: INSERT ... ON CONFLICT ... DO UPDATE SET"""
    quoted_cols = [f'"{c}"' for c in columns]
    col_list = ", ".join(quoted_cols)
    placeholders = ", ".join([":" + c for c in columns])

    quoted_conflict = [f'"{c}"' for c in conflict_cols]
    conflict = ", ".join(quoted_conflict)

    update_set = ", ".join(
        f'"{c}" = EXCLUDED."{c}"' for c in columns if c not in conflict_cols
    )

    if not update_set:
        return f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO NOTHING"

    return f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON CONFLICT ({conflict}) DO UPDATE SET {update_set}"


def _build_mysql_upsert(
    table: str, columns: List[str], conflict_cols: List[str]
) -> str:
    """MySQL/MariaDB: INSERT ... ON DUPLICATE KEY UPDATE"""
    backtick_cols = [f"`{c}`" for c in columns]
    col_list = ", ".join(backtick_cols)
    placeholders = ", ".join([":" + c for c in columns])

    update_set = ", ".join(
        f"`{c}` = VALUES(`{c}`)" for c in columns if c not in conflict_cols
    )

    if not update_set:
        # Just insert, ignore duplicates
        return f"INSERT IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"

    return f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {update_set}"


def _build_mssql_merge(table: str, columns: List[str], conflict_cols: List[str]) -> str:
    """MSSQL: MERGE statement"""
    quoted_cols = [f'"{c}"' for c in columns]
    col_list = ", ".join(quoted_cols)

    # Source values as temp table
    source_cols = ", ".join([f':{c} AS "{c}"' for c in columns])

    # Match condition
    match_cond = " AND ".join([f'target."{c}" = source."{c}"' for c in conflict_cols])

    # Update set
    update_set = ", ".join(
        f'target."{c}" = source."{c}"' for c in columns if c not in conflict_cols
    )

    # Insert values
    insert_cols = ", ".join([f'"{c}"' for c in columns])
    insert_vals = ", ".join([f'source."{c}"' for c in columns])

    sql = f"""
    MERGE INTO {table} AS target
    USING (SELECT {source_cols}) AS source
    ON {match_cond}
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals});
    """
    return sql.strip()


def _build_oracle_merge(
    table: str, columns: List[str], conflict_cols: List[str]
) -> str:
    """Oracle: MERGE statement"""
    quoted_cols = [f'"{c}"' for c in columns]

    # Source values
    source_cols = ", ".join([f':{c} AS "{c}"' for c in columns])

    # Match condition
    match_cond = " AND ".join([f'target."{c}" = source."{c}"' for c in conflict_cols])

    # Update set
    update_set = ", ".join(
        f'target."{c}" = source."{c}"' for c in columns if c not in conflict_cols
    )

    # Insert
    insert_cols = ", ".join([f'"{c}"' for c in columns])
    insert_vals = ", ".join([f'source."{c}"' for c in columns])

    sql = f"""
    MERGE INTO {table} target
    USING (SELECT {source_cols} FROM DUAL) source
    ON ({match_cond})
    WHEN MATCHED THEN
        UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
        INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    return sql.strip()


# ============================================================================
# DATABASE OPERATIONS USING SQLALCHEMY
# ============================================================================


def get_target_count(cfg: Dict[str, str]) -> int:
    """Get current row count of target table using SQLAlchemy."""
    try:
        engine = get_sqlalchemy_engine(cfg)
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) as cnt FROM {cfg['table']}"))
            count = result.fetchone()[0]
        engine.dispose()
        return count
    except Exception:
        return 0


def read_target_data(
    spark: SparkSession, cfg: Dict[str, str], columns: List[str]
) -> Optional[DataFrame]:
    """Read target data with specified columns using Spark JDBC."""
    try:
        db_type = get_db_type(cfg)
        if db_type in ("mysql", "mariadb"):
            col_list = ", ".join([f"`{c}`" for c in columns])
        else:
            col_list = ", ".join([f'"{c}"' for c in columns])

        df = jdbc_read(spark, cfg, f"SELECT {col_list} FROM {cfg['table']}")
        if df.rdd.isEmpty():
            return None
        return df
    except Exception:
        return None


def read_target_keys(
    spark: SparkSession, cfg: Dict[str, str], key_columns: List[str]
) -> Optional[DataFrame]:
    """Read only key columns from target for faster comparison."""
    try:
        db_type = get_db_type(cfg)
        if db_type in ("mysql", "mariadb"):
            col_list = ", ".join([f"`{c}`" for c in key_columns])
        else:
            col_list = ", ".join([f'"{c}"' for c in key_columns])

        df = jdbc_read(spark, cfg, f"SELECT DISTINCT {col_list} FROM {cfg['table']}")
        if df.rdd.isEmpty():
            return None
        return df
    except Exception:
        return None


# ============================================================================
# PARALLEL WRITE FUNCTIONS
# ============================================================================


def _process_batch_worker(
    batch_data: List[Dict],
    cfg: Dict[str, str],
    sql: str,
    worker_id: int,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Worker function to process a single batch.
    Returns (success_rows, failed_rows)
    """
    success_rows = []
    failed_rows = []

    try:
        engine = get_sqlalchemy_engine(cfg, pool_size=2)
        try:
            # Try batch insert first
            with engine.begin() as conn:
                conn.execute(text(sql), batch_data)
            success_rows = batch_data
        except Exception as batch_error:
            # Fallback to mini-batches for error isolation
            mini_batch_size = max(10, len(batch_data) // 10)
            for i in range(0, len(batch_data), mini_batch_size):
                mini_batch = batch_data[i : i + mini_batch_size]
                try:
                    with engine.begin() as conn:
                        conn.execute(text(sql), mini_batch)
                    success_rows.extend(mini_batch)
                except Exception:
                    # Final fallback - row by row for this mini-batch only
                    for row in mini_batch:
                        try:
                            with engine.begin() as conn:
                                conn.execute(text(sql), [row])
                            success_rows.append(row)
                        except Exception as e:
                            row["_error"] = str(e)[:200]  # Truncate error message
                            failed_rows.append(row)
        finally:
            engine.dispose()
    except Exception as e:
        # Connection error - mark all as failed
        for row in batch_data:
            row["_error"] = f"Connection error: {str(e)[:100]}"
            failed_rows.append(row)

    return success_rows, failed_rows


def write_to_db_parallel(
    df: DataFrame,
    cfg: Dict[str, str],
    conflict_cols: List[str] = None,
    batch_size: int = 5000,
    workers: int = 4,
    transaction: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Write DataFrame to database using parallel workers.

    Args:
        df: Spark DataFrame to write
        cfg: Database configuration
        conflict_cols: Columns for upsert conflict detection
        batch_size: Number of rows per batch (default: 5000)
        workers: Number of parallel workers (default: 4)
        transaction: If True, all-or-nothing transaction

    Returns:
        Dict with 'success' and 'failed' row lists
    """
    table = cfg["table"]
    columns = list(df.columns)
    db_type = get_db_type(cfg)
    sql = build_upsert_sql(db_type, table, columns, conflict_cols)

    # Collect data from Spark
    rows = df.collect()
    total_rows = len(rows)

    if total_rows == 0:
        return {"success": [], "failed": []}

    # Convert to list of dicts
    all_data = [{col: row[col] for col in columns} for row in rows]

    # Transaction mode - single batch, all or nothing
    if transaction:
        engine = get_sqlalchemy_engine(cfg, pool_size=2)
        try:
            with engine.begin() as conn:
                # Process in batches but same transaction
                for i in range(0, len(all_data), batch_size):
                    batch = all_data[i : i + batch_size]
                    conn.execute(text(sql), batch)
            return {"success": all_data, "failed": []}
        except Exception as e:
            raise e
        finally:
            engine.dispose()

    # Split into batches
    batches = [
        all_data[i : i + batch_size] for i in range(0, len(all_data), batch_size)
    ]
    num_batches = len(batches)

    print(
        f"      📦 {total_rows:,} rows → {num_batches} batches × {batch_size} rows, {workers} workers"
    )

    all_success = []
    all_failed = []

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_batch_worker, batch, cfg, sql, i): i
            for i, batch in enumerate(batches)
        }

        completed = 0
        for future in as_completed(futures):
            batch_id = futures[future]
            try:
                success, failed = future.result()
                all_success.extend(success)
                all_failed.extend(failed)
                completed += 1
                if completed % 10 == 0 or completed == num_batches:
                    print(f"      ⏳ Progress: {completed}/{num_batches} batches")
            except Exception as e:
                print(f"      ❌ Batch {batch_id} exception: {e}")
                # Mark batch as failed
                for row in batches[batch_id]:
                    row["_error"] = str(e)[:200]
                    all_failed.append(row)

    return {"success": all_success, "failed": all_failed}


def write_to_db(
    df: DataFrame,
    cfg: Dict[str, str],
    conflict_cols: List[str] = None,
    batch_size: int = 5000,
    workers: int = 4,
    transaction: bool = False,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Write DataFrame to database - wrapper that uses parallel write.

    Args:
        df: Spark DataFrame to write
        cfg: Database configuration
        conflict_cols: Columns for upsert conflict detection
        batch_size: Number of rows per batch (default: 5000)
        workers: Number of parallel workers (default: 4)
        transaction: If True, all-or-nothing transaction

    Returns:
        Dict with 'success' and 'failed' row lists
    """
    return write_to_db_parallel(
        df=df,
        cfg=cfg,
        conflict_cols=conflict_cols,
        batch_size=batch_size,
        workers=workers,
        transaction=transaction,
    )


def write_spark_native(
    df: DataFrame,
    cfg: Dict[str, str],
    mode: str = "append",
    batch_size: int = 10000,
    num_partitions: int = 8,
) -> int:
    """
    Write DataFrame using Spark native JDBC (fastest for bulk insert).

    Args:
        df: Spark DataFrame to write
        cfg: Database configuration
        mode: Write mode ('append', 'overwrite')
        batch_size: JDBC batch size
        num_partitions: Number of parallel write partitions

    Returns:
        Number of rows written
    """
    row_count = df.count()

    if row_count == 0:
        return 0

    # Repartition for parallel writes
    df_partitioned = df.repartition(num_partitions)

    (
        df_partitioned.write.format("jdbc")
        .option("url", cfg["jdbc_url"])
        .option("dbtable", cfg["table"])
        .option("user", cfg["user"])
        .option("password", cfg["password"])
        .option("driver", cfg["driver"])
        .option("batchsize", batch_size)
        .option("isolationLevel", "READ_COMMITTED")
        .mode(mode)
        .save()
    )

    return row_count


# ============================================================================
# STATS HELPER
# ============================================================================


def make_stats(**kwargs) -> Dict[str, Any]:
    """Create stats dictionary."""
    return kwargs
