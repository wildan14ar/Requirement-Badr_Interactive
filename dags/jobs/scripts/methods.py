# methods.py
from typing import List, Dict, Any, Tuple
from pyspark.sql import functions as F
from pyspark import StorageLevel
from helpers import (
    jdbc_write,
    jdbc_read,
    jdbc_truncate,
    make_stats,
    get_target_count,
    read_target_data,
    read_target_keys,
    write_to_db,
    write_spark_native,
)


# ============================================================================
# DEFAULT CONFIGURATION
# ============================================================================

DEFAULT_BATCH_SIZE = 5000
DEFAULT_WORKERS = 4
DEFAULT_SPARK_PARTITIONS = 8


# ============================================================================
# OVERWRITE METHOD
# ============================================================================


def overwrite_dataframe(
    spark,
    df,
    target_cfg,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    transaction: bool = False,
) -> Tuple[Dict, List, List]:
    """
    Overwrite target table with dataframe content.
    Uses Spark native write for maximum speed.

    Args:
        spark: SparkSession
        df: Source DataFrame
        target_cfg: Target database configuration
        batch_size: Rows per batch for JDBC
        workers: Number of parallel workers (unused for native write)
        transaction: If True, truncate and insert in single transaction
    """
    print(
        f"    ⚙️  Config: batch_size={batch_size}, partitions={DEFAULT_SPARK_PARTITIONS}"
    )

    count_before = get_target_count(target_cfg)
    source_count = df.count()

    if source_count == 0:
        print("    ⚠️  No data to write")
        return make_stats(processed=0, inserted=0, failed=0), [], []

    try:
        # Truncate first
        jdbc_truncate(spark, target_cfg)

        # Use Spark native write for speed (parallel by partitions)
        written = write_spark_native(
            df,
            target_cfg,
            mode="append",
            batch_size=batch_size,
            num_partitions=DEFAULT_SPARK_PARTITIONS,
        )

        count_after = get_target_count(target_cfg)

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=count_after,
            processed=written,
            inserted=written,
            failed=0,
        )
        return stats, [], []

    except Exception as e:
        print(f"    ❌ Spark native write failed: {e}")
        print(f"    🔄 Falling back to SQLAlchemy write...")

        # Fallback to SQLAlchemy parallel write
        jdbc_truncate(spark, target_cfg)
        results = write_to_db(
            df,
            target_cfg,
            None,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        success_data = results["success"]
        failed_data = results["failed"]

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=get_target_count(target_cfg),
            processed=len(success_data) + len(failed_data),
            inserted=len(success_data),
            failed=len(failed_data),
        )
        return stats, success_data, failed_data


# ============================================================================
# APPEND METHOD
# ============================================================================


def append_dataframe(
    spark,
    df,
    target_cfg,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    transaction: bool = False,
) -> Tuple[Dict, List, List]:
    """
    Append dataframe content to target table.
    Uses Spark native write for maximum speed.

    Args:
        spark: SparkSession
        df: Source DataFrame
        target_cfg: Target database configuration
        batch_size: Rows per batch for JDBC
        workers: Number of parallel workers (unused for native write)
        transaction: If True, all-or-nothing insert
    """
    print(
        f"    ⚙️  Config: batch_size={batch_size}, partitions={DEFAULT_SPARK_PARTITIONS}"
    )

    count_before = get_target_count(target_cfg)
    source_count = df.count()

    if source_count == 0:
        print("    ⚠️  No data to write")
        return make_stats(processed=0, inserted=0, failed=0), [], []

    try:
        # Use Spark native write for speed
        written = write_spark_native(
            df,
            target_cfg,
            mode="append",
            batch_size=batch_size,
            num_partitions=DEFAULT_SPARK_PARTITIONS,
        )

        count_after = get_target_count(target_cfg)

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=count_after,
            processed=written,
            inserted=written,
            failed=0,
        )
        return stats, [], []

    except Exception as e:
        print(f"    ❌ Spark native write failed: {e}")
        print(f"    🔄 Falling back to SQLAlchemy write...")

        # Fallback to SQLAlchemy parallel write
        results = write_to_db(
            df,
            target_cfg,
            None,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        success_data = results["success"]
        failed_data = results["failed"]

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=get_target_count(target_cfg),
            processed=len(success_data) + len(failed_data),
            inserted=len(success_data),
            failed=len(failed_data),
        )
        return stats, success_data, failed_data


# ============================================================================
# TRUNCATE METHOD
# ============================================================================


def truncate_dataframe(
    spark,
    df,
    target_cfg,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    transaction: bool = False,
) -> Tuple[Dict, List, List]:
    """
    Truncate target table first, then insert all data.
    Uses Spark native write for maximum speed.

    Args:
        spark: SparkSession
        df: Source DataFrame
        target_cfg: Target database configuration
        batch_size: Rows per batch for JDBC
        workers: Number of parallel workers
        transaction: If True, all-or-nothing
    """
    print(
        f"    ⚙️  Config: batch_size={batch_size}, partitions={DEFAULT_SPARK_PARTITIONS}"
    )

    count_before = get_target_count(target_cfg)
    source_count = df.count()

    # Always truncate even if no data
    jdbc_truncate(spark, target_cfg)
    print(f"    🗑️  Truncated table (was {count_before:,} rows)")

    if source_count == 0:
        print("    ⚠️  No data to write")
        return make_stats(processed=0, truncated=True, inserted=0, failed=0), [], []

    try:
        # Use Spark native write for speed
        written = write_spark_native(
            df,
            target_cfg,
            mode="append",
            batch_size=batch_size,
            num_partitions=DEFAULT_SPARK_PARTITIONS,
        )

        count_after = get_target_count(target_cfg)

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=count_after,
            truncated=True,
            processed=written,
            inserted=written,
            failed=0,
        )
        return stats, [], []

    except Exception as e:
        print(f"    ❌ Spark native write failed: {e}")
        print(f"    🔄 Falling back to SQLAlchemy write...")

        # Fallback to SQLAlchemy parallel write
        results = write_to_db(
            df,
            target_cfg,
            None,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        success_data = results["success"]
        failed_data = results["failed"]

        stats = make_stats(
            source_count=source_count,
            count_before=count_before,
            count_after=get_target_count(target_cfg),
            truncated=True,
            processed=len(success_data) + len(failed_data),
            inserted=len(success_data),
            failed=len(failed_data),
        )
        return stats, success_data, failed_data


# ============================================================================
# UPSERT METHOD (OPTIMIZED)
# ============================================================================


def upsert_dataframe(
    spark,
    df_source,
    target_cfg,
    conflict_cols: List[str],
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    transaction: bool = False,
) -> Tuple[Dict, List, List]:
    """
    Smart Upsert: Compare source with target, only process new/changed rows.
    Uses SQL UPSERT (ON CONFLICT for PostgreSQL, ON DUPLICATE KEY for MySQL).

    Optimizations:
    - Only reads key columns for initial comparison (faster)
    - Uses parallel workers for write operations
    - Configurable batch size for optimal throughput

    Args:
        spark: SparkSession
        df_source: Source DataFrame
        target_cfg: Target database configuration
        conflict_cols: Columns for conflict detection (primary/unique keys)
        batch_size: Rows per batch (default: 5000)
        workers: Number of parallel workers (default: 4)
        transaction: If True, all-or-nothing
    """
    print(f"    ⚙️  Config: batch_size={batch_size}, workers={workers}")

    columns = list(df_source.columns)

    # Cache source for reuse
    df_source = df_source.persist(StorageLevel.MEMORY_AND_DISK)
    source_count = df_source.count()
    count_before = get_target_count(target_cfg)

    if source_count == 0:
        print("    ⚠️  No source data")
        df_source.unpersist()
        return (
            make_stats(processed=0, inserted=0, updated=0, skipped=0, failed=0),
            [],
            [],
        )

    has_conflict_cols = conflict_cols and len(conflict_cols) > 0

    # ========================================================================
    # FAST PATH: No conflict columns - just append new rows
    # ========================================================================
    if not has_conflict_cols:
        print(f"    ⚠️  No conflict columns - comparing entire rows...")

        # Read target data
        df_target = read_target_data(spark, target_cfg, columns)

        if df_target is None or df_target.count() == 0:
            print(f"    📝 Target empty, inserting all {source_count:,} rows...")
            results = write_to_db(
                df_source,
                target_cfg,
                None,
                batch_size=batch_size,
                workers=workers,
                transaction=transaction,
            )

            df_source.unpersist()
            return (
                make_stats(
                    source_count=source_count,
                    count_before=count_before,
                    inserted=len(results["success"]),
                    updated=0,
                    skipped=0,
                    failed=len(results["failed"]),
                    processed=len(results["success"]) + len(results["failed"]),
                    count_after=get_target_count(target_cfg),
                ),
                results["success"],
                results["failed"],
            )

        # Hash comparison for deduplication
        hash_expr = F.md5(
            F.concat_ws(
                "||",
                *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columns],
            )
        )

        df_source_hashed = df_source.withColumn("_hash", hash_expr)
        df_target_hashed = df_target.withColumn("_hash", hash_expr)

        target_hashes = df_target_hashed.select("_hash").distinct()
        df_new = df_source_hashed.join(target_hashes, on="_hash", how="left_anti").drop(
            "_hash"
        )

        new_count = df_new.count()
        skipped = source_count - new_count

        print(f"    🆕 New: {new_count:,} | ⏭️ Skipped: {skipped:,}")

        if new_count == 0:
            print(f"    ✅ Nothing to process!")
            df_source.unpersist()
            return (
                make_stats(
                    source_count=source_count,
                    count_before=count_before,
                    inserted=0,
                    updated=0,
                    skipped=skipped,
                    failed=0,
                    processed=0,
                    count_after=get_target_count(target_cfg),
                ),
                [],
                [],
            )

        print(f"    🚀 Inserting {new_count:,} rows...")
        results = write_to_db(
            df_new,
            target_cfg,
            None,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        df_source.unpersist()
        return (
            make_stats(
                source_count=source_count,
                count_before=count_before,
                inserted=len(results["success"]),
                updated=0,
                skipped=skipped,
                failed=len(results["failed"]),
                processed=len(results["success"]) + len(results["failed"]),
                count_after=get_target_count(target_cfg),
            ),
            results["success"],
            results["failed"],
        )

    # ========================================================================
    # OPTIMIZED PATH: With conflict columns
    # ========================================================================
    print(f"    🔍 Comparing on: {conflict_cols}")

    # Step 1: Read ONLY key columns from target (faster than full table)
    print(f"    📖 Reading target keys...")
    df_target_keys = read_target_keys(spark, target_cfg, conflict_cols)

    # Handle empty target
    if df_target_keys is None:
        print(f"    📝 Target empty, inserting all {source_count:,} rows...")
        results = write_to_db(
            df_source,
            target_cfg,
            conflict_cols,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        df_source.unpersist()
        return (
            make_stats(
                source_count=source_count,
                count_before=count_before,
                inserted=len(results["success"]),
                updated=0,
                skipped=0,
                failed=len(results["failed"]),
                processed=len(results["success"]) + len(results["failed"]),
                count_after=get_target_count(target_cfg),
            ),
            results["success"],
            results["failed"],
        )

    df_target_keys = df_target_keys.persist(StorageLevel.MEMORY_AND_DISK)
    target_count = df_target_keys.count()

    if target_count == 0:
        print(f"    📝 Target empty, inserting all {source_count:,} rows...")
        results = write_to_db(
            df_source,
            target_cfg,
            conflict_cols,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )

        df_source.unpersist()
        df_target_keys.unpersist()
        return (
            make_stats(
                source_count=source_count,
                count_before=count_before,
                inserted=len(results["success"]),
                updated=0,
                skipped=0,
                failed=len(results["failed"]),
                processed=len(results["success"]) + len(results["failed"]),
                count_after=get_target_count(target_cfg),
            ),
            results["success"],
            results["failed"],
        )

    print(f"    📊 Source: {source_count:,} | Target: {target_count:,}")

    # Step 2: Find NEW rows (not in target by key)
    df_new = df_source.join(df_target_keys, on=conflict_cols, how="left_anti")
    new_count = df_new.count()
    print(f"    🆕 New rows: {new_count:,}")

    # Step 3: Find EXISTING rows (in both source and target by key)
    df_existing = df_source.join(df_target_keys, on=conflict_cols, how="inner")
    existing_count = df_existing.count()

    # Cleanup target keys - no longer needed
    df_target_keys.unpersist()

    print(f"    🔄 Existing rows: {existing_count:,}")

    # Step 4: For existing rows, compare data to find changed rows
    skipped_count = 0
    df_changed = None
    changed_count = 0

    if existing_count > 0:
        print(f"    🔍 Comparing existing rows for changes...")

        # Read full target data for existing IDs only
        # Build a query to get only the rows we need
        existing_ids = [
            row[conflict_cols[0]]
            for row in df_existing.select(conflict_cols[0]).collect()
        ]

        if len(existing_ids) > 0:
            # Read target data for comparison
            df_target_full = read_target_data(spark, target_cfg, columns)

            if df_target_full is not None:
                # Create hash for comparison (exclude timestamp columns that change)
                # Compare all columns except created_at, updated_at (which often change due to NOW())
                compare_cols = [
                    c for c in columns if c not in ["created_at", "updated_at"]
                ]

                hash_expr = F.md5(
                    F.concat_ws(
                        "||",
                        *[
                            F.coalesce(F.col(c).cast("string"), F.lit("NULL"))
                            for c in compare_cols
                        ],
                    )
                )

                df_existing_hashed = df_existing.withColumn("_src_hash", hash_expr)
                df_target_hashed = df_target_full.withColumn("_tgt_hash", hash_expr)

                # Join on conflict cols and compare hashes
                df_compare = df_existing_hashed.alias("src").join(
                    df_target_hashed.alias("tgt"), on=conflict_cols, how="inner"
                )

                # Find rows where hash differs (changed rows)
                df_changed = df_compare.filter(
                    F.col("src._src_hash") != F.col("tgt._tgt_hash")
                ).select([f"src.{c}" for c in columns])

                changed_count = df_changed.count()
                skipped_count = existing_count - changed_count

                print(f"    ✏️  Changed rows: {changed_count:,}")
                print(f"    ⏭️  Skipped (unchanged): {skipped_count:,}")
            else:
                # Fallback: process all existing rows
                df_changed = df_existing
                changed_count = existing_count
                print(f"    ⚠️  Could not compare, processing all existing rows")
        else:
            df_changed = df_existing
            changed_count = existing_count
    else:
        print(f"    ⏭️  Skipped: 0")

    # Step 5: Process only NEW + CHANGED rows
    total_to_process = new_count + changed_count

    if total_to_process == 0:
        print(f"    ✅ Nothing to process - all data is up to date!")
        df_source.unpersist()
        return (
            make_stats(
                source_count=source_count,
                count_before=target_count,
                new_rows=new_count,
                existing_rows=existing_count,
                changed_rows=changed_count,
                inserted=0,
                updated=0,
                skipped=skipped_count,
                failed=0,
                processed=0,
                count_after=get_target_count(target_cfg),
            ),
            [],
            [],
        )

    # Combine new + changed for upsert
    if new_count > 0 and changed_count > 0:
        df_to_process = df_new.union(df_changed)
    elif new_count > 0:
        df_to_process = df_new
    elif changed_count > 0:
        df_to_process = df_changed
    else:
        df_to_process = df_new  # Fallback

    print(
        f"    🚀 Processing {total_to_process:,} rows with UPSERT (new: {new_count:,}, changed: {changed_count:,})..."
    )

    results = write_to_db(
        df_to_process,
        target_cfg,
        conflict_cols,
        batch_size=batch_size,
        workers=workers,
        transaction=transaction,
    )

    success_data = results["success"]
    failed_data = results["failed"]

    df_source.unpersist()

    return (
        make_stats(
            source_count=source_count,
            count_before=target_count,
            new_rows=new_count,
            existing_rows=existing_count,
            changed_rows=changed_count,
            inserted=new_count if len(failed_data) == 0 else "N/A",
            updated=changed_count if len(failed_data) == 0 else "N/A",
            skipped=skipped_count,
            success=len(success_data),
            failed=len(failed_data),
            processed=len(success_data) + len(failed_data),
            count_after=get_target_count(target_cfg),
        ),
        success_data,
        failed_data,
    )


# ============================================================================
# DISPATCHER
# ============================================================================


def run_method(
    spark,
    df,
    target_cfg,
    method: str,
    conflict_columns: List[str] = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    workers: int = DEFAULT_WORKERS,
    transaction: bool = False,
) -> Tuple[Dict, List, List]:
    """
    Dispatcher function to run the appropriate ETL method.

    Args:
        spark: SparkSession instance
        df: Source DataFrame
        target_cfg: Target database configuration
        method: One of 'overwrite', 'append', 'truncate', 'upsert'
        conflict_columns: List of columns for upsert conflict detection
        batch_size: Rows per batch (default: 5000)
        workers: Number of parallel workers (default: 4)
        transaction: If True, all-or-nothing

    Returns:
        Tuple of (stats dictionary, success data list, failed data list)
    """
    print(f"    📋 Method: {method.upper()}")

    if method == "overwrite":
        return overwrite_dataframe(
            spark,
            df,
            target_cfg,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )
    elif method == "append":
        return append_dataframe(
            spark,
            df,
            target_cfg,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )
    elif method == "truncate":
        return truncate_dataframe(
            spark,
            df,
            target_cfg,
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )
    elif method == "upsert":
        return upsert_dataframe(
            spark,
            df,
            target_cfg,
            conflict_columns or [],
            batch_size=batch_size,
            workers=workers,
            transaction=transaction,
        )
    else:
        raise ValueError(
            f"Unknown method: {method}. Supported: overwrite, append, truncate, upsert"
        )
