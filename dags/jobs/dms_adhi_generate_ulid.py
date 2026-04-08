# Update Missing ULID Values in Database Tables
# This script only updates rows that have NULL ulid_id values
# It does NOT create new columns

import os
import psycopg2
from ulid import ULID

# Database Configuration from Environment Variables
DB_CONFIG = {
    "host": os.getenv("host"),
    "port": os.getenv("port"),
    "database": os.getenv("db"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "schema": os.getenv("schema"),
}


def get_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            database=DB_CONFIG["database"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
        )
        # Set schema
        cur = conn.cursor()
        cur.execute(f"SET search_path TO {DB_CONFIG['schema']}")
        conn.commit()
        cur.close()
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def get_tables_with_ulid_column():
    """Get all tables that have ulid_id column"""
    conn = get_connection()
    if not conn:
        return []

    try:
        query = f"""
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = '{DB_CONFIG['schema']}'
        AND column_name = 'ulid_id'
        ORDER BY table_name
        """
        cur = conn.cursor()
        cur.execute(query)
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return tables
    except Exception as e:
        print(f"Error getting tables: {e}")
        if conn:
            conn.close()
        return []


def check_id_column_exists(conn, table_name):
    """Check if table has an 'id' column"""
    try:
        cur = conn.cursor()
        query = """
        SELECT EXISTS (
            SELECT 1 
            FROM information_schema.columns 
            WHERE table_schema = %s 
            AND table_name = %s 
            AND column_name = 'id'
        )
        """
        cur.execute(query, (DB_CONFIG["schema"], table_name))
        exists = cur.fetchone()[0]
        cur.close()
        return exists
    except Exception as e:
        print(f"Error checking id column: {e}")
        return False


def update_missing_ulid(table_name):
    """Update only rows that have NULL ulid_id values"""
    conn = get_connection()
    if not conn:
        return False

    try:
        cur = conn.cursor()

        # Get total row count
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_count = cur.fetchone()[0]
        print(f"Total records: {total_count:,}")

        # Check how many rows need ULID
        cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE ulid_id IS NULL")
        null_count = cur.fetchone()[0]

        if null_count == 0:
            print(f"✓ All records already have ULID values")
            cur.close()
            conn.close()
            return True

        print(f"Found {null_count:,} records without ULID")

        # Check if table has 'id' column
        has_id_column = check_id_column_exists(conn, table_name)

        # Get all records that don't have ULID yet
        if has_id_column:
            print(f"Using 'id' column as identifier...")
            cur.execute(
                f"SELECT id FROM {table_name} WHERE ulid_id IS NULL ORDER BY id"
            )
        else:
            print(f"Using 'ctid' (internal row ID) as identifier...")
            cur.execute(
                f"SELECT ctid FROM {table_name} WHERE ulid_id IS NULL ORDER BY ctid"
            )

        records = cur.fetchall()
        print(f"Generating and updating {len(records):,} ULID values...")

        # Update in batches for performance
        batch_size = 1000
        total_updated = 0

        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]

            # Generate ULIDs for batch
            updates = [(str(ULID()), record[0]) for record in batch]

            # Update batch
            if has_id_column:
                update_query = f"UPDATE {table_name} SET ulid_id = %s WHERE id = %s"
            else:
                update_query = f"UPDATE {table_name} SET ulid_id = %s WHERE ctid = %s"

            cur.executemany(update_query, updates)
            conn.commit()

            total_updated += len(batch)
            progress_pct = (total_updated / len(records)) * 100
            print(
                f"  Progress: {total_updated:,}/{len(records):,} records ({progress_pct:.1f}%)"
            )

        print(f"✓ Successfully updated {total_updated:,} records")

        cur.close()
        conn.close()
        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        if conn:
            conn.rollback()
            conn.close()
        return False


def main():
    print("=" * 80)
    print("UPDATING MISSING ULID VALUES IN DATABASE TABLES")
    print("=" * 80)
    print("\nThis script only updates rows with NULL ulid_id values.")
    print("It does NOT create new columns.\n")

    # Get all tables that have ulid_id column
    tables_with_ulid = get_tables_with_ulid_column()

    if not tables_with_ulid:
        print("✗ No tables found with 'ulid_id' column")
        return

    print(f"Found {len(tables_with_ulid)} tables with 'ulid_id' column\n")

    success_count = 0
    failed_tables = []

    for idx, table_name in enumerate(tables_with_ulid, 1):
        print(f"\n[{idx}/{len(tables_with_ulid)}] Processing {table_name}")
        print("-" * 60)

        result = update_missing_ulid(table_name)
        if result:
            success_count += 1
        else:
            failed_tables.append(table_name)

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\n✓ Successfully processed: {success_count}/{len(tables_with_ulid)} tables")

    if failed_tables:
        print(f"\n✗ Failed tables ({len(failed_tables)}):")
        for table in failed_tables:
            print(f"  - {table}")
    else:
        print("\n🎉 All tables processed successfully!")

    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
