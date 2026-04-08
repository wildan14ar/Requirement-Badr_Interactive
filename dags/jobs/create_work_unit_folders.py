#!/usr/bin/env python3
"""
Script untuk membuat folder work unit di dalam folder official_document_type
yang memiliki scope_level = UNIT_PROJECT

Untuk setiap official_document_type dengan scope_level = UNIT_PROJECT:
- Ambil folder_id nya
- Buat child folder untuk setiap work_unit yang ada di sistem
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from ulid import ULID
import argparse

# Database connection
DATABASE_URL = "postgresql://adhidms:adhidms2O2S@192.168.11.41:5432/postgres"


def generate_ulid():
    """Generate ULID baru"""
    return str(ULID())


def connect_db():
    """Koneksi ke database"""
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        print(f"❌ Error koneksi database: {e}")
        return None


def get_all_work_units(cursor):
    """Get all work units from database"""
    cursor.execute("""
        SELECT id, name, unit_code, parent_division_name, is_active
        FROM work_units 
        WHERE is_active = true
        ORDER BY name
    """)
    return cursor.fetchall()


def get_unit_project_document_types(cursor):
    """Get all official_document_types with scope_level = UNIT_PROJECT"""
    cursor.execute("""
        SELECT odt.id, odt.name, odt.folder_id, odt.scope_level, f.name as folder_name
        FROM official_document_types odt
        LEFT JOIN folders f ON f.id = odt.folder_id
        WHERE odt.scope_level = 'UNIT_PROJECT'
        AND odt.folder_id IS NOT NULL
        ORDER BY odt.name
    """)
    return cursor.fetchall()


def folder_exists(cursor, name, parent_id):
    """Check if folder with name exists under parent_id"""
    cursor.execute("""
        SELECT id FROM folders 
        WHERE name = %s AND parent_id = %s
    """, (name, parent_id))
    result = cursor.fetchone()
    return result['id'] if result else None


def create_folder(cursor, name, parent_id, work_unit_id=None, owner_id=None, created_by=None):
    """Create a folder and return its ID"""
    now = datetime.now()
    folder_id = generate_ulid()
    
    cursor.execute("""
        INSERT INTO folders (
            id, parent_id, name, keywords, 
            access_unit_id, is_active, is_has_children,
            folder_count, document_count,
            owner_id, created_by, type_folder,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s
        )
    """, (
        folder_id, parent_id, name, name.lower().replace(' ', ','),
        work_unit_id, True, False,
        0, 0,
        owner_id, created_by, 'work_unit',
        now, now
    ))
    
    return folder_id


def update_folder_count(cursor, parent_id, increment=1):
    """Update folder_count and is_has_children for parent folder"""
    cursor.execute("""
        UPDATE folders 
        SET folder_count = folder_count + %s,
            is_has_children = true,
            updated_at = %s
        WHERE id = %s
    """, (increment, datetime.now(), parent_id))


def get_admin_user(cursor):
    """Get admin user for created_by and owner_id"""
    cursor.execute("""
        SELECT id FROM users 
        WHERE email LIKE '%admin%' OR username LIKE '%admin%' 
        LIMIT 1
    """)
    result = cursor.fetchone()
    return result['id'] if result else None


def create_work_unit_folders_for_document_type(cursor, doc_type, work_units, admin_id):
    """Create work unit folders inside document type's folder"""
    folder_id = doc_type['folder_id']
    doc_type_name = doc_type['name']
    
    created_count = 0
    skipped_count = 0
    
    for work_unit in work_units:
        work_unit_name = work_unit['name']
        work_unit_id = work_unit['id']
        
        # Check if folder already exists
        existing_id = folder_exists(cursor, work_unit_name, folder_id)
        
        if existing_id:
            skipped_count += 1
            continue
        
        # Create folder
        new_folder_id = create_folder(
            cursor,
            name=work_unit_name,
            parent_id=folder_id,
            work_unit_id=work_unit_id,
            owner_id=admin_id,
            created_by=admin_id
        )
        
        created_count += 1
    
    # Update parent folder count
    if created_count > 0:
        update_folder_count(cursor, folder_id, created_count)
    
    return created_count, skipped_count


def run_create_folders(conn):
    """Main function to create work unit folders"""
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("🚀 CREATE WORK UNIT FOLDERS FOR UNIT_PROJECT DOCUMENT TYPES")
    print("=" * 70)
    
    # Get admin user
    admin_id = get_admin_user(cursor)
    if admin_id:
        print(f"\n✅ Using admin user: {admin_id}")
    else:
        print("\n⚠️  No admin user found")
    
    # Get all work units
    work_units = get_all_work_units(cursor)
    print(f"📋 Found {len(work_units)} active work units")
    
    if not work_units:
        print("❌ No work units found!")
        return
    
    # List work units
    print("\n📂 Work Units:")
    for wu in work_units[:10]:  # Show first 10
        print(f"   • {wu['name']}")
    if len(work_units) > 10:
        print(f"   ... and {len(work_units) - 10} more")
    
    # Get document types with UNIT_PROJECT scope
    doc_types = get_unit_project_document_types(cursor)
    print(f"\n📋 Found {len(doc_types)} document types with scope_level = UNIT_PROJECT")
    
    if not doc_types:
        print("❌ No UNIT_PROJECT document types found!")
        return
    
    # Process each document type
    print("\n" + "-" * 70)
    print("🔄 Creating work unit folders...")
    print("-" * 70)
    
    total_created = 0
    total_skipped = 0
    
    for doc_type in doc_types:
        print(f"\n📁 {doc_type['name']}")
        print(f"   └─ Parent folder: {doc_type['folder_name']} ({doc_type['folder_id']})")
        
        created, skipped = create_work_unit_folders_for_document_type(
            cursor, doc_type, work_units, admin_id
        )
        
        total_created += created
        total_skipped += skipped
        
        print(f"   └─ Created: {created}, Skipped (exists): {skipped}")
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"   Document types processed: {len(doc_types)}")
    print(f"   Work units: {len(work_units)}")
    print(f"   Total folders created: {total_created}")
    print(f"   Total folders skipped: {total_skipped}")
    print(f"   Expected total new folders: {len(doc_types) * len(work_units)}")
    
    return total_created, total_skipped


def run_delete_folders(conn):
    """Delete work unit folders created by this script"""
    cursor = conn.cursor()
    
    print("\n" + "=" * 70)
    print("🗑️  DELETE WORK UNIT FOLDERS")
    print("=" * 70)
    
    # Get document types with UNIT_PROJECT scope
    doc_types = get_unit_project_document_types(cursor)
    print(f"\n📋 Found {len(doc_types)} UNIT_PROJECT document types")
    
    total_deleted = 0
    
    for doc_type in doc_types:
        folder_id = doc_type['folder_id']
        
        # Delete child folders with type_folder = 'work_unit'
        cursor.execute("""
            DELETE FROM folders 
            WHERE parent_id = %s AND type_folder = 'work_unit'
        """, (folder_id,))
        
        deleted = cursor.rowcount
        total_deleted += deleted
        
        # Reset parent folder count
        cursor.execute("""
            UPDATE folders 
            SET folder_count = 0, is_has_children = false, updated_at = %s
            WHERE id = %s
        """, (datetime.now(), folder_id))
        
        print(f"   • {doc_type['name']}: deleted {deleted} folders")
    
    print(f"\n✅ Total deleted: {total_deleted} folders")
    return total_deleted


def main():
    parser = argparse.ArgumentParser(
        description='Create work unit folders inside UNIT_PROJECT document types'
    )
    parser.add_argument(
        '--delete',
        action='store_true',
        help='Delete work unit folders instead of creating'
    )
    parser.add_argument(
        '--auto-commit',
        action='store_true',
        help='Auto commit without confirmation'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("🚀 WORK UNIT FOLDERS GENERATOR")
    print("   For UNIT_PROJECT Document Types")
    if args.delete:
        print("   Mode: DELETE")
    else:
        print("   Mode: CREATE")
    print("=" * 70)
    
    # Connect to database
    print("\n📡 Connecting to database...")
    conn = connect_db()
    if not conn:
        return 1
    
    print("✅ Connected!")
    
    try:
        if args.delete:
            run_delete_folders(conn)
        else:
            run_create_folders(conn)
        
        # Commit confirmation
        if args.auto_commit:
            conn.commit()
            print("\n✅ Changes committed!")
        else:
            print("\n" + "-" * 70)
            choice = input("Commit changes to database? (yes/no): ").lower()
            if choice in ['yes', 'y']:
                conn.commit()
                print("\n✅ Changes committed!")
            else:
                conn.rollback()
                print("\n❌ Changes rolled back!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    finally:
        conn.close()
        print("\n🔌 Database connection closed")
    
    return 0


if __name__ == "__main__":
    exit(main())
