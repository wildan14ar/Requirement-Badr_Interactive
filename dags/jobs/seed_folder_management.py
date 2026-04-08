#!/usr/bin/env python3
"""
Seed Folder Management - PT ADHI KARYA (Persero) Tbk.
Arsitektur Dokumen Naskah Dinas

This script creates the folder structure for document management system
based on the official document architecture.

Usage:
    python seed_folder_management.py              # Seed folders
    python seed_folder_management.py --delete     # Delete all seeded folders
    python seed_folder_management.py --reset      # Delete then re-seed
"""

import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from ulid import ULID

# Database configuration
DB_CONFIG = {
    'host': '192.168.11.41',
    'port': 5432,
    'database': 'postgres',
    'user': 'adhidms',
    'password': 'adhidms2O2S'
}

# Folder structure based on "ARSITEKTUR DOKUMEN NASKAH DINAS PT ADHI KARYA (Persero) Tbk."
FOLDER_STRUCTURE = {
    "Naskah Dinas Arahan": {
        "Naskah Dinas Pengaturan": {
            "Anggaran Dasar Perusahaan": {},
            "Kebijakan": {},
            "Tata Kelola": {},
            "Prosedur": {},
            "Petunjuk Kerja": {},
        },
        "Naskah Dinas Penetapan": {
            "Keputusan RUPS": {},
            "Rencana Strategis Perusahaan": {
                "Rencana Jangka Panjang Perusahaan (RJPP)": {},
                "Rencana Kerja Anggaran Perusahaan (RKAP)": {},
                "Kontrak Manajemen Tahunan": {},
                "Rencana Strategis TI": {},
            },
            "BOD dan BOC Charter": {},
            "Surat Keputusan Direksi": {},
            "Risalah Rapat Gabungan Direksi dan Komisaris": {},
            "Risalah Rapat Direksi": {},
        },
        "Naskah Dinas Penugasan": {
            "Surat Tugas": {
                "Surat Ketetapan Penugasan": {},
                "Surat Tugas Khusus": {},
                "Surat Tugas Lain-Lain": {},
            },
        },
    },
    "Naskah Dinas Korespondensi": {
        "Korespondensi Internal": {
            "Nota Dinas": {
                "Nota Dinas Direksi": {},
                "Nota Dinas + Lembar Catatan": {},
                "Nota Dinas": {},
            },
            "Memo": {
                "Memo Direksi": {},
                "Memo Dinas Unit Kerja": {},
                "Memo Dinas Unit Proyek": {},
            },
            "Disposisi": {},
        },
        "Korespondensi Eksternal": {
            "Surat Dinas": {},
            "Surat Undangan": {
                "Surat Undangan Internal": {},
                "Surat Undangan Eksternal": {},
            },
        },
    },
    "Naskah Dinas Khusus": {
        "Surat Edaran": {},
        "Surat Perjanjian": {
            "Perjanjian Pengadaan": {},
            "Perjanjian Non-Pengadaan": {},
        },
        "Surat Kuasa": {},
        "Surat Keterangan": {},
        "Surat Pengantar": {},
        "Surat Pernyataan": {},
        "Berita Acara": {},
        "Notulensi": {},
        "Sertifikat": {},
        "Laporan": {
            "Laporan Umum": {},
            "Pelaporan BUMN": {},
            "Laporan Tahunan": {},
            "Dipublikasi (Annual Report)": {},
        },
        "Formulir": {},
        "Naskah Dinas Elektronik": {},
    },
}

# Keywords for main categories
FOLDER_KEYWORDS = {
    "Naskah Dinas Arahan": "arahan,direktif,kebijakan,pengaturan,penetapan,penugasan",
    "Naskah Dinas Korespondensi": "korespondensi,surat,memo,nota,internal,eksternal",
    "Naskah Dinas Khusus": "khusus,edaran,perjanjian,kuasa,keterangan,laporan",
    "Naskah Dinas Pengaturan": "pengaturan,anggaran,kebijakan,tata kelola,prosedur",
    "Naskah Dinas Penetapan": "penetapan,keputusan,rups,strategis,charter,risalah",
    "Naskah Dinas Penugasan": "penugasan,surat tugas,ketetapan",
    "Korespondensi Internal": "internal,nota dinas,memo,disposisi",
    "Korespondensi Eksternal": "eksternal,surat dinas,undangan",
}


def generate_ulid():
    """Generate ULID string"""
    return str(ULID())


def get_connection():
    """Get database connection"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)


def execute_query(conn, query, params=None, fetch=False):
    """Execute a database query"""
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        print(f"Query: {query}")
        if params:
            print(f"Params: {params}")
        conn.rollback()
        raise e


def get_admin_user(conn):
    """Get admin user for created_by and owner_id"""
    query = "SELECT id FROM users WHERE email LIKE '%admin%' OR username LIKE '%admin%' LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    if result:
        return result[0]['id']
    
    # Fallback: get any user
    query = "SELECT id FROM users LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    if result:
        return result[0]['id']
    
    return None


def folder_exists(conn, name, parent_id=None):
    """Check if folder exists by name and parent_id"""
    if parent_id:
        query = "SELECT id FROM folders WHERE name = %s AND parent_id = %s"
        result = execute_query(conn, query, (name, parent_id), fetch=True)
    else:
        query = "SELECT id FROM folders WHERE name = %s AND parent_id IS NULL"
        result = execute_query(conn, query, (name,), fetch=True)
    
    if result:
        return result[0]['id']
    return None


def create_folder(conn, name, parent_id=None, keywords=None, owner_id=None, created_by=None):
    """Create a folder and return its ID"""
    # Check if folder already exists
    existing_id = folder_exists(conn, name, parent_id)
    if existing_id:
        print(f"  ⏭️  Folder exists: {name} ({existing_id})")
        return existing_id
    
    folder_id = generate_ulid()
    now = datetime.now()
    
    # Get keywords from predefined or use name
    if keywords is None:
        keywords = FOLDER_KEYWORDS.get(name, name.lower().replace(" ", ","))
    
    query = """
        INSERT INTO folders (
            id, parent_id, name, keywords, 
            is_active, folder_count, document_count,
            owner_id, created_by, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s
        )
    """
    params = (
        folder_id, parent_id, name, keywords,
        True, 0, 0,
        owner_id, created_by, now, now
    )
    
    execute_query(conn, query, params)
    print(f"  ✅ Created folder: {name} ({folder_id})")
    return folder_id


def update_folder_count(conn, parent_id, count):
    """Update folder_count for a parent folder"""
    if parent_id:
        query = "UPDATE folders SET folder_count = %s, updated_at = %s WHERE id = %s"
        execute_query(conn, query, (count, datetime.now(), parent_id))


def create_folder_structure(conn, structure, parent_id=None, depth=0, owner_id=None, created_by=None):
    """Recursively create folder structure"""
    created_folders = []
    indent = "  " * depth
    
    for folder_name, children in structure.items():
        if depth == 0:
            print(f"\n📁 {folder_name}")
        
        folder_id = create_folder(
            conn, 
            folder_name, 
            parent_id=parent_id,
            owner_id=owner_id,
            created_by=created_by
        )
        created_folders.append(folder_id)
        
        # Create children recursively
        if children:
            child_folders = create_folder_structure(
                conn, 
                children, 
                parent_id=folder_id, 
                depth=depth + 1,
                owner_id=owner_id,
                created_by=created_by
            )
            # Update folder count
            update_folder_count(conn, folder_id, len(child_folders))
            created_folders.extend(child_folders)
    
    return created_folders


def delete_seeded_folders(conn):
    """Delete all seeded folders (top-level folders from our structure)"""
    print("\n" + "=" * 70)
    print("🗑️  Deleting Seeded Folders")
    print("=" * 70)
    
    # Get root folder names from our structure
    root_folder_names = list(FOLDER_STRUCTURE.keys())
    
    deleted_count = 0
    for name in root_folder_names:
        # Find folder by name (root folders have no parent)
        query = "SELECT id FROM folders WHERE name = %s AND parent_id IS NULL"
        result = execute_query(conn, query, (name,), fetch=True)
        
        if result:
            folder_id = result[0]['id']
            # Delete folder (CASCADE will delete children)
            query = "DELETE FROM folders WHERE id = %s"
            execute_query(conn, query, (folder_id,))
            print(f"  ✅ Deleted folder tree: {name}")
            deleted_count += 1
        else:
            print(f"  ⏭️  Folder not found: {name}")
    
    print(f"\n✅ Deleted {deleted_count} root folders (with all children)")
    return deleted_count


def count_folders_recursive(structure):
    """Count total folders in structure"""
    count = 0
    for name, children in structure.items():
        count += 1
        if children:
            count += count_folders_recursive(children)
    return count


def run_seed(conn):
    """Run the seeding process"""
    print("\n" + "=" * 70)
    print("🌱 SEEDING FOLDER MANAGEMENT")
    print("   Arsitektur Dokumen Naskah Dinas PT ADHI KARYA")
    print("=" * 70)
    
    # Get admin user
    admin_id = get_admin_user(conn)
    if admin_id:
        print(f"\n✅ Using admin user: {admin_id}")
    else:
        print("\n⚠️  No admin user found, folders will have no owner")
    
    # Count total folders
    total_expected = count_folders_recursive(FOLDER_STRUCTURE)
    print(f"📊 Total folders to create: {total_expected}")
    
    # Create folder structure
    print("\n" + "=" * 70)
    print("📁 Creating Folder Structure")
    print("=" * 70)
    
    created_folders = create_folder_structure(
        conn, 
        FOLDER_STRUCTURE,
        owner_id=admin_id,
        created_by=admin_id
    )
    
    # Summary
    print("\n" + "=" * 70)
    print("🎉 SEEDING COMPLETED!")
    print("=" * 70)
    print(f"\n📊 SUMMARY:")
    print(f"  • Total folders created/verified: {len(created_folders)}")
    print(f"  • Root categories: {len(FOLDER_STRUCTURE)}")
    
    # Print tree structure
    print("\n📂 FOLDER TREE:")
    print_folder_tree(FOLDER_STRUCTURE)
    
    return created_folders


def print_folder_tree(structure, prefix="", is_last=True):
    """Print folder tree structure"""
    items = list(structure.items())
    for i, (name, children) in enumerate(items):
        is_last_item = (i == len(items) - 1)
        connector = "└── " if is_last_item else "├── "
        print(f"{prefix}{connector}{name}")
        
        if children:
            extension = "    " if is_last_item else "│   "
            print_folder_tree(children, prefix + extension, is_last_item)


def main():
    parser = argparse.ArgumentParser(
        description='Seed Folder Management for PT ADHI KARYA DMS'
    )
    parser.add_argument(
        '--delete', 
        action='store_true',
        help='Delete all seeded folders'
    )
    parser.add_argument(
        '--reset', 
        action='store_true',
        help='Delete all seeded folders then re-seed'
    )
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("🚀 FOLDER MANAGEMENT SEEDER")
    print("   PT ADHI KARYA (Persero) Tbk.")
    if args.delete:
        print("   Mode: DELETE")
    elif args.reset:
        print("   Mode: RESET (Delete + Seed)")
    else:
        print("   Mode: SEED")
    print("=" * 70)
    
    try:
        conn = get_connection()
        print("✅ Connected to database")
        
        if args.delete:
            delete_seeded_folders(conn)
        elif args.reset:
            delete_seeded_folders(conn)
            run_seed(conn)
        else:
            run_seed(conn)
        
        conn.close()
        print("\n🔌 Database connection closed")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
