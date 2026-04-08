"""
Script untuk inject dummy data Outgoing & Incoming Letter ke Database PostgreSQL
Sesuai dengan struktur sistem baru dengan relasi lengkap

Requirements:
    pip install psycopg2-binary python-ulid

Usage:
    python inject_dummy_letters.py                  # Inject all data (upsert users)
    python inject_dummy_letters.py --mode inject    # Inject all data (upsert users)
    python inject_dummy_letters.py --mode delete-dummy   # Delete dummy users & related data
    python inject_dummy_letters.py --mode delete-test    # Delete test users & related data  
    python inject_dummy_letters.py --mode delete-all     # Delete both dummy & test users & related data
    python inject_dummy_letters.py --mode clean          # Delete all dummy/test data then re-inject
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
from ulid import ULID
import sys
import argparse


# Database Configuration
DB_CONFIG = {
    'host': '192.168.11.41',
    'port': 5432,
    'database': 'postgres',
    'user': 'adhidms',
    'password': 'adhidms2O2S'
}

# Email patterns for dummy and test users
DUMMY_EMAIL_PATTERN = '%@adhi.co.id'
DUMMY_EMAIL_LIKE = 'dummydms%@adhi.co.id'
TEST_EMAIL_PATTERN = '%@testing.com'


def generate_ulid():
    """Generate ULID string"""
    return str(ULID())


def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        sys.exit(1)


def execute_query(conn, query, params=None, fetch=False):
    """Execute SQL query"""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            if fetch:
                return cur.fetchall()
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        print(f"❌ Error executing query: {e}")
        print(f"Query: {query}")
        print(f"Params: {params}")
        raise


def check_user_exists(conn, email):
    """Check if user exists"""
    query = "SELECT id FROM users WHERE email = %s"
    result = execute_query(conn, query, (email,), fetch=True)
    return result[0] if result else None


def get_users_by_pattern(conn, email_pattern):
    """Get all users matching email pattern"""
    query = "SELECT id, email, name FROM users WHERE email LIKE %s"
    result = execute_query(conn, query, (email_pattern,), fetch=True)
    return result if result else []


def delete_user_related_data(conn, user_id, user_email):
    """Delete all data related to a specific user (cascade delete)"""
    print(f"\n  🗑️  Deleting related data for user: {user_email}")
    
    # Order matters - delete child records first
    delete_queries = [
        # Discussion forum messages where user created
        ("DELETE FROM discussion_forum_messages WHERE created_by = %s", "discussion_forum_messages"),
        
        # Message reads
        ("DELETE FROM message_reads WHERE user_id = %s", "message_reads"),
        
        # Letter participants
        ("DELETE FROM letter_participants WHERE user_id = %s", "letter_participants"),
        
        # Workflow reviewers
        ("DELETE FROM workflow_reviewers WHERE user_id = %s", "workflow_reviewers"),
        
        # Workflow approvers  
        ("DELETE FROM workflow_approvers WHERE user_id = %s", "workflow_approvers"),
        
        # Carbon copies
        ("DELETE FROM carbon_copies WHERE user_id = %s", "carbon_copies"),
        
        # Target users
        ("DELETE FROM target_users WHERE user_id = %s", "target_users"),
        
        # Letter dispositions (as sender or receiver)
        ("DELETE FROM letter_dispositions WHERE sender_id = %s OR receiver_id = %s", "letter_dispositions (sender/receiver)"),
        
        # Access rights
        ("DELETE FROM access_rights WHERE user_id = %s", "access_rights"),
        
        # Folder pics
        ("DELETE FROM folder_pics WHERE user_id = %s", "folder_pics"),
        
        # Work unit pics
        ("DELETE FROM work_unit_pics WHERE user_id = %s", "work_unit_pics"),
        
        # Document type reviewers
        ("DELETE FROM document_type_reviewers WHERE user_id = %s", "document_type_reviewers"),
        
        # User access histories
        ("DELETE FROM user_access_histories WHERE user_id = %s", "user_access_histories"),
    ]
    
    for query, table_name in delete_queries:
        try:
            # Handle queries with multiple params
            if query.count('%s') == 2:
                execute_query(conn, query, (user_id, user_id))
            else:
                execute_query(conn, query, (user_id,))
        except Exception as e:
            print(f"    ⚠️  Warning deleting from {table_name}: {e}")


def delete_letters_created_by_user(conn, user_id, user_email):
    """Delete outgoing and incoming letters created by user, with all related data"""
    print(f"  🗑️  Deleting letters created by: {user_email}")
    
    # Get outgoing letters created by user
    outgoing_query = "SELECT id FROM outgoing_letters WHERE created_by_user_id = %s OR drafter_user_id = %s"
    outgoing_letters = execute_query(conn, outgoing_query, (user_id, user_id), fetch=True)
    
    for letter in (outgoing_letters or []):
        letter_id = letter['id']
        delete_outgoing_letter_cascade(conn, letter_id)
    
    if outgoing_letters:
        print(f"    ✅ Deleted {len(outgoing_letters)} outgoing letters")
    
    # Get incoming letters created by user
    incoming_query = "SELECT id FROM incoming_letters WHERE created_by_user_id = %s"
    incoming_letters = execute_query(conn, incoming_query, (user_id,), fetch=True)
    
    for letter in (incoming_letters or []):
        letter_id = letter['id']
        delete_incoming_letter_cascade(conn, letter_id)
    
    if incoming_letters:
        print(f"    ✅ Deleted {len(incoming_letters)} incoming letters")


def delete_outgoing_letter_cascade(conn, letter_id):
    """Delete outgoing letter with all related data"""
    delete_queries = [
        "DELETE FROM discussion_forum_messages WHERE discussion_forum_id IN (SELECT id FROM discussion_forums WHERE outgoing_letter_id = %s)",
        "DELETE FROM discussion_forums WHERE outgoing_letter_id = %s",
        "DELETE FROM letter_participants WHERE outgoing_letter_id = %s",
        "DELETE FROM workflow_reviewers WHERE outgoing_letter_id = %s",
        "DELETE FROM workflow_approvers WHERE outgoing_letter_id = %s",
        "DELETE FROM carbon_copies WHERE outgoing_letter_id = %s",
        "DELETE FROM target_users WHERE outgoing_letter_id = %s",
        "DELETE FROM target_work_units WHERE outgoing_letter_id = %s",
        "DELETE FROM documents WHERE outgoing_letter_id = %s",
        "DELETE FROM outgoing_letter_attachments WHERE outgoing_letter_id = %s",
        "DELETE FROM outgoing_letter_folder_documents WHERE outgoing_letter_id = %s",
        "DELETE FROM letter_document_relations WHERE outgoing_letter_id = %s",
        "DELETE FROM access_rights WHERE outgoing_letter_id = %s",
        "DELETE FROM outgoing_letters WHERE id = %s",
    ]
    
    for query in delete_queries:
        try:
            execute_query(conn, query, (letter_id,))
        except Exception as e:
            pass  # Ignore errors, some tables may not have the record


def delete_incoming_letter_cascade(conn, letter_id):
    """Delete incoming letter with all related data"""
    delete_queries = [
        "DELETE FROM discussion_forum_messages WHERE discussion_forum_id IN (SELECT id FROM discussion_forums WHERE incoming_letter_id = %s)",
        "DELETE FROM discussion_forums WHERE incoming_letter_id = %s",
        "DELETE FROM letter_participants WHERE incoming_letter_id = %s",
        "DELETE FROM letter_dispositions WHERE incoming_letter_id = %s",
        "DELETE FROM documents WHERE incoming_letter_id = %s",
        "DELETE FROM incoming_letter_attachments WHERE incoming_letter_id = %s",
        "DELETE FROM letter_document_relations WHERE incoming_letter_id = %s",
        "DELETE FROM access_rights WHERE incoming_letter_id = %s",
        "DELETE FROM incoming_letters WHERE id = %s",
    ]
    
    for query in delete_queries:
        try:
            execute_query(conn, query, (letter_id,))
        except Exception as e:
            pass  # Ignore errors


def delete_users_by_pattern(conn, email_pattern, user_type="users"):
    """Delete all users matching email pattern with cascade delete"""
    print(f"\n" + "=" * 70)
    print(f"🗑️  DELETING {user_type.upper()} (pattern: {email_pattern})")
    print("=" * 70)
    
    users = get_users_by_pattern(conn, email_pattern)
    
    if not users:
        print(f"  ℹ️  No {user_type} found matching pattern: {email_pattern}")
        return 0
    
    print(f"  📋 Found {len(users)} {user_type} to delete")
    
    deleted_count = 0
    for user in users:
        user_id = user['id']
        user_email = user['email']
        
        try:
            # Delete related data first
            delete_user_related_data(conn, user_id, user_email)
            delete_letters_created_by_user(conn, user_id, user_email)
            
            # Finally delete the user
            execute_query(conn, "DELETE FROM users WHERE id = %s", (user_id,))
            print(f"  ✅ Deleted user: {user_email}")
            deleted_count += 1
        except Exception as e:
            print(f"  ❌ Error deleting user {user_email}: {e}")
    
    print(f"\n✅ Deleted {deleted_count}/{len(users)} {user_type}")
    return deleted_count


# Default password hash for testing users
DEFAULT_PASSWORD_HASH = '$argon2id$v=19$m=65536,t=4,p=1$dUQvSGhDd1BlNXVuWVBjRw$m8Fcpyweu8iDdd0Kfd/cn2fUbysNVANtx1mPx172c+M'


def upsert_user(conn, email, name, position, work_unit_name, acl_role_id, npp=None, no_telp=None, password_hash=None):
    """Insert or update user"""
    existing = check_user_exists(conn, email)
    
    # Create work unit
    work_unit_id = create_work_unit(conn, work_unit_name)
    
    # Use provided password hash or default
    if password_hash is None:
        password_hash = DEFAULT_PASSWORD_HASH
    
    # Generate unique username
    base_username = email.split('@')[0]
    domain = email.split('@')[1] if '@' in email else ''
    
    # Add suffix for testing.com to avoid conflicts
    if domain == 'testing.com':
        username = f"{base_username}_test"
    else:
        username = base_username
    
    if existing:
        # Update existing user
        query = """
            UPDATE users SET 
                name = %s, position = %s, work_unit_id = %s, 
                acl_role_id = %s, npp = %s, no_telp = %s, password = %s, updated_at = %s
            WHERE id = %s
        """
        params = (name, position, work_unit_id, acl_role_id, npp, no_telp, password_hash, datetime.now(), existing['id'])
        execute_query(conn, query, params)
        print(f"  🔄 Updated user: {email} ({existing['id']})")
        return existing['id']
    else:
        # Check if username already exists, add random suffix if needed
        username_check = "SELECT id FROM users WHERE username = %s"
        if execute_query(conn, username_check, (username,), fetch=True):
            # Username exists, add unique suffix
            username = f"{username}_{generate_ulid()[-6:]}"
        
        # Create new user
        user_id = generate_ulid()
        query = """
            INSERT INTO users (
                id, name, email, username, password, 
                position, work_unit_id, acl_role_id, npp, no_telp,
                is_active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s,
                %s, %s, %s
            )
        """
        params = (
            user_id, name, email, username, password_hash,
            position, work_unit_id, acl_role_id, npp, no_telp,
            True, datetime.now(), datetime.now()
        )
        execute_query(conn, query, params)
        print(f"  ✅ Created user: {email} ({user_id})")
        return user_id


def create_work_unit(conn, name):
    """Create or get work unit"""
    # Check if exists
    query = "SELECT id FROM work_units WHERE name = %s"
    result = execute_query(conn, query, (name,), fetch=True)
    if result:
        return result[0]['id']
    
    # Get max unit_code to generate next one (unit_code is integer but may need casting)
    max_code_query = "SELECT COALESCE(MAX(unit_code::integer), 0) + 1 as next_code FROM work_units"
    max_code_result = execute_query(conn, max_code_query, fetch=True)
    next_code = max_code_result[0]['next_code'] if max_code_result else 1
    
    # Create new
    work_unit_id = generate_ulid()
    query = """
        INSERT INTO work_units (id, name, unit_code, is_active, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
    """
    params = (work_unit_id, name, next_code, True, datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created work_unit: {name} ({work_unit_id})")
    return work_unit_id


def get_acl_role(conn, role_name):
    """Get ACL role by name (does NOT create new role)"""
    # Check if exists (case insensitive)
    query = "SELECT id FROM acl_roles WHERE name ILIKE %s"
    result = execute_query(conn, query, (role_name,), fetch=True)
    if result:
        return result[0]['id']
    
    # Role not found, return None
    print(f"  ⚠️  ACL role not found: {role_name} - skipping role assignment")
    return None


def create_dummy_user(conn, email, name, position, work_unit_name, role_name='User'):
    """Create or update dummy user with work unit and role (UPSERT)"""
    # Get ACL role from existing roles only (don't create new)
    acl_role_id = get_acl_role(conn, role_name)
    
    # Use upsert function (acl_role_id can be None)
    return upsert_user(conn, email, name, position, work_unit_name, acl_role_id)


def create_user_with_acl_role_id(conn, email, name, position, work_unit_name, acl_role_id):
    """Create or update user with specific ACL role ID (UPSERT)"""
    return upsert_user(conn, email, name, position, work_unit_name, acl_role_id)


def get_all_acl_roles(conn):
    """Get all ACL roles from database"""
    query = "SELECT id, name FROM acl_roles ORDER BY name"
    result = execute_query(conn, query, fetch=True)
    return result if result else []


def create_role_testing_users(conn):
    """Create testing users for each role from acl_roles table with email format: role_name@testing.com"""
    print("\n" + "=" * 70)
    print("📋 Creating Role Testing Users from acl_roles table")
    print("=" * 70)
    
    # Get all roles from acl_roles table
    acl_roles = get_all_acl_roles(conn)
    
    if not acl_roles:
        print("  ⚠️  No ACL roles found in database")
        return {}
    
    print(f"  📋 Found {len(acl_roles)} ACL roles in database")
    
    created_users = {}
    for role in acl_roles:
        role_id = role['id']
        role_name = role['name']
        
        # Create email-safe version of role name (lowercase, replace spaces with underscore)
        email_name = role_name.lower().replace(' ', '_').replace('-', '_')
        # Remove any non-alphanumeric characters except underscore
        email_name = ''.join(c if c.isalnum() or c == '_' else '' for c in email_name)
        
        email = f"{email_name}@testing.com"
        name = f"Test {role_name}"
        position = role_name
        work_unit = "Testing Unit"
        
        user_id = create_user_with_acl_role_id(conn, email, name, position, work_unit, role_id)
        created_users[email_name] = user_id
    
    print(f"\n✅ Created {len(created_users)} role testing users")
    return created_users


def get_master_data_ids(conn):
    """Get required master data IDs"""
    data = {}
    
    # Get letter category
    query = "SELECT id FROM letter_categories LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    data['category_id'] = result[0]['id'] if result else None
    
    # Get letter sub category
    query = "SELECT id FROM letter_sub_categories LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    data['sub_category_id'] = result[0]['id'] if result else None
    
    # Get official document type
    query = "SELECT id FROM official_document_types LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    data['official_document_type_id'] = result[0]['id'] if result else None
    
    # Get warehouse location
    query = "SELECT id FROM warehouse_locations LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    data['warehouse_location_id'] = result[0]['id'] if result else None
    
    # Get security classification
    query = "SELECT id FROM security_classifications LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    data['security_classification_id'] = result[0]['id'] if result else None
    
    return data


def create_outgoing_letter(conn, users, master_data, letter_number=1):
    """Create outgoing letter with complete workflow"""
    print(f"\n📤 Creating Outgoing Letter #{letter_number}...")
    
    # Create outgoing letter
    letter_id = generate_ulid()
    query = """
        INSERT INTO outgoing_letters (
            id, recipient_type, workflow_type, subject, letter_number, letter_date,
            is_draft, is_bulk_upload, is_deleted, created_by_user_id, drafter_user_id,
            category_id, sub_category_id, official_document_type_id, warehouse_location_id,
            status, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        RETURNING id
    """
    
    today = date.today()
    letter_num = f"OUT/{today.year}/{today.month:02d}/{letter_number:04d}"
    
    params = (
        letter_id, 'internal', 'sequential', 
        f'Dummy Outgoing Letter - Test Workflow #{letter_number}',
        letter_num, today,
        False, False, False, users['creator'], users['drafter'],
        master_data.get('category_id'), master_data.get('sub_category_id'),
        master_data.get('official_document_type_id'), master_data.get('warehouse_location_id'),
        'Menunggu Review', datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created outgoing_letter: {letter_num} ({letter_id})")
    
    # Create WorkflowReviewer (dummydms2)
    reviewer_id = generate_ulid()
    query = """
        INSERT INTO workflow_reviewers (
            id, outgoing_letter_id, user_id, "order", review_status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    params = (reviewer_id, letter_id, users['reviewer'], 1, 'pending', datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created workflow_reviewer: {users['reviewer']}")
    
    # Create WorkflowApprover 1 (dummydms3 - GM)
    approver1_id = generate_ulid()
    query = """
        INSERT INTO workflow_approvers (
            id, outgoing_letter_id, user_id, "order", 
            sign_page, sign_llx, sign_lly, sign_urx, sign_ury, sign_visible, ttd_user,
            sign_status, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        approver1_id, letter_id, users['approver1'], 1,
        '1', '12', '15', '112', '65', '1', 'ttd1',
        'pending', datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created workflow_approver 1: {users['approver1']}")
    
    # Create WorkflowApprover 2 (dummydms4 - Direksi)
    approver2_id = generate_ulid()
    params = (
        approver2_id, letter_id, users['approver2'], 2,
        '1', '12', '80', '112', '130', '1', 'ttd2',
        'pending', datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created workflow_approver 2: {users['approver2']}")
    
    # Create CarbonCopy - Tembusan (dummydms5)
    cc_id = generate_ulid()
    query = """
        INSERT INTO carbon_copies (id, outgoing_letter_id, user_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = (cc_id, letter_id, users['cc'], datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created carbon_copy: {users['cc']}")
    
    # Create TargetUser - Penerima (dummydms6)
    target_id = generate_ulid()
    query = """
        INSERT INTO target_users (id, outgoing_letter_id, user_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s)
    """
    params = (target_id, letter_id, users['recipient'], datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created target_user: {users['recipient']}")
    
    # Create LetterParticipants untuk semua user yang terlibat
    participants = [
        (users['creator'], 'outgoing_letter', 'Menunggu Review'),
        (users['drafter'], 'outgoing_letter', 'Menunggu Review'),
        (users['reviewer'], 'review', 'Menunggu Review'),
        (users['approver1'], 'approve', 'Belum Baca'),
        (users['approver2'], 'approve', 'Belum Baca'),
        (users['cc'], 'carbon_copy', 'Belum Baca'),
        (users['recipient'], 'recipient', 'Belum Baca'),
    ]
    
    for user_id, segment_type, status in participants:
        participant_id = generate_ulid()
        query = """
            INSERT INTO letter_participants (
                id, outgoing_letter_id, user_id, segment_type, status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        params = (participant_id, letter_id, user_id, segment_type, status, datetime.now(), datetime.now())
        execute_query(conn, query, params)
    print(f"  ✅ Created {len(participants)} letter_participants")
    
    # Create Document/Attachment
    doc_id = generate_ulid()
    query = """
        INSERT INTO documents (
            id, outgoing_letter_id, file_name, file_url, category, version, 
            status, size, uploaded_at, created_by_user_id, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        doc_id, letter_id, f'outgoing-letter-{letter_number}.pdf',
        'https://example.com/dummy-file.pdf', 'outgoing_letter', 1,
        'active', 1024000, datetime.now(), users['creator'], datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created document attachment")
    
    # Create Discussion Forum
    forum_id = generate_ulid()
    query = """
        INSERT INTO discussion_forums (id, outgoing_letter_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
    """
    params = (forum_id, letter_id, datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created discussion_forum")
    
    print(f"  ✨ Outgoing Letter #{letter_number} created successfully! ID: {letter_id}")
    return letter_id


def create_incoming_letter(conn, users, master_data, letter_number=1):
    """Create incoming letter with disposition"""
    print(f"\n📥 Creating Incoming Letter #{letter_number}...")
    
    # Create incoming letter
    letter_id = generate_ulid()
    query = """
        INSERT INTO incoming_letters (
            id, work_unit_id, target_user_id, security_classification_id, warehouse_location_id,
            origin_name, origin_institution, subject, letter_number, letter_date,
            keyword, received_date, received_via, receiver_name,
            is_draft, is_deleted, created_by_user_id, recipient_type,
            created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s
        )
        RETURNING id
    """
    
    today = date.today()
    letter_num = f"IN/{today.year}/{today.month:02d}/{letter_number:04d}"
    
    # Get work_unit_id of recipient
    work_unit_query = "SELECT work_unit_id FROM users WHERE id = %s"
    work_unit = execute_query(conn, work_unit_query, (users['recipient'],), fetch=True)
    work_unit_id = work_unit[0]['work_unit_id'] if work_unit else None
    
    params = (
        letter_id, work_unit_id, users['recipient'], 
        master_data.get('security_classification_id'), master_data.get('warehouse_location_id'),
        'PT External Company', 'External Institution',
        f'Dummy Incoming Letter - Test Disposition #{letter_number}',
        letter_num, today,
        'dummy,test,incoming', today, 'Email', 'Admin Reception',
        False, False, users['creator'], 'internal',
        datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created incoming_letter: {letter_num} ({letter_id})")
    
    # Create Letter Disposition (dummydms7 sebagai Pendok/Sekretaris Direksi)
    disposition_id = generate_ulid()
    query = """
        INSERT INTO letter_dispositions (
            id, incoming_letter_id, sender_id, receiver_id, 
            description, status, is_closed, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        disposition_id, letter_id, users['recipient'], users['disposition'],
        'Mohon untuk ditindaklanjuti sesuai prosedur yang berlaku.',
        'pending', False, datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created letter_disposition: {users['disposition']}")
    
    # Create LetterParticipants
    participants = [
        (users['creator'], 'incoming_letter', 'Sudah Baca'),
        (users['recipient'], 'recipient', 'Didisposisi'),
        (users['disposition'], 'disposition', 'Belum Baca'),
    ]
    
    for user_id, segment_type, status in participants:
        participant_id = generate_ulid()
        query = """
            INSERT INTO letter_participants (
                id, incoming_letter_id, user_id, segment_type, status, 
                disposition_status, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        disp_status = 'pending' if segment_type == 'disposition' else None
        params = (
            participant_id, letter_id, user_id, segment_type, status,
            disp_status, datetime.now(), datetime.now()
        )
        execute_query(conn, query, params)
    print(f"  ✅ Created {len(participants)} letter_participants")
    
    # Create Attachment
    attachment_id = generate_ulid()
    query = """
        INSERT INTO incoming_letter_attachments (
            id, incoming_letter_id, file_name, file_url, file_extension, 
            size, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        attachment_id, letter_id, f'incoming-letter-{letter_number}.pdf',
        'https://example.com/dummy-incoming.pdf', 'pdf',
        2048000, datetime.now(), datetime.now()
    )
    execute_query(conn, query, params)
    print(f"  ✅ Created incoming_letter_attachment")
    
    # Create Discussion Forum
    forum_id = generate_ulid()
    query = """
        INSERT INTO discussion_forums (id, incoming_letter_id, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
    """
    params = (forum_id, letter_id, datetime.now(), datetime.now())
    execute_query(conn, query, params)
    print(f"  ✅ Created discussion_forum")
    
    print(f"  ✨ Incoming Letter #{letter_number} created successfully! ID: {letter_id}")
    return letter_id


def get_or_create_dummy_acl_role(conn):
    """Get existing 'User' ACL role or create one if not exists"""
    # Try to find existing role named 'User' or similar
    query = "SELECT id FROM acl_roles WHERE name ILIKE '%user%' OR name ILIKE '%dummy%' LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    if result:
        print(f"  ✅ Found existing ACL role: {result[0]['id']}")
        return result[0]['id']
    
    # If not found, get any existing role
    query = "SELECT id, name FROM acl_roles LIMIT 1"
    result = execute_query(conn, query, fetch=True)
    if result:
        print(f"  ✅ Using existing ACL role '{result[0]['name']}': {result[0]['id']}")
        return result[0]['id']
    
    # If no roles exist, create one
    role_id = generate_ulid()
    query = """
        INSERT INTO acl_roles (id, name, created_at, updated_at)
        VALUES (%s, %s, %s, %s)
    """
    execute_query(conn, query, (role_id, 'Dummy User', datetime.now(), datetime.now()))
    print(f"  ✅ Created new ACL role 'Dummy User': {role_id}")
    return role_id


def run_inject(conn):
    """Run the injection process (create users and letters)"""
    # STEP 1: Create Users
    print("\n" + "=" * 70)
    print("📋 STEP 1: Creating/Updating Dummy Users (UPSERT)")
    print("=" * 70)
    
    # Get or create ACL role for dummy users
    dummy_acl_role_id = get_or_create_dummy_acl_role(conn)
    
    users = {}
    
    # User configurations matching existing data
    # Format: (email, name, position, work_unit, npp, no_telp, password_hash)
    user_configs = [
        ('dummydms1@adhi.co.id', 'Drafter', 'Staff', 'Biro Dokumentasi', '28494232', '089638492748', '$argon2id$v=19$m=65536,t=4,p=1$dWY4d1FCV1Q3UXI0aEtKMQ$WjMSKzPtiWjc0K0v9waWGhrZNrjgudgxsSJf7eLvmu0'),
        ('dummydms2@adhi.co.id', 'Reviewer', 'CorSec', 'Biro Kesekretariatan', '232132322', '089637483837', '$argon2id$v=19$m=65536,t=4,p=1$ck5lNEkuU2J2TFBUeHZsQg$brY3q6tgyo733SaE8yAOIPNJ4HpSTvW2PZRZzzjrYjU'),
        ('dummydms3@adhi.co.id', 'Approver 1', 'General Manager', 'Divisi Manajemen', '234234234', '089638749342', '$argon2id$v=19$m=65536,t=4,p=1$Yk91S2ZKWWRvbHVYQTFwaQ$zIGcF/EuHf9Jpy+msz2moiQ/Jlcz8OqZjNiqVVKY/Ac'),
        ('dummydms4@adhi.co.id', 'Approver 2', 'Direksi', 'Direksi', '28494232', '089638483848', '$argon2id$v=19$m=65536,t=4,p=1$aVhqdkVKbUhGREUwRmFSaA$O8pMIkihbrbeaiRq3iehORaGn2W25QErLyORgOHyW6Y'),
        ('dummydms5@adhi.co.id', 'Tembusan', 'Sekretaris Bendahara', 'Bagian Keuangan', '232432224', '089638483838', '$argon2id$v=19$m=65536,t=4,p=1$Nktycm1RelY0ZU9rNms2Tw$fdHG7qJmF7nQnGnj/P8kIGbfllizzORKAvM6z/U7Xng'),
        ('dummydms6@adhi.co.id', 'Penerima', 'PIC Unit Kerja', 'Unit Operasional', '23843348', '089638492838', '$argon2id$v=19$m=65536,t=4,p=1$TloyYjhHUmVldHJWNnh5Zg$3jdnmSz6rICi26/53F9HI6B6x9VrJ0Znm9yZbi10PHg'),
        ('dummydms7@adhi.co.id', 'Pendok', 'Sekretaris Direksi', 'Sekretariat Direksi', '39392423', '089638492744', '$argon2id$v=19$m=65536,t=4,p=1$OGc4MjdXYW8zLkU5MmVCWA$oEWvEbnGYg0reogURxdPlJ3Nw0rU6zEDG65u1QfE40o'),
    ]
    
    user_keys = ['creator', 'reviewer', 'approver1', 'approver2', 'cc', 'recipient', 'disposition']
    
    for i, (email, name, position, work_unit, npp, no_telp, pwd_hash) in enumerate(user_configs):
        user_id = upsert_user(conn, email, name, position, work_unit, dummy_acl_role_id, npp, no_telp, pwd_hash)
        users[user_keys[i]] = user_id
        if i == 0:  # Use first user as drafter too
            users['drafter'] = user_id
    
    print(f"\n✅ Processed {len(user_configs)} dummy users")
    
    # STEP 1b: Create Role Testing Users
    role_testing_users = create_role_testing_users(conn)
    
    # STEP 2: Get Master Data
    print("\n" + "=" * 70)
    print("📋 STEP 2: Getting Master Data IDs")
    print("=" * 70)
    
    master_data = get_master_data_ids(conn)
    for key, value in master_data.items():
        status = "✅" if value else "⚠️ "
        print(f"  {status} {key}: {value}")
    
    # STEP 3: Create Outgoing Letters
    print("\n" + "=" * 70)
    print("📋 STEP 3: Creating Outgoing Letters (25 letters)")
    print("=" * 70)
    
    outgoing_letters = []
    for i in range(1, 26):  # Create 25 outgoing letters
        letter_id = create_outgoing_letter(conn, users, master_data, i)
        outgoing_letters.append(letter_id)
    
    print(f"\n✅ Created {len(outgoing_letters)} outgoing letters")
    
    # STEP 4: Create Incoming Letters
    print("\n" + "=" * 70)
    print("📋 STEP 4: Creating Incoming Letters (25 letters)")
    print("=" * 70)
    
    incoming_letters = []
    for i in range(1, 26):  # Create 25 incoming letters
        letter_id = create_incoming_letter(conn, users, master_data, i)
        incoming_letters.append(letter_id)
    
    print(f"\n✅ Created {len(incoming_letters)} incoming letters")
    
    # SUMMARY
    print("\n" + "=" * 70)
    print("🎉 DATA INJECTION COMPLETED SUCCESSFULLY!")
    print("=" * 70)
    print(f"\n📊 SUMMARY:")
    print(f"  • Dummy users processed: {len(user_configs)}")
    print(f"  • Test users processed: {len(role_testing_users)}")
    print(f"  • Outgoing letters: {len(outgoing_letters)}")
    print(f"  • Incoming letters: {len(incoming_letters)}")
    print("\n📋 USER CREDENTIALS:")
    for email, name, position, work_unit, npp, no_telp, pwd_hash in user_configs:
        print(f"  • {email:30s} - {name} ({position})")
    
    print("\n✅ All dummy data injected successfully!")


def run_delete_dummy(conn):
    """Delete all dummy users and their related data"""
    delete_users_by_pattern(conn, DUMMY_EMAIL_LIKE, "dummy users")


def run_delete_test(conn):
    """Delete all test users and their related data"""
    delete_users_by_pattern(conn, TEST_EMAIL_PATTERN, "test users")


def run_delete_all(conn):
    """Delete both dummy and test users"""
    run_delete_dummy(conn)
    run_delete_test(conn)


def run_clean(conn):
    """Clean (delete all) then re-inject"""
    print("\n" + "=" * 70)
    print("🧹 CLEAN MODE: Deleting all existing data first...")
    print("=" * 70)
    run_delete_all(conn)
    
    print("\n" + "=" * 70)
    print("🔄 RE-INJECTING fresh data...")
    print("=" * 70)
    run_inject(conn)


def main():
    """Main execution function with CLI arguments"""
    parser = argparse.ArgumentParser(
        description='Inject or delete dummy/test letter data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python inject_dummy_letters.py                    # Inject all data (upsert)
  python inject_dummy_letters.py --mode inject      # Inject all data (upsert)
  python inject_dummy_letters.py --mode delete-dummy   # Delete dummy users & data
  python inject_dummy_letters.py --mode delete-test    # Delete test users & data
  python inject_dummy_letters.py --mode delete-all     # Delete all dummy & test
  python inject_dummy_letters.py --mode clean          # Delete all then re-inject
        """
    )
    parser.add_argument(
        '--mode', '-m',
        choices=['inject', 'delete-dummy', 'delete-test', 'delete-all', 'clean'],
        default='inject',
        help='Operation mode (default: inject)'
    )
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 DUMMY LETTER DATA MANAGEMENT SCRIPT")
    print(f"   Mode: {args.mode.upper()}")
    print("=" * 70)
    
    conn = get_db_connection()
    print("✅ Connected to database")
    
    try:
        if args.mode == 'inject':
            run_inject(conn)
        elif args.mode == 'delete-dummy':
            run_delete_dummy(conn)
        elif args.mode == 'delete-test':
            run_delete_test(conn)
        elif args.mode == 'delete-all':
            run_delete_all(conn)
        elif args.mode == 'clean':
            run_clean(conn)
        
        print("\n" + "=" * 70)
        print(f"✅ Operation '{args.mode}' completed successfully!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    main()
