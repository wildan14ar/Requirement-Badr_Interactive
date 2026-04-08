# main.py
import argparse
import sys
import os

# Add scripts directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, TimestampType
from helpers import build_jdbc_url, jdbc_read
from methods import run_method


# =============================================================================
# ACL ROLE ID CONSTANTS (Production Database)
# These are the ULID IDs for roles in the acl_roles table.
# Used for mapping legacy role flags to new ACL system.
# =============================================================================
ACL_ROLE_IDS = {
    "SUPERADMIN": "01KDYV25K4T9B1P29S1GXKXN4Z",
    "DIREKSI": "01KGP5EXS1D6A1CMFEW96R5B22",
    "KOMISARIS": "01KGP5EXT987AMANZF1S2WZ4ZG",
    "MANAGER": "01KGP5EXVHN37V3GZM7H6RDFN0",
    "SEKRETARIS": "01KGP5EXYFFM8C99YXAS51QCE0",
    "STAFF": "01KGP5EXX96N32N6J52N33Q3BJ",
    "RECEPTIONIST": "01KGP5EY1B9F4K2GVVVN2Y57DR",
    "WAREHOUSE": "01KGP5EY38F4X7CVD5S13TB8SF",
    "USER": "01KG5T5EARYYPCB6RXCGVNNZNJ",  # Default role
}

# Legacy flag to role mapping (priority order - first match wins):
# isadmin=1      → SUPERADMIN (System administrator)
# isdir=1        → DIREKSI (Directors/Board level)
# iskom=1        → KOMISARIS (Commissioners)
# isgm/isdepthead=1 → MANAGER (General Managers & Dept Heads)
# sekdir=1       → SEKRETARIS (Director's Secretary)
# isbiro/ispro/issu=1 → STAFF (Bureau/Project/Unit Staff)
# isrecept=1     → RECEPTIONIST (Front desk)
# iswh=1         → WAREHOUSE (Document archive)
# else           → USER (Regular user, default)


# =============================================================================
# HELPER FUNCTIONS FOR DATE PARSING (Python/Spark based)
# =============================================================================
def parse_date_column(df, column_name, output_type="date"):
    """
    Parse date column with multiple format support using Spark.
    Uses pattern matching with F.when to safely handle different date formats.
    Handles: UNIX timestamps, YYYY-MM-DD, DD.MM.YYYY, DD-MM-YYYY, DD/MM/YYYY
    
    Returns proper DateType or TimestampType for PostgreSQL compatibility
    """
    if column_name not in df.columns:
        return df

    col_str = F.col(column_name).cast("string")

    # Pattern-based date parsing using F.when chains
    parsed_timestamp = F.coalesce(
        # UNIX timestamp (10 digits)
        F.when(
            col_str.rlike("^[0-9]{10}$"),
            F.from_unixtime(col_str.cast("bigint")),
        ),
        # UNIX timestamp (13 digits - milliseconds)
        F.when(
            col_str.rlike("^[0-9]{13}$"),
            F.from_unixtime((col_str.cast("bigint") / 1000).cast("bigint")),
        ),
        # Timestamp format: yyyy-MM-dd HH:mm:ss
        F.when(
            col_str.rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"),
            F.to_timestamp(col_str, "yyyy-MM-dd HH:mm:ss"),
        ),
        # Standard date format: yyyy-MM-dd (already good)
        F.when(
            col_str.rlike("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"),
            F.to_timestamp(col_str, "yyyy-MM-dd"),
        ),
        # European format: dd.MM.yyyy
        F.when(
            col_str.rlike("^[0-9]{2}\\.[0-9]{2}\\.[0-9]{4}"),
            F.to_timestamp(col_str, "dd.MM.yyyy"),
        ),
        # Dash format: dd-MM-yyyy
        F.when(
            col_str.rlike("^[0-9]{2}-[0-9]{2}-[0-9]{4}"),
            F.to_timestamp(col_str, "dd-MM-yyyy"),
        ),
        # Slash format: dd/MM/yyyy
        F.when(
            col_str.rlike("^[0-9]{2}/[0-9]{2}/[0-9]{4}"),
            F.to_timestamp(col_str, "dd/MM/yyyy"),
        ),
        # Return NULL for unparseable formats
        F.lit(None).cast(TimestampType()),
    )

    if output_type == "date":
        # Cast timestamp to date - Spark will handle proper formatting to PostgreSQL
        return df.withColumn(
            column_name,
            F.to_date(parsed_timestamp)
        )
    else:
        # Keep as timestamp
        return df.withColumn(column_name, parsed_timestamp.cast(TimestampType()))


def parse_timestamp_from_unix(df, column_name):
    """
    Parse UNIX timestamp column to proper timestamp.
    Returns default timestamp '2024-01-01 00:00:00' if value is NULL or <= 0.
    """
    if column_name not in df.columns:
        return df

    return df.withColumn(
        column_name,
        F.when(
            F.col(column_name).isNotNull() & (F.col(column_name) > 0),
            F.from_unixtime(F.col(column_name).cast("bigint")),
        ).otherwise(F.lit("2024-01-01 00:00:00").cast(TimestampType())),
    )


def parse_exp_to_timestamp(df, column_name):
    """
    Parse expiration column (string) to timestamp.
    Handles: numeric unix timestamps, empty strings, '0' values
    """
    if column_name not in df.columns:
        return df

    return df.withColumn(
        column_name,
        F.when(
            F.col(column_name).rlike("^[0-9]+$")
            & (F.col(column_name).cast("bigint") > 0),
            F.from_unixtime(F.col(column_name).cast("bigint")),
        ).otherwise(F.lit(None)),
    )


def add_is_access_forever(df, exp_column, target_column="is_access_forever"):
    """
    Determine if access is forever based on exp column.
    """
    if exp_column not in df.columns:
        return df.withColumn(target_column, F.lit(True))

    return df.withColumn(
        target_column,
        F.when(
            (F.col(exp_column) == "0")
            | (F.col(exp_column) == "")
            | F.col(exp_column).isNull()
            | ~F.col(exp_column).rlike("^[0-9]+$"),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )


def deduplicate_email_column(df, email_column="email"):
    """
    Deduplicate email column by adding incremental suffix for duplicates.

    Original data (with duplicates):
        budi@mail.com
        budi@mail.com
        budi@mail.com

    Result:
        budi@mail.com
        budi_2@mail.com
        budi_3@mail.com

    The first occurrence keeps the original email, subsequent duplicates
    get _2, _3, etc. suffix inserted before the @ symbol.
    """
    from pyspark.sql.window import Window

    if email_column not in df.columns:
        return df

    # Window to count occurrences of each email, ordered by some column to ensure consistent ordering
    # Using row_number to assign sequential numbers to duplicate emails
    window_spec = Window.partitionBy(F.col(email_column)).orderBy(F.col(email_column))

    # Add row number within each email group
    df = df.withColumn("_email_row_num", F.row_number().over(window_spec))

    # Create deduplicated email:
    # - If row_num = 1, keep original email
    # - If row_num > 1, add suffix _N before @
    df = df.withColumn(
        email_column,
        F.when(F.col("_email_row_num") == 1, F.col(email_column)).otherwise(
            # Split email at @, add suffix to local part, rejoin
            F.concat(
                F.split(F.col(email_column), "@").getItem(0),
                F.lit("_"),
                F.col("_email_row_num").cast("string"),
                F.lit("@"),
                F.split(F.col(email_column), "@").getItem(1),
            )
        ),
    )

    # Drop the temporary column
    df = df.drop("_email_row_num")

    return df


def deduplicate_username_column(df, username_column="username"):
    """
    Deduplicate username column by adding incremental suffix for duplicates.

    Original data (with duplicates):
        admin
        admin
        admin

    Result:
        admin
        admin_2
        admin_3

    The first occurrence keeps the original username, subsequent duplicates
    get _2, _3, etc. suffix appended.
    """
    from pyspark.sql.window import Window

    if username_column not in df.columns:
        return df

    # Window to count occurrences of each username
    window_spec = Window.partitionBy(F.col(username_column)).orderBy(
        F.col(username_column)
    )

    # Add row number within each username group
    df = df.withColumn("_username_row_num", F.row_number().over(window_spec))

    # Create deduplicated username:
    # - If row_num = 1, keep original username
    # - If row_num > 1, add suffix _N
    df = df.withColumn(
        username_column,
        F.when(F.col("_username_row_num") == 1, F.col(username_column)).otherwise(
            F.concat(
                F.col(username_column),
                F.lit("_"),
                F.col("_username_row_num").cast("string"),
            )
        ),
    )

    # Drop the temporary column
    df = df.drop("_username_row_num")

    return df


# =============================================================================
# ETL JOBS CONFIGURATION - ORDERED BY PHASE & DEPENDENCY
# =============================================================================
# Jobs are ordered by execution sequence:
# Phase 1: Master Tables → Phase 2: Sub-Master → Phase 3: Users → Phase 4: Folders
# Phase 5: Folder Documents → Phase 6: Letters & Attachments → Phase 7: Participants & Workflows
# Phase 8: Discussions → Phase 9: Access & Histories → Phase 10: Relations
# =============================================================================
ETL_JOBS = [
    # =========================================================================
    # PHASE 1: MASTER TABLES (No Dependencies)
    # =========================================================================
    {
        "name": "work_units",
        "sql": """
            SELECT
                g.ulid_id AS id,
                g.name AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                CAST(g.id AS INTEGER) AS unit_code,
                parent_group.name AS parent_division_name,
                TRUE AS is_active
            FROM public.tblgroups g
            LEFT JOIN public.tblgroups parent_group ON g.sekber = parent_group.id
            WHERE g.ulid_id IS NOT NULL
            """,
        "target_table": "public.work_units",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
    # ========================================================================= final
    {
        "name": "security_classifications",
        "sql": """
            SELECT
                ulid_id AS id,
                judul AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                NULL AS description,
                TRUE AS is_active,
                CASE WHEN LOWER(judul) = 'biasa' THEN TRUE ELSE FALSE END AS can_access_public,
                CASE
                    WHEN LOWER(judul) IN ('biasa', 'terbatas') THEN TRUE
                    ELSE FALSE
                END AS readable_in_advance_search
            FROM public.surat_sifat
            WHERE ulid_id IS NOT NULL
        """,
        "target_table": "public.security_classifications",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
    {
        "name": "letter_categories",
        "sql": """
            SELECT
                ulid_id AS id,
                judul AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at
            FROM public.surat_kategori
            WHERE ulid_id IS NOT NULL
              AND kode NOT LIKE '%-%'
              AND kode IS NOT NULL
              AND kode != ''
        """,
        "target_table": "public.letter_categories",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # ========================================================================= final
    # PHASE 1: WAREHOUSE LOCATIONS (Combined: Main + Sub-locations)
    # Combines warehouse categories AND sub-locations (racks/shelves) in one query
    # =========================================================================
    {
        "name": "warehouse_locations",
        "sql": """
            -- Main warehouse locations
            SELECT
                ulid_id AS id,
                name AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                CONCAT('WH', LPAD(CAST(id AS VARCHAR), 3, '0')) AS code,
                name AS location,
                TRUE AS is_active
            FROM public.tblkeywordcategories
            WHERE ulid_id IS NOT NULL
            
            UNION ALL
            
            -- Sub-locations (racks/shelves within categories)
            SELECT
                k.ulid_id AS id,
                CASE
                    WHEN k.keywords IS NOT NULL AND k.keywords != ''
                    THEN k.keywords
                    ELSE c.name
                END AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                CONCAT('WH', LPAD(CAST(c.id AS VARCHAR), 3, '0'), '-', LPAD(CAST(ROW_NUMBER() OVER (PARTITION BY k.category ORDER BY k.id) AS VARCHAR), 2, '0')) AS code,
                CONCAT(c.name, ' - ', COALESCE(NULLIF(k.keywords, ''), 'Sub ' || k.id)) AS location,
                TRUE AS is_active
            FROM public.tblkeywords k
            INNER JOIN public.tblkeywordcategories c ON k.category = c.id
            WHERE k.ulid_id IS NOT NULL
              AND c.ulid_id IS NOT NULL
        """,
        "target_table": "public.warehouse_locations",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 1: OFFICIAL DOCUMENT TYPES (No Dependencies)
    # =========================================================================
    {
        "name": "official_document_types",
        "sql": """
            SELECT
                tpl.ulid_id AS id,
                tpl.templatename AS name,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                NULL AS letter_category_id,
                NULL AS letter_sub_category_id,
                NULL AS numbering_format,
                tpl.templatefile AS template_path,
                5 AS retention_period_years,
                TRUE AS is_active,
                grp_folder.ulid_id AS folder_id,
                NULL AS security_classification_id,
                NULL::JSON AS reminders,
                NULL AS format_numbering,
                NULL AS type_numbering
            FROM public.tbldocumenttemplate tpl
            LEFT JOIN public.tblfolders grp_folder ON tpl.templategroup = grp_folder.id
            WHERE tpl.ulid_id IS NOT NULL
        """,
        "target_table": "public.official_document_types",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 2: SUB-MASTER (Depends on: letter_categories)
    # =========================================================================
    {
        "name": "letter_sub_categories",
        "sql": """
                SELECT DISTINCT ON (sub.ulid_id)
                    sub.ulid_id AS id,
                    parent.ulid_id AS category_id,
                    CASE 
                        WHEN sub.judul IS NOT NULL AND sub.kode IS NOT NULL 
                        THEN sub.kode || ' - ' || sub.judul
                        WHEN sub.judul IS NOT NULL THEN sub.judul
                        ELSE sub.kode
                    END AS name,
                    TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                    TIMESTAMP '2024-01-01 00:00:00' AS updated_at
                FROM public.surat_kategori sub
                LEFT JOIN public.surat_kategori parent
                    ON SPLIT_PART(sub.kode, '-', 1) = parent.kode
                    AND parent.kode NOT LIKE '%-%'
                    AND parent.kode IS NOT NULL
                    AND parent.kode != ''
                    AND parent.id != sub.id
                    AND parent.ulid_id IS NOT NULL
                WHERE sub.ulid_id IS NOT NULL
                AND sub.kode LIKE '%-%'
                AND sub.kode IS NOT NULL
                ORDER BY sub.ulid_id, LENGTH(parent.kode) DESC NULLS LAST, parent.id ASC
        """,
        "target_table": "public.letter_sub_categories",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 2.5: ACL ROLES (Pre-requisite for USERS)
    # Hardcoded ACL roles that must exist before users migration
    # =========================================================================
    {
        "name": "acl_roles",
        "sql": """
            SELECT * FROM (
                VALUES
                    ('01KDYV25K4T9B1P29S1GXKXN4Z', 'SUPERADMIN', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EXS1D6A1CMFEW96R5B22', 'DIREKSI', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EXT987AMANZF1S2WZ4ZG', 'KOMISARIS', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EXVHN37V3GZM7H6RDFN0', 'MANAGER', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EXX96N32N6J52N33Q3BJ', 'STAFF', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EXYFFM8C99YXAS51QCE0', 'SEKRETARIS', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EY1B9F4K2GVVVN2Y57DR', 'RECEPTIONIST', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KGP5EY38F4X7CVD5S13TB8SF', 'WAREHOUSE', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00'),
                    ('01KG5T5EARYYPCB6RXCGVNNZNJ', 'USER', TIMESTAMP '2024-01-01 00:00:00', TIMESTAMP '2024-01-01 00:00:00')
            ) AS roles(id, name, created_at, updated_at)
        """,
        "target_table": "public.acl_roles",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 2.6: ACCESS CONTROL LISTS (Depends on: acl_roles)
    # Uses direct SQL execution to avoid Spark schema resolution issues
    # SUPERADMIN gets full access to all features/actions (192 permissions)
    # Other roles get specific permissions based on organizational hierarchy
    # =========================================================================
    {
        "name": "access_control_lists",
        "execution_engine": "sqlalchemy",
        "sql": """
            WITH all_permissions AS (
                -- Get all actual feature-action combinations from production
                -- Production has 32 features × 6 actions (create, read, update, delete, export, import) = 192 permissions
                SELECT DISTINCT
                    f.name as feature_name,
                    a.name as action_name
                FROM public.acl_features f
                CROSS JOIN public.acl_actions a
                WHERE a.feature_name = f.name
            ),
            role_permissions AS (
                -- SUPERADMIN: All permissions (192 total)
                SELECT 
                    '01KDYV25K4T9B1P29S1GXKXN4Z' as role_id,
                    'SUPERADMIN' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                
                UNION ALL
                
                -- DIREKSI: Read all, create/update/delete select areas, export reports
                SELECT 
                    '01KGP5EXS1D6A1CMFEW96R5B22' as role_id,
                    'DIREKSI' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (action_name = 'read')
                   OR (feature_name IN ('outgoing_letters', 'incoming_letters', 'approvals', 'reviews') 
                       AND action_name IN ('create', 'update', 'delete'))
                   OR (feature_name IN ('reports', 'analytics', 'audit_logs') AND action_name = 'export')
                
                UNION ALL
                
                -- KOMISARIS: Read most, approve workflows, export reports
                SELECT 
                    '01KGP5EXT987AMANZF1S2WZ4ZG' as role_id,
                    'KOMISARIS' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (action_name = 'read' AND feature_name NOT IN ('roles', 'api_keys', 'file_configurations', 'retention_periods'))
                   OR (feature_name IN ('approvals', 'reviews') AND action_name IN ('create', 'update'))
                   OR (feature_name IN ('reports', 'analytics', 'audit_logs') AND action_name = 'export')
                
                UNION ALL
                
                -- MANAGER: Read most, full CRUD documents/letters, approve/review
                SELECT 
                    '01KGP5EXVHN37V3GZM7H6RDFN0' as role_id,
                    'MANAGER' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (action_name = 'read' AND feature_name NOT IN ('roles', 'api_keys', 'file_configurations', 'retention_periods'))
                   OR (feature_name IN ('documents', 'folders', 'incoming_letters', 'outgoing_letters', 
                                         'letter_dispositions', 'letter_document_relations', 'folder_documents') 
                       AND action_name IN ('create', 'update', 'delete'))
                   OR (feature_name IN ('approvals', 'reviews') AND action_name IN ('create', 'update'))
                   OR (feature_name IN ('reports', 'analytics') AND action_name = 'export')
                
                UNION ALL
                
                -- SEKRETARIS: Manage letters & dispositions, create documents
                SELECT 
                    '01KGP5EXYFFM8C99YXAS51QCE0' as role_id,
                    'SEKRETARIS' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (action_name = 'read' AND feature_name NOT IN ('roles', 'api_keys', 'users', 'work_units', 
                                                                       'file_configurations', 'retention_periods', 'audit_logs'))
                   OR (feature_name IN ('incoming_letters', 'outgoing_letters', 'letter_dispositions', 
                                         'letter_document_relations', 'carbon_copies') 
                       AND action_name IN ('create', 'update', 'delete'))
                   OR (feature_name IN ('documents', 'folders', 'folder_documents') 
                       AND action_name IN ('create', 'update'))
                   OR (feature_name = 'reviews' AND action_name IN ('create', 'update'))
                
                UNION ALL
                
                -- STAFF: Read limited, create/update own documents & discussions
                SELECT 
                    '01KGP5EXX96N32N6J52N33Q3BJ' as role_id,
                    'STAFF' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (action_name = 'read' AND feature_name IN ('documents', 'folders', 'incoming_letters', 
                                                                  'outgoing_letters', 'letter_categories', 
                                                                  'letter_sub_categories', 'official_document_types',
                                                                  'discussion_forums', 'messages', 'security_classifications'))
                   OR (feature_name IN ('documents', 'folder_documents', 'outgoing_letters') 
                       AND action_name IN ('create', 'update'))
                   OR (feature_name IN ('discussion_forums', 'messages') 
                       AND action_name IN ('create', 'update'))
                
                UNION ALL
                
                -- RECEPTIONIST: Manage incoming letters & dispositions
                SELECT 
                    '01KGP5EY1B9F4K2GVVVN2Y57DR' as role_id,
                    'RECEPTIONIST' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (feature_name IN ('incoming_letters', 'letter_dispositions', 'carbon_copies') 
                       AND action_name IN ('read', 'create', 'update'))
                   OR (feature_name IN ('documents', 'folders', 'letter_categories', 'letter_sub_categories', 
                                         'official_document_types', 'security_classifications') 
                       AND action_name = 'read')
                
                UNION ALL
                
                -- WAREHOUSE: Manage warehouse & view documents
                SELECT 
                    '01KGP5EY38F4X7CVD5S13TB8SF' as role_id,
                    'WAREHOUSE' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (feature_name = 'warehouse_locations' 
                       AND action_name IN ('read', 'create', 'update', 'delete'))
                   OR (feature_name IN ('documents', 'folders', 'incoming_letters', 'outgoing_letters', 
                                         'recycle_bin', 'retentions') 
                       AND action_name = 'read')
                
                UNION ALL
                
                -- USER: Basic read-only access + participate in discussions
                SELECT 
                    '01KG5T5EARYYPCB6RXCGVNNZNJ' as role_id,
                    'USER' as role_name,
                    feature_name,
                    action_name
                FROM all_permissions
                WHERE (feature_name IN ('documents', 'folders', 'incoming_letters', 'outgoing_letters',
                                         'letter_categories', 'letter_sub_categories', 'dashboard') 
                       AND action_name = 'read')
                   OR (feature_name IN ('discussion_forums', 'messages') 
                       AND action_name IN ('read', 'create'))
            )
            SELECT 
                -- Generate deterministic ULID-like ID: role_id prefix + feature + action hash
                SUBSTRING(role_id, 1, 10) || 
                LPAD(TO_HEX(ABS(HASHTEXT(feature_name || action_name))), 16, '0') AS id,
                role_id,
                feature_name,
                action_name,
                NULL as created_by_id,
                NULL as updated_by_id,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM role_permissions
            ORDER BY role_name, feature_name, action_name
        """,
        "target_table": "public.access_control_lists",
        "method": "upsert",
        "source_is_target": True,  # Flag to indicate query runs on target DB
        "conflict_columns": ["role_id", "feature_name", "action_name"]
    },
    # =========================================================================
    # PHASE 3: USERS (Depends on: work_units, acl_roles)
    # Role Mapping based on legacy flags:
    #   isadmin=1      → SUPERADMIN
    #   isdir=1        → DIREKSI
    #   iskom=1        → KOMISARIS
    #   isgm/isdepthead=1 → MANAGER
    #   sekdir=1       → SEKRETARIS
    #   isbiro/ispro/issu=1 → STAFF
    #   isrecept=1     → RECEPTIONIST
    #   iswh=1         → WAREHOUSE
    #   else           → USER
    # =========================================================================
    {
        "name": "users",
        "sql": """
            SELECT DISTINCT ON (u.ulid_id)
                u.ulid_id AS id,
                COALESCE(u.fullname, u.login, 'Unknown') AS name,
                CASE
                    WHEN u.email = 'adele@adhi.co.id' THEN CONCAT('adele_', SUBSTRING(u.ulid_id, 1, 8), '@adhi.co.id')
                    ELSE COALESCE(u.email, CONCAT(u.login, '@adhi.co.id'))
                END AS email,
                CASE WHEN u.isactivated = 1 THEN TRUE ELSE FALSE END AS is_active,
                -- Role column mapping (used by BE for display & some queries like 'Direksi')
                CASE
                    WHEN COALESCE(u.isadmin, 0) = 1 THEN 'admin'
                    WHEN COALESCE(u.isdir, 0) = 1 THEN 'Direksi'
                    WHEN COALESCE(u.iskom, 0) = 1 THEN 'Komisaris'
                    WHEN COALESCE(u.isgm, 0) = 1 THEN 'General Manager'
                    WHEN COALESCE(u.isdepthead, 0) = 1 THEN 'Dept Head'
                    WHEN COALESCE(u.sekdir, 0) = 1 THEN 'Sekretaris Direksi'
                    WHEN COALESCE(u.isbiro, 0) = 1 THEN 'Biro'
                    WHEN COALESCE(u.ispro, 0) = 1 THEN 'Staff Pro'
                    WHEN COALESCE(u.issu, 0) = 1 THEN 'Staff Unit'
                    WHEN COALESCE(u.isrecept, 0) = 1 THEN 'Receptionist'
                    WHEN COALESCE(u.iswh, 0) = 1 THEN 'Warehouse'
                    ELSE 'user'
                END AS role,
                NULL::TIMESTAMP AS email_verified_at,
                '$argon2id$v=19$m=65536,t=4,p=1$dUQvSGhDd1BlNXVuWVBjRw$m8Fcpyweu8iDdd0Kfd/cn2fUbysNVANtx1mPx172c+M' AS password,
                NULL AS remember_token,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                -- ACL Role ID mapping based on legacy flags (priority order)
                CASE
                    WHEN COALESCE(u.isadmin, 0) = 1 THEN '01KDYV25K4T9B1P29S1GXKXN4Z'  -- SUPERADMIN
                    WHEN COALESCE(u.isdir, 0) = 1 THEN '01KGP5EXS1D6A1CMFEW96R5B22'    -- DIREKSI
                    WHEN COALESCE(u.iskom, 0) = 1 THEN '01KGP5EXT987AMANZF1S2WZ4ZG'    -- KOMISARIS
                    WHEN COALESCE(u.isgm, 0) = 1 OR COALESCE(u.isdepthead, 0) = 1 THEN '01KGP5EXVHN37V3GZM7H6RDFN0'  -- MANAGER
                    WHEN COALESCE(u.sekdir, 0) = 1 THEN '01KGP5EXYFFM8C99YXAS51QCE0'   -- SEKRETARIS
                    WHEN COALESCE(u.isbiro, 0) = 1 OR COALESCE(u.ispro, 0) = 1 OR COALESCE(u.issu, 0) = 1 THEN '01KGP5EXX96N32N6J52N33Q3BJ'  -- STAFF
                    WHEN COALESCE(u.isrecept, 0) = 1 THEN '01KGP5EY1B9F4K2GVVVN2Y57DR' -- RECEPTIONIST
                    WHEN COALESCE(u.iswh, 0) = 1 THEN '01KGP5EY38F4X7CVD5S13TB8SF'    -- WAREHOUSE
                    ELSE '01KG5T5EARYYPCB6RXCGVNNZNJ'  -- USER (default)
                END AS acl_role_id,
                CASE
                    WHEN u.login = 'admin' THEN CONCAT('admin_', SUBSTRING(u.ulid_id, 1, 8))
                    ELSE u.login
                END AS username,
                -- Get work_unit_id from ANY group membership (not just manager=1)
                gm_group.ulid_id AS work_unit_id,
                NULL AS photo_url
            FROM public.tblusers u
            LEFT JOIN public.tblgroupmembers gm
                ON u.id = gm.userid
            LEFT JOIN public.tblgroups gm_group ON gm.groupid = gm_group.id
            WHERE u.ulid_id IS NOT NULL
            ORDER BY u.ulid_id, gm.manager DESC NULLS LAST
        """,
        "target_table": "public.users",
        "method": "upsert",
        "conflict_columns": ["id"],
        "transform": [
            "deduplicate_email:email",
            "deduplicate_username:username",
        ],
    },
    # =========================================================================
    # PHASE 4: FOLDERS (Depends on: users) - Step 1: Insert without parent
    # =========================================================================
    {
        "name": "folders",
        "sql": """
            WITH user_primary_group AS (
                -- Get only ONE group per user to prevent row duplication
                SELECT DISTINCT ON (gm.userid)
                    gm.userid,
                    g.ulid_id as group_ulid
                FROM public.tblgroupmembers gm
                INNER JOIN public.tblgroups g ON gm.groupid = g.id
                WHERE g.ulid_id IS NOT NULL
                ORDER BY gm.userid, gm.manager DESC, gm.groupid ASC
            )
            SELECT
                f.ulid_id AS id,
                NULL AS parent_id,
                f.name AS name,
                NULL AS keywords,
                -- FIX: Get access_unit_id from owner's work_unit (via CTE)
                upg.group_ulid AS access_unit_id,
                TRUE AS is_active,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                0 AS folder_count,
                0 AS document_count,
                owner_user.ulid_id AS owner_id,
                owner_user.ulid_id AS created_by,
                NULL AS type_folder,
                FALSE AS is_has_children
            FROM public.tblfolders f
            LEFT JOIN public.tblusers owner_user ON f.owner = owner_user.id
            LEFT JOIN user_primary_group upg ON owner_user.id = upg.userid
            WHERE f.ulid_id IS NOT NULL
        """,
        "target_table": "public.folders",
        "method": "upsert",
        "conflict_columns": ["id"],
        "compare_cols": ["id", "name", "keywords", "access_unit_id", "is_active", "owner_id", "created_by"]
    },
    # =========================================================================
    # PHASE 4B: FOLDERS - Step 2: Update parent_id
    # =========================================================================
    {
        "name": "folders_update_parent",
        "sql": """
            WITH user_primary_group AS (
                -- Get only ONE group per user to prevent row duplication
                SELECT DISTINCT ON (gm.userid)
                    gm.userid,
                    g.ulid_id as group_ulid
                FROM public.tblgroupmembers gm
                INNER JOIN public.tblgroups g ON gm.groupid = g.id
                WHERE g.ulid_id IS NOT NULL
                ORDER BY gm.userid, gm.manager DESC, gm.groupid ASC
            )
            SELECT
                f.ulid_id AS id,
                parent_folder.ulid_id AS parent_id,
                f.name AS name,
                NULL AS keywords,
                -- FIX: Get access_unit_id from owner's work_unit (via CTE)
                upg.group_ulid AS access_unit_id,
                TRUE AS is_active,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at,
                0 AS folder_count,
                0 AS document_count,
                owner_user.ulid_id AS owner_id,
                owner_user.ulid_id AS created_by,
                NULL AS type_folder,
                FALSE AS is_has_children
            FROM public.tblfolders f
            INNER JOIN public.tblfolders parent_folder ON f.parent = parent_folder.id
            LEFT JOIN public.tblusers owner_user ON f.owner = owner_user.id
            LEFT JOIN user_primary_group upg ON owner_user.id = upg.userid
            WHERE f.ulid_id IS NOT NULL
              AND parent_folder.ulid_id IS NOT NULL
              AND f.parent IS NOT NULL
              AND f.parent > 0
        """,
        "target_table": "public.folders",
        "method": "upsert",
        "conflict_columns": ["id"],
        "compare_cols": ["id", "parent_id"]
    },
    # =========================================================================
    # PHASE 5: FOLDER DOCUMENTS (Depends on: folders, users, warehouse_locations)
    # warehouse_location_id is mapped by matching doc.keywords with tblkeywords.keywords
    # ~770 documents akan ter-map ke warehouse_location_id
    # =========================================================================
    {
        "name": "folder_documents",
        "sql": """
            SELECT
                doc.ulid_id AS id,
                folder.ulid_id AS folder_id,
                doc.name AS name,
                doc.keywords AS keyword,
                COALESCE(doc.masuk_nomor, doc.keluar_nosurat_plain) AS document_number,
                -- Map warehouse_location_id by matching keywords (case-insensitive)
                kw.ulid_id AS warehouse_location_id,
                CASE
                    WHEN doc.rem1 > 0 AND doc.rem1 < 4102444800 THEN TO_TIMESTAMP(doc.rem1)
                    WHEN doc.rem1 >= 4102444800 AND doc.rem1 < 4102444800000 THEN TO_TIMESTAMP(doc.rem1 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '10 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '3 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '1 year'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                END AS retention_date_start,
                CASE
                    WHEN doc.rem2 > 0 AND doc.rem2 < 4102444800 THEN TO_TIMESTAMP(doc.rem2)
                    WHEN doc.rem2 >= 4102444800 AND doc.rem2 < 4102444800000 THEN TO_TIMESTAMP(doc.rem2 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '15 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '7 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                END AS retention_date_end,
                -- is_draft = TRUE ONLY if no workflow exists (true draft from FE POST)
                CASE 
                    WHEN doc.kind = 0 AND NOT EXISTS (SELECT 1 FROM tbldocumentreviewers WHERE documentid = doc.id) 
                         AND NOT EXISTS (SELECT 1 FROM tbldocumentapprovers WHERE documentid = doc.id) THEN TRUE
                    ELSE FALSE
                END AS is_draft,
                created_user.ulid_id AS created_by,
                CASE
                    WHEN doc.wftype IS NOT NULL AND doc.wftype > 0 THEN 'sequential'
                    ELSE 'parallel'
                END AS workflow_type,
                CASE
                    WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date)
                    WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000)
                    ELSE TIMESTAMP '2024-01-01 00:00:00'
                END AS created_at,
                CASE
                    WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date)
                    WHEN doc.date >= 4102444800 AND doc.date < 4102444800000 THEN TO_TIMESTAMP(doc.date / 1000)
                    ELSE TIMESTAMP '2024-01-01 00:00:00'
                END AS updated_at,
                created_user.ulid_id AS drafter_user_id
            FROM public.tbldocuments doc
            INNER JOIN public.tblfolders folder
                ON doc.folder = folder.id
            LEFT JOIN public.tblusers created_user
                ON doc.owner = created_user.id
            -- Join with tblkeywords to get warehouse_location_id based on exact keyword match
            LEFT JOIN public.tblkeywords kw
                ON LOWER(TRIM(doc.keywords)) = LOWER(TRIM(kw.keywords))
                AND kw.keywords IS NOT NULL
                AND kw.keywords != ''
                AND kw.ulid_id IS NOT NULL
            WHERE doc.ulid_id IS NOT NULL
              AND folder.ulid_id IS NOT NULL
              AND doc.folder IS NOT NULL
              AND doc.folder > 0
        """,
        "target_table": "public.folder_documents",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
    # =========================================================================
    # PHASE 5B [EXEC #11B]: FOLDERS - Update folder_count & document_count
    # Depends on: folder_documents
    # NOTE: This query runs on TARGET database (production), not staging!
    # =========================================================================
    # Updates both folder_count (child folders) and document_count in one query
    {
        "name": "folders_update_document_count",
        "sql": """
            SELECT
                f.id,
                f.parent_id,
                f.name,
                f.keywords,
                f.access_unit_id,
                f.is_active,
                f.created_at,
                f.updated_at,
                CAST(COALESCE(child_counts.folder_count, 0) AS INTEGER) AS folder_count,
                CAST(COALESCE(doc_counts.document_count, 0) AS INTEGER) AS document_count,
                f.owner_id,
                f.created_by,
                f.type_folder,
                CASE WHEN child_counts.folder_count > 0 THEN TRUE ELSE FALSE END AS is_has_children
            FROM public.folders f
            LEFT JOIN (
                SELECT parent_id, COUNT(*) AS folder_count
                FROM public.folders
                WHERE parent_id IS NOT NULL
                GROUP BY parent_id
            ) child_counts ON f.id = child_counts.parent_id
            LEFT JOIN (
                SELECT folder_id, COUNT(*) AS document_count
                FROM public.folder_documents
                GROUP BY folder_id
            ) doc_counts ON f.id = doc_counts.folder_id
        """,
        "target_table": "public.folders",
        "method": "upsert",
        "conflict_columns": ["id"],
        "source_is_target": True,  # Flag to indicate query runs on target DB
        "compare_cols": ["id", "folder_count", "document_count", "is_has_children"]
    },
    # =========================================================================
    # PHASE 6 [EXEC #12]: INCOMING LETTERS (Combined: External + Internal)
    # Depends on: work_units, users, security_classifications
    # =========================================================================
    # 1. External incoming letters from tbldocuments (kind IN 0,1)
    # 2. Internal incoming letters from surat_masuk_chained (distributions)
    {
        "name": "incoming_letters",
        "sql": """
            -- 1. EXTERNAL INCOMING LETTERS (from tbldocuments)
            -- FIX: Get only ONE group per user to prevent row duplication
            WITH user_primary_group AS (
                SELECT DISTINCT ON (gm.userid)
                    gm.userid,
                    g.ulid_id as group_ulid
                FROM public.tblgroupmembers gm
                INNER JOIN public.tblgroups g ON gm.groupid = g.id
                WHERE g.ulid_id IS NOT NULL
                ORDER BY gm.userid, gm.manager DESC, gm.groupid ASC
            )
            SELECT
                doc.ulid_id AS id,
                COALESCE(work_unit.ulid_id, upg.group_ulid) AS work_unit_id,
                target_user.ulid_id AS target_user_id,
                security.ulid_id AS security_classification_id,
                NULL AS warehouse_location_id,
                doc.masuk_nama_pengirim AS origin_name,
                doc.masuk_instansi AS origin_institution,
                doc.name AS subject,
                doc.masuk_nomor AS letter_number,
                NULLIF(doc.masuk_tanggal, '') AS letter_date,
                doc.keywords AS keyword,
                TRUE AS is_retention,
                (CASE 
                    WHEN doc.rem1 > 0 AND doc.rem1 < 4102444800 THEN TO_TIMESTAMP(doc.rem1) 
                    WHEN doc.rem1 >= 4102444800 THEN TO_TIMESTAMP(doc.rem1 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '10 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '3 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '1 year'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                END)::DATE AS retention_period_start,
                (CASE 
                    WHEN doc.rem2 > 0 AND doc.rem2 < 4102444800 THEN TO_TIMESTAMP(doc.rem2) 
                    WHEN doc.rem2 >= 4102444800 THEN TO_TIMESTAMP(doc.rem2 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '15 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '7 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                END)::DATE AS retention_period_end,
                CASE WHEN doc.downloadable = 1 THEN TRUE ELSE FALSE END AS is_downloadable,
                CASE
                    WHEN doc.masuk_tanggal_terima ~ '^[0-9]+$' AND CAST(doc.masuk_tanggal_terima AS BIGINT) > 0 AND CAST(doc.masuk_tanggal_terima AS BIGINT) < 4102444800
                    THEN TO_TIMESTAMP(CAST(doc.masuk_tanggal_terima AS BIGINT))::DATE::VARCHAR
                    WHEN doc.masuk_tanggal_terima ~ '^[0-9]+$' AND CAST(doc.masuk_tanggal_terima AS BIGINT) >= 4102444800
                    THEN TO_TIMESTAMP(CAST(doc.masuk_tanggal_terima AS BIGINT) / 1000)::DATE::VARCHAR
                    ELSE NULL
                END AS received_date,
                doc.masuk_via AS received_via,
                doc.masuk_nama_penerima AS receiver_name,
                doc.masuk_terima_note AS note,
                doc.masuk_jlampiran AS attachment_type,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                created_user.ulid_id AS created_by_user_id,
                FALSE AS is_draft,
                'incoming_letter' AS segment_type,
                FALSE AS is_deleted,
                NULL AS deleted_at,
                NULL AS deleted_by,
                CASE
                    WHEN NULLIF(doc.masuk_unit_penerima, '') IS NOT NULL
                         OR NULLIF(doc.masuk_nama_penerima_user, '') IS NOT NULL
                    THEN 'internal'
                    ELSE 'external'
                END AS recipient_type,
                -- folder_document_id: Self-reference jika letter ada dalam folder
                CASE 
                    WHEN doc.folder IS NOT NULL AND doc.folder > 0 
                    THEN doc.ulid_id
                    ELSE NULL 
                END AS folder_document_id
            FROM public.tbldocuments doc
            LEFT JOIN public.tblgroups work_unit
                ON CAST(NULLIF(doc.masuk_unit_penerima, '') AS BIGINT) = work_unit.id
            LEFT JOIN public.tblusers target_user
                ON CAST(NULLIF(doc.masuk_nama_penerima_user, '') AS BIGINT) = target_user.id
            LEFT JOIN public.surat_sifat security
                ON doc.masuk_sifat = security.judul
            LEFT JOIN public.tblusers created_user
                ON doc.owner = created_user.id
            LEFT JOIN user_primary_group upg
                ON created_user.id = upg.userid
            WHERE doc.ulid_id IS NOT NULL
              AND doc.kind IN (0, 1)
            
            UNION ALL
            
            -- 2. INTERNAL INCOMING LETTERS (from surat_masuk_chained)
            SELECT
                smc.ulid_id AS id,
                work_unit.ulid_id AS work_unit_id,
                target_user.ulid_id AS target_user_id,
                security.ulid_id AS security_classification_id,
                NULL AS warehouse_location_id,
                drafter.fullname AS origin_name,
                drafter_unit.name AS origin_institution,
                outgoing.name AS subject,
                outgoing.keluar_nosurat_plain AS letter_number,
                CASE 
                    WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date)::DATE::VARCHAR
                    WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000)::DATE::VARCHAR
                    ELSE NULL 
                END AS letter_date,
                outgoing.keywords AS keyword,
                TRUE AS is_retention,
                (CASE 
                    WHEN outgoing.rem1 > 0 AND outgoing.rem1 < 4102444800 THEN TO_TIMESTAMP(outgoing.rem1) 
                    WHEN outgoing.rem1 >= 4102444800 THEN TO_TIMESTAMP(outgoing.rem1 / 1000)
                    WHEN outgoing.name ILIKE '%kontrak%' OR outgoing.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '10 years'
                    WHEN outgoing.name ILIKE '%laporan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN outgoing.name ILIKE '%tugas%' OR outgoing.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '3 years'
                    WHEN outgoing.name ILIKE '%undangan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '1 year'
                    ELSE 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                END)::DATE AS retention_period_start,
                (CASE 
                    WHEN outgoing.rem2 > 0 AND outgoing.rem2 < 4102444800 THEN TO_TIMESTAMP(outgoing.rem2) 
                    WHEN outgoing.rem2 >= 4102444800 THEN TO_TIMESTAMP(outgoing.rem2 / 1000)
                    WHEN outgoing.name ILIKE '%kontrak%' OR outgoing.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '15 years'
                    WHEN outgoing.name ILIKE '%laporan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '7 years'
                    WHEN outgoing.name ILIKE '%tugas%' OR outgoing.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN outgoing.name ILIKE '%undangan%' THEN 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                    ELSE 
                        (CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                END)::DATE AS retention_period_end,
                CASE WHEN outgoing.downloadable = 1 THEN TRUE ELSE FALSE END AS is_downloadable,
                CASE
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) > 0 AND CAST(smc.date AS BIGINT) < 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT))::DATE::VARCHAR
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) >= 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT) / 1000)::DATE::VARCHAR
                    ELSE NULL
                END AS received_date,
                'internal' AS received_via,
                target_user.fullname AS receiver_name,
                NULL AS note,
                NULL AS attachment_type,
                CASE
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) > 0 AND CAST(smc.date AS BIGINT) < 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT))
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) >= 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT) / 1000)
                    ELSE TIMESTAMP '2024-01-01 00:00:00'
                END AS created_at,
                CASE
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) > 0 AND CAST(smc.date AS BIGINT) < 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT))
                    WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) >= 4102444800
                    THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT) / 1000)
                    ELSE TIMESTAMP '2024-01-01 00:00:00'
                END AS updated_at,
                drafter.ulid_id AS created_by_user_id,
                FALSE AS is_draft,
                'incoming_letter' AS segment_type,
                FALSE AS is_deleted,
                NULL AS deleted_at,
                NULL AS deleted_by,
                'internal' AS recipient_type,
                -- folder_document_id: Ambil dari outgoing letter jika ada
                CASE 
                    WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 
                    THEN outgoing.ulid_id
                    ELSE NULL 
                END AS folder_document_id
            FROM public.surat_masuk_chained smc
            INNER JOIN public.tbldocuments outgoing
                ON smc.id_surat_keluar = outgoing.id
                AND outgoing.kind IN (2, 3)
                AND outgoing.keluar_penerima = 1
            INNER JOIN public.tblusers target_user
                ON smc."user" = target_user.id
            LEFT JOIN public.tblgroups work_unit
                ON smc.dept_surat_masuk = work_unit.id
            LEFT JOIN public.tblusers drafter
                ON outgoing.owner = drafter.id
            LEFT JOIN public.tblgroups drafter_unit
                ON CAST(NULLIF(outgoing.keluar_dept_from, '') AS INTEGER) = drafter_unit.id
            LEFT JOIN public.surat_sifat security
                ON outgoing.keluar_core_sifat_surat = security.judul
            WHERE smc.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND target_user.ulid_id IS NOT NULL
        """,
        "target_table": "public.incoming_letters",
        "method": "upsert",
        "conflict_columns": ["id"],
        "transform": ["parse_date:letter_date", "parse_date:received_date"],
    },
    # =========================================================================
    # PHASE 6: OUTGOING LETTERS (Depends on: work_units, users, letter_categories, folders)
    # =========================================================================
    {
        "name": "outgoing_letters",
        "sql": """
            WITH first_recipients AS (
                -- OUTGOING LETTER target_user_id (first recipient):
                -- Case Internal: first recipient dari surat_masuk_chained (distribusi ke user internal)
                -- Case External: NULL (gunakan external_target_user_name untuk entitas luar)
                SELECT
                    smc.id_surat_keluar as doc_id,
                    u.ulid_id as first_user_ulid,
                    ROW_NUMBER() OVER (PARTITION BY smc.id_surat_keluar ORDER BY smc.id) as rn
                FROM surat_masuk_chained smc
                INNER JOIN tblusers u ON smc.user = u.id
                WHERE u.ulid_id IS NOT NULL
            ),
            -- Get only ONE group per user to prevent row duplication
            user_primary_group AS (
                SELECT DISTINCT ON (gm.userid)
                    gm.userid,
                    g.ulid_id as group_ulid
                FROM public.tblgroupmembers gm
                INNER JOIN public.tblgroups g ON gm.groupid = g.id
                WHERE g.ulid_id IS NOT NULL
                ORDER BY gm.userid, gm.manager DESC, gm.groupid ASC
            ),
            -- Extract category code from letter number (format: XXX/KODE-YYY/BULAN/TAHUN)
            letter_category_match AS (
                SELECT
                    doc.id,
                    doc.ulid_id,
                    -- Extract the second part after first '/' which usually contains category code
                    SPLIT_PART(SPLIT_PART(doc.keluar_nosurat_plain, '/', 2), '-', 1) AS extracted_code,
                    kat.ulid_id AS matched_category_id
                FROM public.tbldocuments doc
                LEFT JOIN public.surat_kategori kat
                    ON SPLIT_PART(SPLIT_PART(doc.keluar_nosurat_plain, '/', 2), '-', 1) = kat.kode
                    AND kat.kode NOT LIKE '%-%'
                    AND kat.ulid_id IS NOT NULL
                WHERE doc.kind IN (2, 3)
                AND doc.ulid_id IS NOT NULL
            )
            SELECT
                doc.ulid_id AS id,
                CASE
                    WHEN doc.keluar_penerima = 1 THEN 'internal'
                    ELSE 'external'
                END AS recipient_type,
                -- Fallback to created_user's work_unit if keluar_dept_from is empty
                COALESCE(work_unit.ulid_id, upg.group_ulid) AS work_unit_id,
                NULL AS external_work_unit_name,
                -- target_user_id: Internal = first recipient, External = NULL
                CASE WHEN doc.keluar_penerima = 1 THEN fr.first_user_ulid ELSE NULL END AS target_user_id,
                -- external_target_user_name: untuk external recipient (entitas luar sistem)
                doc.keluar_core_penerima AS external_target_user_name,
                -- Auto-match category from letter number pattern (XXX/KODE-YYY/BULAN/TAHUN)
                lcm.matched_category_id AS category_id,
                NULL AS sub_category_id,
                NULL AS official_document_type_id,
                doc.name AS subject,
                doc.keluar_nosurat_plain AS letter_number,
                CASE 
                    WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date)::DATE::VARCHAR
                    WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000)::DATE::VARCHAR
                    ELSE NULL 
                END AS letter_date,
                NULL AS warehouse_location_id,
                FALSE AS is_bulk_upload,
                CASE
                    WHEN CAST(doc.wftype AS VARCHAR) ~ '^[0-9]+$'
                         AND CAST(doc.wftype AS INTEGER) > 0
                    THEN 'sequential'
                    ELSE 'parallel'
                END AS workflow_type,
                -- is_draft = FALSE untuk semua (system lama tidak ada field ini)
                FALSE AS is_draft,
                created_user.ulid_id AS drafter_user_id,
                created_user.ulid_id AS created_by_user_id,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                -- Status mapping based on workflow logs and document flags
                CASE
                    -- Check rejected first (review or approval rejected)
                    WHEN EXISTS (SELECT 1 FROM tbldocumentreviewlog rl INNER JOIN tbldocumentreviewers r ON rl.reviewid = r.reviewid WHERE r.documentid = doc.id AND rl.status = -1) THEN 'Ditolak'
                    WHEN EXISTS (SELECT 1 FROM tbldocumentapprovelog al INNER JOIN tbldocumentapprovers a ON al.approveid = a.approveid WHERE a.documentid = doc.id AND al.status = -1) THEN 'Ditolak'
                    -- Released = sent + published
                    WHEN doc.terkirim = 1 AND doc.spub = 1 THEN 'Dirilis'
                    -- Sent
                    WHEN doc.terkirim = 1 THEN 'Terkirim'
                    -- Check approval status
                    WHEN EXISTS (SELECT 1 FROM tbldocumentapprovelog al INNER JOIN tbldocumentapprovers a ON al.approveid = a.approveid WHERE a.documentid = doc.id AND al.status = 1) THEN 'Sudah Approve'
                    WHEN EXISTS (SELECT 1 FROM tbldocumentapprovelog al INNER JOIN tbldocumentapprovers a ON al.approveid = a.approveid WHERE a.documentid = doc.id AND al.status = 0) THEN 'Menunggu Approval'
                    -- Check review status
                    WHEN EXISTS (SELECT 1 FROM tbldocumentreviewlog rl INNER JOIN tbldocumentreviewers r ON rl.reviewid = r.reviewid WHERE r.documentid = doc.id AND rl.status = 1) THEN 'Sudah Review'
                    WHEN EXISTS (SELECT 1 FROM tbldocumentreviewlog rl INNER JOIN tbldocumentreviewers r ON rl.reviewid = r.reviewid WHERE r.documentid = doc.id AND rl.status = 0) THEN 'Menunggu Review'
                    -- Default untuk semua document (dengan atau tanpa workflow)
                    ELSE 'Belum Baca'
                END AS status,
                NULL AS upload_proof,
                doc.keywords AS keyword,
                NULL AS hardcopy_receiver_name,
                NULL AS hardcopy_receipt_number,
                NULL AS hardcopy_shipping_tracking_link,
                NULL AS softcopy_email_draft,
                folder.ulid_id AS folder_id,
                -- All documents now have retention dates (original rem1/rem2 OR calculated defaults)
                (CASE 
                    WHEN doc.rem1 > 0 AND doc.rem1 < 4102444800 THEN TO_TIMESTAMP(doc.rem1) 
                    WHEN doc.rem1 >= 4102444800 THEN TO_TIMESTAMP(doc.rem1 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '10 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '3 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '1 year'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                END)::DATE AS retention_period_start,
                (CASE 
                    WHEN doc.rem2 > 0 AND doc.rem2 < 4102444800 THEN TO_TIMESTAMP(doc.rem2) 
                    WHEN doc.rem2 >= 4102444800 THEN TO_TIMESTAMP(doc.rem2 / 1000)
                    WHEN doc.name ILIKE '%kontrak%' OR doc.name ILIKE '%perjanjian%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '15 years'
                    WHEN doc.name ILIKE '%laporan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '7 years'
                    WHEN doc.name ILIKE '%tugas%' OR doc.name ILIKE '%penugasan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                    WHEN doc.name ILIKE '%undangan%' THEN 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '2 years'
                    ELSE 
                        (CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END) + INTERVAL '5 years'
                END)::DATE AS retention_period_end,
                FALSE AS is_deleted,
                NULL AS deleted_at,
                NULL AS deleted_by,
                NULL AS pic,
                -- folder_document_id: Self-reference jika letter ada dalam folder
                CASE 
                    WHEN doc.folder IS NOT NULL AND doc.folder > 0 
                    THEN doc.ulid_id
                    ELSE NULL 
                END AS folder_document_id
            FROM public.tbldocuments doc
            LEFT JOIN public.tblgroups work_unit
                ON CAST(NULLIF(doc.keluar_dept_from, '') AS INTEGER) = work_unit.id
            LEFT JOIN public.tblusers created_user
                ON doc.owner = created_user.id
            LEFT JOIN user_primary_group upg
                ON created_user.id = upg.userid
            LEFT JOIN public.tblfolders folder
                ON doc.folder = folder.id
            -- First recipient untuk internal outgoing (dari surat_masuk_chained)
            LEFT JOIN first_recipients fr
                ON doc.id = fr.doc_id AND fr.rn = 1
            -- Match category from letter number pattern
            LEFT JOIN letter_category_match lcm
                ON doc.id = lcm.id
            WHERE doc.ulid_id IS NOT NULL
              AND doc.kind IN (2, 3)
        """,
        "target_table": "public.outgoing_letters",
        "method": "upsert",
        "conflict_columns": ["id"],
        "transform": ["parse_date:letter_date"],
    },
    # =========================================================================
    # PHASE 6 [EXEC #14]: INCOMING LETTER ATTACHMENTS
    # Depends on: incoming_letters
    # Lampiran tambahan dari tbldocumentfiles saja (bukan dokumen utama)
    # =========================================================================
    {
        "name": "incoming_letter_attachments",
        "sql": """
            SELECT
                df.ulid_id AS id,
                incoming.ulid_id AS incoming_letter_id,
                COALESCE(df.name, df.orgfilename, 'unknown') AS file_name,
                CONCAT('https://dms-storage.adhi.co.id/storage/', CAST(df.document AS VARCHAR), '/f', CAST(df.id AS VARCHAR), COALESCE(df.filetype, '.pdf')) AS file_url,
                COALESCE(df.filetype,
                    CASE
                        WHEN df.orgfilename LIKE '%.pdf' THEN 'pdf'
                        WHEN df.orgfilename LIKE '%.doc%' THEN 'doc'
                        WHEN df.orgfilename LIKE '%.xls%' THEN 'xls'
                        WHEN df.orgfilename LIKE '%.jpg' OR df.orgfilename LIKE '%.jpeg' THEN 'jpg'
                        WHEN df.orgfilename LIKE '%.png' THEN 'png'
                        ELSE 'unknown'
                    END
                ) AS file_extension,
                0.0 AS size,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocumentfiles df
            LEFT JOIN public.tbldocuments incoming
                ON df.document = incoming.id
            WHERE df.ulid_id IS NOT NULL
              AND incoming.ulid_id IS NOT NULL
              AND incoming.kind IN (0, 1)
        """,
        "target_table": "public.incoming_letter_attachments",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 6: OUTGOING LETTER ATTACHMENTS
    # Depends on: outgoing_letters
    # Lampiran tambahan dari tbldocumentfiles saja (bukan dokumen utama)
    # =========================================================================
    {
        "name": "outgoing_letter_attachments",
        "sql": """
            SELECT
                df.ulid_id AS id,
                outgoing.ulid_id AS outgoing_letter_id,
                COALESCE(df.name, df.orgfilename, 'unknown') AS file_name,
                CONCAT('https://dms-storage.adhi.co.id/storage/', CAST(df.document AS VARCHAR), '/f', CAST(df.id AS VARCHAR), COALESCE(df.filetype, '.pdf')) AS file_url,
                COALESCE(df.filetype,
                    CASE
                        WHEN df.orgfilename LIKE '%.pdf' THEN 'pdf'
                        WHEN df.orgfilename LIKE '%.doc%' THEN 'doc'
                        WHEN df.orgfilename LIKE '%.xls%' THEN 'xls'
                        WHEN df.orgfilename LIKE '%.jpg' OR df.orgfilename LIKE '%.jpeg' THEN 'jpg'
                        WHEN df.orgfilename LIKE '%.png' THEN 'png'
                        ELSE 'unknown'
                    END
                ) AS file_extension,
                0.0 AS size,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocumentfiles df
            LEFT JOIN public.tbldocuments outgoing
                ON df.document = outgoing.id
            WHERE df.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND outgoing.kind IN (2, 3)
        """,
        "target_table": "public.outgoing_letter_attachments",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 6: FOLDER DOCUMENT ATTACHMENTS
    # Depends on: folder_documents
    # Lampiran tambahan dari tbldocumentfiles saja (bukan dokumen utama)
    # =========================================================================
    {
        "name": "folder_document_attachments",
        "sql": """
            SELECT
                df.ulid_id AS id,
                folder_doc.ulid_id AS folder_document_id,
                COALESCE(df.name, df.orgfilename, 'unknown') AS file_name,
                CONCAT('https://dms-storage.adhi.co.id/storage/', CAST(df.document AS VARCHAR), '/f', CAST(df.id AS VARCHAR), COALESCE(df.filetype, '.pdf')) AS file_url,
                COALESCE(df.filetype,
                    CASE
                        WHEN df.orgfilename LIKE '%.pdf' THEN 'pdf'
                        WHEN df.orgfilename LIKE '%.doc%' THEN 'doc'
                        WHEN df.orgfilename LIKE '%.xls%' THEN 'xls'
                        WHEN df.orgfilename LIKE '%.jpg' OR df.orgfilename LIKE '%.jpeg' THEN 'jpg'
                        WHEN df.orgfilename LIKE '%.png' THEN 'png'
                        ELSE 'unknown'
                    END
                ) AS file_extension,
                0.0 AS size,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN df.date > 0 AND df.date < 4102444800 THEN TO_TIMESTAMP(df.date) WHEN df.date >= 4102444800 THEN TO_TIMESTAMP(df.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocumentfiles df
            LEFT JOIN public.tbldocuments folder_doc
                ON df.document = folder_doc.id
            LEFT JOIN public.tblfolders folder
                ON folder_doc.folder = folder.id
            WHERE df.ulid_id IS NOT NULL
              AND folder_doc.ulid_id IS NOT NULL
              AND folder_doc.folder IS NOT NULL
              AND folder_doc.folder > 0
              AND folder.ulid_id IS NOT NULL
              AND folder_doc.kind NOT IN (0, 1, 2, 3)
        """,
        "target_table": "public.folder_document_attachments",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 6: DOCUMENTS (Main Document Content Only)
    # Dokumen utama dari tbldocumentcontent saja (bukan lampiran tambahan)
    # =========================================================================
    {
        "name": "documents",
        "sql": """
            SELECT
                dc.ulid_id AS id,
                CASE
                    WHEN doc.kind IN (0, 1)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS incoming_letter_id,
                COALESCE(dc.orgfilename, 'unknown') AS file_name,
                CONCAT('https://dms-storage.adhi.co.id/', dc.dir, CAST(dc.version AS VARCHAR), COALESCE(dc.filetype, '.pdf')) AS file_url,
                CASE
                    WHEN doc.kind IN (0, 1) THEN 'incoming_letter'
                    WHEN doc.kind IN (2, 3) THEN 'outgoing_letter'
                    ELSE 'folder_document'
                END AS category,
                CASE WHEN dc.date > 0 AND dc.date < 4102444800 THEN TO_TIMESTAMP(dc.date) WHEN dc.date >= 4102444800 THEN TO_TIMESTAMP(dc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN dc.date > 0 AND dc.date < 4102444800 THEN TO_TIMESTAMP(dc.date) WHEN dc.date >= 4102444800 THEN TO_TIMESTAMP(dc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE
                    WHEN doc.kind IN (2, 3)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS outgoing_letter_id,
                CAST(dc.version AS VARCHAR) AS version,
                0.0 AS size,
                CASE
                    WHEN dc.skact = 1 THEN 'active'
                    ELSE 'inactive'
                END AS status,
                CASE WHEN dc.date > 0 AND dc.date < 4102444800 THEN TO_TIMESTAMP(dc.date) WHEN dc.date >= 4102444800 THEN TO_TIMESTAMP(dc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS uploaded_at,
                created_user.ulid_id AS created_by_user_id,
                CASE
                    WHEN doc.folder IS NOT NULL AND doc.folder > 0 AND folder.ulid_id IS NOT NULL
                    THEN doc.ulid_id
                    ELSE NULL
                END AS folder_document_id
            FROM public.tbldocumentcontent dc
            INNER JOIN public.tbldocuments doc ON dc.document = doc.id
            LEFT JOIN public.tblfolders folder ON doc.folder = folder.id
            LEFT JOIN public.tblusers created_user ON dc.createdby = created_user.id
            WHERE dc.ulid_id IS NOT NULL
              AND doc.ulid_id IS NOT NULL
        """,
        "target_table": "public.documents",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 6: TARGET USERS (Combined: External + Internal)
    # External recipients from keluar_core_penerima AND internal from surat_masuk_chained
    # =========================================================================
    {
        "name": "target_users",
        "sql": """
            -- External recipients
            SELECT
                SUBSTRING(MD5(CONCAT(doc.ulid_id, '-TU-', doc.keluar_core_penerima)), 1, 26) AS id,
                doc.ulid_id AS outgoing_letter_id,
                NULL AS user_id,
                doc.keluar_core_penerima AS external_user_name,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocuments doc
            WHERE doc.ulid_id IS NOT NULL
              AND doc.keluar_core_penerima IS NOT NULL
              AND doc.keluar_core_penerima != ''
              AND doc.kind IN (2, 3)
            
            UNION ALL
            
            -- Internal recipients
            SELECT
                SUBSTRING(MD5(CONCAT(doc.ulid_id, '-TUINT')), 1, 26) AS id,
                doc.ulid_id AS outgoing_letter_id,
                fr.user_ulid AS user_id,
                NULL AS external_user_name,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocuments doc
            INNER JOIN (
                SELECT
                    smc.id_surat_keluar as outgoing_doc_id,
                    u.ulid_id as user_ulid,
                    ROW_NUMBER() OVER (PARTITION BY smc.id_surat_keluar ORDER BY smc.id) as rn
                FROM surat_masuk_chained smc
                INNER JOIN tblusers u ON smc.user = u.id
                WHERE u.ulid_id IS NOT NULL
            ) fr ON doc.id = fr.outgoing_doc_id AND fr.rn = 1
            WHERE doc.ulid_id IS NOT NULL
              AND doc.keluar_penerima = 1
              AND doc.kind IN (2, 3)
        """,
        "target_table": "public.target_users",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 6: TARGET WORK UNITS (Depends on: outgoing_letters, work_units)
    # =========================================================================
    {
        "name": "target_work_units",
        "sql": """
            SELECT
                SUBSTRING(MD5(CONCAT(doc.ulid_id, '-TWU')), 1, 26) AS id,
                doc.ulid_id AS outgoing_letter_id,
                work_unit.ulid_id AS work_unit_id,
                CASE
                    WHEN work_unit.ulid_id IS NULL
                    THEN doc.keluar_dept_from
                    ELSE NULL
                END AS external_work_unit_name,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocuments doc
            LEFT JOIN public.tblgroups work_unit
                ON CAST(NULLIF(doc.keluar_dept_from, '') AS INTEGER) = work_unit.id
            WHERE doc.ulid_id IS NOT NULL
              AND doc.keluar_dept_from IS NOT NULL
              AND doc.kind IN (2, 3)
        """,
        "target_table": "public.target_work_units",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # # =========================================================================
    # # PHASE 7: LETTER PARTICIPANTS (Combined: All 9 Sources)
    # # Combines all participant sources into one unified query with UNION ALL
    # # Sources: Incoming notifications, Outgoing creators, Reviewers, Approvers,
    # #          Internal incoming, Pendok, Recipients, Dispositions, Carbon copies
    # # =========================================================================
    # {
    #     "name": "letter_participants",
    #     "sql": """
    #         -- 1. Incoming Letter Notifications
    #         SELECT
    #             notif.ulid_id AS id,
    #             incoming.ulid_id AS incoming_letter_id,
    #             NULL AS outgoing_letter_id,
    #             NULL AS folder_document_id,
    #             'incoming_letter' AS segment_type,
    #             CASE WHEN notif.tampil = 1 THEN 'Sudah Baca' ELSE 'Belum Baca' END AS status,
    #             'pending' AS disposition_status,
    #             user_ref.ulid_id AS user_id,
    #             CASE WHEN notif.added > 0 AND notif.added < 4102444800 THEN TO_TIMESTAMP(notif.added) WHEN notif.added >= 4102444800 THEN TO_TIMESTAMP(notif.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN notif.added > 0 AND notif.added < 4102444800 THEN TO_TIMESTAMP(notif.added) WHEN notif.added >= 4102444800 THEN TO_TIMESTAMP(notif.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.surat_masuk_notif notif
    #         INNER JOIN public.tbldocuments incoming ON notif.surat_masuk_id = incoming.id AND incoming.kind IN (0, 1)
    #         INNER JOIN public.tblusers user_ref ON notif."user" = user_ref.id
    #         WHERE notif.ulid_id IS NOT NULL AND incoming.ulid_id IS NOT NULL AND user_ref.ulid_id IS NOT NULL
            
    #         UNION ALL
            
    #         -- 2. Outgoing Main View (creators/drafters)
    #         SELECT
    #             ('OL' || SUBSTRING(doc.ulid_id, 3, 24)) AS id,
    #             NULL AS incoming_letter_id,
    #             doc.ulid_id AS outgoing_letter_id,
    #             CASE WHEN doc.folder IS NOT NULL AND doc.folder > 0 THEN doc.ulid_id ELSE NULL END AS folder_document_id,
    #             'outgoing_letter' AS segment_type,
    #             'Terkirim' AS status,
    #             NULL AS disposition_status,
    #             creator_user.ulid_id AS user_id,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tbldocuments doc
    #         INNER JOIN public.tblusers creator_user ON doc.owner = creator_user.id
    #         WHERE doc.ulid_id IS NOT NULL AND creator_user.ulid_id IS NOT NULL AND doc.kind IN (2, 3)
            
    #         UNION ALL
            
    #         -- 3. Outgoing Reviewers
    #         SELECT
    #             rev.ulid_id AS id,
    #             NULL AS incoming_letter_id,
    #             outgoing.ulid_id AS outgoing_letter_id,
    #             CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
    #             'review' AS segment_type,
    #             CASE WHEN rev.wftype = 1 THEN 'Sudah Review' WHEN rev.wftype = 2 THEN 'Ditolak' ELSE 'Menunggu Review' END AS status,
    #             NULL AS disposition_status,
    #             reviewer_user.ulid_id AS user_id,
    #             CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tbldocumentreviewers rev
    #         INNER JOIN public.tbldocuments outgoing ON rev.documentid = outgoing.id
    #         INNER JOIN public.tblusers reviewer_user ON rev.required = reviewer_user.id
    #         WHERE rev.ulid_id IS NOT NULL AND outgoing.ulid_id IS NOT NULL AND reviewer_user.ulid_id IS NOT NULL AND outgoing.kind IN (2, 3)
            
    #         UNION ALL
            
    #         -- 4. Outgoing Approvers
    #         SELECT
    #             appr.ulid_id AS id,
    #             NULL AS incoming_letter_id,
    #             outgoing.ulid_id AS outgoing_letter_id,
    #             CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
    #             'approve' AS segment_type,
    #             CASE WHEN appr.type = 1 THEN 'Sudah Approve' WHEN appr.type = 2 THEN 'Ditolak' ELSE 'Menunggu Approval' END AS status,
    #             NULL AS disposition_status,
    #             approver_user.ulid_id AS user_id,
    #             CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tbldocumentapprovers appr
    #         INNER JOIN public.tbldocuments outgoing ON appr.documentid = outgoing.id
    #         INNER JOIN public.tblusers approver_user ON appr.required = approver_user.id
    #         WHERE appr.ulid_id IS NOT NULL AND outgoing.ulid_id IS NOT NULL AND approver_user.ulid_id IS NOT NULL AND outgoing.kind IN (2, 3)
            
    #         UNION ALL
            
    #         -- 5. Incoming Internal (from surat_masuk_chained)
    #         SELECT
    #             ('LI' || SUBSTRING(smc.ulid_id, 3, 24)) AS id,
    #             smc.ulid_id AS incoming_letter_id,
    #             NULL AS outgoing_letter_id,
    #             NULL AS folder_document_id,
    #             'incoming_letter' AS segment_type,
    #             CASE WHEN smc.dibaca = 1 THEN 'Sudah Baca' ELSE 'Belum Baca' END AS status,
    #             'pending' AS disposition_status,
    #             target_user.ulid_id AS user_id,
    #             CASE WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) > 0 AND CAST(smc.date AS BIGINT) < 4102444800 THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT)) WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) >= 4102444800 THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT) / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) > 0 AND CAST(smc.date AS BIGINT) < 4102444800 THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT)) WHEN smc.date ~ '^[0-9]+$' AND CAST(smc.date AS BIGINT) >= 4102444800 THEN TO_TIMESTAMP(CAST(smc.date AS BIGINT) / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             CASE WHEN smc.tampil = 0 THEN TRUE ELSE FALSE END AS is_hide
    #         FROM public.surat_masuk_chained smc
    #         INNER JOIN public.tbldocuments outgoing ON smc.id_surat_keluar = outgoing.id AND outgoing.kind IN (2, 3) AND outgoing.keluar_penerima = 1
    #         INNER JOIN public.tblusers target_user ON smc."user" = target_user.id
    #         WHERE smc.ulid_id IS NOT NULL AND outgoing.ulid_id IS NOT NULL AND target_user.ulid_id IS NOT NULL
            
    #         UNION ALL
            
    #         -- 6. Incoming Creators/Pendok
    #         SELECT
    #             ('PD' || SUBSTRING(doc.ulid_id, 3, 24)) AS id,
    #             doc.ulid_id AS incoming_letter_id,
    #             NULL AS outgoing_letter_id,
    #             CASE WHEN doc.folder IS NOT NULL AND doc.folder > 0 THEN doc.ulid_id ELSE NULL END AS folder_document_id,
    #             'pendok' AS segment_type,
    #             'Terkirim' AS status,
    #             NULL AS disposition_status,
    #             creator_user.ulid_id AS user_id,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tbldocuments doc
    #         INNER JOIN public.tblusers creator_user ON doc.owner = creator_user.id
    #         WHERE doc.ulid_id IS NOT NULL AND creator_user.ulid_id IS NOT NULL AND doc.kind IN (0, 1)
            
    #         UNION ALL
            
    #         -- 7. Incoming Recipients (Target Users)
    #         SELECT
    #             ('RC' || SUBSTRING(doc.ulid_id, 3, 24)) AS id,
    #             doc.ulid_id AS incoming_letter_id,
    #             NULL AS outgoing_letter_id,
    #             CASE WHEN doc.folder IS NOT NULL AND doc.folder > 0 THEN doc.ulid_id ELSE NULL END AS folder_document_id,
    #             'incoming_letter' AS segment_type,
    #             'Belum Baca' AS status,
    #             NULL AS disposition_status,
    #             target_user.ulid_id AS user_id,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tbldocuments doc
    #         INNER JOIN public.tblusers target_user ON CAST(NULLIF(doc.masuk_nama_penerima_user, '') AS BIGINT) = target_user.id
    #         WHERE doc.ulid_id IS NOT NULL AND target_user.ulid_id IS NOT NULL AND doc.kind IN (0, 1)
            
    #         UNION ALL
            
    #         -- 8. Incoming Dispositions
    #         SELECT
    #             disp.ulid_id AS id,
    #             incoming.ulid_id AS incoming_letter_id,
    #             NULL AS outgoing_letter_id,
    #             CASE WHEN incoming.folder IS NOT NULL AND incoming.folder > 0 THEN incoming.ulid_id ELSE NULL END AS folder_document_id,
    #             'disposition' AS segment_type,
    #             CASE WHEN disp.dibaca = 1 THEN 'Sudah Baca' ELSE 'Belum Baca' END AS status,
    #             CASE WHEN disp.balasan IS NOT NULL AND disp.balasan != '' THEN 'replied' ELSE 'Berlangsung' END AS disposition_status,
    #             receiver_user.ulid_id AS user_id,
    #             CASE WHEN disp.added > 0 AND disp.added < 4102444800 THEN TO_TIMESTAMP(disp.added) WHEN disp.added >= 4102444800 THEN TO_TIMESTAMP(disp.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN disp.added > 0 AND disp.added < 4102444800 THEN TO_TIMESTAMP(disp.added) WHEN disp.added >= 4102444800 THEN TO_TIMESTAMP(disp.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.surat_masuk_disposisi disp
    #         INNER JOIN public.tbldocuments incoming ON disp.id_surat_masuk = incoming.id
    #         INNER JOIN public.tblusers receiver_user ON disp.userid = receiver_user.id
    #         WHERE disp.ulid_id IS NOT NULL AND incoming.ulid_id IS NOT NULL AND receiver_user.ulid_id IS NOT NULL AND incoming.kind IN (0, 1)
            
    #         UNION ALL
            
    #         -- 9. Carbon Copies
    #         SELECT
    #             cc.ulid_id AS id,
    #             NULL AS incoming_letter_id,
    #             outgoing.ulid_id AS outgoing_letter_id,
    #             NULL AS folder_document_id,
    #             'carbon_copy' AS segment_type,
    #             'Belum Baca' AS status,
    #             NULL AS disposition_status,
    #             cc_user.ulid_id AS user_id,
    #             CASE WHEN cc.added > 0 AND cc.added < 4102444800 THEN TO_TIMESTAMP(cc.added) WHEN cc.added >= 4102444800 THEN TO_TIMESTAMP(cc.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
    #             CASE WHEN cc.added > 0 AND cc.added < 4102444800 THEN TO_TIMESTAMP(cc.added) WHEN cc.added >= 4102444800 THEN TO_TIMESTAMP(cc.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
    #             FALSE AS is_hide
    #         FROM public.tblkeluartembusan cc
    #         INNER JOIN public.tbldocuments outgoing ON cc.document = outgoing.id
    #         INNER JOIN public.tblusers cc_user ON cc.usertembusan = cc_user.id
    #         WHERE cc.ulid_id IS NOT NULL AND cc_user.ulid_id IS NOT NULL AND outgoing.ulid_id IS NOT NULL AND outgoing.kind IN (2, 3)
    #     """,
    #     "target_table": "public.letter_participants",
    #     "method": "upsert",
    #     "conflict_columns": ["id"]
    # },
    # =========================================================================
    # PHASE 7: LETTER DISPOSITIONS (Depends on: incoming_letters, users)
    # =========================================================================
    {
        "name": "letter_dispositions",
        "sql": """
            SELECT
                disp.ulid_id AS id,
                incoming.ulid_id AS incoming_letter_id,
                NULL AS parent_id,
                sender_user.ulid_id AS sender_id,
                receiver_user.ulid_id AS receiver_id,
                disp.keterangan AS description,
                disp.balasan AS disposition_response,
                CASE WHEN disp.dibaca = 1 AND disp.dibaca_date > 0 AND disp.dibaca_date < 4102444800 THEN TO_TIMESTAMP(disp.dibaca_date) WHEN disp.dibaca = 1 AND disp.dibaca_date >= 4102444800 THEN TO_TIMESTAMP(disp.dibaca_date / 1000) ELSE NULL END AS read_at,
                CASE
                    WHEN disp.dibaca = 1 THEN 'read'
                    ELSE 'unread'
                END AS status,
                CASE WHEN disp.added > 0 AND disp.added < 4102444800 THEN TO_TIMESTAMP(disp.added) WHEN disp.added >= 4102444800 THEN TO_TIMESTAMP(disp.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN disp.added > 0 AND disp.added < 4102444800 THEN TO_TIMESTAMP(disp.added) WHEN disp.added >= 4102444800 THEN TO_TIMESTAMP(disp.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE WHEN disp.balasan IS NOT NULL AND disp.balasan != '' AND disp.dibalas_date > 0 AND disp.dibalas_date < 4102444800 THEN TO_TIMESTAMP(disp.dibalas_date) WHEN disp.balasan IS NOT NULL AND disp.balasan != '' AND disp.dibalas_date >= 4102444800 THEN TO_TIMESTAMP(disp.dibalas_date / 1000) ELSE NULL END AS response_at,
                FALSE AS is_closed
            FROM public.surat_masuk_disposisi disp  
            INNER JOIN public.tbldocuments incoming
                ON disp.id_surat_masuk = incoming.id
                AND incoming.kind IN (0, 1)
            LEFT JOIN public.tblusers sender_user
                ON disp.pengirim = sender_user.id
            LEFT JOIN public.tblusers receiver_user
                ON disp.userid = receiver_user.id
            WHERE disp.ulid_id IS NOT NULL
              AND incoming.ulid_id IS NOT NULL
              AND receiver_user.ulid_id IS NOT NULL
        """,
        "target_table": "public.letter_dispositions",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 7: WORKFLOW REVIEWERS (Combined: Normal + Serial)
    # Combines normal reviewers AND sequential workflow reviewers
    # =========================================================================
    {
        "name": "workflow_reviewers",
        "sql": """
            -- Normal reviewers
            SELECT
                rev.ulid_id AS id,
                outgoing.ulid_id AS outgoing_letter_id,
                reviewer_user.ulid_id AS user_id,
                rev.version AS "order",
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE
                    WHEN review_log.status = 1 THEN 'disetujui'
                    WHEN review_log.status IN (-1, -2) THEN 'ditolak'
                    ELSE NULL
                END AS review_status,
                review_log.comment AS notes,
                CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
                FALSE AS is_hide
            FROM public.tbldocumentreviewers rev
            INNER JOIN public.tbldocuments outgoing
                ON rev.documentid = outgoing.id
                AND outgoing.kind IN (2, 3)
            INNER JOIN public.tblusers reviewer_user
                ON rev.required = reviewer_user.id
            LEFT JOIN LATERAL (
                SELECT status, comment FROM public.tbldocumentreviewlog
                WHERE reviewid = rev.reviewid
                ORDER BY reviewlogid DESC LIMIT 1
            ) review_log ON true
            WHERE rev.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND reviewer_user.ulid_id IS NOT NULL
            
            UNION ALL
            
            -- Serial reviewers (sequential workflow)
            SELECT
                revs.ulid_id AS id,
                outgoing.ulid_id AS outgoing_letter_id,
                reviewer_user.ulid_id AS user_id,
                revs.version AS "order",
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE
                    WHEN review_log.status = 1 THEN 'disetujui'
                    WHEN review_log.status IN (-1, -2) THEN 'ditolak'
                    ELSE NULL
                END AS review_status,
                review_log.comment AS notes,
                CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
                CASE WHEN revs.is_removed = 1 THEN TRUE ELSE FALSE END AS is_hide
            FROM public.tbldocumentreviewersserial revs
            INNER JOIN public.tbldocuments outgoing
                ON revs.documentid = outgoing.id
                AND outgoing.kind IN (2, 3)
            INNER JOIN public.tblusers reviewer_user
                ON revs.required = reviewer_user.id
            LEFT JOIN LATERAL (
                SELECT status, comment FROM public.tbldocumentreviewlog
                WHERE reviewid = revs.reviewid
                ORDER BY reviewlogid DESC LIMIT 1
            ) review_log ON true
            WHERE revs.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND reviewer_user.ulid_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM public.tbldocumentreviewers r
                  WHERE r.documentid = revs.documentid AND r.required = revs.required
              )
        """,
        "target_table": "public.workflow_reviewers",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 7: WORKFLOW APPROVERS (Combined: Normal + Serial)
    # Combines normal approvers AND sequential workflow approvers
    # =========================================================================
    {
        "name": "workflow_approvers",
        "sql": """
            -- Normal approvers
            SELECT
                appr.ulid_id AS id,
                CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
                outgoing.ulid_id AS outgoing_letter_id,
                approver_user.ulid_id AS user_id,
                appr.version AS "order",
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE
                    WHEN approve_log.status = 1 THEN 'disetujui'
                    WHEN approve_log.status IN (-1, -2) THEN 'ditolak'
                    ELSE NULL
                END AS approve_status,
                approve_log.comment AS notes,
                FALSE AS is_hide
            FROM public.tbldocumentapprovers appr
            INNER JOIN public.tbldocuments outgoing
                ON appr.documentid = outgoing.id
                AND outgoing.kind IN (2, 3)
            INNER JOIN public.tblusers approver_user
                ON appr.required = approver_user.id
            LEFT JOIN LATERAL (
                SELECT status, comment FROM public.tbldocumentapprovelog
                WHERE approveid = appr.approveid
                ORDER BY approvelogid DESC LIMIT 1
            ) approve_log ON true
            WHERE appr.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND approver_user.ulid_id IS NOT NULL
            
            UNION ALL
            
            -- Serial approvers (sequential workflow)
            SELECT
                apprs.ulid_id AS id,
                CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
                outgoing.ulid_id AS outgoing_letter_id,
                approver_user.ulid_id AS user_id,
                apprs.version AS "order",
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN outgoing.date > 0 AND outgoing.date < 4102444800 THEN TO_TIMESTAMP(outgoing.date) WHEN outgoing.date >= 4102444800 THEN TO_TIMESTAMP(outgoing.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at,
                CASE
                    WHEN approve_log.status = 1 THEN 'disetujui'
                    WHEN approve_log.status IN (-1, -2) THEN 'ditolak'
                    ELSE NULL
                END AS approve_status,
                approve_log.comment AS notes,
                FALSE AS is_hide
            FROM public.tbldocumentapproversserial apprs
            INNER JOIN public.tbldocuments outgoing
                ON apprs.documentid = outgoing.id
                AND outgoing.kind IN (2, 3)
            INNER JOIN public.tblusers approver_user
                ON apprs.required = approver_user.id
            LEFT JOIN LATERAL (
                SELECT status, comment FROM public.tbldocumentapprovelog
                WHERE approveid = apprs.approveid
                ORDER BY approvelogid DESC LIMIT 1
            ) approve_log ON true
            WHERE apprs.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND approver_user.ulid_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM public.tbldocumentapprovers a
                  WHERE a.documentid = apprs.documentid AND a.required = apprs.required
              )
        """,
        "target_table": "public.workflow_approvers",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 7: CARBON COPIES (Depends on: outgoing_letters, users)
    # =========================================================================
    {
        "name": "carbon_copies",
        "sql": """
            SELECT
                cc.ulid_id AS id,
                CASE WHEN outgoing.folder IS NOT NULL AND outgoing.folder > 0 THEN outgoing.ulid_id ELSE NULL END AS folder_document_id,
                outgoing.ulid_id AS outgoing_letter_id,
                cc_user.ulid_id AS user_id,
                CASE WHEN cc.added > 0 AND cc.added < 4102444800 THEN TO_TIMESTAMP(cc.added) WHEN cc.added >= 4102444800 THEN TO_TIMESTAMP(cc.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN cc.added > 0 AND cc.added < 4102444800 THEN TO_TIMESTAMP(cc.added) WHEN cc.added >= 4102444800 THEN TO_TIMESTAMP(cc.added / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tblkeluartembusan cc
            INNER JOIN public.tbldocuments outgoing
                ON cc.document = outgoing.id
                AND outgoing.kind IN (2, 3)
            INNER JOIN public.tblusers cc_user
                ON cc.usertembusan = cc_user.id
            WHERE cc.ulid_id IS NOT NULL
              AND outgoing.ulid_id IS NOT NULL
              AND cc_user.ulid_id IS NOT NULL
        """,
        "target_table": "public.carbon_copies",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 8: DISCUSSION FORUMS (Depends on: incoming_letters, outgoing_letters)
    # =========================================================================
    {
        "name": "discussion_forums",
        "sql": """
            SELECT DISTINCT ON (forum.documentid)
                forum.ulid_id AS id,
                CASE
                    WHEN doc.masuk_nomor IS NOT NULL
                         AND doc.masuk_nomor != ''
                         AND doc.kind IN (0, 1)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS incoming_letter_id,
                CASE
                    WHEN doc.kind IN (2, 3)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS outgoing_letter_id,
                CAST(forum.date AS TIMESTAMP) AS created_at,
                CAST(forum.date AS TIMESTAMP) AS updated_at,
                CASE
                    WHEN doc.folder IS NOT NULL AND doc.folder > 0
                    THEN doc.ulid_id
                    ELSE NULL
                END AS folder_document_id
            FROM public.tblforum forum
            INNER JOIN public.tbldocuments doc ON forum.documentid = doc.id
            WHERE forum.ulid_id IS NOT NULL
              AND doc.ulid_id IS NOT NULL
            ORDER BY forum.documentid, forum.forumid ASC
        """,
        "target_table": "public.discussion_forums",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 8: DISCUSSION FORUM MESSAGES (Depends on: discussion_forums, users)
    # =========================================================================
    {
        "name": "discussion_forum_messages",
        "sql": """
            SELECT
                msg.ulid_id AS id,
                first_forum.ulid_id AS discussion_forum_id,
                msg.comment AS message,
                created_user.ulid_id AS created_by,
                FALSE AS is_read,
                CAST(msg.date AS TIMESTAMP) AS created_at,
                CAST(msg.date AS TIMESTAMP) AS updated_at
            FROM public.tblforum msg
            INNER JOIN (
                SELECT DISTINCT ON (f.documentid) f.ulid_id, f.documentid
                FROM public.tblforum f
                INNER JOIN public.tbldocuments d ON f.documentid = d.id
                WHERE f.ulid_id IS NOT NULL
                  AND d.ulid_id IS NOT NULL
                ORDER BY f.documentid, f.forumid ASC
            ) first_forum ON msg.documentid = first_forum.documentid
            INNER JOIN public.tblusers created_user
                ON msg.userid = created_user.id
            WHERE msg.ulid_id IS NOT NULL
              AND created_user.ulid_id IS NOT NULL
              AND first_forum.ulid_id IS NOT NULL
        """,
        "target_table": "public.discussion_forum_messages",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
    # =========================================================================
    # PHASE 8: MESSAGE READS (Depends on: discussion_forum_messages, users)
    # =========================================================================
    {
        "name": "message_reads",
        "sql": """
            SELECT
                fl.ulid_id AS id,
                forum_msg.ulid_id AS message_id,
                reader_user.ulid_id AS user_id,
                CAST(fl.date AS TIMESTAMP) AS read_at
            FROM public.tblforumlog fl
            INNER JOIN public.tblforum forum_msg ON fl.forumid = forum_msg.forumid
            INNER JOIN public.tbldocuments doc ON forum_msg.documentid = doc.id
            INNER JOIN public.tblusers reader_user ON fl.userid = reader_user.id
            WHERE fl.ulid_id IS NOT NULL
              AND forum_msg.ulid_id IS NOT NULL
              AND doc.ulid_id IS NOT NULL
              AND reader_user.ulid_id IS NOT NULL
              AND fl.status = 1
        """,
        "target_table": "public.message_reads",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
    # =========================================================================
    # PHASE 9: ACCESS RIGHTS (Depends on: users, work_units, letters)
    # =========================================================================
    {
        "name": "access_rights",
        "sql": """
            SELECT
                acl.ulid_id AS id,
                user_ref.ulid_id AS user_id,
                CASE
                    WHEN acl.targettype = 1
                         AND doc.kind IN (0, 1)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS incoming_letter_id,
                CASE
                    WHEN acl.targettype = 1
                         AND doc.kind IN (2, 3)
                    THEN doc.ulid_id
                    ELSE NULL
                END AS outgoing_letter_id,
                CASE WHEN acl.targettype = 1 THEN doc.ulid_id ELSE NULL END AS folder_document_id,
                group_ref.ulid_id AS work_unit_id,
                CASE
                    WHEN acl.mode = 1 THEN 'read'
                    WHEN acl.mode = 2 THEN 'write'
                    WHEN acl.mode IN (3, 4) THEN 'full'
                    ELSE 'read'
                END AS access_type,
                NULL AS access_start_date,
                acl.exp AS access_end_date_raw,
                acl.exp AS exp_raw,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tblacls acl
            LEFT JOIN public.tblusers user_ref ON acl.userid = user_ref.id
            LEFT JOIN public.tblgroups group_ref ON acl.groupid = group_ref.id
            LEFT JOIN public.tbldocuments doc ON acl.target = doc.id AND acl.targettype = 1
            WHERE acl.ulid_id IS NOT NULL
              AND (user_ref.ulid_id IS NOT NULL OR group_ref.ulid_id IS NOT NULL)
        """,
        "target_table": "public.access_rights",
        "method": "upsert",
        "conflict_columns": ["id"],
        "transform": [
            "parse_exp:access_end_date_raw:access_end_date",
            "is_access_forever:exp_raw",
        ],
    },
    # =========================================================================
    # PHASE 9: USER ACCESS HISTORIES (Depends on: users)
    # =========================================================================
    {
        "name": "user_access_histories",
        "sql": """
            SELECT
                log.ulid_id AS id,
                user_ref.ulid_id AS user_id,
                log.loginat AS accessed_at,
                CAST(log.ip AS VARCHAR) AS ip_address,
                NULL AS user_agent,
                log.loginat AS created_at_raw,
                log.loginat AS updated_at_raw
            FROM public.tbluserlogs log
            INNER JOIN public.tblusers user_ref ON log."user" = user_ref.id
            WHERE log.ulid_id IS NOT NULL
              AND user_ref.ulid_id IS NOT NULL
        """,
        "target_table": "public.user_access_histories",
        "method": "upsert",
        "conflict_columns": ["id"],
        "transform": [
            "parse_unix_timestamp:accessed_at",
            "parse_unix_timestamp:created_at_raw:created_at",
            "parse_unix_timestamp:updated_at_raw:updated_at",
        ],
    },
    # =========================================================================
    # PHASE 9: WORK UNIT PICS (Depends on: work_units, users)
    # =========================================================================
    {
        "name": "work_unit_pics",
        "sql": """
            SELECT
                gm.ulid_id AS id,
                group_ref.ulid_id AS work_unit_id,
                user_ref.ulid_id AS user_id,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at
            FROM public.tblgroupmembers gm
            INNER JOIN public.tblgroups group_ref ON gm.groupid = group_ref.id
            INNER JOIN public.tblusers user_ref ON gm.userid = user_ref.id
            WHERE gm.ulid_id IS NOT NULL
              AND gm.manager = 1
              AND group_ref.ulid_id IS NOT NULL
              AND user_ref.ulid_id IS NOT NULL
        """,
        "target_table": "public.work_unit_pics",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 9: FOLDER PICS (Depends on: folders, users)
    # Combines both sources:
    # - targettype=1: Direct folder notification (570 rows)
    # - targettype=2: Document notification → get folder from document (12,018 rows)
    # Expected: ~12,588 rows total
    # =========================================================================
    {
        "name": "folder_pics",
        "sql": """
            -- Source 1: Direct folder notifications (targettype=1)
            SELECT
                notify.ulid_id AS id,
                folder_ref.ulid_id AS folder_id,
                user_ref.ulid_id AS user_id,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at
            FROM public.tblnotify notify
            INNER JOIN public.tblfolders folder_ref ON notify.target = folder_ref.id
            INNER JOIN public.tblusers user_ref ON notify.userid = user_ref.id
            WHERE notify.ulid_id IS NOT NULL
              AND notify.targettype = 1
              AND folder_ref.ulid_id IS NOT NULL
              AND user_ref.ulid_id IS NOT NULL
            UNION ALL
            -- Source 2: Document notifications → get folder from document (targettype=2)
            SELECT
                notify.ulid_id AS id,
                folder_ref.ulid_id AS folder_id,
                user_ref.ulid_id AS user_id,
                TIMESTAMP '2024-01-01 00:00:00' AS created_at,
                TIMESTAMP '2024-01-01 00:00:00' AS updated_at
            FROM public.tblnotify notify
            INNER JOIN public.tbldocuments doc ON notify.target = doc.id
            INNER JOIN public.tblfolders folder_ref ON doc.folder = folder_ref.id
            INNER JOIN public.tblusers user_ref ON notify.userid = user_ref.id
            WHERE notify.ulid_id IS NOT NULL
              AND notify.targettype = 2
              AND doc.ulid_id IS NOT NULL
              AND folder_ref.ulid_id IS NOT NULL
              AND user_ref.ulid_id IS NOT NULL
        """,
        "target_table": "public.folder_pics",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 10: LETTER DOCUMENT RELATIONS (Depends on: letters, folder_documents)
    # Link antar dokumen - folder_document_id REQUIRED (NOT NULL di schema)
    # =========================================================================
    {
        "name": "letter_document_relations",
        "sql": """
            SELECT
                link.ulid_id AS id,
                CASE
                    WHEN source_doc.masuk_nomor IS NOT NULL
                         AND source_doc.masuk_nomor != ''
                         AND source_doc.kind IN (0, 1)
                    THEN source_doc.ulid_id
                    ELSE NULL
                END AS incoming_letter_id,
                CASE
                    WHEN source_doc.keluar_nosurat_plain IS NOT NULL
                         AND source_doc.keluar_nosurat_plain != ''
                         AND source_doc.kind IN (2, 3)
                    THEN source_doc.ulid_id
                    ELSE NULL
                END AS outgoing_letter_id,
                source_doc.ulid_id AS folder_document_id,
                CASE WHEN source_doc.date > 0 AND source_doc.date < 4102444800 THEN TO_TIMESTAMP(source_doc.date) WHEN source_doc.date >= 4102444800 THEN TO_TIMESTAMP(source_doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN source_doc.date > 0 AND source_doc.date < 4102444800 THEN TO_TIMESTAMP(source_doc.date) WHEN source_doc.date >= 4102444800 THEN TO_TIMESTAMP(source_doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocumentlinks link
            INNER JOIN public.tbldocuments source_doc ON link.document = source_doc.id
            WHERE link.ulid_id IS NOT NULL
              AND source_doc.ulid_id IS NOT NULL
              AND source_doc.folder IS NOT NULL
              AND source_doc.folder > 0
        """,
        "target_table": "public.letter_document_relations",
        "method": "upsert",
        "conflict_columns": ["id"]
    },
    # =========================================================================
    # PHASE 10: OUTGOING LETTER FOLDER DOCUMENTS (Depends on: outgoing_letters, folder_documents)
    # =========================================================================
    {
        "name": "outgoing_letter_folder_documents",
        "sql": """
            SELECT
                SUBSTRING(MD5(CONCAT(doc.ulid_id, '-OLFD')), 1, 26) AS id,
                doc.ulid_id AS folder_document_id,
                doc.ulid_id AS outgoing_letter_id,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS created_at,
                CASE WHEN doc.date > 0 AND doc.date < 4102444800 THEN TO_TIMESTAMP(doc.date) WHEN doc.date >= 4102444800 THEN TO_TIMESTAMP(doc.date / 1000) ELSE TIMESTAMP '2024-01-01 00:00:00' END AS updated_at
            FROM public.tbldocuments doc
            INNER JOIN public.tblfolders folder ON doc.folder = folder.id
            WHERE doc.ulid_id IS NOT NULL
              AND folder.ulid_id IS NOT NULL
              AND doc.folder IS NOT NULL
              AND doc.folder > 0
              AND doc.keluar_nosurat_plain IS NOT NULL
              AND doc.keluar_nosurat_plain != ''
              AND doc.kind IN (2, 3)
        """,
        "target_table": "public.outgoing_letter_folder_documents",
        "method": "upsert",
        "conflict_columns": ["id"],
    },
]


# =============================================================================
# TRANSFORMATION FUNCTIONS
# =============================================================================
def apply_transforms(df, transforms):
    """
    Apply transformation rules to the DataFrame.
    Supported transforms:
    - parse_date:<column>
    - parse_timestamp:<column>
    - parse_unix_timestamp:<column>
    - parse_exp:<source>:<target>
    - is_access_forever:<exp_column>
    - deduplicate_email:<column> - Deduplicate emails with incremental suffix (_2, _3, etc.)
    """
    if not transforms:
        return df

    for transform in transforms:
        parts = transform.split(":")
        transform_type = parts[0]

        if transform_type == "parse_date" and len(parts) >= 2:
            column = parts[1]
            df = parse_date_column(df, column, "date")

        elif transform_type == "parse_timestamp" and len(parts) >= 2:
            column = parts[1]
            df = parse_date_column(df, column, "timestamp")

        elif transform_type == "parse_unix_timestamp" and len(parts) >= 2:
            source_col = parts[1]
            target_col = parts[2] if len(parts) >= 3 else source_col
            if source_col in df.columns:
                df = df.withColumn(
                    target_col,
                    F.when(
                        F.col(source_col).isNotNull() & (F.col(source_col) > 0),
                        F.from_unixtime(F.col(source_col).cast("bigint")),
                    ).otherwise(F.lit("2024-01-01 00:00:00").cast(TimestampType())),
                )
                # Drop source column if different from target
                if source_col != target_col:
                    df = df.drop(source_col)

        elif transform_type == "parse_exp" and len(parts) >= 3:
            source_col = parts[1]
            target_col = parts[2]
            if source_col in df.columns:
                df = df.withColumn(
                    target_col,
                    F.when(
                        F.col(source_col).rlike("^[0-9]+$")
                        & (F.col(source_col).cast("bigint") > 0),
                        F.from_unixtime(F.col(source_col).cast("bigint")),
                    ).otherwise(F.lit(None)),
                )
                # Drop the raw column
                df = df.drop(source_col)

        elif transform_type == "is_access_forever" and len(parts) >= 2:
            exp_col = parts[1]
            df = add_is_access_forever(df, exp_col)
            # Drop the raw exp column
            if exp_col in df.columns:
                df = df.drop(exp_col)

        elif transform_type == "deduplicate_email" and len(parts) >= 2:
            email_col = parts[1]
            df = deduplicate_email_column(df, email_col)

        elif transform_type == "deduplicate_username" and len(parts) >= 2:
            username_col = parts[1]
            df = deduplicate_username_column(df, username_col)

    return df


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source_type", required=True)
    p.add_argument("--source_host", required=True)
    p.add_argument("--source_port", required=True)
    p.add_argument("--source_db", required=True)
    p.add_argument("--source_user", required=True)
    p.add_argument("--source_password", required=True)
    p.add_argument("--target_type", required=True)
    p.add_argument("--target_host", required=True)
    p.add_argument("--target_port", required=True)
    p.add_argument("--target_db", required=True)
    p.add_argument("--target_user", required=True)
    p.add_argument("--target_password", required=True)
    # Performance tuning parameters
    p.add_argument(
        "--batch_size", type=int, default=5000, help="Rows per batch (default: 5000)"
    )
    p.add_argument(
        "--workers", type=int, default=4, help="Parallel workers (default: 4)"
    )
    return p.parse_args()


def main():
    args = parse_args()

    # Print performance config
    print("=" * 60)
    print("🚀 DMS ADHI ETL - Staging to Production")
    print("=" * 60)
    print(f"⚙️  Performance Config:")
    print(f"   - Batch Size: {args.batch_size:,} rows")
    print(f"   - Workers: {args.workers}")
    print("=" * 60)

    spark = SparkSession.builder.appName("DMS-Adhi-Staging-To-Prod-ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    source_cfg = {
        "jdbc_url": build_jdbc_url(
            args.source_type, args.source_host, args.source_port, args.source_db
        ),
        "user": args.source_user,
        "password": args.source_password,
        "driver": {
            "postgres": "org.postgresql.Driver",
            "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "mariadb": "org.mariadb.jdbc.Driver",
            "oracle": "oracle.jdbc.driver.OracleDriver",
        }[args.source_type],
    }

    target_cfg = {
        "jdbc_url": build_jdbc_url(
            args.target_type, args.target_host, args.target_port, args.target_db
        ),
        "user": args.target_user,
        "password": args.target_password,
        "driver": {
            "postgres": "org.postgresql.Driver",
            "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "mariadb": "org.mariadb.jdbc.Driver",
            "oracle": "oracle.jdbc.driver.OracleDriver",
        }[args.target_type],
    }

    summary = {"success": 0, "failed": 0, "total_rows": 0}
    import time

    start_time = time.time()

    # ETL_JOBS is already ordered by execution sequence (Phase 1 → Phase 10)
    # Simply iterate through the jobs in order
    for i, job in enumerate(ETL_JOBS, 1):
        job_start = time.time()
        print(f"\n{'='*60}")
        print(f"▶ [{i}/{len(ETL_JOBS)}] {job['name']}")
        print(f"{'='*60}")

        try:
            # Check if this job should read from target instead of source
            if job.get("source_is_target", False):
                print(f"  📖 Reading from TARGET database (production)")
                df = jdbc_read(spark, target_cfg, job["sql"])
            else:
                df = jdbc_read(spark, source_cfg, job["sql"])

            # Apply Python/Spark transformations if specified
            if "transform" in job:
                print(f"  🔄 Applying transforms: {job['transform']}")
                df = apply_transforms(df, job["transform"])

            # FK Handling: Set invalid FK values to NULL (instead of filtering)
            # Supports both single filter_fk and list filter_fk_list
            fk_configs = []
            if "filter_fk" in job:
                fk_configs = [job["filter_fk"]]
            elif "filter_fk_list" in job:
                fk_configs = job["filter_fk_list"]

            for fk_config in fk_configs:
                fk_column = fk_config["column"]
                fk_target_table = fk_config["target_table"]
                fk_target_column = fk_config["target_column"]

                print(
                    f"  🔗 Validating FK: {fk_column} → {fk_target_table}.{fk_target_column}"
                )

                try:
                    # Read existing IDs from target table
                    temp_cfg = target_cfg.copy()
                    temp_cfg["table"] = fk_target_table
                    fk_query = f"SELECT {fk_target_column} FROM {fk_target_table} WHERE {fk_target_column} IS NOT NULL"
                    df_fk_ids = jdbc_read(spark, temp_cfg, fk_query)

                    # Get valid IDs
                    valid_ids = set(
                        [row[fk_target_column] for row in df_fk_ids.collect()]
                    )
                    print(f"    Found {len(valid_ids):,} valid FK references in target")

                    # Count invalid FK values before setting to NULL
                    invalid_count = df.filter(
                        F.col(fk_column).isNotNull()
                        & ~F.col(fk_column).isin(list(valid_ids))
                    ).count()

                    # SET NULL for invalid FK values (instead of filtering)
                    # This keeps ALL rows, but sets invalid FK to NULL
                    df = df.withColumn(
                        fk_column,
                        F.when(
                            F.col(fk_column).isNull()
                            | F.col(fk_column).isin(list(valid_ids)),
                            F.col(fk_column),
                        ).otherwise(F.lit(None)),
                    )

                    valid_count = df.filter(F.col(fk_column).isNotNull()).count()
                    print(
                        f"    ✅ Valid FK: {valid_count:,} rows | ⚠️ Set to NULL (invalid FK): {invalid_count:,} rows"
                    )

                except Exception as e:
                    print(f"    ⚠️ FK validation failed: {e}")
                    print(f"    Continuing without FK validation...")

            target_cfg["table"] = job["target_table"]

            # Get job-specific overrides or use global config
            job_batch_size = job.get("batch_size", args.batch_size)
            job_workers = job.get("workers", args.workers)

            stats, data_success, data_failed = run_method(
                spark,
                df,
                target_cfg,
                job["method"],
                job.get("conflict_columns"),
                batch_size=job_batch_size,
                workers=job_workers,
                transaction=job.get("transaction", False),
            )

            job_time = time.time() - job_start
            rows_processed = stats.get("processed", 0) or 0

            print(f"  📊 Stats: {stats}")
            print(f"  ✅ Success: {len(data_success):,} rows")
            print(f"  ❌ Failed: {len(data_failed):,} rows")
            print(
                f"  ⏱️  Time: {job_time:.2f}s ({rows_processed/max(job_time, 0.1):.0f} rows/sec)"
            )

            if len(data_failed) > 0:
                print(f"  ⚠️  Sample Error: {data_failed[0].get('_error')}")

            summary["success"] += 1
            summary["total_rows"] += rows_processed

        except Exception as e:
            # Check if this job is optional (can be skipped on error)
            if job.get("optional", False):
                print(f"⚠️  {job['name']} skipped (optional): {e}")
                summary["skipped"] = summary.get("skipped", 0) + 1
                continue
            else:
                print(f"❌ {job['name']} failed: {e}")
                summary["failed"] += 1
                spark.stop()
                sys.exit(1)

    total_time = time.time() - start_time
    spark.stop()

    print("\n" + "=" * 60)
    print("📋 FINAL SUMMARY")
    print("=" * 60)
    print(f"  ✅ Jobs Successful: {summary['success']}")
    print(f"  ⚠️  Jobs Skipped: {summary.get('skipped', 0)}")
    print(f"  ❌ Jobs Failed: {summary['failed']}")
    print(f"  📊 Total Rows Processed: {summary['total_rows']:,}")
    print(f"  ⏱️  Total Time: {total_time:.2f}s")
    print(
        f"  🚀 Average Throughput: {summary['total_rows']/max(total_time, 0.1):.0f} rows/sec"
    )
    print("=" * 60)


if __name__ == "__main__":
    main()
