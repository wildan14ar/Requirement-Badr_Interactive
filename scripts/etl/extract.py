"""
ETL Extract Script
==================
Purpose: Extract data from source database (recruitment_dev)
Tables: stocks, batches, master_materials, entities, entity_tags, 
        entity_entity_tags, entity_has_master_materials, 
        entity_master_material_activities, master_activities,
        provinces, regencies

Date: 2026-04-07
"""

import os
import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_extract.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Extract data from source database"""
    
    def __init__(self):
        """Initialize database connection"""
        self.source_engine = self._create_source_engine()
        self.extracted_data = {}
        
    def _create_source_engine(self):
        """Create SQLAlchemy engine for source database"""
        host = os.getenv('SOURCE_DB_HOST', '10.10.0.30')
        port = os.getenv('SOURCE_DB_PORT', '3306')
        database = os.getenv('SOURCE_DB_NAME', 'recruitment_dev')
        username = os.getenv('SOURCE_DB_USER', 'devel')
        password = os.getenv('SOURCE_DB_PASS', 'recruitment2024')
        
        connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
        
        try:
            engine = create_engine(
                connection_string,
                pool_pre_ping=True,
                pool_recycle=3600,
                connect_args={
                    'connect_timeout': 30,
                    'read_timeout': 300,
                    'write_timeout': 300
                }
            )
            logger.info(f"Successfully connected to source database: {host}:{port}/{database}")
            return engine
        except Exception as e:
            logger.error(f"Failed to connect to source database: {e}")
            raise
    
    def extract_table(self, table_name: str, columns: List[str] = None, 
                     conditions: str = None, limit: int = None) -> pd.DataFrame:
        """
        Extract data from a single table
        
        Args:
            table_name: Name of the table to extract
            columns: List of columns to select (None for all)
            conditions: WHERE clause conditions
            limit: Maximum number of rows to extract
            
        Returns:
            DataFrame with extracted data
        """
        try:
            # Build query
            select_cols = ", ".join(columns) if columns else "*"
            query = f"SELECT {select_cols} FROM {table_name}"
            
            # Add conditions if provided
            if conditions:
                query += f" WHERE {conditions}"
            
            # Add limit if provided
            if limit:
                query += f" LIMIT {limit}"
            
            logger.info(f"Extracting from {table_name}...")
            
            # Execute query
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} rows from {table_name}")
            
            # Store in extracted_data dict
            self.extracted_data[table_name] = df
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract from {table_name}: {e}")
            raise
    
    def extract_stocks(self) -> pd.DataFrame:
        """Extract stock data with related information"""
        logger.info("="*60)
        logger.info("Extracting stocks table")
        logger.info("="*60)
        
        query = """
        SELECT 
            s.id as stock_id,
            s.batch_id,
            s.status,
            s.qty as stock_quantity,
            s.created_by,
            s.updated_by,
            s.createdAt as created_at,
            s.updatedAt as updated_at,
            s.material_entity_id,
            s.allocated,
            s.in_transit,
            s.entity_has_material_id as entity_has_material_id,
            s.activity_id,
            s.open_vial,
            s.extermination_discard_qty,
            s.extermination_received_qty,
            s.extermination_qty,
            s.extermination_shipped_qty,
            s.budget_source,
            s.year as stock_year,
            s.price,
            s.total_price
        FROM stocks s
        WHERE s.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} stock records")
            self.extracted_data['stocks'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract stocks: {e}")
            raise
    
    def extract_batches(self) -> pd.DataFrame:
        """Extract batch data"""
        logger.info("="*60)
        logger.info("Extracting batches table")
        logger.info("="*60)
        
        query = """
        SELECT 
            b.id as batch_id,
            b.code as batch_number,
            b.expired_date,
            b.production_date,
            b.manufacture_id,
            b.status as batch_status,
            b.created_at as batch_created_at,
            b.updated_at as batch_updated_at
        FROM batches b
        WHERE b.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} batch records")
            self.extracted_data['batches'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract batches: {e}")
            raise
    
    def extract_master_materials(self) -> pd.DataFrame:
        """Extract master materials data"""
        logger.info("="*60)
        logger.info("Extracting master_materials table")
        logger.info("="*60)
        
        query = """
        SELECT 
            mm.id as material_id,
            mm.name as material_name,
            mm.code as material_code,
            mm.unit_of_distribution,
            mm.unit,
            mm.pieces_per_unit,
            mm.temperature_sensitive,
            mm.temperature_min,
            mm.temperature_max,
            mm.is_vaccine,
            mm.status as material_status,
            mm.managed_in_batch,
            mm.description,
            mm.kfa_code,
            mm.created_at as material_created_at,
            mm.updated_at as material_updated_at
        FROM master_materials mm
        WHERE mm.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} material records")
            self.extracted_data['master_materials'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract master_materials: {e}")
            raise
    
    def extract_entities(self) -> pd.DataFrame:
        """Extract entity (healthcare facility) data"""
        logger.info("="*60)
        logger.info("Extracting entities table")
        logger.info("="*60)
        
        query = """
        SELECT 
            e.id as entity_id,
            e.name as entity_name,
            e.code as entity_code,
            e.type as entity_type,
            e.status as entity_status,
            e.province_id,
            e.regency_id,
            e.sub_district_id,
            e.address,
            e.lat,
            e.lng,
            e.postal_code,
            e.country,
            e.is_puskesmas,
            e.is_vendor,
            e.created_at as entity_created_at,
            e.updated_at as entity_updated_at
        FROM entities e
        WHERE e.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} entity records")
            self.extracted_data['entities'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract entities: {e}")
            raise
    
    def extract_entity_tags(self) -> pd.DataFrame:
        """Extract entity tags"""
        logger.info("="*60)
        logger.info("Extracting entity_tags table")
        logger.info("="*60)
        
        query = """
        SELECT 
            et.id as tag_id,
            et.title as tag_name,
            et.created_at as tag_created_at,
            et.updated_at as tag_updated_at
        FROM entity_tags et
        WHERE et.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} entity tag records")
            self.extracted_data['entity_tags'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract entity_tags: {e}")
            raise
    
    def extract_entity_entity_tags(self) -> pd.DataFrame:
        """Extract entity-entity tag mapping (pivot table)"""
        logger.info("="*60)
        logger.info("Extracting entity_entity_tags table")
        logger.info("="*60)
        
        query = """
        SELECT 
            eet.id,
            eet.entity_id,
            eet.entity_tag_id as tag_id,
            eet.created_at,
            eet.updated_at
        FROM entity_entity_tags eet
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} entity-entity-tag mapping records")
            self.extracted_data['entity_entity_tags'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract entity_entity_tags: {e}")
            raise
    
    def extract_entity_has_master_materials(self) -> pd.DataFrame:
        """Extract entity-material mapping"""
        logger.info("="*60)
        logger.info("Extracting entity_has_master_materials table")
        logger.info("="*60)
        
        query = """
        SELECT 
            ehmm.id as entity_material_id,
            ehmm.entity_id,
            ehmm.master_material_id as material_id,
            ehmm.min,
            ehmm.max,
            ehmm.on_hand_stock,
            ehmm.stock_last_update,
            ehmm.allocated_stock,
            ehmm.total_open_vial,
            ehmm.created_at,
            ehmm.updated_at
        FROM entity_has_master_materials ehmm
        WHERE ehmm.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} entity-material mapping records")
            self.extracted_data['entity_has_master_materials'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract entity_has_master_materials: {e}")
            raise
    
    def extract_master_activities(self) -> pd.DataFrame:
        """Extract activities data"""
        logger.info("="*60)
        logger.info("Extracting master_activities table")
        logger.info("="*60)
        
        query = """
        SELECT 
            ma.id as activity_id,
            ma.name as activity_name,
            ma.code as activity_code,
            ma.is_ordered_sales,
            ma.is_ordered_purchase,
            ma.is_patient_id,
            ma.created_at as activity_created_at,
            ma.updated_at as activity_updated_at
        FROM master_activities ma
        WHERE ma.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} activity records")
            self.extracted_data['master_activities'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract master_activities: {e}")
            raise
    
    def extract_provinces(self) -> pd.DataFrame:
        """Extract provinces data"""
        logger.info("="*60)
        logger.info("Extracting provinces table")
        logger.info("="*60)
        
        query = """
        SELECT 
            p.id as province_id,
            p.name as province_name,
            p.lat as province_lat,
            p.lng as province_lng,
            p.province_id_old,
            p.province_id_new,
            p.created_at as province_created_at,
            p.updated_at as province_updated_at
        FROM provinces p
        WHERE p.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} province records")
            self.extracted_data['provinces'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract provinces: {e}")
            raise
    
    def extract_regencies(self) -> pd.DataFrame:
        """Extract regencies data"""
        logger.info("="*60)
        logger.info("Extracting regencies table")
        logger.info("="*60)
        
        query = """
        SELECT 
            r.id as regency_id,
            r.name as regency_name,
            r.province_id,
            r.lat as regency_lat,
            r.lng as regency_lng,
            r.regency_id_old,
            r.regency_id_new,
            r.province_id_old,
            r.province_id_new,
            r.created_at as regency_created_at,
            r.updated_at as regency_updated_at
        FROM regencies r
        WHERE r.deleted_at IS NULL
        """
        
        try:
            with self.source_engine.connect() as conn:
                df = pd.read_sql(text(query), conn)
            
            logger.info(f"Extracted {len(df)} regency records")
            self.extracted_data['regencies'] = df
            return df
        except Exception as e:
            logger.error(f"Failed to extract regencies: {e}")
            raise
    
    def extract_all(self) -> Dict[str, pd.DataFrame]:
        """
        Extract all required tables
        
        Returns:
            Dictionary with table names as keys and DataFrames as values
        """
        logger.info("="*60)
        logger.info("STARTING FULL EXTRACTION")
        logger.info("="*60)
        
        try:
            # Extract all tables
            self.extract_stocks()
            self.extract_batches()
            self.extract_master_materials()
            self.extract_entities()
            self.extract_entity_tags()
            self.extract_entity_entity_tags()
            self.extract_entity_has_master_materials()
            self.extract_master_activities()
            self.extract_provinces()
            self.extract_regencies()
            
            logger.info("="*60)
            logger.info("EXTRACTION COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            
            # Print summary
            logger.info("\nExtraction Summary:")
            for table_name, df in self.extracted_data.items():
                logger.info(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
            
            return self.extracted_data
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def close(self):
        """Close database connections"""
        if self.source_engine:
            self.source_engine.dispose()
            logger.info("Database connections closed")


if __name__ == "__main__":
    # Run extraction
    extractor = DataExtractor()
    
    try:
        extracted_data = extractor.extract_all()
        logger.info(f"\nExtracted {len(extracted_data)} tables successfully")
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        extractor.close()
