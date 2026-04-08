"""
ETL Load Script - ClickHouse
============================
Purpose: Load transformed data into ClickHouse data mart
- Load dimension tables first (independent)
- Load fact table with foreign key references
- Use ClickHouse batch inserts for performance
- Validate loaded data

Date: 2026-04-07
"""

import os
import logging
import pandas as pd
from typing import Dict, Tuple, List
from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_load.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ClickHouseLoader:
    """Load transformed data into ClickHouse data mart"""
    
    def __init__(self):
        """Initialize ClickHouse connection"""
        self.client = self._create_client()
        
    def _create_client(self) -> Client:
        """Create ClickHouse client"""
        host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        port = int(os.getenv('CLICKHOUSE_PORT', '9000'))
        database = os.getenv('CLICKHOUSE_DB', 'datamart_badr_interactive')
        username = os.getenv('CLICKHOUSE_USER', 'default')
        password = os.getenv('CLICKHOUSE_PASSWORD', '')
        
        try:
            client = Client(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                settings={
                    'insert_block_size': 100000,
                    'max_threads': 4
                }
            )
            
            # Test connection
            client.execute('SELECT 1')
            logger.info(f"Successfully connected to ClickHouse: {host}:{port}/{database}")
            return client
            
        except ClickHouseError as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def load_table(self, df: pd.DataFrame, table_name: str, 
                   batch_size: int = 100000) -> int:
        """
        Load DataFrame into ClickHouse table
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            batch_size: Number of records per batch
            
        Returns:
            Number of records loaded
        """
        logger.info(f"Loading {len(df)} records to {table_name}...")
        
        try:
            # Convert DataFrame to list of tuples
            # Replace NaN with None for ClickHouse
            records = df.where(pd.notnull(df), None).values.tolist()
            columns = df.columns.tolist()
            
            # Convert numpy types to Python types
            cleaned_records = []
            for record in tqdm(records, desc=f"Processing {table_name}", leave=False):
                cleaned_record = []
                for val in record:
                    if isinstance(val, (pd.Timestamp,)):
                        cleaned_record.append(val.to_pydatetime())
                    elif isinstance(val, (pd.NaT, type(None))):
                        cleaned_record.append(None)
                    elif hasattr(val, 'item'):  # numpy types
                        cleaned_record.append(val.item())
                    else:
                        cleaned_record.append(val)
                cleaned_records.append(cleaned_record)
            
            # Insert in batches
            total_inserted = 0
            for i in range(0, len(cleaned_records), batch_size):
                batch = cleaned_records[i:i + batch_size]
                
                # Build INSERT query
                columns_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                query = f"INSERT INTO {table_name} ({columns_str}) VALUES"
                
                # Execute insert
                self.client.execute(
                    query,
                    batch,
                    types_check=True
                )
                
                total_inserted += len(batch)
                logger.info(f"  Inserted batch {i//batch_size + 1}: {len(batch)} records")
            
            logger.info(f"✓ Successfully loaded {total_inserted} records to {table_name}")
            return total_inserted
            
        except ClickHouseError as e:
            logger.error(f"Failed to load {table_name}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error loading {table_name}: {e}")
            raise
    
    def get_existing_keys(self, table_name: str, key_column: str) -> set:
        """
        Get existing keys from table to avoid duplicates
        
        Args:
            table_name: Table name
            key_column: Column name for key
            
        Returns:
            Set of existing keys
        """
        try:
            query = f"SELECT {key_column} FROM {table_name}"
            result = self.client.execute(query)
            return set(row[0] for row in result)
        except ClickHouseError as e:
            logger.warning(f"Could not fetch existing keys from {table_name}: {e}")
            return set()
    
    def load_dim_material(self, df: pd.DataFrame) -> int:
        """Load material dimension table"""
        logger.info("="*60)
        logger.info("Loading dim_material")
        logger.info("="*60)
        
        # Get existing material_ids to avoid duplicates
        existing_keys = self.get_existing_keys('dim_material', 'material_id')
        
        # Filter out existing records
        if existing_keys:
            df_new = df[~df['material_id'].isin(existing_keys)]
        else:
            df_new = df.copy()
        
        logger.info(f"  New records: {len(df_new)}, Existing: {len(existing_keys)}")
        
        if len(df_new) == 0:
            logger.info("  No new records to load")
            return 0
        
        return self.load_table(df_new, 'dim_material')
    
    def load_dim_entity(self, df: pd.DataFrame) -> int:
        """Load entity dimension table"""
        logger.info("="*60)
        logger.info("Loading dim_entity")
        logger.info("="*60)
        
        existing_keys = self.get_existing_keys('dim_entity', 'entity_id')
        
        if existing_keys:
            df_new = df[~df['entity_id'].isin(existing_keys)]
        else:
            df_new = df.copy()
        
        logger.info(f"  New records: {len(df_new)}, Existing: {len(existing_keys)}")
        
        if len(df_new) == 0:
            return 0
        
        return self.load_table(df_new, 'dim_entity')
    
    def load_dim_location(self, df: pd.DataFrame) -> int:
        """Load location dimension table"""
        logger.info("="*60)
        logger.info("Loading dim_location")
        logger.info("="*60)
        
        # For location, check composite key (province_id, regency_id)
        try:
            existing = self.client.execute(
                "SELECT province_id, regency_id FROM dim_location"
            )
            existing_keys = set((row[0], row[1]) for row in existing)
        except ClickHouseError:
            existing_keys = set()
        
        # Filter new records
        if existing_keys:
            df_new = df[
                ~df[['province_id', 'regency_id']].apply(tuple, axis=1).isin(existing_keys)
            ]
        else:
            df_new = df.copy()
        
        logger.info(f"  New records: {len(df_new)}, Existing: {len(existing_keys)}")
        
        if len(df_new) == 0:
            return 0
        
        return self.load_table(df_new, 'dim_location')
    
    def load_dim_activity(self, df: pd.DataFrame) -> int:
        """Load activity dimension table"""
        logger.info("="*60)
        logger.info("Loading dim_activity")
        logger.info("="*60)
        
        existing_keys = self.get_existing_keys('dim_activity', 'activity_id')
        
        if existing_keys:
            df_new = df[~df['activity_id'].isin(existing_keys)]
        else:
            df_new = df.copy()
        
        logger.info(f"  New records: {len(df_new)}, Existing: {len(existing_keys)}")
        
        if len(df_new) == 0:
            return 0
        
        return self.load_table(df_new, 'dim_activity')
    
    def load_dim_entity_tag(self, df: pd.DataFrame) -> int:
        """Load entity tag dimension table"""
        logger.info("="*60)
        logger.info("Loading dim_entity_tag")
        logger.info("="*60)
        
        existing_keys = self.get_existing_keys('dim_entity_tag', 'tag_id')
        
        if existing_keys:
            df_new = df[~df['tag_id'].isin(existing_keys)]
        else:
            df_new = df.copy()
        
        logger.info(f"  New records: {len(df_new)}, Existing: {len(existing_keys)}")
        
        if len(df_new) == 0:
            return 0
        
        return self.load_table(df_new, 'dim_entity_tag')
    
    def load_fact_stock(self, df: pd.DataFrame) -> int:
        """
        Load fact_stock table
        
        Args:
            df: Transformed fact stock DataFrame
            
        Returns:
            Number of records loaded
        """
        logger.info("="*60)
        logger.info("Loading fact_stock")
        logger.info("="*60)
        
        # Remove records with missing required keys
        required_cols = ['material_key', 'entity_key', 'date_key']
        df_clean = df.dropna(subset=required_cols)
        
        logger.info(f"  Valid records: {len(df_clean)} (removed {len(df) - len(df_clean)} invalid)")
        
        if len(df_clean) == 0:
            logger.warning("  No valid records to load")
            return 0
        
        return self.load_table(df_clean, 'fact_stock')
    
    def validate_load(self) -> Dict[str, int]:
        """
        Validate loaded data
        
        Returns:
            Dictionary with table counts
        """
        logger.info("="*60)
        logger.info("Validating loaded data")
        logger.info("="*60)
        
        table_counts = {}
        tables = [
            'dim_date', 'dim_material', 'dim_entity', 'dim_location',
            'dim_activity', 'dim_entity_tag', 'fact_stock'
        ]
        
        try:
            for table in tables:
                query = f"SELECT count() FROM {table}"
                result = self.client.execute(query)
                count = result[0][0]
                table_counts[table] = count
                logger.info(f"  {table}: {count:,} records")
            
            return table_counts
            
        except ClickHouseError as e:
            logger.error(f"Validation failed: {e}")
            raise
    
    def load_all(self, transformed_data: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """
        Load all transformed data into ClickHouse
        
        Args:
            transformed_data: Dictionary of transformed DataFrames
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("="*60)
        logger.info("STARTING DATA LOAD TO CLICKHOUSE")
        logger.info("="*60)
        
        try:
            load_stats = {}
            
            # Load dimensions first (in order of dependencies)
            load_stats['dim_location'] = self.load_dim_location(transformed_data['dim_location'])
            load_stats['dim_material'] = self.load_dim_material(transformed_data['dim_material'])
            load_stats['dim_entity'] = self.load_dim_entity(transformed_data['dim_entity'])
            load_stats['dim_activity'] = self.load_dim_activity(transformed_data['dim_activity'])
            load_stats['dim_entity_tag'] = self.load_dim_entity_tag(transformed_data['dim_entity_tag'])
            
            # Load fact table
            # Note: For ClickHouse, we need to map source IDs to surrogate keys
            # This should be done in the transform phase or here before loading
            if 'fact_stock_with_keys' in transformed_data:
                load_stats['fact_stock'] = self.load_fact_stock(transformed_data['fact_stock_with_keys'])
            else:
                load_stats['fact_stock'] = self.load_fact_stock(transformed_data['fact_stock'])
            
            logger.info("="*60)
            logger.info("DATA LOAD COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            
            # Print summary
            logger.info("\nLoad Summary:")
            for table, count in load_stats.items():
                logger.info(f"  {table}: {count:,} records loaded")
            
            # Validate
            logger.info("\nValidation Results:")
            validation = self.validate_load()
            
            return load_stats
            
        except Exception as e:
            logger.error(f"Data load failed: {e}")
            raise
    
    def close(self):
        """Close ClickHouse connection"""
        if self.client:
            self.client.disconnect()
            logger.info("ClickHouse connection closed")


if __name__ == "__main__":
    logger.info("This script should be called from main.py orchestrator")
    logger.info("Do not run this script directly")
