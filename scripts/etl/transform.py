"""
ETL Transform Script
====================
Purpose: Transform extracted data for data mart loading
- Clean data
- Join related tables
- Create surrogate keys
- Normalize and standardize values
- Calculate derived fields

Date: 2026-04-07
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, Tuple
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)


class DataTransformer:
    """Transform extracted data for data mart"""
    
    def __init__(self, extracted_data: Dict[str, pd.DataFrame]):
        """
        Initialize with extracted data
        
        Args:
            extracted_data: Dictionary of extracted DataFrames
        """
        self.extracted_data = extracted_data
        self.transformed_data = {}
        
    def transform_dim_material(self) -> pd.DataFrame:
        """
        Transform master materials to dim_material
        
        Returns:
            Transformed DataFrame for dim_material
        """
        logger.info("Transforming dim_material...")
        
        df = self.extracted_data['master_materials'].copy()
        
        # Create category based on is_vaccine flag
        df['category'] = df['is_vaccine'].apply(
            lambda x: 'Vaccine' if x == 1 else 'Non-Vaccine'
        )
        
        # Ensure boolean values
        df['is_vaccine'] = df['is_vaccine'].apply(lambda x: bool(x))
        df['is_active'] = df['material_status'].apply(lambda x: x == 1)
        
        # Select and rename columns for dimension table
        dim_material = df[[
            'material_id', 'material_code', 'material_name', 
            'unit_of_distribution', 'unit', 'pieces_per_unit',
            'temperature_sensitive', 'temperature_min', 'temperature_max',
            'is_vaccine', 'is_active', 'material_created_at', 'material_updated_at'
        ]].copy()
        
        # Rename columns to match dimension schema
        dim_material = dim_material.rename(columns={
            'material_created_at': 'source_created_at',
            'material_updated_at': 'source_updated_at'
        })
        
        # Handle NULL values
        dim_material['material_name'] = dim_material['material_name'].fillna('Unknown')
        dim_material['material_code'] = dim_material['material_code'].fillna('')
        dim_material['unit'] = dim_material['unit'].fillna('')
        
        logger.info(f"Transformed {len(dim_material)} material records")
        self.transformed_data['dim_material'] = dim_material
        return dim_material
    
    def transform_dim_entity(self) -> pd.DataFrame:
        """
        Transform entities to dim_entity
        
        Returns:
            Transformed DataFrame for dim_entity
        """
        logger.info("Transforming dim_entity...")
        
        df = self.extracted_data['entities'].copy()
        
        # Determine entity type description
        def get_entity_type_desc(type_code):
            type_map = {
                1: 'Dinas Kesehatan Provinsi',
                2: 'Dinas Kesehatan Kabupaten/Kota',
                3: 'Puskesmas',
                4: 'Rumah Sakit',
                5: 'Klinik',
                6: 'Apotek',
                7: 'Laboratorium'
            }
            return type_map.get(type_code, 'Lainnya')
        
        df['entity_type'] = df['entity_type'].apply(get_entity_type_desc)
        df['is_active'] = df['entity_status'].apply(lambda x: x == 1)
        
        # Select and rename columns
        dim_entity = df[[
            'entity_id', 'entity_code', 'entity_name', 'entity_type',
            'type', 'status', 'province_id', 'regency_id', 'address',
            'lat', 'lng', 'postal_code', 'country', 'is_puskesmas',
            'is_active', 'entity_created_at', 'entity_updated_at'
        ]].copy()
        
        # Rename columns
        dim_entity = dim_entity.rename(columns={
            'entity_created_at': 'source_created_at',
            'entity_updated_at': 'source_updated_at'
        })
        
        # Handle NULL values
        dim_entity['entity_name'] = dim_entity['entity_name'].fillna('Unknown Facility')
        dim_entity['entity_code'] = dim_entity['entity_code'].fillna('')
        dim_entity['province_id'] = dim_entity['province_id'].fillna('')
        dim_entity['regency_id'] = dim_entity['regency_id'].fillna('')
        
        logger.info(f"Transformed {len(dim_entity)} entity records")
        self.transformed_data['dim_entity'] = dim_entity
        return dim_entity
    
    def transform_dim_location(self) -> pd.DataFrame:
        """
        Transform provinces and regencies to dim_location
        
        Returns:
            Transformed DataFrame for dim_location
        """
        logger.info("Transforming dim_location...")
        
        provinces = self.extracted_data['provinces'].copy()
        regencies = self.extracted_data['regencies'].copy()
        
        # Merge provinces with regencies
        dim_location = regencies.merge(
            provinces,
            on='province_id',
            how='left',
            suffixes=('_regency', '_province')
        )
        
        # Create full location string
        dim_location['full_location'] = dim_location.apply(
            lambda row: f"{row['province_name']} - {row['regency_name']}" 
            if pd.notna(row['province_name']) and pd.notna(row['regency_name'])
            else row.get('regency_name', row.get('province_name', 'Unknown')),
            axis=1
        )
        
        # Select and rename columns
        dim_location = dim_location[[
            'province_id', 'province_name', 'province_lat',
            'regency_id', 'regency_name', 'regency_lat', 'regency_lng',
            'full_location'
        ]].copy()
        
        # Rename for clarity
        dim_location = dim_location.rename(columns={
            'province_lat': 'lat',
            'regency_lat': 'regency_lat',
            'regency_lng': 'lng'
        })
        
        # Handle NULL values
        dim_location['province_name'] = dim_location['province_name'].fillna('Unknown Province')
        dim_location['regency_name'] = dim_location['regency_name'].fillna('Unknown Regency')
        
        # Remove duplicates
        dim_location = dim_location.drop_duplicates(subset=['province_id', 'regency_id'])
        
        logger.info(f"Transformed {len(dim_location)} location records")
        self.transformed_data['dim_location'] = dim_location
        return dim_location
    
    def transform_dim_activity(self) -> pd.DataFrame:
        """
        Transform master_activities to dim_activity
        
        Returns:
            Transformed DataFrame for dim_activity
        """
        logger.info("Transforming dim_activity...")
        
        df = self.extracted_data['master_activities'].copy()
        
        # Activity is active if not deleted
        df['is_active'] = True
        
        # Select and rename columns
        dim_activity = df[[
            'activity_id', 'activity_code', 'activity_name',
            'is_ordered_sales', 'is_ordered_purchase', 'is_patient_id',
            'is_active', 'activity_created_at', 'activity_updated_at'
        ]].copy()
        
        # Rename columns
        dim_activity = dim_activity.rename(columns={
            'activity_created_at': 'source_created_at',
            'activity_updated_at': 'source_updated_at'
        })
        
        # Handle NULL values
        dim_activity['activity_name'] = dim_activity['activity_name'].fillna('Unknown Activity')
        dim_activity['activity_code'] = dim_activity['activity_code'].fillna('')
        
        logger.info(f"Transformed {len(dim_activity)} activity records")
        self.transformed_data['dim_activity'] = dim_activity
        return dim_activity
    
    def transform_dim_entity_tag(self) -> pd.DataFrame:
        """
        Transform entity_tags to dim_entity_tag
        
        Returns:
            Transformed DataFrame for dim_entity_tag
        """
        logger.info("Transforming dim_entity_tag...")
        
        df = self.extracted_data['entity_tags'].copy()
        
        # Create tag category based on tag name
        def categorize_tag(tag_name):
            tag_name_lower = tag_name.lower() if tag_name else ''
            
            if 'puskesmas' in tag_name_lower:
                return 'Puskesmas'
            elif 'dinkes provinsi' in tag_name_lower or 'dinas kesehatan provinsi' in tag_name_lower:
                return 'Dinas Kesehatan Provinsi'
            elif 'dinkes kab' in tag_name_lower or 'dinas kesehatan kabupaten' in tag_name_lower:
                return 'Dinas Kesehatan Kabupaten'
            elif 'rumah sakit' in tag_name_lower or 'rs' in tag_name_lower:
                return 'Rumah Sakit'
            elif 'klinik' in tag_name_lower:
                return 'Klinik'
            else:
                return 'Lainnya'
        
        df['tag_category'] = df['tag_name'].apply(categorize_tag)
        df['is_active'] = True
        
        # Select and rename columns
        dim_entity_tag = df[[
            'tag_id', 'tag_name', 'tag_category', 'is_active',
            'tag_created_at', 'tag_updated_at'
        ]].copy()
        
        # Rename columns
        dim_entity_tag = dim_entity_tag.rename(columns={
            'tag_created_at': 'source_created_at',
            'tag_updated_at': 'source_updated_at'
        })
        
        # Handle NULL values
        dim_entity_tag['tag_name'] = dim_entity_tag['tag_name'].fillna('Unknown Tag')
        
        logger.info(f"Transformed {len(dim_entity_tag)} entity tag records")
        self.transformed_data['dim_entity_tag'] = dim_entity_tag
        return dim_entity_tag
    
    def transform_fact_stock(self) -> pd.DataFrame:
        """
        Transform stocks to fact_stock with all dimension mappings
        
        Returns:
            Transformed DataFrame for fact_stock
        """
        logger.info("Transforming fact_stock...")
        
        # Get extracted data
        stocks = self.extracted_data['stocks'].copy()
        batches = self.extracted_data['batches'].copy()
        entity_materials = self.extracted_data['entity_has_master_materials'].copy()
        
        # Merge stocks with batches
        logger.info("Merging stocks with batches...")
        stocks_with_batches = stocks.merge(
            batches,
            on='batch_id',
            how='left'
        )
        
        # Merge with entity-material mapping to get entity_id and material_id
        logger.info("Merging with entity-material mapping...")
        stocks_full = stocks_with_batches.merge(
            entity_materials,
            left_on='entity_has_material_id',
            right_on='entity_material_id',
            how='left'
        )
        
        # Extract date_key from created_at
        stocks_full['created_at_date'] = pd.to_datetime(stocks_full['created_at']).dt.date
        stocks_full['date_key'] = pd.to_datetime(stocks_full['created_at']).dt.strftime('%Y%m%d').astype(int)
        
        # Calculate total price if missing
        stocks_full['total_price'] = stocks_full.apply(
            lambda row: row['stock_quantity'] * row['price'] 
            if pd.isna(row['total_price']) and pd.notna(row['price'])
            else row['total_price'],
            axis=1
        )
        
        # Select columns for fact table
        fact_stock = stocks_full[[
            'stock_id', 'batch_id', 'batch_number', 'stock_quantity',
            'unit', 'allocated', 'in_transit', 'open_vial',
            'extermination_discard_qty', 'extermination_received_qty',
            'extermination_qty', 'extermination_shipped_qty',
            'expiry_date', 'budget_source', 'stock_year', 'price', 'total_price',
            'entity_id', 'material_id', 'activity_id',
            'date_key', 'source_created_at'
        ]].copy()
        
        # Handle NULL values
        fact_stock['stock_quantity'] = fact_stock['stock_quantity'].fillna(0)
        fact_stock['allocated'] = fact_stock['allocated'].fillna(0)
        fact_stock['in_transit'] = fact_stock['in_transit'].fillna(0)
        fact_stock['batch_number'] = fact_stock['batch_number'].fillna('')
        
        logger.info(f"Transformed {len(fact_stock)} stock records")
        self.transformed_data['fact_stock'] = fact_stock
        return fact_stock
    
    def create_entity_tag_mapping(self) -> pd.DataFrame:
        """
        Create mapping between entity_id and tag_id
        
        Returns:
            DataFrame with entity_id and tag_id mapping
        """
        logger.info("Creating entity-tag mapping...")
        
        entity_tags = self.extracted_data['entity_entity_tags'].copy()
        
        # One entity can have multiple tags, we'll use the first tag for simplicity
        # In production, you might want to handle this differently
        entity_tag_mapping = entity_tags.groupby('entity_id')['tag_id'].first().reset_index()
        entity_tag_mapping = entity_tag_mapping.rename(columns={'tag_id': 'tag_id'})
        
        logger.info(f"Created entity-tag mapping for {len(entity_tag_mapping)} entities")
        self.transformed_data['entity_tag_mapping'] = entity_tag_mapping
        return entity_tag_mapping
    
    def transform_all(self) -> Dict[str, pd.DataFrame]:
        """
        Transform all extracted data
        
        Returns:
            Dictionary with transformed DataFrames
        """
        logger.info("="*60)
        logger.info("STARTING TRANSFORMATION")
        logger.info("="*60)
        
        try:
            # Transform dimensions
            self.transform_dim_material()
            self.transform_dim_entity()
            self.transform_dim_location()
            self.transform_dim_activity()
            self.transform_dim_entity_tag()
            
            # Create mappings
            self.create_entity_tag_mapping()
            
            # Transform fact table
            self.transform_fact_stock()
            
            logger.info("="*60)
            logger.info("TRANSFORMATION COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            
            # Print summary
            logger.info("\nTransformation Summary:")
            for table_name, df in self.transformed_data.items():
                logger.info(f"  {table_name}: {len(df)} rows, {len(df.columns)} columns")
            
            return self.transformed_data
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
