"""
ETL Main Orchestrator
=====================
Purpose: Orchestrate the complete ETL pipeline
- Extract data from source database
- Transform data for data mart
- Load data into data mart
- Validate results
- Log execution statistics

Usage:
    python main.py                  # Run full ETL
    python main.py --phase extract  # Run only extraction
    python main.py --phase transform # Run only transformation
    python main.py --phase load     # Run only loading
    python main.py --verbose        # Run with verbose logging

Date: 2026-04-07
"""

import os
import sys
import logging
import argparse
import time
from datetime import datetime
from typing import Dict, Any
import pandas as pd

from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL Pipeline orchestrator"""
    
    def __init__(self, verbose: bool = False):
        """
        Initialize ETL Pipeline
        
        Args:
            verbose: Enable verbose logging
        """
        if verbose:
            logger.setLevel(logging.DEBUG)
        
        self.extractor = DataExtractor()
        self.transformer = None
        self.loader = DataLoader()
        
        self.extracted_data = {}
        self.transformed_data = {}
        self.load_stats = {}
        
        self.start_time = None
        self.end_time = None
    
    def run_extract(self) -> Dict[str, pd.DataFrame]:
        """
        Run extraction phase
        
        Returns:
            Dictionary of extracted DataFrames
        """
        logger.info("="*80)
        logger.info("PHASE 1: EXTRACTION")
        logger.info("="*80)
        logger.info("Extracting data from source database (recruitment_dev)")
        logger.info("="*80)
        
        phase_start = time.time()
        
        try:
            self.extracted_data = self.extractor.extract_all()
            
            phase_duration = time.time() - phase_start
            logger.info(f"\nExtraction completed in {phase_duration:.2f} seconds")
            logger.info(f"Extracted {len(self.extracted_data)} tables")
            
            return self.extracted_data
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def run_transform(self, extracted_data: Dict[str, pd.DataFrame] = None) -> Dict[str, pd.DataFrame]:
        """
        Run transformation phase
        
        Args:
            extracted_data: Extracted data (if not provided, uses existing)
            
        Returns:
            Dictionary of transformed DataFrames
        """
        logger.info("="*80)
        logger.info("PHASE 2: TRANSFORMATION")
        logger.info("="*80)
        logger.info("Transforming data for data mart")
        logger.info("="*80)
        
        phase_start = time.time()
        
        try:
            # Use provided data or existing data
            data_to_transform = extracted_data or self.extracted_data
            
            if not data_to_transform:
                raise ValueError("No extracted data available. Run extraction first.")
            
            # Initialize transformer
            self.transformer = DataTransformer(data_to_transform)
            
            # Run transformations
            self.transformed_data = self.transformer.transform_all()
            
            phase_duration = time.time() - phase_start
            logger.info(f"\nTransformation completed in {phase_duration:.2f} seconds")
            logger.info(f"Transformed {len(self.transformed_data)} datasets")
            
            return self.transformed_data
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def run_load(self, transformed_data: Dict[str, pd.DataFrame] = None) -> Dict[str, int]:
        """
        Run loading phase
        
        Args:
            transformed_data: Transformed data (if not provided, uses existing)
            
        Returns:
            Dictionary with load statistics
        """
        logger.info("="*80)
        logger.info("PHASE 3: LOADING")
        logger.info("="*80)
        logger.info("Loading data into data mart (datamart_badr_interactive)")
        logger.info("="*80)
        
        phase_start = time.time()
        
        try:
            # Use provided data or existing data
            data_to_load = transformed_data or self.transformed_data
            
            if not data_to_load:
                raise ValueError("No transformed data available. Run transformation first.")
            
            # Run loading
            self.load_stats = self.loader.load_all(data_to_load)
            
            phase_duration = time.time() - phase_start
            logger.info(f"\nLoading completed in {phase_duration:.2f} seconds")
            
            return self.load_stats
            
        except Exception as e:
            logger.error(f"Loading failed: {e}")
            raise
    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """
        Run complete ETL pipeline
        
        Returns:
            Dictionary with execution results
        """
        logger.info("="*80)
        logger.info("STARTING FULL ETL PIPELINE")
        logger.info("="*80)
        logger.info(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*80)
        
        self.start_time = time.time()
        
        try:
            # Phase 1: Extract
            self.run_extract()
            
            # Phase 2: Transform
            self.run_transform()
            
            # Phase 3: Load
            self.run_load()
            
            # Calculate total duration
            self.end_time = time.time()
            total_duration = self.end_time - self.start_time
            
            # Print final summary
            logger.info("\n" + "="*80)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("="*80)
            logger.info(f"Total execution time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
            logger.info("="*80)
            
            # Detailed summary
            logger.info("\nDetailed Summary:")
            logger.info("-" * 80)
            
            logger.info("\nExtraction Results:")
            for table, df in self.extracted_data.items():
                logger.info(f"  {table:40s} {len(df):>8,} rows")
            
            logger.info("\nTransformation Results:")
            for table, df in self.transformed_data.items():
                logger.info(f"  {table:40s} {len(df):>8,} rows")
            
            logger.info("\nLoading Results:")
            for table, count in self.load_stats.items():
                logger.info(f"  {table:40s} {count:>8,} records")
            
            logger.info("-" * 80)
            
            return {
                'status': 'success',
                'duration_seconds': total_duration,
                'extracted_tables': len(self.extracted_data),
                'transformed_datasets': len(self.transformed_data),
                'loaded_tables': len(self.load_stats),
                'extracted_data': self.extracted_data,
                'transformed_data': self.transformed_data,
                'load_stats': self.load_stats
            }
            
        except Exception as e:
            self.end_time = time.time()
            duration = self.end_time - self.start_time
            
            logger.error("\n" + "="*80)
            logger.error("ETL PIPELINE FAILED")
            logger.error("="*80)
            logger.error(f"Error: {e}")
            logger.error(f"Execution time before failure: {duration:.2f} seconds")
            logger.error("="*80)
            
            return {
                'status': 'failed',
                'duration_seconds': duration,
                'error': str(e)
            }
        
        finally:
            # Cleanup
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        logger.info("\nCleaning up resources...")
        try:
            self.extractor.close()
            self.loader.close()
            logger.info("Cleanup completed")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='ETL Pipeline for Data Mart')
    parser.add_argument(
        '--phase',
        choices=['extract', 'transform', 'load', 'all'],
        default='all',
        help='ETL phase to run (default: all)'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = ETLPipeline(verbose=args.verbose)
    
    try:
        if args.phase == 'extract':
            result = pipeline.run_extract()
        elif args.phase == 'transform':
            # Need to extract first
            extracted = pipeline.run_extract()
            result = pipeline.run_transform(extracted)
        elif args.phase == 'load':
            # Need to extract and transform first
            extracted = pipeline.run_extract()
            transformed = pipeline.run_transform(extracted)
            result = pipeline.run_load(transformed)
        else:  # all
            result = pipeline.run_full_pipeline()
        
        # Exit with appropriate code
        if result.get('status') == 'success':
            logger.info("\n✅ ETL Pipeline completed successfully")
            sys.exit(0)
        else:
            logger.error("\n❌ ETL Pipeline failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.warning("\n⚠️ ETL Pipeline interrupted by user")
        pipeline.cleanup()
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ ETL Pipeline failed with error: {e}")
        pipeline.cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()
