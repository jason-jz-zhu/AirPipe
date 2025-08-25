"""
Sample data extraction logic for simple workflows.
"""

import pandas as pd
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class SampleDataExtractor:
    """Extract sample data for testing and demonstration."""
    
    def extract_sample_data(self, 
                           num_records: int = 100,
                           categories: Optional[list] = None) -> pd.DataFrame:
        """
        Generate sample data for testing.
        
        Args:
            num_records: Number of records to generate
            categories: List of categories to use
            
        Returns:
            DataFrame with sample data
        """
        if categories is None:
            categories = ['A', 'B', 'C']
        
        # Generate sample data
        data = pd.DataFrame({
            'id': range(1, num_records + 1),
            'value': [i * 10 for i in range(1, num_records + 1)],
            'category': categories * (num_records // len(categories)) + categories[:num_records % len(categories)]
        })
        
        logger.info(f"Generated {len(data)} sample records with categories: {categories}")
        return data
    
    def extract_with_metadata(self,
                            num_records: int = 100,
                            include_timestamp: bool = True) -> pd.DataFrame:
        """
        Generate sample data with metadata.
        
        Args:
            num_records: Number of records to generate
            include_timestamp: Whether to include extraction timestamp
            
        Returns:
            DataFrame with sample data and metadata
        """
        df = self.extract_sample_data(num_records)
        
        # Add metadata
        if include_timestamp:
            df['extracted_at'] = pd.Timestamp.now()
        
        df['source'] = 'sample_generator'
        df['version'] = '1.0'
        
        logger.info(f"Added metadata to {len(df)} records")
        return df