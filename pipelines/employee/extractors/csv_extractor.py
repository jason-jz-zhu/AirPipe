"""
Employee-specific CSV extraction logic.
"""

import pandas as pd
from typing import Optional
from airpipe.utils.extractors.csv_utils import CSVUtils
import logging

logger = logging.getLogger(__name__)


class EmployeeCSVExtractor:
    """Extract employee data from CSV files with business logic."""
    
    def __init__(self):
        self.csv_utils = CSVUtils()
    
    def extract_current_employees(self, 
                                 filepath: str = "examples/sample_data.csv") -> pd.DataFrame:
        """
        Extract current employee roster from CSV.
        
        Args:
            filepath: Path to employee CSV file
            
        Returns:
            DataFrame with validated employee data
        """
        # Use utility for generic CSV reading
        df = self.csv_utils.read_csv(filepath)
        
        # Validate employee-specific columns
        required_columns = ['id', 'name', 'department', 'salary']
        self.csv_utils.validate_columns(df, required_columns)
        
        # Add business logic specific to employee data
        df['source'] = 'csv_current'
        df['extracted_at'] = pd.Timestamp.now()
        
        # Validate salary is positive
        if (df['salary'] < 0).any():
            negative_count = (df['salary'] < 0).sum()
            logger.warning(f"Found {negative_count} employees with negative salary, setting to 0")
            df.loc[df['salary'] < 0, 'salary'] = 0
        
        # Ensure employee ID is unique
        if df['id'].duplicated().any():
            duplicates = df['id'].duplicated().sum()
            logger.warning(f"Found {duplicates} duplicate employee IDs")
            df = df.drop_duplicates(subset=['id'], keep='first')
        
        logger.info(f"Extracted {len(df)} current employees from CSV")
        return df
    
    def extract_historical_employees(self, 
                                    filepath: str) -> pd.DataFrame:
        """
        Extract historical employee data from CSV.
        
        Args:
            filepath: Path to historical data CSV
            
        Returns:
            DataFrame with historical employee data
        """
        df = self.csv_utils.read_csv(filepath)
        
        # Historical data might have different columns
        df['source'] = 'csv_historical'
        
        # Convert date columns if present
        date_columns = ['hire_date', 'termination_date', 'last_review_date']
        existing_date_cols = [col for col in date_columns if col in df.columns]
        if existing_date_cols:
            df = self.csv_utils.infer_dtypes(df, date_columns=existing_date_cols)
        
        logger.info(f"Extracted {len(df)} historical employee records")
        return df
    
    def extract_employee_benefits(self, 
                                 filepath: str) -> pd.DataFrame:
        """
        Extract employee benefits data from CSV.
        
        Args:
            filepath: Path to benefits CSV file
            
        Returns:
            DataFrame with benefits data
        """
        df = self.csv_utils.read_csv(filepath)
        
        # Benefits-specific validation
        if 'employee_id' in df.columns:
            df = df.rename(columns={'employee_id': 'id'})
        
        df['source'] = 'csv_benefits'
        
        # Handle missing values in benefits data
        df = self.csv_utils.handle_missing_values(
            df, 
            strategy='fill',
            fill_value=0,
            columns=['health_insurance', 'dental_insurance', '401k_match']
        )
        
        logger.info(f"Extracted benefits data for {len(df)} employees")
        return df