"""
CSV extraction utilities for reuse across pipelines.
"""

import pandas as pd
from typing import Optional, Dict, Any, List
import logging

logger = logging.getLogger(__name__)


class CSVUtils:
    """Reusable CSV extraction operations."""
    
    @staticmethod
    def read_csv(filepath: str, 
                 encoding: str = 'utf-8',
                 delimiter: str = ',',
                 **kwargs) -> pd.DataFrame:
        """
        Generic CSV reading with error handling.
        
        Args:
            filepath: Path to CSV file
            encoding: File encoding
            delimiter: CSV delimiter
            **kwargs: Additional pandas read_csv parameters
            
        Returns:
            DataFrame with CSV data
            
        Raises:
            FileNotFoundError: If file doesn't exist
            ValueError: If CSV is malformed
        """
        try:
            logger.info(f"Reading CSV file: {filepath}")
            df = pd.read_csv(filepath, encoding=encoding, delimiter=delimiter, **kwargs)
            logger.info(f"Successfully read {len(df)} rows from {filepath}")
            return df
        except FileNotFoundError:
            logger.error(f"CSV file not found: {filepath}")
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        except pd.errors.EmptyDataError:
            logger.error(f"CSV file is empty: {filepath}")
            raise ValueError(f"CSV file is empty: {filepath}")
        except Exception as e:
            logger.error(f"Error reading CSV file {filepath}: {str(e)}")
            raise
    
    @staticmethod
    def validate_columns(df: pd.DataFrame, 
                        required_columns: List[str],
                        raise_on_missing: bool = True) -> bool:
        """
        Validate that required columns exist in DataFrame.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            raise_on_missing: Whether to raise exception on missing columns
            
        Returns:
            True if all columns exist, False otherwise
            
        Raises:
            ValueError: If raise_on_missing=True and columns are missing
        """
        missing = set(required_columns) - set(df.columns)
        if missing:
            msg = f"Missing required columns: {missing}"
            logger.error(msg)
            if raise_on_missing:
                raise ValueError(msg)
            return False
        return True
    
    @staticmethod
    def infer_dtypes(df: pd.DataFrame, 
                     date_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Infer and convert data types in DataFrame.
        
        Args:
            df: DataFrame to process
            date_columns: List of columns to convert to datetime
            
        Returns:
            DataFrame with inferred types
        """
        # Convert date columns if specified
        if date_columns:
            for col in date_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                        logger.debug(f"Converted {col} to datetime")
                    except Exception as e:
                        logger.warning(f"Could not convert {col} to datetime: {e}")
        
        # Try to infer numeric types
        df = df.infer_objects()
        
        return df
    
    @staticmethod
    def handle_missing_values(df: pd.DataFrame,
                            strategy: str = 'drop',
                            fill_value: Any = None,
                            columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Handle missing values in DataFrame.
        
        Args:
            df: DataFrame to process
            strategy: How to handle missing values ('drop', 'fill', 'forward', 'backward')
            fill_value: Value to use when strategy='fill'
            columns: Specific columns to apply strategy to
            
        Returns:
            DataFrame with missing values handled
        """
        df_copy = df.copy()
        
        if columns:
            target_df = df_copy[columns]
        else:
            target_df = df_copy
        
        if strategy == 'drop':
            return df_copy.dropna(subset=columns)
        elif strategy == 'fill':
            if columns:
                df_copy[columns] = df_copy[columns].fillna(fill_value)
            else:
                df_copy = df_copy.fillna(fill_value)
        elif strategy == 'forward':
            if columns:
                df_copy[columns] = df_copy[columns].fillna(method='ffill')
            else:
                df_copy = df_copy.fillna(method='ffill')
        elif strategy == 'backward':
            if columns:
                df_copy[columns] = df_copy[columns].fillna(method='bfill')
            else:
                df_copy = df_copy.fillna(method='bfill')
        
        return df_copy