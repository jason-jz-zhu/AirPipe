"""
File loading utilities for saving data to various formats.
"""

import pandas as pd
import json
from pathlib import Path
from typing import Optional, Dict, Any, Union
import logging

logger = logging.getLogger(__name__)


class FileUtils:
    """Reusable file saving operations."""
    
    @staticmethod
    def save_to_csv(df: pd.DataFrame,
                   filepath: str,
                   index: bool = False,
                   encoding: str = 'utf-8',
                   create_dir: bool = True,
                   **kwargs) -> None:
        """
        Save DataFrame to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            index: Whether to include index
            encoding: File encoding
            create_dir: Create directory if it doesn't exist
            **kwargs: Additional pandas to_csv parameters
        """
        try:
            # Create directory if needed
            if create_dir:
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_csv(filepath, index=index, encoding=encoding, **kwargs)
            logger.info(f"Saved {len(df)} rows to CSV: {filepath}")
        except Exception as e:
            logger.error(f"Error saving CSV to {filepath}: {e}")
            raise
    
    @staticmethod
    def save_to_json(data: Union[pd.DataFrame, Dict, list],
                    filepath: str,
                    orient: str = 'records',
                    indent: int = 2,
                    encoding: str = 'utf-8',
                    create_dir: bool = True) -> None:
        """
        Save data to JSON file.
        
        Args:
            data: Data to save (DataFrame, dict, or list)
            filepath: Output file path
            orient: DataFrame orientation for JSON
            indent: JSON indentation
            encoding: File encoding
            create_dir: Create directory if it doesn't exist
        """
        try:
            # Create directory if needed
            if create_dir:
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            if isinstance(data, pd.DataFrame):
                data.to_json(filepath, orient=orient, indent=indent)
                logger.info(f"Saved {len(data)} rows to JSON: {filepath}")
            else:
                with open(filepath, 'w', encoding=encoding) as f:
                    json.dump(data, f, indent=indent, default=str)
                logger.info(f"Saved data to JSON: {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSON to {filepath}: {e}")
            raise
    
    @staticmethod
    def save_to_excel(df: pd.DataFrame,
                     filepath: str,
                     sheet_name: str = 'Sheet1',
                     index: bool = False,
                     create_dir: bool = True,
                     **kwargs) -> None:
        """
        Save DataFrame to Excel file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            sheet_name: Name of the Excel sheet
            index: Whether to include index
            create_dir: Create directory if it doesn't exist
            **kwargs: Additional pandas to_excel parameters
        """
        try:
            # Create directory if needed
            if create_dir:
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_excel(filepath, sheet_name=sheet_name, index=index, **kwargs)
            logger.info(f"Saved {len(df)} rows to Excel: {filepath}")
        except Exception as e:
            logger.error(f"Error saving Excel to {filepath}: {e}")
            raise
    
    @staticmethod
    def save_to_parquet(df: pd.DataFrame,
                       filepath: str,
                       compression: str = 'snappy',
                       create_dir: bool = True,
                       **kwargs) -> None:
        """
        Save DataFrame to Parquet file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            compression: Compression to use
            create_dir: Create directory if it doesn't exist
            **kwargs: Additional pandas to_parquet parameters
        """
        try:
            # Create directory if needed
            if create_dir:
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            df.to_parquet(filepath, compression=compression, **kwargs)
            logger.info(f"Saved {len(df)} rows to Parquet: {filepath}")
        except Exception as e:
            logger.error(f"Error saving Parquet to {filepath}: {e}")
            raise
    
    @staticmethod
    def append_to_csv(df: pd.DataFrame,
                     filepath: str,
                     header: bool = None,
                     **kwargs) -> None:
        """
        Append DataFrame to existing CSV file.
        
        Args:
            df: DataFrame to append
            filepath: Output file path
            header: Write header (auto-detects if None)
            **kwargs: Additional pandas to_csv parameters
        """
        try:
            file_exists = Path(filepath).exists()
            
            # Auto-detect header requirement
            if header is None:
                header = not file_exists
            
            df.to_csv(filepath, mode='a', header=header, index=False, **kwargs)
            logger.info(f"Appended {len(df)} rows to CSV: {filepath}")
        except Exception as e:
            logger.error(f"Error appending to CSV {filepath}: {e}")
            raise
    
    @staticmethod
    def save_to_jsonl(data: Union[pd.DataFrame, list],
                     filepath: str,
                     encoding: str = 'utf-8',
                     create_dir: bool = True) -> None:
        """
        Save data to JSON Lines file (one JSON object per line).
        
        Args:
            data: DataFrame or list of dicts to save
            filepath: Output file path
            encoding: File encoding
            create_dir: Create directory if it doesn't exist
        """
        try:
            # Create directory if needed
            if create_dir:
                Path(filepath).parent.mkdir(parents=True, exist_ok=True)
            
            if isinstance(data, pd.DataFrame):
                records = data.to_dict('records')
            else:
                records = data
            
            with open(filepath, 'w', encoding=encoding) as f:
                for record in records:
                    f.write(json.dumps(record, default=str) + '\n')
            
            logger.info(f"Saved {len(records)} records to JSONL: {filepath}")
        except Exception as e:
            logger.error(f"Error saving JSONL to {filepath}: {e}")
            raise
    
    @staticmethod
    def create_backup(filepath: str, 
                     backup_suffix: str = '.bak') -> Optional[str]:
        """
        Create backup of existing file before overwriting.
        
        Args:
            filepath: File to backup
            backup_suffix: Suffix for backup file
            
        Returns:
            Path to backup file if created, None otherwise
        """
        try:
            source = Path(filepath)
            if source.exists():
                backup_path = str(source) + backup_suffix
                source.rename(backup_path)
                logger.info(f"Created backup: {backup_path}")
                return backup_path
            return None
        except Exception as e:
            logger.error(f"Error creating backup for {filepath}: {e}")
            raise