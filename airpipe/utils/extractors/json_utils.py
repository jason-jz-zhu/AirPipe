"""
JSON extraction utilities for reuse across pipelines.
"""

import json
import pandas as pd
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class JSONUtils:
    """Reusable JSON extraction operations."""
    
    @staticmethod
    def read_json(filepath: str, 
                  encoding: str = 'utf-8',
                  **kwargs) -> Union[Dict, List]:
        """
        Read JSON file and return parsed data.
        
        Args:
            filepath: Path to JSON file
            encoding: File encoding
            **kwargs: Additional json.load parameters
            
        Returns:
            Parsed JSON data (dict or list)
            
        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If JSON is malformed
        """
        try:
            logger.info(f"Reading JSON file: {filepath}")
            with open(filepath, 'r', encoding=encoding) as f:
                data = json.load(f, **kwargs)
            logger.info(f"Successfully read JSON from {filepath}")
            return data
        except FileNotFoundError:
            logger.error(f"JSON file not found: {filepath}")
            raise FileNotFoundError(f"JSON file not found: {filepath}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {filepath}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error reading JSON file {filepath}: {str(e)}")
            raise
    
    @staticmethod
    def json_to_dataframe(data: Union[Dict, List],
                         normalize: bool = True,
                         record_path: Optional[str] = None,
                         meta: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Convert JSON data to DataFrame.
        
        Args:
            data: JSON data (dict or list)
            normalize: Whether to normalize nested data
            record_path: Path to records in nested JSON
            meta: Fields to include from parent object
            
        Returns:
            DataFrame representation of JSON data
        """
        try:
            if normalize and isinstance(data, dict):
                # Use json_normalize for nested structures
                df = pd.json_normalize(data, record_path=record_path, meta=meta)
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                # Simple dict, convert directly
                df = pd.DataFrame([data])
            
            logger.info(f"Converted JSON to DataFrame with {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error converting JSON to DataFrame: {e}")
            raise
    
    @staticmethod
    def read_jsonl(filepath: str, 
                   encoding: str = 'utf-8') -> pd.DataFrame:
        """
        Read JSON Lines file (one JSON object per line).
        
        Args:
            filepath: Path to JSONL file
            encoding: File encoding
            
        Returns:
            DataFrame with all records
        """
        records = []
        try:
            logger.info(f"Reading JSONL file: {filepath}")
            with open(filepath, 'r', encoding=encoding) as f:
                for line_num, line in enumerate(f, 1):
                    if line.strip():  # Skip empty lines
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping invalid JSON on line {line_num}: {e}")
            
            df = pd.DataFrame(records)
            logger.info(f"Successfully read {len(df)} records from JSONL file")
            return df
        except FileNotFoundError:
            logger.error(f"JSONL file not found: {filepath}")
            raise FileNotFoundError(f"JSONL file not found: {filepath}")
        except Exception as e:
            logger.error(f"Error reading JSONL file {filepath}: {str(e)}")
            raise
    
    @staticmethod
    def flatten_json(data: Dict, 
                     parent_key: str = '', 
                     sep: str = '_') -> Dict:
        """
        Flatten nested JSON structure.
        
        Args:
            data: Nested dictionary
            parent_key: Key prefix for nested items
            sep: Separator for nested keys
            
        Returns:
            Flattened dictionary
        """
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(JSONUtils.flatten_json(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                # Convert list to string or handle as needed
                items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
        return dict(items)
    
    @staticmethod
    def validate_schema(data: Union[Dict, List],
                       required_fields: List[str]) -> bool:
        """
        Validate JSON data has required fields.
        
        Args:
            data: JSON data to validate
            required_fields: List of required field names
            
        Returns:
            True if all fields exist
            
        Raises:
            ValueError: If required fields are missing
        """
        if isinstance(data, list):
            # Check first record for list of records
            if not data:
                raise ValueError("Empty data, cannot validate schema")
            data = data[0]
        
        missing = []
        for field in required_fields:
            if '.' in field:
                # Handle nested fields
                parts = field.split('.')
                current = data
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        missing.append(field)
                        break
            else:
                if field not in data:
                    missing.append(field)
        
        if missing:
            raise ValueError(f"Missing required fields: {missing}")
        
        return True