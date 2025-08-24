"""Mock objects for testing."""

import pandas as pd
from typing import Any, Dict, List, Optional, Generator
from unittest.mock import Mock, MagicMock
import random
import time

from airpipe.artifacts.data_artifact import DataArtifact


class MockExtractor:
    """Mock extractor for testing."""
    
    def __init__(self, data: Optional[pd.DataFrame] = None, 
                 fail_after: Optional[int] = None):
        """
        Initialize mock extractor.
        
        Args:
            data: Data to return
            fail_after: Fail after N calls
        """
        self.data = data
        self.fail_after = fail_after
        self.call_count = 0
        
    def extract(self) -> pd.DataFrame:
        """Extract mock data."""
        self.call_count += 1
        
        if self.fail_after and self.call_count > self.fail_after:
            raise Exception("Mock extraction failed")
            
        if self.data is not None:
            return self.data
            
        # Generate default data
        return pd.DataFrame({
            'id': range(10),
            'value': [random.random() * 100 for _ in range(10)]
        })
    
    def extract_with_params(self, limit: int = 100, 
                           source: str = "mock") -> pd.DataFrame:
        """Extract with parameters."""
        return pd.DataFrame({
            'id': range(limit),
            'value': [random.random() * 100 for _ in range(limit)],
            'source': [source] * limit
        })


class MockTransformer:
    """Mock transformer for testing."""
    
    def __init__(self, transform_func: Optional[callable] = None):
        """Initialize mock transformer."""
        self.transform_func = transform_func
        self.call_count = 0
        
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform data."""
        self.call_count += 1
        
        if self.transform_func:
            return self.transform_func(df)
            
        # Default transformation
        df_copy = df.copy()
        if 'value' in df_copy.columns:
            df_copy['transformed_value'] = df_copy['value'] * 2
        return df_copy
    
    def filter_data(self, df: pd.DataFrame, 
                   threshold: float = 50) -> pd.DataFrame:
        """Filter data by threshold."""
        if 'value' in df.columns:
            return df[df['value'] > threshold]
        return df


class MockLoader:
    """Mock loader for testing."""
    
    def __init__(self):
        """Initialize mock loader."""
        self.loaded_data = []
        self.call_count = 0
        
    def load(self, data: Any, destination: str = "mock") -> bool:
        """Load data to mock destination."""
        self.call_count += 1
        self.loaded_data.append({
            'data': data,
            'destination': destination,
            'timestamp': pd.Timestamp.now()
        })
        return True
    
    def get_loaded_data(self) -> List[Dict]:
        """Get all loaded data."""
        return self.loaded_data
    
    def reset(self):
        """Reset loader state."""
        self.loaded_data = []
        self.call_count = 0


class MockDataSource:
    """Mock data source for streaming tests."""
    
    def __init__(self, total_records: int = 1000,
                 batch_size: int = 100,
                 delay: float = 0.1,
                 fail_at_batch: Optional[int] = None):
        """
        Initialize mock data source.
        
        Args:
            total_records: Total records to generate
            batch_size: Records per batch
            delay: Delay between batches
            fail_at_batch: Fail at specific batch number
        """
        self.total_records = total_records
        self.batch_size = batch_size
        self.delay = delay
        self.fail_at_batch = fail_at_batch
        self.current_position = 0
        self.batch_count = 0
        
    def read_batch(self) -> Optional[pd.DataFrame]:
        """Read next batch of data."""
        if self.current_position >= self.total_records:
            return None
            
        self.batch_count += 1
        
        if self.fail_at_batch and self.batch_count == self.fail_at_batch:
            raise Exception(f"Mock source failed at batch {self.batch_count}")
            
        # Simulate processing delay
        if self.delay > 0:
            time.sleep(self.delay)
            
        # Calculate batch size
        remaining = self.total_records - self.current_position
        current_batch_size = min(self.batch_size, remaining)
        
        # Generate batch data
        batch_data = pd.DataFrame({
            'id': range(self.current_position, 
                       self.current_position + current_batch_size),
            'value': [random.random() * 100 
                     for _ in range(current_batch_size)],
            'timestamp': pd.Timestamp.now()
        })
        
        self.current_position += current_batch_size
        return batch_data
    
    def reset(self):
        """Reset source to beginning."""
        self.current_position = 0
        self.batch_count = 0
    
    def get_progress(self) -> float:
        """Get progress percentage."""
        return (self.current_position / self.total_records) * 100


class MockFileSystem:
    """Mock file system for testing file operations."""
    
    def __init__(self):
        """Initialize mock file system."""
        self.files = {}
        self.directories = set(['/'])
        
    def write_file(self, path: str, content: Any) -> bool:
        """Write file to mock filesystem."""
        # Extract directory
        dir_path = '/'.join(path.split('/')[:-1])
        if dir_path:
            self.directories.add(dir_path)
            
        self.files[path] = content
        return True
    
    def read_file(self, path: str) -> Any:
        """Read file from mock filesystem."""
        if path not in self.files:
            raise FileNotFoundError(f"File not found: {path}")
        return self.files[path]
    
    def exists(self, path: str) -> bool:
        """Check if file exists."""
        return path in self.files
    
    def list_files(self, directory: str = '/') -> List[str]:
        """List files in directory."""
        return [f for f in self.files.keys() 
                if f.startswith(directory)]
    
    def delete_file(self, path: str) -> bool:
        """Delete file."""
        if path in self.files:
            del self.files[path]
            return True
        return False
    
    def reset(self):
        """Reset filesystem."""
        self.files = {}
        self.directories = set(['/'])


class MockDatabase:
    """Mock database for testing database operations."""
    
    def __init__(self):
        """Initialize mock database."""
        self.tables = {}
        self.connection_open = False
        
    def connect(self) -> bool:
        """Open database connection."""
        self.connection_open = True
        return True
    
    def disconnect(self) -> bool:
        """Close database connection."""
        self.connection_open = False
        return True
    
    def insert_dataframe(self, df: pd.DataFrame, 
                        table_name: str) -> int:
        """Insert DataFrame into table."""
        if not self.connection_open:
            raise Exception("Database connection not open")
            
        if table_name not in self.tables:
            self.tables[table_name] = pd.DataFrame()
            
        self.tables[table_name] = pd.concat(
            [self.tables[table_name], df], 
            ignore_index=True
        )
        return len(df)
    
    def read_table(self, table_name: str, 
                  limit: Optional[int] = None) -> pd.DataFrame:
        """Read data from table."""
        if not self.connection_open:
            raise Exception("Database connection not open")
            
        if table_name not in self.tables:
            return pd.DataFrame()
            
        df = self.tables[table_name]
        if limit:
            return df.head(limit)
        return df
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute mock SQL query."""
        if not self.connection_open:
            raise Exception("Database connection not open")
            
        # Simple mock implementation
        if "SELECT" in query.upper():
            # Return mock data
            return pd.DataFrame({
                'col1': [1, 2, 3],
                'col2': ['a', 'b', 'c']
            })
        return pd.DataFrame()
    
    def reset(self):
        """Reset database."""
        self.tables = {}
        self.connection_open = False


def create_mock_artifact(name: str = "mock_artifact",
                        data_type: str = "dataframe") -> Mock:
    """Create a mock DataArtifact."""
    mock = Mock(spec=DataArtifact)
    mock.name = name
    
    if data_type == "dataframe":
        mock.data = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        mock.as_dataframe = Mock(return_value=mock.data)
        mock.as_dict = Mock(return_value=mock.data.to_dict('records'))
    elif data_type == "dict":
        mock.data = {'key1': 'value1', 'key2': 'value2'}
        mock.as_dict = Mock(return_value=mock.data)
        mock.as_dataframe = Mock(return_value=pd.DataFrame([mock.data]))
    elif data_type == "list":
        mock.data = [1, 2, 3, 4, 5]
        mock.as_list = Mock(return_value=mock.data)
        mock.as_dataframe = Mock(return_value=pd.DataFrame({'values': mock.data}))
        
    mock.metadata.row_count = len(mock.data)
    mock.metadata.created_at = pd.Timestamp.now()
    
    return mock