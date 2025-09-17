"""
Data artifact system for sharing data between pipeline components.
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import pandas as pd
import json
import pickle
import hashlib
from pathlib import Path


class DataFormat(Enum):
    """Supported data formats for artifacts."""
    PANDAS_DATAFRAME = "pandas_dataframe"
    SPARK_DATAFRAME = "spark_dataframe"
    DICT = "dict"
    LIST = "list"
    JSON = "json"
    BYTES = "bytes"
    CUSTOM = "custom"


@dataclass
class ArtifactMetadata:
    """Metadata for data artifacts."""
    created_at: datetime = field(default_factory=datetime.now)
    modified_at: datetime = field(default_factory=datetime.now)
    source_component: Optional[str] = None
    format: DataFormat = DataFormat.CUSTOM
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    size_bytes: Optional[int] = None
    checksum: Optional[str] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    lineage: List[str] = field(default_factory=list)


class DataArtifact:
    """
    Container for data that flows through the pipeline.
    Supports multiple data formats and provides shared access across components.
    """
    
    def __init__(
        self,
        data: Any,
        name: str,
        metadata: Optional[ArtifactMetadata] = None,
        persist: bool = False,
        persist_path: Optional[Path] = None
    ):
        """
        Initialize data artifact.
        
        Args:
            data: The actual data
            name: Unique name for the artifact
            metadata: Optional metadata
            persist: Whether to persist artifact to disk
            persist_path: Path for persistence
        """
        self.name = name
        self._data = data
        self.metadata = metadata or ArtifactMetadata()
        self.persist = persist
        self.persist_path = persist_path
        
        # Auto-detect format if not specified
        if self.metadata.format == DataFormat.CUSTOM:
            self.metadata.format = self._detect_format(data)
        
        # Update metadata
        self._update_metadata()
        
        # Persist if requested
        if self.persist and self.persist_path:
            self.save_to_disk()
    
    @property
    def data(self) -> Any:
        """Get the underlying data."""
        return self._data
    
    @data.setter
    def data(self, value: Any) -> None:
        """Set the data and update metadata."""
        self._data = value
        self.metadata.modified_at = datetime.now()
        self.metadata.format = self._detect_format(value)
        self._update_metadata()
        
        if self.persist and self.persist_path:
            self.save_to_disk()
    
    def _detect_format(self, data: Any) -> DataFormat:
        """Auto-detect data format."""
        if isinstance(data, pd.DataFrame):
            return DataFormat.PANDAS_DATAFRAME
        elif self._is_spark_dataframe(data):
            return DataFormat.SPARK_DATAFRAME
        elif isinstance(data, dict):
            return DataFormat.DICT
        elif isinstance(data, list):
            return DataFormat.LIST
        elif isinstance(data, bytes):
            return DataFormat.BYTES
        else:
            return DataFormat.CUSTOM
    
    def _is_spark_dataframe(self, data: Any) -> bool:
        """Check if data is a Spark DataFrame."""
        try:
            from pyspark.sql import DataFrame as SparkDataFrame
            return isinstance(data, SparkDataFrame)
        except ImportError:
            return False
    
    def _update_metadata(self) -> None:
        """Update artifact metadata based on current data."""
        if self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            df = self._data
            self.metadata.row_count = len(df)
            self.metadata.column_count = len(df.columns)
            self.metadata.size_bytes = df.memory_usage(deep=True).sum()
        elif self.metadata.format == DataFormat.SPARK_DATAFRAME:
            # For Spark DataFrames, some operations can be expensive
            # Only get column count which is cheap
            self.metadata.column_count = len(self._data.columns)
            # Row count is expensive, so we skip it unless explicitly requested
            self.metadata.row_count = None  # Can be populated later if needed
        elif self.metadata.format == DataFormat.LIST:
            self.metadata.row_count = len(self._data)
        elif self.metadata.format == DataFormat.DICT:
            self.metadata.row_count = len(self._data)
        
        # Calculate checksum
        self.metadata.checksum = self._calculate_checksum()
    
    def _calculate_checksum(self) -> str:
        """Calculate checksum of the data."""
        try:
            if self.metadata.format == DataFormat.PANDAS_DATAFRAME:
                data_str = self._data.to_json()
            elif self.metadata.format in [DataFormat.DICT, DataFormat.LIST]:
                data_str = json.dumps(self._data, sort_keys=True, default=str)
            elif self.metadata.format == DataFormat.BYTES:
                data_str = self._data.decode('utf-8', errors='ignore')
            else:
                data_str = str(self._data)
            
            return hashlib.md5(data_str.encode()).hexdigest()
        except Exception:
            return "unknown"
    
    def as_dataframe(self) -> pd.DataFrame:
        """Convert data to pandas DataFrame."""
        if self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            return self._data
        elif self.metadata.format == DataFormat.SPARK_DATAFRAME:
            # Convert Spark DataFrame to Pandas
            from airpipe.utils.spark import spark_to_pandas
            return spark_to_pandas(self._data)
        elif self.metadata.format == DataFormat.DICT:
            return pd.DataFrame([self._data])
        elif self.metadata.format == DataFormat.LIST:
            return pd.DataFrame(self._data)
        else:
            raise ValueError(f"Cannot convert {self.metadata.format} to DataFrame")
    
    def as_dict(self) -> Dict:
        """Convert data to dictionary."""
        if self.metadata.format == DataFormat.DICT:
            return self._data
        elif self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            return self._data.to_dict('records')
        elif self.metadata.format == DataFormat.SPARK_DATAFRAME:
            # Convert Spark DataFrame to dict via Pandas
            from airpipe.utils.spark import spark_to_pandas
            return spark_to_pandas(self._data).to_dict('records')
        elif self.metadata.format == DataFormat.LIST:
            return {"data": self._data}
        else:
            raise ValueError(f"Cannot convert {self.metadata.format} to dict")
    
    def as_list(self) -> List:
        """Convert data to list."""
        if self.metadata.format == DataFormat.LIST:
            return self._data
        elif self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            return self._data.to_dict('records')
        elif self.metadata.format == DataFormat.SPARK_DATAFRAME:
            # Convert Spark DataFrame to list via Pandas
            from airpipe.utils.spark import spark_to_pandas
            return spark_to_pandas(self._data).to_dict('records')
        elif self.metadata.format == DataFormat.DICT:
            return [self._data]
        else:
            raise ValueError(f"Cannot convert {self.metadata.format} to list")
    
    def as_spark_dataframe(self):
        """Convert data to Spark DataFrame."""
        if self.metadata.format == DataFormat.SPARK_DATAFRAME:
            return self._data
        elif self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            from airpipe.utils.spark import pandas_to_spark
            return pandas_to_spark(self._data)
        elif self.metadata.format == DataFormat.LIST:
            # Convert list to Pandas first, then to Spark
            from airpipe.utils.spark import pandas_to_spark
            df = pd.DataFrame(self._data)
            return pandas_to_spark(df)
        elif self.metadata.format == DataFormat.DICT:
            # Convert dict to Pandas first, then to Spark
            from airpipe.utils.spark import pandas_to_spark
            df = pd.DataFrame([self._data])
            return pandas_to_spark(df)
        else:
            raise ValueError(f"Cannot convert {self.metadata.format} to Spark DataFrame")
    
    def transform(self, func: callable) -> 'DataArtifact':
        """
        Apply a transformation function to the data.
        
        Args:
            func: Function to apply to the data
            
        Returns:
            New DataArtifact with transformed data
        """
        transformed_data = func(self._data)
        new_artifact = DataArtifact(
            data=transformed_data,
            name=f"{self.name}_transformed",
            metadata=ArtifactMetadata(
                source_component=self.metadata.source_component,
                lineage=self.metadata.lineage + [self.name]
            ),
            persist=self.persist,
            persist_path=self.persist_path
        )
        return new_artifact
    
    def add_lineage(self, component_name: str) -> None:
        """Add component to lineage tracking."""
        self.metadata.lineage.append(component_name)
        self.metadata.modified_at = datetime.now()
    
    def add_tag(self, key: str, value: Any) -> None:
        """Add a tag to the artifact."""
        self.metadata.tags[key] = value
    
    def save_to_disk(self) -> None:
        """Persist artifact to disk."""
        if not self.persist_path:
            raise ValueError("Persist path not specified")
        
        path = Path(self.persist_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Save data based on format
        if self.metadata.format == DataFormat.PANDAS_DATAFRAME:
            self._data.to_parquet(path.with_suffix('.parquet'))
        elif self.metadata.format == DataFormat.JSON:
            with open(path.with_suffix('.json'), 'w') as f:
                json.dump(self._data, f, default=str)
        else:
            # Use pickle for other formats
            with open(path.with_suffix('.pkl'), 'wb') as f:
                pickle.dump(self._data, f)
        
        # Save metadata
        metadata_path = path.with_suffix('.metadata.json')
        with open(metadata_path, 'w') as f:
            metadata_dict = {
                'created_at': self.metadata.created_at.isoformat(),
                'modified_at': self.metadata.modified_at.isoformat(),
                'source_component': self.metadata.source_component,
                'format': self.metadata.format.value,
                'row_count': self.metadata.row_count,
                'column_count': self.metadata.column_count,
                'size_bytes': self.metadata.size_bytes,
                'checksum': self.metadata.checksum,
                'tags': self.metadata.tags,
                'lineage': self.metadata.lineage
            }
            json.dump(metadata_dict, f, indent=2)
    
    @classmethod
    def load_from_disk(cls, name: str, path: Path) -> 'DataArtifact':
        """Load artifact from disk."""
        path = Path(path)
        
        # Load metadata
        metadata_path = path.with_suffix('.metadata.json')
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata_dict = json.load(f)
                metadata = ArtifactMetadata(
                    created_at=datetime.fromisoformat(metadata_dict['created_at']),
                    modified_at=datetime.fromisoformat(metadata_dict['modified_at']),
                    source_component=metadata_dict.get('source_component'),
                    format=DataFormat(metadata_dict['format']),
                    row_count=metadata_dict.get('row_count'),
                    column_count=metadata_dict.get('column_count'),
                    size_bytes=metadata_dict.get('size_bytes'),
                    checksum=metadata_dict.get('checksum'),
                    tags=metadata_dict.get('tags', {}),
                    lineage=metadata_dict.get('lineage', [])
                )
        else:
            metadata = ArtifactMetadata()
        
        # Load data based on format
        if path.with_suffix('.parquet').exists():
            data = pd.read_parquet(path.with_suffix('.parquet'))
        elif path.with_suffix('.json').exists():
            with open(path.with_suffix('.json'), 'r') as f:
                data = json.load(f)
        elif path.with_suffix('.pkl').exists():
            with open(path.with_suffix('.pkl'), 'rb') as f:
                data = pickle.load(f)
        else:
            raise FileNotFoundError(f"No data file found for {path}")
        
        return cls(data=data, name=name, metadata=metadata, persist=True, persist_path=path)
    
    def __repr__(self) -> str:
        return (
            f"DataArtifact(name='{self.name}', "
            f"format={self.metadata.format.value}, "
            f"rows={self.metadata.row_count}, "
            f"checksum={self.metadata.checksum[:8]}...)"
        )


class ArtifactStore:
    """Store for managing shared artifacts across pipeline runs."""
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Initialize artifact store.
        
        Args:
            base_path: Base path for artifact storage
        """
        self.base_path = base_path or Path.cwd() / "artifacts"
        self.base_path.mkdir(parents=True, exist_ok=True)
        self._artifacts: Dict[str, DataArtifact] = {}
    
    def put(self, artifact: DataArtifact) -> None:
        """Store an artifact."""
        self._artifacts[artifact.name] = artifact
        
        # Persist if configured
        if artifact.persist and not artifact.persist_path:
            artifact.persist_path = self.base_path / artifact.name
            artifact.save_to_disk()
    
    def get(self, name: str) -> Optional[DataArtifact]:
        """Retrieve an artifact by name."""
        # Check in-memory store first
        if name in self._artifacts:
            return self._artifacts[name]
        
        # Try to load from disk
        path = self.base_path / name
        if path.with_suffix('.metadata.json').exists():
            artifact = DataArtifact.load_from_disk(name, path)
            self._artifacts[name] = artifact
            return artifact
        
        return None
    
    def exists(self, name: str) -> bool:
        """Check if an artifact exists."""
        return name in self._artifacts or (self.base_path / name).with_suffix('.metadata.json').exists()
    
    def list_artifacts(self) -> List[str]:
        """List all available artifacts."""
        # In-memory artifacts
        artifacts = set(self._artifacts.keys())
        
        # Persisted artifacts
        for metadata_file in self.base_path.glob('*.metadata.json'):
            artifact_name = metadata_file.stem.replace('.metadata', '')
            artifacts.add(artifact_name)
        
        return sorted(list(artifacts))
    
    def clear(self) -> None:
        """Clear all artifacts from memory (not disk)."""
        self._artifacts.clear()
    
    def delete(self, name: str, from_disk: bool = False) -> None:
        """
        Delete an artifact.
        
        Args:
            name: Artifact name
            from_disk: Whether to also delete from disk
        """
        # Remove from memory
        if name in self._artifacts:
            del self._artifacts[name]
        
        # Remove from disk if requested
        if from_disk:
            path = self.base_path / name
            for suffix in ['.parquet', '.json', '.pkl', '.metadata.json']:
                file_path = path.with_suffix(suffix)
                if file_path.exists():
                    file_path.unlink()