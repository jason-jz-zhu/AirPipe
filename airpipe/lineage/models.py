"""
Spline data models for lineage events.

These models represent the data structures used by Apache Spline
for capturing and representing data lineage.
"""

from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
import uuid
import json


class OperationType(Enum):
    """Types of operations in data lineage."""
    READ = "Read"
    WRITE = "Write"
    TRANSFORMATION = "Transformation"
    FILTER = "Filter"
    JOIN = "Join"
    AGGREGATE = "Aggregate"
    SORT = "Sort"
    PROJECT = "Project"


@dataclass
class DataSource:
    """Represents a data source or sink in the lineage."""
    uri: str
    type: str  # e.g., "file", "database", "memory"
    format: Optional[str] = None  # e.g., "csv", "parquet", "pandas"
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class Attribute:
    """Represents a data attribute/column."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    dataType: Optional[str] = None
    nullable: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v}


@dataclass
class Schema:
    """Represents the schema of a dataset."""
    attributes: List[Attribute]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "attrs": [attr.to_dict() for attr in self.attributes]
        }


@dataclass
class Operation:
    """Represents an operation in the execution plan."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    type: OperationType = OperationType.TRANSFORMATION
    inputIds: List[str] = field(default_factory=list)
    outputIds: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type.value,
            "childIds": self.inputIds,
            "outputIds": self.outputIds,
            "params": self.params
        }


@dataclass
class Dataset:
    """Represents a dataset in the lineage."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    schema: Optional[Schema] = None
    source: Optional[DataSource] = None
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "id": self.id,
            "name": self.name
        }
        if self.schema:
            result["schema"] = self.schema.to_dict()
        if self.source:
            result["uri"] = self.source.uri
            result["type"] = self.source.type
        return result


@dataclass
class ExecutionPlan:
    """Represents the execution plan for Spline."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    operations: List[Operation] = field(default_factory=list)
    datasets: List[Dataset] = field(default_factory=list)
    systemInfo: Dict[str, Any] = field(default_factory=dict)
    agentInfo: Dict[str, Any] = field(default_factory=dict)
    extraInfo: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "operations": {
                "write": {
                    "outputSource": self._get_output_source(),
                    "append": False,
                    "id": str(uuid.uuid4()),
                    "name": "Write",
                    "childIds": [op.id for op in self.operations[-1:]] if self.operations else [],
                    "params": {}
                },
                "other": [op.to_dict() for op in self.operations]
            },
            "datasets": [ds.to_dict() for ds in self.datasets],
            "systemInfo": self.systemInfo,
            "agentInfo": self.agentInfo,
            "extraInfo": self.extraInfo
        }
    
    def _get_output_source(self) -> str:
        """Get the output source URI."""
        if self.datasets:
            for ds in reversed(self.datasets):
                if ds.source:
                    return ds.source.uri
        return "memory://unknown"


@dataclass
class ExecutionEvent:
    """Represents an execution event for Spline."""
    planId: str
    timestamp: int = field(default_factory=lambda: int(datetime.now().timestamp() * 1000))
    duration: Optional[int] = None  # milliseconds
    error: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        result = {
            "planId": self.planId,
            "timestamp": self.timestamp
        }
        if self.duration is not None:
            result["durationNs"] = self.duration * 1000000  # Convert to nanoseconds
        if self.error:
            result["error"] = {"message": self.error}
        if self.extra:
            result["extra"] = self.extra
        return result


@dataclass 
class LineageEvent:
    """Combined lineage event containing plan and execution details."""
    executionPlan: ExecutionPlan
    executionEvent: Optional[ExecutionEvent] = None
    
    def to_json(self) -> str:
        """Convert to JSON string for Spline API."""
        data = {
            "executionPlan": self.executionPlan.to_dict()
        }
        if self.executionEvent:
            data["executionEvent"] = self.executionEvent.to_dict()
        return json.dumps(data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        data = {
            "executionPlan": self.executionPlan.to_dict()
        }
        if self.executionEvent:
            data["executionEvent"] = self.executionEvent.to_dict()
        return data


class SplineModelBuilder:
    """Helper class to build Spline models from AirPipe metadata."""
    
    @staticmethod
    def create_dataset_from_artifact(artifact_name: str, artifact_metadata: Dict[str, Any]) -> Dataset:
        """Create a Spline Dataset from an AirPipe artifact."""
        dataset = Dataset(
            name=artifact_name,
            source=DataSource(
                uri=f"airpipe://artifact/{artifact_name}",
                type="memory",
                format=artifact_metadata.get('format', 'unknown')
            )
        )
        
        # Add schema if available
        if 'columns' in artifact_metadata:
            attributes = [
                Attribute(name=col, dataType=str(dtype))
                for col, dtype in artifact_metadata['columns'].items()
            ]
            dataset.schema = Schema(attributes=attributes)
        
        return dataset
    
    @staticmethod
    def create_operation_from_task(task_name: str, task_type: str, 
                                  input_artifacts: List[str], 
                                  output_artifacts: List[str]) -> Operation:
        """Create a Spline Operation from an AirPipe task."""
        # Map task types to operation types
        type_mapping = {
            'extractor': OperationType.READ,
            'transformer': OperationType.TRANSFORMATION,
            'loader': OperationType.WRITE
        }
        
        return Operation(
            name=task_name,
            type=type_mapping.get(task_type, OperationType.TRANSFORMATION),
            inputIds=input_artifacts,
            outputIds=output_artifacts,
            params={"taskType": task_type}
        )