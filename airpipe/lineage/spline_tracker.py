"""
Apache Spline lineage tracker for AirPipe.

This module provides integration with Apache Spline for capturing and sending
data lineage information from AirPipe pipeline executions.
"""

import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
import requests
import json
from collections import defaultdict

from .config import SplineConfig
from .models import (
    ExecutionPlan, ExecutionEvent, LineageEvent,
    Operation, Dataset, DataSource, Schema, Attribute,
    OperationType, SplineModelBuilder
)


class SplineLineageTracker:
    """
    Tracks data lineage for AirPipe pipelines and sends to Apache Spline.
    
    This tracker captures:
    - Task execution flow
    - Data dependencies between tasks
    - Input/output datasets
    - Transformation operations
    - Execution metrics
    """
    
    def __init__(self, config: Optional[SplineConfig] = None):
        """
        Initialize the Spline lineage tracker.
        
        Args:
            config: Spline configuration. If None, loads from environment.
        """
        self.config = config or SplineConfig.from_env()
        self.logger = logging.getLogger(__name__)
        
        # Track current pipeline execution
        self.current_plan: Optional[ExecutionPlan] = None
        self.task_operations: Dict[str, Operation] = {}
        self.artifact_datasets: Dict[str, Dataset] = {}
        self.execution_start_time: Optional[datetime] = None
        self.execution_metrics: Dict[str, Any] = defaultdict(dict)
        
        # Batch events if configured
        self.event_batch: List[LineageEvent] = []
        
        if self.config.enabled:
            self.logger.info(f"Spline lineage tracking enabled. Server: {self.config.spline_url}")
        else:
            self.logger.info("Spline lineage tracking disabled")
    
    def start_pipeline(self, pipeline_name: str, pipeline_metadata: Dict[str, Any]) -> None:
        """
        Start tracking a new pipeline execution.
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_metadata: Additional metadata about the pipeline
        """
        if not self.config.enabled:
            return
        
        self.logger.debug(f"Starting lineage tracking for pipeline: {pipeline_name}")
        
        # Create new execution plan
        self.current_plan = ExecutionPlan(
            name=pipeline_name,
            systemInfo={
                "name": self.config.application_name,
                "version": self.config.application_version
            },
            agentInfo={
                "name": "AirPipe Spline Agent",
                "version": "1.0.0"
            },
            extraInfo={
                "environment": self.config.environment,
                "pipeline_metadata": pipeline_metadata,
                **self.config.custom_metadata
            }
        )
        
        self.execution_start_time = datetime.now()
        self.task_operations.clear()
        self.artifact_datasets.clear()
        self.execution_metrics.clear()
    
    def track_task_start(self, task_name: str, task_type: str, 
                        dependencies: List[str], metadata: Dict[str, Any]) -> None:
        """
        Track the start of a task execution.
        
        Args:
            task_name: Name of the task
            task_type: Type of task (extractor, transformer, loader)
            dependencies: List of task dependencies
            metadata: Additional task metadata
        """
        if not self.config.enabled or not self.current_plan:
            return
        
        self.logger.debug(f"Tracking task start: {task_name}")
        
        # Record task start time
        self.execution_metrics[task_name]['start_time'] = datetime.now()
        self.execution_metrics[task_name]['type'] = task_type
        self.execution_metrics[task_name]['dependencies'] = dependencies
    
    def track_task_complete(self, task_name: str, result: Any, 
                          input_artifacts: List[str], 
                          output_artifact: Optional[str]) -> None:
        """
        Track the completion of a task execution.
        
        Args:
            task_name: Name of the task
            result: Task execution result
            input_artifacts: List of input artifact names
            output_artifact: Name of output artifact (if any)
        """
        if not self.config.enabled or not self.current_plan:
            return
        
        self.logger.debug(f"Tracking task complete: {task_name}")
        
        # Calculate execution time
        if task_name in self.execution_metrics:
            start_time = self.execution_metrics[task_name].get('start_time')
            if start_time:
                duration = (datetime.now() - start_time).total_seconds()
                self.execution_metrics[task_name]['duration'] = duration
        
        # Create operation for this task
        task_type = self.execution_metrics[task_name].get('type', 'transformer')
        
        # Map input artifacts to dataset IDs
        input_ids = []
        for artifact_name in input_artifacts:
            if artifact_name not in self.artifact_datasets:
                # Create dataset for input artifact
                dataset = Dataset(
                    name=artifact_name,
                    source=DataSource(
                        uri=f"airpipe://artifact/{artifact_name}",
                        type="memory",
                        format="unknown"
                    )
                )
                self.artifact_datasets[artifact_name] = dataset
                self.current_plan.datasets.append(dataset)
            input_ids.append(self.artifact_datasets[artifact_name].id)
        
        # Create dataset for output artifact
        output_ids = []
        if output_artifact:
            if output_artifact not in self.artifact_datasets:
                dataset = Dataset(
                    name=output_artifact,
                    source=DataSource(
                        uri=f"airpipe://artifact/{output_artifact}",
                        type="memory",
                        format="unknown"
                    )
                )
                self.artifact_datasets[output_artifact] = dataset
                self.current_plan.datasets.append(dataset)
            output_ids.append(self.artifact_datasets[output_artifact].id)
        
        # Create operation
        operation = SplineModelBuilder.create_operation_from_task(
            task_name=task_name,
            task_type=task_type,
            input_artifacts=input_ids,
            output_artifacts=output_ids
        )
        
        # Add execution metrics to operation params
        if self.config.capture_execution_time:
            operation.params['duration_seconds'] = self.execution_metrics[task_name].get('duration', 0)
        
        self.task_operations[task_name] = operation
        self.current_plan.operations.append(operation)
    
    def track_artifact_created(self, artifact_name: str, artifact_data: Any, 
                             metadata: Dict[str, Any]) -> None:
        """
        Track the creation of a data artifact.
        
        Args:
            artifact_name: Name of the artifact
            artifact_data: The actual data (for schema extraction)
            metadata: Artifact metadata
        """
        if not self.config.enabled or not self.current_plan:
            return
        
        self.logger.debug(f"Tracking artifact created: {artifact_name}")
        
        # Create or update dataset for this artifact
        if artifact_name not in self.artifact_datasets:
            dataset = Dataset(
                name=artifact_name,
                source=DataSource(
                    uri=f"airpipe://artifact/{artifact_name}",
                    type="memory",
                    format=metadata.get('format', 'unknown')
                )
            )
            
            # Extract schema if configured and data is available
            if self.config.capture_schemas:
                schema = self._extract_schema(artifact_data)
                if schema:
                    dataset.schema = schema
            
            self.artifact_datasets[artifact_name] = dataset
            
            # Add to current plan if not already there
            if dataset not in self.current_plan.datasets:
                self.current_plan.datasets.append(dataset)
        
        # Update metadata
        if self.config.capture_row_counts and hasattr(artifact_data, '__len__'):
            try:
                row_count = len(artifact_data)
                self.execution_metrics[artifact_name]['row_count'] = row_count
            except:
                pass
    
    def end_pipeline(self, success: bool = True, error: Optional[str] = None) -> None:
        """
        End tracking for the current pipeline and send lineage to Spline.
        
        Args:
            success: Whether the pipeline completed successfully
            error: Error message if pipeline failed
        """
        if not self.config.enabled or not self.current_plan:
            return
        
        self.logger.debug("Ending pipeline lineage tracking")
        
        # Calculate total execution time
        duration = None
        if self.execution_start_time:
            duration = int((datetime.now() - self.execution_start_time).total_seconds() * 1000)
        
        # Create execution event
        execution_event = ExecutionEvent(
            planId=self.current_plan.id,
            duration=duration,
            error=error if not success else None,
            extra={
                "success": success,
                "task_count": len(self.task_operations),
                "artifact_count": len(self.artifact_datasets),
                **self.execution_metrics
            }
        )
        
        # Create lineage event
        lineage_event = LineageEvent(
            executionPlan=self.current_plan,
            executionEvent=execution_event
        )
        
        # Send or batch the event
        if self.config.batch_size > 1:
            self.event_batch.append(lineage_event)
            if len(self.event_batch) >= self.config.batch_size:
                self._send_batch()
        else:
            self._send_event(lineage_event)
        
        # Reset for next pipeline
        self.current_plan = None
        self.execution_start_time = None
    
    def _extract_schema(self, data: Any) -> Optional[Schema]:
        """
        Extract schema information from data.
        
        Args:
            data: The data to extract schema from
            
        Returns:
            Schema object or None
        """
        try:
            import pandas as pd
            
            if isinstance(data, pd.DataFrame):
                attributes = []
                for col_name, dtype in data.dtypes.items():
                    attributes.append(Attribute(
                        name=str(col_name),
                        dataType=str(dtype),
                        nullable=data[col_name].isnull().any()
                    ))
                return Schema(attributes=attributes)
            
            elif hasattr(data, 'schema'):  # Spark DataFrame
                attributes = []
                for field in data.schema.fields:
                    attributes.append(Attribute(
                        name=field.name,
                        dataType=str(field.dataType),
                        nullable=field.nullable
                    ))
                return Schema(attributes=attributes)
            
        except Exception as e:
            self.logger.debug(f"Could not extract schema: {e}")
        
        return None
    
    def _send_event(self, event: LineageEvent) -> bool:
        """
        Send a single lineage event to Spline.
        
        Args:
            event: The lineage event to send
            
        Returns:
            True if successful, False otherwise
        """
        if not self.config.enabled:
            return False
        
        url = self.config.producer_url
        headers = self.config.get_headers()
        
        for attempt in range(self.config.retry_count):
            try:
                self.logger.debug(f"Sending lineage event to Spline (attempt {attempt + 1})")
                
                response = requests.post(
                    url,
                    json=event.to_dict(),
                    headers=headers,
                    timeout=self.config.timeout_seconds,
                    verify=self.config.verify_ssl
                )
                
                if response.status_code == 200:
                    self.logger.info("Successfully sent lineage event to Spline")
                    return True
                else:
                    self.logger.warning(
                        f"Failed to send lineage event. Status: {response.status_code}, "
                        f"Response: {response.text}"
                    )
                    
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Error sending lineage event: {e}")
                
            # Wait before retry
            if attempt < self.config.retry_count - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return False
    
    def _send_batch(self) -> bool:
        """
        Send all batched events to Spline.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.event_batch:
            return True
        
        self.logger.debug(f"Sending batch of {len(self.event_batch)} events to Spline")
        
        success = True
        for event in self.event_batch:
            if not self._send_event(event):
                success = False
        
        self.event_batch.clear()
        return success
    
    def flush(self) -> None:
        """Force send any batched events."""
        if self.event_batch:
            self._send_batch()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - flush any pending events."""
        self.flush()