"""
Tests for Apache Spline integration with AirPipe.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import json

from airpipe.core.task import TaskPipeline
from airpipe.lineage.spline_tracker import SplineLineageTracker
from airpipe.lineage.config import SplineConfig
from airpipe.lineage.models import (
    ExecutionPlan, ExecutionEvent, LineageEvent,
    Operation, Dataset, DataSource, OperationType
)


class TestSplineConfig:
    """Test Spline configuration management."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = SplineConfig()
        
        assert config.spline_url == "http://localhost:8080"
        assert config.producer_api_path == "/producer/v1/lineage"
        assert config.enabled == True
        assert config.capture_schemas == True
        assert config.capture_row_counts == True
    
    def test_config_from_env(self, monkeypatch):
        """Test loading configuration from environment variables."""
        monkeypatch.setenv("SPLINE_URL", "http://test-spline:9090")
        monkeypatch.setenv("SPLINE_AUTH_TOKEN", "test-token")
        monkeypatch.setenv("SPLINE_ENABLED", "false")
        monkeypatch.setenv("SPLINE_ENVIRONMENT", "testing")
        
        config = SplineConfig.from_env()
        
        assert config.spline_url == "http://test-spline:9090"
        assert config.auth_token == "test-token"
        assert config.auth_enabled == True
        assert config.enabled == False
        assert config.environment == "testing"
    
    def test_producer_url(self):
        """Test producer URL construction."""
        config = SplineConfig(
            spline_url="http://example.com",
            producer_api_path="/api/lineage"
        )
        
        assert config.producer_url == "http://example.com/api/lineage"
    
    def test_get_headers(self):
        """Test HTTP headers generation."""
        # Without auth
        config = SplineConfig(auth_enabled=False)
        headers = config.get_headers()
        
        assert headers['Content-Type'] == 'application/json'
        assert 'Authorization' not in headers
        
        # With auth
        config = SplineConfig(auth_enabled=True, auth_token="bearer-token")
        headers = config.get_headers()
        
        assert headers['Authorization'] == 'Bearer bearer-token'


class TestSplineModels:
    """Test Spline data models."""
    
    def test_operation_creation(self):
        """Test creating Spline operations."""
        op = Operation(
            name="transform_data",
            type=OperationType.TRANSFORMATION,
            inputIds=["input1", "input2"],
            outputIds=["output1"]
        )
        
        op_dict = op.to_dict()
        
        assert op_dict['name'] == "transform_data"
        assert op_dict['type'] == "Transformation"
        assert op_dict['childIds'] == ["input1", "input2"]
        assert op_dict['outputIds'] == ["output1"]
    
    def test_dataset_creation(self):
        """Test creating Spline datasets."""
        dataset = Dataset(
            name="employee_data",
            source=DataSource(
                uri="airpipe://artifact/employee_data",
                type="memory",
                format="pandas"
            )
        )
        
        ds_dict = dataset.to_dict()
        
        assert ds_dict['name'] == "employee_data"
        assert ds_dict['uri'] == "airpipe://artifact/employee_data"
        assert ds_dict['type'] == "memory"
    
    def test_execution_plan(self):
        """Test execution plan creation."""
        plan = ExecutionPlan(
            name="test_pipeline",
            operations=[
                Operation(name="op1", type=OperationType.READ),
                Operation(name="op2", type=OperationType.TRANSFORMATION)
            ],
            datasets=[
                Dataset(name="ds1"),
                Dataset(name="ds2")
            ]
        )
        
        plan_dict = plan.to_dict()
        
        assert plan_dict['name'] == "test_pipeline"
        assert len(plan_dict['operations']['other']) == 2
        assert len(plan_dict['datasets']) == 2


class TestSplineLineageTracker:
    """Test Spline lineage tracker."""
    
    @pytest.fixture
    def tracker(self):
        """Create a tracker with mocked config."""
        config = SplineConfig(enabled=True)
        return SplineLineageTracker(config)
    
    def test_tracker_initialization(self, tracker):
        """Test tracker initialization."""
        assert tracker.config.enabled == True
        assert tracker.current_plan is None
        assert len(tracker.task_operations) == 0
        assert len(tracker.artifact_datasets) == 0
    
    def test_start_pipeline(self, tracker):
        """Test starting pipeline tracking."""
        tracker.start_pipeline("test_pipeline", {"version": "1.0"})
        
        assert tracker.current_plan is not None
        assert tracker.current_plan.name == "test_pipeline"
        assert tracker.current_plan.extraInfo["pipeline_metadata"]["version"] == "1.0"
        assert tracker.execution_start_time is not None
    
    def test_track_task_execution(self, tracker):
        """Test tracking task execution."""
        # Start pipeline
        tracker.start_pipeline("test_pipeline", {})
        
        # Track task start
        tracker.track_task_start(
            task_name="extract_data",
            task_type="extractor",
            dependencies=[],
            metadata={"source": "csv"}
        )
        
        assert "extract_data" in tracker.execution_metrics
        assert tracker.execution_metrics["extract_data"]["type"] == "extractor"
        
        # Track task complete
        tracker.track_task_complete(
            task_name="extract_data",
            result=pd.DataFrame({"col1": [1, 2, 3]}),
            input_artifacts=[],
            output_artifact="raw_data"
        )
        
        assert "extract_data" in tracker.task_operations
        assert "raw_data" in tracker.artifact_datasets
    
    def test_track_artifact_with_schema(self, tracker):
        """Test tracking artifact with schema extraction."""
        tracker.start_pipeline("test_pipeline", {})
        
        # Create a DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", None],
            "score": [95.5, 87.3, 92.1]
        })
        
        # Track artifact creation
        tracker.track_artifact_created(
            artifact_name="test_data",
            artifact_data=df,
            metadata={"format": "pandas"}
        )
        
        # Check dataset was created
        assert "test_data" in tracker.artifact_datasets
        dataset = tracker.artifact_datasets["test_data"]
        
        # Check schema was extracted
        if tracker.config.capture_schemas:
            assert dataset.schema is not None
            assert len(dataset.schema.attributes) == 3
    
    @patch('requests.post')
    def test_send_event(self, mock_post, tracker):
        """Test sending event to Spline."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # Create a simple event
        plan = ExecutionPlan(name="test")
        event = ExecutionEvent(planId=plan.id)
        lineage_event = LineageEvent(
            executionPlan=plan,
            executionEvent=event
        )
        
        # Send event
        result = tracker._send_event(lineage_event)
        
        assert result == True
        mock_post.assert_called_once()
        
        # Check request details
        call_args = mock_post.call_args
        assert call_args[1]['json']['executionPlan']['name'] == "test"
    
    @patch('requests.post')
    def test_end_pipeline(self, mock_post, tracker):
        """Test ending pipeline and sending lineage."""
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # Start and end pipeline
        tracker.start_pipeline("test_pipeline", {})
        tracker.end_pipeline(success=True)
        
        # Check that event was sent
        mock_post.assert_called_once()
        
        # Verify pipeline was reset
        assert tracker.current_plan is None
        assert tracker.execution_start_time is None


class TestPipelineIntegration:
    """Test integration with AirPipe TaskPipeline."""
    
    @patch('requests.post')
    def test_pipeline_with_lineage_tracking(self, mock_post):
        """Test running a pipeline with lineage tracking."""
        # Mock successful Spline response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        
        # Create tracker and pipeline
        config = SplineConfig(enabled=True)
        tracker = SplineLineageTracker(config)
        pipeline = TaskPipeline("test_pipeline", lineage_tracker=tracker)
        
        # Define simple workflow
        @pipeline.task(produces="data")
        def extract():
            df = pd.DataFrame({"value": [1, 2, 3]})
            return pipeline.create_artifact(df, "data")
        
        @pipeline.task(depends_on=["extract"], consumes="data", produces="result")
        def transform():
            data = pipeline.get_artifact("data")
            df = data.as_dataframe()
            df["doubled"] = df["value"] * 2
            return pipeline.create_artifact(df, "result")
        
        # Execute pipeline
        results = pipeline.execute(parallel=False)
        
        # Verify execution
        assert results["status"] == "completed"
        assert results["tasks_executed"] == 2
        
        # Verify lineage was sent to Spline
        assert mock_post.called
        
        # Check the lineage event structure
        call_args = mock_post.call_args
        lineage_data = call_args[1]['json']
        
        assert lineage_data['executionPlan']['name'] == "test_pipeline"
        assert 'executionEvent' in lineage_data
    
    def test_pipeline_without_lineage(self):
        """Test that pipeline works without lineage tracking."""
        # Create pipeline without tracker
        pipeline = TaskPipeline("test_pipeline", lineage_tracker=None)
        
        @pipeline.task()
        def simple_task():
            return {"result": "success"}
        
        # Execute pipeline
        results = pipeline.execute()
        
        assert results["status"] == "completed"
        assert results["tasks_executed"] == 1


class TestDisabledTracking:
    """Test behavior when tracking is disabled."""
    
    def test_disabled_tracker(self):
        """Test that disabled tracker doesn't track."""
        config = SplineConfig(enabled=False)
        tracker = SplineLineageTracker(config)
        
        # These should all be no-ops
        tracker.start_pipeline("test", {})
        assert tracker.current_plan is None
        
        tracker.track_task_start("task1", "extractor", [], {})
        assert len(tracker.execution_metrics) == 0
        
        tracker.track_task_complete("task1", None, [], None)
        assert len(tracker.task_operations) == 0
        
        tracker.end_pipeline(success=True)
        # Should complete without error


if __name__ == "__main__":
    pytest.main([__file__, "-v"])