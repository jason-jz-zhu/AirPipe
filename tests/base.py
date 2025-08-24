"""Base test classes and utilities."""

import unittest
import tempfile
import shutil
from pathlib import Path
from typing import Optional
import pandas as pd
import logging


class BaseTestCase(unittest.TestCase):
    """Base test case with common setup and utilities."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directory for file operations
        self.temp_dir = tempfile.mkdtemp()
        self.temp_path = Path(self.temp_dir)
        
        # Set up logging
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Common test data
        self.sample_df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10.5, 20.3, 30.7, 40.2, 50.9],
            'category': ['A', 'B', 'A', 'C', 'B']
        })
        
    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        if Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
            
    def create_temp_file(self, filename: str, content: str = "") -> Path:
        """
        Create a temporary file for testing.
        
        Args:
            filename: Name of the file
            content: File content
            
        Returns:
            Path to created file
        """
        file_path = self.temp_path / filename
        file_path.write_text(content)
        return file_path
    
    def create_temp_csv(self, filename: str, 
                       df: Optional[pd.DataFrame] = None) -> Path:
        """
        Create a temporary CSV file.
        
        Args:
            filename: Name of the CSV file
            df: DataFrame to save (uses sample_df if None)
            
        Returns:
            Path to created CSV file
        """
        if df is None:
            df = self.sample_df
            
        file_path = self.temp_path / filename
        df.to_csv(file_path, index=False)
        return file_path
    
    def create_temp_json(self, filename: str, data: dict) -> Path:
        """
        Create a temporary JSON file.
        
        Args:
            filename: Name of the JSON file
            data: Dictionary to save as JSON
            
        Returns:
            Path to created JSON file
        """
        import json
        file_path = self.temp_path / filename
        with open(file_path, 'w') as f:
            json.dump(data, f)
        return file_path
    
    def assert_dataframes_equal(self, df1: pd.DataFrame, 
                               df2: pd.DataFrame,
                               check_dtype: bool = True):
        """
        Assert that two DataFrames are equal.
        
        Args:
            df1: First DataFrame
            df2: Second DataFrame
            check_dtype: Whether to check data types
        """
        pd.testing.assert_frame_equal(df1, df2, check_dtype=check_dtype)
    
    def assert_artifacts_equal(self, artifact1, artifact2):
        """
        Assert that two artifacts are equal.
        
        Args:
            artifact1: First artifact
            artifact2: Second artifact
        """
        self.assertEqual(artifact1.name, artifact2.name)
        self.assertEqual(artifact1.metadata.format, artifact2.metadata.format)
        
        # Compare data based on format
        if hasattr(artifact1.data, 'equals'):
            # DataFrame comparison
            self.assertTrue(artifact1.data.equals(artifact2.data))
        else:
            # Other data types
            self.assertEqual(artifact1.data, artifact2.data)


class AsyncTestCase(BaseTestCase):
    """Base test case for async/streaming tests."""
    
    def setUp(self):
        """Set up async test fixtures."""
        super().setUp()
        self.event_log = []
        
    def log_event(self, event: str, data: Optional[dict] = None):
        """Log an event for testing."""
        self.event_log.append({
            'event': event,
            'data': data,
            'timestamp': pd.Timestamp.now()
        })
    
    def assert_events_occurred(self, expected_events: list):
        """Assert that expected events occurred in order."""
        actual_events = [e['event'] for e in self.event_log]
        
        for expected in expected_events:
            self.assertIn(expected, actual_events,
                         f"Event '{expected}' not found in log")
            
    def assert_event_order(self, event1: str, event2: str):
        """Assert that event1 occurred before event2."""
        events = [e['event'] for e in self.event_log]
        
        idx1 = events.index(event1)
        idx2 = events.index(event2)
        
        self.assertLess(idx1, idx2,
                       f"Event '{event1}' should occur before '{event2}'")


class PipelineTestCase(BaseTestCase):
    """Base test case for pipeline tests."""
    
    def setUp(self):
        """Set up pipeline test fixtures."""
        super().setUp()
        from airpipe.core.task import TaskPipeline
        self.pipeline = None
        
    def create_test_pipeline(self, name: str = "test_pipeline") -> 'TaskPipeline':
        """Create a test pipeline."""
        from airpipe.core.task import TaskPipeline
        self.pipeline = TaskPipeline(name)
        return self.pipeline
    
    def assert_task_exists(self, task_name: str):
        """Assert that a task exists in the pipeline."""
        self.assertIsNotNone(self.pipeline)
        self.assertIn(task_name, self.pipeline.tasks,
                     f"Task '{task_name}' not found in pipeline")
    
    def assert_task_dependencies(self, task_name: str, 
                                expected_deps: list):
        """Assert task has expected dependencies."""
        self.assertIsNotNone(self.pipeline)
        task = self.pipeline.tasks.get(task_name)
        self.assertIsNotNone(task)
        
        actual_deps = set(task.dependencies)
        expected_deps = set(expected_deps)
        
        self.assertEqual(actual_deps, expected_deps,
                        f"Task '{task_name}' dependencies mismatch")
    
    def assert_artifact_created(self, artifact_name: str):
        """Assert that an artifact was created."""
        self.assertIsNotNone(self.pipeline)
        self.assertIn(artifact_name, self.pipeline.named_artifacts,
                     f"Artifact '{artifact_name}' not found")
    
    def run_pipeline_task(self, task_name: str):
        """Run a specific pipeline task."""
        self.assertIsNotNone(self.pipeline)
        task = self.pipeline.tasks.get(task_name)
        self.assertIsNotNone(task, f"Task '{task_name}' not found")
        
        # Execute the task function
        return task.func()