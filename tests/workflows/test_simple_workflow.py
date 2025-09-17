"""Integration tests for simple_task_workflow."""

import unittest
from unittest.mock import patch, Mock
import pandas as pd
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from tests.base import PipelineTestCase
from tests.fixtures.factories import DataFactory


class TestSimpleWorkflow(PipelineTestCase):
    """Test simple_task_workflow end-to-end."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        # Create output directory
        self.output_dir = self.temp_path / "output"
        self.output_dir.mkdir(exist_ok=True)
        
    def test_workflow_structure(self):
        """Test that workflow has correct structure."""
        from workflows.simple_task_workflow import pipeline
        
        # Check tasks are registered
        self.assertIn('extract', pipeline.tasks)
        self.assertIn('transform', pipeline.tasks)
        self.assertIn('load', pipeline.tasks)
        
        # Check dependencies
        transform_task = pipeline.tasks['transform']
        self.assertIn('extract', transform_task.dependencies)
        
        load_task = pipeline.tasks['load']
        self.assertIn('transform', load_task.dependencies)
        
    def test_workflow_data_flow(self):
        """Test data flow through the workflow."""
        from workflows.simple_task_workflow import pipeline
        
        # Clear any existing artifacts
        pipeline.artifacts.clear()
        pipeline.named_artifacts.clear()
        
        # Execute pipeline
        results = pipeline.execute(parallel=False)
        
        # Check execution results
        self.assertEqual(results['tasks_executed'], 3)
        self.assertIn('raw_data', pipeline.named_artifacts)
        self.assertIn('transformed_data', pipeline.named_artifacts)
        
    def test_extract_task(self):
        """Test the extract task independently."""
        from workflows.simple_task_workflow import pipeline, extractor
        
        # Mock the extractor
        with patch.object(extractor, 'extract_sample_data') as mock_extract:
            mock_extract.return_value = DataFactory.create_sample_dataframe(50)
            
            # Run extract task
            extract_task = pipeline.tasks['extract']
            result = extract_task.func()
            
            # Verify
            mock_extract.assert_called_once_with(num_records=100)
            self.assertIsNotNone(result)
            self.assertEqual(result.name, 'raw_data')
            
    def test_transform_task(self):
        """Test the transform task independently."""
        from workflows.simple_task_workflow import pipeline, transformer
        
        # Create test data
        test_df = DataFactory.create_sample_dataframe(100)
        pipeline.named_artifacts['raw_data'] = pipeline.create_artifact(
            test_df, 'raw_data'
        )
        
        # Mock the transformer
        with patch.object(transformer, 'filter_and_transform') as mock_transform:
            transformed_df = test_df[test_df.iloc[:, 0] > 50]
            mock_transform.return_value = transformed_df
            
            # Run transform task
            transform_task = pipeline.tasks['transform']
            result = transform_task.func()
            
            # Verify
            mock_transform.assert_called_once()
            self.assertIsNotNone(result)
            self.assertEqual(result.name, 'transformed_data')
            
    def test_load_task(self):
        """Test the load task independently."""
        from workflows.simple_task_workflow import pipeline, loader
        
        # Create test data
        test_df = DataFactory.create_sample_dataframe(50)
        pipeline.named_artifacts['transformed_data'] = pipeline.create_artifact(
            test_df, 'transformed_data'
        )
        
        # Mock the loader
        with patch.object(loader, 'save_results') as mock_save:
            # Run load task
            load_task = pipeline.tasks['load']
            load_task.func()
            
            # Verify
            mock_save.assert_called_once()
            args = mock_save.call_args[0]
            pd.testing.assert_frame_equal(args[0], test_df)
            self.assertEqual(args[1], 'output/simple_output.csv')
            
    def test_workflow_with_error_handling(self):
        """Test workflow error handling."""
        from workflows.simple_task_workflow import pipeline, transformer
        
        # Inject error in transformer
        with patch.object(transformer, 'filter_and_transform') as mock_transform:
            mock_transform.side_effect = ValueError("Transform failed")
            
            # Clear artifacts
            pipeline.artifacts.clear()
            pipeline.named_artifacts.clear()
            
            # Should raise error
            with self.assertRaises(ValueError) as ctx:
                pipeline.execute()
                
            self.assertIn("Transform failed", str(ctx.exception))
            
            # Load task should not have been executed
            self.assertNotIn('transformed_data', pipeline.named_artifacts)
            
    def test_workflow_parallel_execution(self):
        """Test that independent tasks can run in parallel."""
        from workflows.simple_task_workflow import pipeline
        
        # Clear artifacts
        pipeline.artifacts.clear()
        pipeline.named_artifacts.clear()
        
        # Execute with parallel flag
        results = pipeline.execute(parallel=True)
        
        # Should complete successfully
        self.assertEqual(results['tasks_executed'], 3)
        
    def test_workflow_artifact_persistence(self):
        """Test that artifacts are properly persisted."""
        from workflows.simple_task_workflow import pipeline
        
        # Clear artifacts
        pipeline.artifacts.clear()
        pipeline.named_artifacts.clear()
        
        # Execute pipeline
        pipeline.execute()
        
        # Check artifacts exist and have data
        raw_data = pipeline.get_artifact('raw_data')
        self.assertIsNotNone(raw_data)
        self.assertGreater(len(raw_data.as_dataframe()), 0)
        
        transformed_data = pipeline.get_artifact('transformed_data')
        self.assertIsNotNone(transformed_data)
        self.assertGreater(len(transformed_data.as_dataframe()), 0)
        
        # Check data relationship
        # Transformed should have less or equal rows (due to filtering)
        self.assertLessEqual(
            len(transformed_data.as_dataframe()),
            len(raw_data.as_dataframe())
        )
        
    def test_workflow_main_function(self):
        """Test the main function entry point."""
        from workflows.simple_task_workflow import main
        
        # Mock components to control execution
        with patch('workflows.simple_task_workflow.extractor') as mock_ext:
            with patch('workflows.simple_task_workflow.transformer') as mock_trans:
                with patch('workflows.simple_task_workflow.loader') as mock_load:
                    # Setup mocks
                    mock_ext.extract_sample_data.return_value = DataFactory.create_sample_dataframe(50)
                    mock_trans.filter_and_transform.return_value = DataFactory.create_sample_dataframe(30)
                    
                    # Run main
                    results = main()
                    
                    # Verify results
                    self.assertIn('tasks_executed', results)
                    self.assertIn('artifacts_created', results)
                    self.assertEqual(results['tasks_executed'], 3)


class TestSimpleWorkflowPerformance(PipelineTestCase):
    """Performance tests for simple workflow."""
    
    def test_large_data_processing(self):
        """Test workflow with large dataset."""
        from workflows.simple_task_workflow import pipeline, extractor, transformer
        
        # Mock with large dataset
        large_df = DataFactory.create_sample_dataframe(10000)
        
        with patch.object(extractor, 'extract_sample_data') as mock_ext:
            with patch.object(transformer, 'filter_and_transform') as mock_trans:
                mock_ext.return_value = large_df
                mock_trans.return_value = large_df[large_df.iloc[:, 0] > 50]
                
                import time
                start = time.time()
                results = pipeline.execute()
                elapsed = time.time() - start
                
                # Should complete in reasonable time
                self.assertLess(elapsed, 5.0)  # 5 seconds max
                self.assertEqual(results['tasks_executed'], 3)


if __name__ == "__main__":
    unittest.main()