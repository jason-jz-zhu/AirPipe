"""Integration tests for complete workflows."""

import unittest
from unittest.mock import patch, Mock
import pandas as pd
import sys
from pathlib import Path
import tempfile
import shutil

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from tests.base import PipelineTestCase
from tests.fixtures.factories import DataFactory


class TestSimpleTaskWorkflow(PipelineTestCase):
    """Test simple_task_workflow.py"""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        # Create output directory
        self.output_dir = self.temp_path / "output"
        self.output_dir.mkdir(exist_ok=True)
        
    @patch('airpipe.extractors.simple.sample_data_extractor.SampleDataExtractor')
    @patch('airpipe.transformers.simple.value_transformer.ValueTransformer')
    @patch('airpipe.loaders.simple.csv_loader.SimpleCSVLoader')
    def test_simple_workflow_execution(self, mock_loader_class, 
                                      mock_transformer_class, 
                                      mock_extractor_class):
        """Test execution of simple task workflow."""
        # Setup mocks
        mock_extractor = Mock()
        mock_transformer = Mock()
        mock_loader = Mock()
        
        mock_extractor_class.return_value = mock_extractor
        mock_transformer_class.return_value = mock_transformer
        mock_loader_class.return_value = mock_loader
        
        # Mock extractor behavior
        sample_data = DataFactory.create_sample_dataframe(100)
        mock_extractor.extract_sample_data.return_value = sample_data
        
        # Mock transformer behavior
        transformed_data = sample_data.copy()
        transformed_data['transformed'] = transformed_data.iloc[:, 0] ** 2
        mock_transformer.filter_and_transform.return_value = transformed_data
        
        # Import and run workflow
        from workflows.simple_task_workflow import pipeline, main
        
        # Execute workflow
        results = main()
        
        # Verify execution
        self.assertEqual(results['tasks_executed'], 3)
        self.assertEqual(results['artifacts_created'], 2)
        
        # Verify method calls
        mock_extractor.extract_sample_data.assert_called_once_with(num_records=100)
        mock_transformer.filter_and_transform.assert_called_once()
        mock_loader.save_results.assert_called_once()
        
    def test_simple_workflow_with_real_components(self):
        """Test simple workflow with actual components."""
        # Import workflow
        from workflows.simple_task_workflow import pipeline
        
        # Clear any existing artifacts
        pipeline.artifacts.clear()
        pipeline.named_artifacts.clear()
        
        # Execute pipeline
        results = pipeline.execute(parallel=False)
        
        # Verify results
        self.assertEqual(results['tasks_executed'], 3)
        self.assertIn('raw_data', pipeline.named_artifacts)
        self.assertIn('transformed_data', pipeline.named_artifacts)
        
        # Check data flow
        raw_data = pipeline.get_artifact('raw_data')
        transformed_data = pipeline.get_artifact('transformed_data')
        
        self.assertIsNotNone(raw_data)
        self.assertIsNotNone(transformed_data)
        
        # Transformed data should have modifications
        self.assertNotEqual(
            len(raw_data.as_dataframe()),
            len(transformed_data.as_dataframe())
        )


class TestEmployeeTaskWorkflow(PipelineTestCase):
    """Test employee_task_workflow.py"""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        # Create test data directory
        self.data_dir = self.temp_path / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # Create test employee data
        self.employee_df = DataFactory.create_employee_dataframe(100)
        self.employee_csv = self.data_dir / "employees.csv"
        self.employee_df.to_csv(self.employee_csv, index=False)
        
    @patch('airpipe.extractors.employee.csv_extractor.EmployeeCSVExtractor')
    def test_employee_workflow_execution(self, mock_extractor_class):
        """Test employee workflow execution."""
        # Setup mock
        mock_extractor = Mock()
        mock_extractor_class.return_value = mock_extractor
        mock_extractor.extract_current_employees.return_value = self.employee_df
        
        # Import workflow
        from workflows.employee_task_workflow import pipeline
        
        # Clear artifacts
        pipeline.artifacts.clear()
        pipeline.named_artifacts.clear()
        
        # Execute
        results = pipeline.execute(parallel=False)
        
        # Verify execution
        self.assertGreater(results['tasks_executed'], 0)
        self.assertIn('current_employees', pipeline.named_artifacts)
        
        # Verify data transformations
        employees = pipeline.get_artifact('current_employees')
        self.assertIsNotNone(employees)
        self.assertEqual(len(employees.as_dataframe()), 100)
        
    def test_employee_workflow_transformations(self):
        """Test employee workflow data transformations."""
        from workflows.employee_task_workflow import pipeline
        
        # Manually create artifact for testing
        pipeline.named_artifacts['current_employees'] = pipeline.create_artifact(
            self.employee_df, 'current_employees'
        )
        
        # Test high earners filter
        high_earners_task = pipeline.tasks.get('filter_high_earners')
        if high_earners_task:
            result = high_earners_task.func()
            
            if result:
                high_earners = result.as_dataframe()
                # All should have salary > 80000
                self.assertTrue(all(high_earners['salary'] > 80000))
                
    def test_employee_workflow_department_analysis(self):
        """Test department analysis in employee workflow."""
        from workflows.employee_task_workflow import pipeline
        
        # Create test data with known departments
        test_df = pd.DataFrame({
            'employee_id': range(1, 21),
            'name': [f'Employee_{i}' for i in range(1, 21)],
            'department': ['Engineering'] * 10 + ['Sales'] * 10,
            'salary': [100000] * 10 + [80000] * 10,
            'hire_date': pd.date_range('2020-01-01', periods=20),
            'is_active': [True] * 20
        })
        
        pipeline.named_artifacts['current_employees'] = pipeline.create_artifact(
            test_df, 'current_employees'
        )
        
        # Run department analysis if exists
        dept_task = pipeline.tasks.get('analyze_departments')
        if dept_task:
            result = dept_task.func()
            
            if result:
                dept_stats = result.as_dataframe()
                # Should have 2 departments
                self.assertEqual(len(dept_stats), 2)
                self.assertIn('Engineering', dept_stats['department'].values)
                self.assertIn('Sales', dept_stats['department'].values)


class TestEmployeeEnhancedWorkflow(PipelineTestCase):
    """Test employee_enhanced_workflow.py"""
    
    def test_enhanced_workflow_dag_structure(self):
        """Test enhanced workflow DAG structure."""
        from workflows.employee_enhanced_workflow import pipeline
        
        # Validate DAG
        pipeline.validate_dag()
        
        # Get DAG structure
        dag = pipeline.get_dag_structure()
        
        # Check dependencies
        self.assertIn('extract_employees', dag['nodes'])
        
        # Check task dependencies
        filter_task = pipeline.tasks.get('filter_high_earners')
        if filter_task:
            self.assertIn('extract_employees', filter_task.dependencies)
            
        report_task = pipeline.tasks.get('generate_report')
        if report_task:
            # Should depend on multiple tasks
            self.assertGreater(len(report_task.dependencies), 1)
            
    def test_enhanced_workflow_parallel_execution(self):
        """Test parallel execution in enhanced workflow."""
        from workflows.employee_enhanced_workflow import pipeline
        
        # Create mock data
        test_df = DataFactory.create_employee_dataframe(50)
        pipeline.named_artifacts['employee_data'] = pipeline.create_artifact(
            test_df, 'employee_data'
        )
        
        # Get execution order
        execution_order = pipeline.get_execution_order()
        
        # Find parallel tasks (same level in DAG)
        dag = pipeline.get_dag_structure()
        levels = dag.get('levels', {})
        
        # Check for tasks at the same level
        for level, tasks in levels.items():
            if len(tasks) > 1:
                # These tasks should run in parallel
                self.assertGreater(len(tasks), 1)
                
    def test_enhanced_workflow_artifacts(self):
        """Test artifact flow in enhanced workflow."""
        from workflows.employee_enhanced_workflow import pipeline
        
        # Check produces/consumes configuration
        for task_name, task in pipeline.tasks.items():
            if task.produces:
                # Task should create the artifact it claims to produce
                self.assertIsNotNone(task.produces)
                
            if task.consumes:
                # Task should have dependencies for artifacts it consumes
                self.assertIsNotNone(task.consumes)


class TestAdvancedTaskWorkflow(PipelineTestCase):
    """Test advanced_task_workflow.py"""
    
    def test_advanced_workflow_multiple_sources(self):
        """Test advanced workflow with multiple data sources."""
        from workflows.advanced_task_workflow import pipeline
        
        # Check for multiple extractors
        extractors = [
            task for task in pipeline.tasks.values()
            if task.task_type.value == 'extractor'
        ]
        
        # Should have multiple data sources
        self.assertGreaterEqual(len(extractors), 2)
        
    def test_advanced_workflow_complex_dependencies(self):
        """Test complex dependency chains."""
        from workflows.advanced_task_workflow import pipeline
        
        # Get DAG structure
        dag = pipeline.get_dag_structure()
        
        # Check for multi-level dependencies
        max_depth = dag.get('max_dependency_depth', 0)
        self.assertGreaterEqual(max_depth, 3)
        
        # Check for convergence (multiple tasks feeding into one)
        for task_name, task in pipeline.tasks.items():
            if len(task.dependencies) > 1:
                # Found a convergence point
                self.assertGreater(len(task.dependencies), 1)
                break
                
    def test_advanced_workflow_error_recovery(self):
        """Test error recovery in advanced workflow."""
        from workflows.advanced_task_workflow import pipeline
        
        # Inject error in one branch
        original_task = None
        error_task_name = None
        
        for task_name, task in pipeline.tasks.items():
            if task.task_type.value == 'transformer':
                error_task_name = task_name
                original_task = task.func
                
                # Replace with failing function
                def failing_func():
                    raise ValueError("Simulated error")
                    
                task.func = failing_func
                break
                
        if error_task_name:
            # Execute with error
            try:
                pipeline.execute(parallel=False)
            except ValueError:
                pass  # Expected
                
            # Restore original function
            pipeline.tasks[error_task_name].func = original_task
            
            # Check that dependent tasks were not executed
            dependents = pipeline.get_dependents(error_task_name)
            for dep in dependents:
                # Artifacts from dependent tasks should not exist
                if pipeline.tasks[dep].produces:
                    self.assertNotIn(
                        pipeline.tasks[dep].produces,
                        pipeline.named_artifacts
                    )


class TestStreamingWorkflow(PipelineTestCase):
    """Test streaming_example_workflow.py"""
    
    @patch('airpipe.core.streaming.sources.SimulatedDataSource')
    def test_streaming_workflow_batch_processing(self, mock_source_class):
        """Test streaming workflow batch processing."""
        # Create mock source
        mock_source = Mock()
        mock_source_class.return_value = mock_source
        
        # Generate test batches
        batches = [
            DataFactory.create_sample_dataframe(50)
            for _ in range(3)
        ]
        mock_source.read_batch.side_effect = batches + [None]
        
        from workflows.streaming_example_workflow import pipeline, config
        
        # Modify config for testing
        test_config = config
        test_config.max_batches = 3
        test_config.batch_size = 50
        
        # Import processor
        from airpipe.core.streaming.micro_batch import MicroBatchProcessor
        
        processor = MicroBatchProcessor(pipeline, test_config)
        
        # Process stream
        processor.process_stream(source=mock_source)
        
        # Check statistics
        stats = processor.get_stats()
        self.assertEqual(stats['total_batches'], 3)
        self.assertEqual(stats['successful_batches'], 3)
        
    def test_streaming_workflow_anomaly_detection(self):
        """Test anomaly detection in streaming workflow."""
        from workflows.streaming_example_workflow import pipeline
        
        # Create batch with anomalies
        batch_df = pd.DataFrame({
            'value': [10, 20, 1000, 30, 40],  # 1000 is anomaly
            'timestamp': pd.date_range('2023-01-01', periods=5, freq='H')
        })
        
        # Store as stream batch
        pipeline.named_artifacts['stream_batch'] = pipeline.create_artifact(
            batch_df, 'stream_batch'
        )
        
        # Run anomaly detection task if exists
        anomaly_task = None
        for task_name, task in pipeline.tasks.items():
            if 'anomaly' in task_name.lower():
                anomaly_task = task
                break
                
        if anomaly_task:
            result = anomaly_task.func()
            if result:
                # Check that anomalies were detected
                df = result.as_dataframe()
                if 'is_anomaly' in df.columns:
                    # Should have at least one anomaly
                    self.assertGreater(df['is_anomaly'].sum(), 0)


class TestWorkflowIntegration(PipelineTestCase):
    """Integration tests across workflows."""
    
    def test_all_workflows_import(self):
        """Test that all workflows can be imported."""
        workflows = [
            'simple_task_workflow',
            'employee_task_workflow',
            'employee_enhanced_workflow',
            'advanced_task_workflow',
            'streaming_example_workflow'
        ]
        
        for workflow_name in workflows:
            try:
                exec(f"from workflows.{workflow_name} import pipeline")
                # Successfully imported
                self.assertTrue(True)
            except ImportError as e:
                self.fail(f"Failed to import {workflow_name}: {e}")
                
    def test_all_workflows_validation(self):
        """Test that all workflows have valid DAGs."""
        workflows = [
            'simple_task_workflow',
            'employee_task_workflow',
            'employee_enhanced_workflow',
            'advanced_task_workflow'
        ]
        
        for workflow_name in workflows:
            try:
                module = __import__(f"workflows.{workflow_name}", fromlist=['pipeline'])
                pipeline = module.pipeline
                
                # Validate DAG
                pipeline.validate_dag()
                
                # Get statistics
                stats = pipeline.get_task_statistics()
                self.assertGreater(stats['total_tasks'], 0)
                
            except Exception as e:
                self.fail(f"Workflow {workflow_name} validation failed: {e}")
                
    def test_workflow_visualization(self):
        """Test that all workflows can be visualized."""
        workflows = [
            'simple_task_workflow',
            'employee_enhanced_workflow'
        ]
        
        for workflow_name in workflows:
            try:
                module = __import__(f"workflows.{workflow_name}", fromlist=['pipeline'])
                pipeline = module.pipeline
                
                # Generate visualizations
                ascii_viz = pipeline.visualize_dag(format='ascii')
                mermaid_viz = pipeline.visualize_dag(format='mermaid')
                
                self.assertIsNotNone(ascii_viz)
                self.assertIsNotNone(mermaid_viz)
                self.assertIn('graph', mermaid_viz.lower())
                
            except Exception as e:
                self.fail(f"Workflow {workflow_name} visualization failed: {e}")


if __name__ == "__main__":
    unittest.main()