"""Tests for TaskPipeline and task decorator."""

import unittest
from unittest.mock import Mock, patch
import pandas as pd
import pytest

from airpipe.core.task import TaskPipeline, Task, TaskType
from airpipe.artifacts.data_artifact import DataArtifact
from tests.base import PipelineTestCase
from tests.fixtures.factories import DataFactory, PipelineFactory


class TestTaskPipeline(PipelineTestCase):
    """Test TaskPipeline class."""
    
    def test_pipeline_creation(self):
        """Test creating a new pipeline."""
        pipeline = self.create_test_pipeline("test_pipeline")
        
        self.assertEqual(pipeline.name, "test_pipeline")
        self.assertEqual(len(pipeline.tasks), 0)
        self.assertEqual(len(pipeline.artifacts), 0)
        
    def test_task_registration(self):
        """Test registering tasks with decorator."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def extract_data():
            return pipeline.create_artifact([1, 2, 3], "data")
        
        self.assert_task_exists("extract_data")
        task = pipeline.tasks["extract_data"]
        self.assertEqual(task.name, "extract_data")
        self.assertEqual(task.task_type, TaskType.EXTRACTOR)
        
    def test_task_with_explicit_dependencies(self):
        """Test task with explicit dependencies."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task(produces="raw_data")
        def extract():
            return pipeline.create_artifact([1, 2, 3], "raw_data")
        
        @pipeline.task(
            depends_on=["extract"],
            consumes="raw_data",
            produces="transformed_data"
        )
        def transform():
            raw = pipeline.get_artifact("raw_data")
            return pipeline.create_artifact(raw.data, "transformed_data")
        
        self.assert_task_exists("extract")
        self.assert_task_exists("transform")
        self.assert_task_dependencies("transform", ["extract"])
        
        transform_task = pipeline.tasks["transform"]
        self.assertEqual(transform_task.produces, "transformed_data")
        self.assertEqual(transform_task.consumes, ["raw_data"])
        
    def test_task_type_inference(self):
        """Test automatic task type inference."""
        pipeline = self.create_test_pipeline()
        
        # Extractor (no artifact input)
        @pipeline.task()
        def extractor():
            return pipeline.create_artifact([1, 2, 3], "data")
        
        # Transformer (takes artifact, returns artifact)
        @pipeline.task()
        def transformer(data):
            return pipeline.create_artifact(data.data, "transformed")
        
        # Loader (takes artifact, returns None)
        @pipeline.task()
        def loader(data):
            print(f"Loading {data}")
            return None
        
        self.assertEqual(pipeline.tasks["extractor"].task_type, TaskType.EXTRACTOR)
        self.assertEqual(pipeline.tasks["transformer"].task_type, TaskType.TRANSFORMER)
        self.assertEqual(pipeline.tasks["loader"].task_type, TaskType.LOADER)
        
    def test_implicit_dependencies(self):
        """Test implicit dependencies from function parameters."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def task1():
            return pipeline.create_artifact([1, 2, 3], "result1")
        
        @pipeline.task()
        def task2(result1):
            return pipeline.create_artifact(result1.data, "result2")
        
        @pipeline.task()
        def task3(result1, result2):
            return None
        
        self.assert_task_dependencies("task1", [])
        self.assert_task_dependencies("task2", ["task1"])
        self.assert_task_dependencies("task3", ["task1", "task2"])
        
    def test_create_artifact(self):
        """Test creating artifacts."""
        pipeline = self.create_test_pipeline()
        
        # Create DataFrame artifact
        df = DataFactory.create_sample_dataframe(10)
        artifact = pipeline.create_artifact(df, "test_df")
        
        self.assertIsInstance(artifact, DataArtifact)
        self.assertEqual(artifact.name, "test_df")
        self.assertIn("test_df", pipeline.named_artifacts)
        
        # Create dict artifact
        dict_data = {"key": "value"}
        artifact2 = pipeline.create_artifact(dict_data, "test_dict")
        
        self.assertEqual(artifact2.name, "test_dict")
        self.assertIn("test_dict", pipeline.named_artifacts)
        
    def test_get_artifact(self):
        """Test retrieving artifacts."""
        pipeline = self.create_test_pipeline()
        
        # Create and store artifact
        data = [1, 2, 3, 4, 5]
        original = pipeline.create_artifact(data, "test_data")
        
        # Retrieve artifact
        retrieved = pipeline.get_artifact("test_data")
        
        self.assertEqual(retrieved.name, original.name)
        self.assertEqual(retrieved.data, original.data)
        
        # Test non-existent artifact
        with self.assertRaises(KeyError):
            pipeline.get_artifact("non_existent")
            
    def test_get_dependencies(self):
        """Test getting task dependencies."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def task1():
            pass
        
        @pipeline.task(depends_on=["task1"])
        def task2():
            pass
        
        @pipeline.task(depends_on=["task1", "task2"])
        def task3():
            pass
        
        deps1 = pipeline.get_dependencies("task1")
        deps2 = pipeline.get_dependencies("task2")
        deps3 = pipeline.get_dependencies("task3")
        
        self.assertEqual(deps1, [])
        self.assertEqual(deps2, ["task1"])
        self.assertEqual(set(deps3), {"task1", "task2"})
        
    def test_get_dependents(self):
        """Test getting task dependents."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        extract_deps = pipeline.get_dependents("extract")
        transform_deps = pipeline.get_dependents("transform")
        load_deps = pipeline.get_dependents("load")
        
        self.assertIn("transform", extract_deps)
        self.assertIn("load", transform_deps)
        self.assertEqual(load_deps, [])
        
    def test_topological_sort(self):
        """Test topological sorting of tasks."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        sorted_tasks = pipeline.get_execution_order()
        
        # Extract should come before transform
        extract_idx = sorted_tasks.index("extract")
        transform_idx = sorted_tasks.index("transform")
        load_idx = sorted_tasks.index("load")
        
        self.assertLess(extract_idx, transform_idx)
        self.assertLess(transform_idx, load_idx)
        
    def test_validate_dag(self):
        """Test DAG validation."""
        pipeline = self.create_test_pipeline()
        
        # Valid DAG
        @pipeline.task()
        def task1():
            pass
        
        @pipeline.task(depends_on=["task1"])
        def task2():
            pass
        
        # Should not raise
        pipeline.validate_dag()
        
        # Create cycle manually (normally not possible through decorator)
        pipeline.tasks["task1"].dependencies = ["task2"]
        
        with self.assertRaises(ValueError) as ctx:
            pipeline.validate_dag()
        self.assertIn("cycle", str(ctx.exception).lower())
        
    def test_execute_pipeline(self):
        """Test pipeline execution."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        results = pipeline.execute(parallel=False)
        
        self.assertIn("tasks_executed", results)
        self.assertIn("artifacts_created", results)
        
        # Check tasks were executed
        self.assertEqual(results["tasks_executed"], 3)
        
        # Check artifacts were created
        self.assertIn("raw_data", pipeline.named_artifacts)
        self.assertIn("transformed_data", pipeline.named_artifacts)
        
    def test_parallel_execution(self):
        """Test parallel pipeline execution."""
        pipeline = PipelineFactory.create_parallel_pipeline()
        
        results = pipeline.execute(parallel=True)
        
        # Both extractors should run in parallel
        self.assertEqual(results["tasks_executed"], 3)
        
        # Check final artifact
        self.assertIn("merged_data", pipeline.named_artifacts)
        merged = pipeline.get_artifact("merged_data")
        self.assertEqual(len(merged.as_dataframe()), 70)  # 30 + 40 rows
        
    def test_error_handling(self):
        """Test error handling during execution."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def failing_task():
            raise ValueError("Task failed!")
        
        @pipeline.task(depends_on=["failing_task"])
        def dependent_task():
            return pipeline.create_artifact([1, 2, 3], "data")
        
        with self.assertRaises(ValueError):
            pipeline.execute()
            
        # Dependent task should not have been executed
        self.assertNotIn("data", pipeline.named_artifacts)
        
    def test_get_task_statistics(self):
        """Test getting pipeline statistics."""
        pipeline = PipelineFactory.create_complex_pipeline()
        
        stats = pipeline.get_task_statistics()
        
        self.assertIn("total_tasks", stats)
        self.assertIn("task_types", stats)
        self.assertIn("max_dependency_depth", stats)
        
        self.assertEqual(stats["total_tasks"], 7)
        self.assertEqual(stats["task_types"]["extractor"], 2)
        self.assertEqual(stats["task_types"]["transformer"], 4)
        self.assertEqual(stats["task_types"]["loader"], 1)
        
    def test_multiple_consumers(self):
        """Test task consuming multiple artifacts."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task(produces="data1")
        def source1():
            return pipeline.create_artifact([1, 2, 3], "data1")
        
        @pipeline.task(produces="data2")
        def source2():
            return pipeline.create_artifact([4, 5, 6], "data2")
        
        @pipeline.task(
            depends_on=["source1", "source2"],
            consumes=["data1", "data2"],
            produces="combined"
        )
        def combine():
            d1 = pipeline.get_artifact("data1")
            d2 = pipeline.get_artifact("data2")
            combined = d1.data + d2.data
            return pipeline.create_artifact(combined, "combined")
        
        results = pipeline.execute()
        
        combined = pipeline.get_artifact("combined")
        self.assertEqual(combined.data, [1, 2, 3, 4, 5, 6])
        
    def test_dag_structure(self):
        """Test getting DAG structure."""
        pipeline = PipelineFactory.create_complex_pipeline()
        
        dag = pipeline.get_dag_structure()
        
        self.assertIn("nodes", dag)
        self.assertIn("edges", dag)
        self.assertIn("execution_order", dag)
        
        # Check node count
        self.assertEqual(len(dag["nodes"]), 7)
        
        # Check edges
        edges = dag["edges"]
        # Convert edge dict format to tuples for checking
        edge_tuples = [(e['from'], e['to']) for e in edges]
        self.assertIn(("extract_source1", "filter_source1"), edge_tuples)
        self.assertIn(("filter_source1", "aggregate1"), edge_tuples)
        self.assertIn(("aggregate1", "create_report"), edge_tuples)


class TestTaskClass(unittest.TestCase):
    """Test Task dataclass."""
    
    def test_task_creation(self):
        """Test creating a Task instance."""
        func = lambda: None
        task = Task(
            name="test_task",
            func=func,
            task_type=TaskType.EXTRACTOR,
            dependencies=["dep1", "dep2"],
            produces="output",
            consumes="input"
        )
        
        self.assertEqual(task.name, "test_task")
        self.assertEqual(task.func, func)
        self.assertEqual(task.task_type, TaskType.EXTRACTOR)
        self.assertEqual(task.dependencies, ["dep1", "dep2"])
        self.assertEqual(task.produces, "output")
        self.assertEqual(task.consumes, ["input"])  # Normalized to list
        
    def test_task_consumes_normalization(self):
        """Test that consumes is normalized to list."""
        task1 = Task(
            name="task1",
            func=lambda: None,
            task_type=TaskType.TRANSFORMER,
            consumes="single_input"
        )
        self.assertEqual(task1.consumes, ["single_input"])
        
        task2 = Task(
            name="task2",
            func=lambda: None,
            task_type=TaskType.TRANSFORMER,
            consumes=["input1", "input2"]
        )
        self.assertEqual(task2.consumes, ["input1", "input2"])


class TestTaskIntegration(PipelineTestCase):
    """Integration tests for task execution."""
    
    def test_full_etl_pipeline(self):
        """Test a complete ETL pipeline."""
        pipeline = self.create_test_pipeline("etl_pipeline")
        
        # Extract
        @pipeline.task(produces="raw_data")
        def extract():
            df = DataFactory.create_employee_dataframe(100)
            return pipeline.create_artifact(df, "raw_data")
        
        # Transform - filter high earners
        @pipeline.task(
            depends_on=["extract"],
            consumes="raw_data",
            produces="high_earners"
        )
        def filter_high_earners():
            data = pipeline.get_artifact("raw_data")
            df = data.as_dataframe()
            filtered = df[df['salary'] > 80000]
            return pipeline.create_artifact(filtered, "high_earners")
        
        # Transform - aggregate by department
        @pipeline.task(
            depends_on=["filter_high_earners"],
            consumes="high_earners",
            produces="dept_summary"
        )
        def summarize_by_dept():
            data = pipeline.get_artifact("high_earners")
            df = data.as_dataframe()
            summary = df.groupby('department').agg({
                'salary': 'mean',
                'employee_id': 'count'
            }).reset_index()
            summary.columns = ['department', 'avg_salary', 'count']
            return pipeline.create_artifact(summary, "dept_summary")
        
        # Load
        @pipeline.task(
            depends_on=["summarize_by_dept"],
            consumes="dept_summary"
        )
        def save_report():
            data = pipeline.get_artifact("dept_summary")
            df = data.as_dataframe()
            # Simulate saving
            output_path = self.temp_path / "report.csv"
            df.to_csv(output_path, index=False)
            return None
        
        # Execute pipeline
        results = pipeline.execute(parallel=True)
        
        # Verify execution
        self.assertEqual(results["tasks_executed"], 4)
        self.assertEqual(results["artifacts_created"], 3)
        
        # Verify output file
        output_path = self.temp_path / "report.csv"
        self.assertTrue(output_path.exists())
        
        # Verify data
        summary = pd.read_csv(output_path)
        self.assertGreater(len(summary), 0)
        self.assertIn('department', summary.columns)
        self.assertIn('avg_salary', summary.columns)


if __name__ == "__main__":
    unittest.main()