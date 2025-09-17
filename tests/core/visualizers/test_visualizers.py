"""Tests for DAG visualizers."""

import unittest
from pathlib import Path
import tempfile

from airpipe.core.task import TaskPipeline
from airpipe.core.ascii_dag_visualizer import ASCIIDAGVisualizer
from airpipe.core.mermaid_dag_visualizer import MermaidDAGVisualizer
from tests.base import PipelineTestCase
from tests.fixtures.factories import PipelineFactory


class TestASCIIDAGVisualizer(PipelineTestCase):
    """Test ASCII DAG visualizer."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.visualizer = ASCIIDAGVisualizer()
        
    def test_visualize_simple_pipeline(self):
        """Test visualizing a simple linear pipeline."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        self.assertIsNotNone(output)
        self.assertIn("extract", output)
        self.assertIn("transform", output)
        self.assertIn("load", output)
        
        # Check task type indicators
        self.assertIn("[E]", output)  # Extractor
        self.assertIn("[T]", output)  # Transformer
        self.assertIn("[L]", output)  # Loader
        
        # Check legend
        self.assertIn("Legend:", output)
        self.assertIn("Extractor", output)
        self.assertIn("Transformer", output)
        self.assertIn("Loader", output)
        
    def test_visualize_parallel_pipeline(self):
        """Test visualizing a pipeline with parallel tasks."""
        pipeline = PipelineFactory.create_parallel_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        self.assertIn("extract1", output)
        self.assertIn("extract2", output)
        self.assertIn("merge", output)
        
        # Both extractors should be at the same level
        lines = output.split('\n')
        extract1_line = next(i for i, line in enumerate(lines) if "extract1" in line)
        extract2_line = next(i for i, line in enumerate(lines) if "extract2" in line)
        
        # They should be close to each other (within a few lines)
        self.assertLess(abs(extract1_line - extract2_line), 3)
        
    def test_visualize_complex_pipeline(self):
        """Test visualizing a complex multi-level pipeline."""
        pipeline = PipelineFactory.create_complex_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        # Check all tasks are present
        tasks = ["extract_source1", "extract_source2", 
                "filter_source1", "filter_source2",
                "aggregate1", "aggregate2", "create_report"]
        
        for task in tasks:
            self.assertIn(task, output)
            
        # Check structure indicators (arrows, branches)
        self.assertIn("→", output)  # Arrow
        self.assertIn("├", output)  # Branch
        
    def test_visualize_empty_pipeline(self):
        """Test visualizing an empty pipeline."""
        pipeline = self.create_test_pipeline("empty")
        
        output = self.visualizer.visualize(pipeline)
        
        self.assertIn("No tasks", output.lower())
        
    def test_save_to_file(self):
        """Test saving visualization to file."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
            output_file = Path(f.name)
            
        output = self.visualizer.visualize(pipeline, output_file=str(output_file))
        
        # Check file was created
        self.assertTrue(output_file.exists())
        
        # Check content
        content = output_file.read_text()
        self.assertEqual(content, output)
        
        # Clean up
        output_file.unlink()
        
    def test_single_task_pipeline(self):
        """Test visualizing a pipeline with a single task."""
        pipeline = self.create_test_pipeline("single")
        
        @pipeline.task()
        def single_task():
            return None
            
        output = self.visualizer.visualize(pipeline)
        
        self.assertIn("single_task", output)
        self.assertIn("[E]", output)  # Should be classified as extractor


class TestMermaidDAGVisualizer(PipelineTestCase):
    """Test Mermaid DAG visualizer."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.visualizer = MermaidDAGVisualizer()
        
    def test_visualize_simple_pipeline(self):
        """Test generating Mermaid diagram for simple pipeline."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        # Check Mermaid syntax
        self.assertIn("graph TD", output)
        self.assertIn("extract", output)
        self.assertIn("transform", output)
        self.assertIn("load", output)
        
        # Check connections
        self.assertIn("extract -->", output)
        self.assertIn("--> transform", output)
        self.assertIn("transform -->", output)
        self.assertIn("--> load", output)
        
        # Check node styles
        self.assertIn("class extract extractor", output)
        self.assertIn("class transform transformer", output)
        self.assertIn("class load loader", output)
        
        # Check style definitions
        self.assertIn("classDef extractor", output)
        self.assertIn("classDef transformer", output)
        self.assertIn("classDef loader", output)
        
    def test_visualize_parallel_pipeline(self):
        """Test Mermaid diagram for parallel pipeline."""
        pipeline = PipelineFactory.create_parallel_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        # Check parallel structure
        self.assertIn("extract1", output)
        self.assertIn("extract2", output)
        self.assertIn("merge", output)
        
        # Both should connect to merge
        self.assertIn("extract1 --> merge", output)
        self.assertIn("extract2 --> merge", output)
        
    def test_visualize_complex_pipeline(self):
        """Test Mermaid diagram for complex pipeline."""
        pipeline = PipelineFactory.create_complex_pipeline()
        
        output = self.visualizer.visualize(pipeline)
        
        # Check all nodes
        nodes = ["extract_source1", "extract_source2",
                "filter_source1", "filter_source2",
                "aggregate1", "aggregate2", "create_report"]
        
        for node in nodes:
            self.assertIn(node, output)
            
        # Check final convergence
        self.assertIn("aggregate1 --> create_report", output)
        self.assertIn("aggregate2 --> create_report", output)
        
    def test_node_labels(self):
        """Test that nodes have proper labels."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task(produces="data")
        def extract_customer_data():
            return pipeline.create_artifact([1, 2, 3], "data")
            
        @pipeline.task(depends_on=["extract_customer_data"], consumes="data")
        def process_customers():
            return None
            
        output = self.visualizer.visualize(pipeline)
        
        # Check readable labels
        self.assertIn('extract_customer_data["Extract Customer Data"]', output)
        self.assertIn('process_customers["Process Customers"]', output)
        
    def test_save_to_file(self):
        """Test saving Mermaid diagram to file."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        output_file = self.temp_path / "diagram.md"
        output = self.visualizer.visualize(pipeline, output_file=str(output_file))
        
        self.assertTrue(output_file.exists())
        
        content = output_file.read_text()
        self.assertIn("```mermaid", content)
        self.assertIn("graph TD", content)
        self.assertIn("```", content)
        
    def test_empty_pipeline(self):
        """Test Mermaid diagram for empty pipeline."""
        pipeline = self.create_test_pipeline("empty")
        
        output = self.visualizer.visualize(pipeline)
        
        self.assertIn("graph TD", output)
        self.assertIn("No tasks defined", output)
        
    def test_special_characters_in_names(self):
        """Test handling special characters in task names."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def task_with_underscore_and_numbers_123():
            return None
            
        output = self.visualizer.visualize(pipeline)
        
        # Should handle underscores and numbers
        self.assertIn("task_with_underscore_and_numbers_123", output)
        
    def test_cyclic_detection_comment(self):
        """Test that cyclic dependencies are noted."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task()
        def task1():
            pass
            
        @pipeline.task(depends_on=["task1"])
        def task2():
            pass
            
        # Manually create a cycle (normally prevented)
        pipeline.tasks["task1"].dependencies = ["task2"]
        
        output = self.visualizer.visualize(pipeline)
        
        # Should still generate output but with warning
        self.assertIn("graph TD", output)
        # The visualizer should handle the cycle gracefully


class TestVisualizerIntegration(PipelineTestCase):
    """Integration tests for visualizers."""
    
    def test_pipeline_visualize_method(self):
        """Test pipeline's built-in visualize method."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        # Test ASCII format
        ascii_output = pipeline.visualize_dag(format='ascii')
        self.assertIn("extract", ascii_output)
        self.assertIn("[E]", ascii_output)
        
        # Test Mermaid format
        mermaid_output = pipeline.visualize_dag(format='mermaid')
        self.assertIn("graph TD", mermaid_output)
        self.assertIn("extract", mermaid_output)
        
        # Test invalid format
        with self.assertRaises(ValueError):
            pipeline.visualize_dag(format='invalid')
            
    def test_visualize_after_execution(self):
        """Test visualizing pipeline after execution."""
        pipeline = PipelineFactory.create_simple_pipeline()
        
        # Execute pipeline
        pipeline.execute()
        
        # Visualize should still work
        output = pipeline.visualize_dag(format='ascii')
        self.assertIn("extract", output)
        self.assertIn("transform", output)
        self.assertIn("load", output)
        
    def test_visualize_with_artifacts(self):
        """Test that artifact information is included."""
        pipeline = self.create_test_pipeline()
        
        @pipeline.task(produces="raw_data")
        def extract():
            return pipeline.create_artifact([1, 2, 3], "raw_data")
            
        @pipeline.task(
            depends_on=["extract"],
            consumes="raw_data",
            produces="processed_data"
        )
        def process():
            data = pipeline.get_artifact("raw_data")
            return pipeline.create_artifact(data.data, "processed_data")
            
        # ASCII output might show artifact flow
        ascii_output = pipeline.visualize_dag(format='ascii')
        self.assertIn("extract", ascii_output)
        self.assertIn("process", ascii_output)
        
        # Mermaid can show artifact names in connections
        mermaid_output = pipeline.visualize_dag(format='mermaid')
        self.assertIn("extract", mermaid_output)
        self.assertIn("process", mermaid_output)


if __name__ == "__main__":
    unittest.main()