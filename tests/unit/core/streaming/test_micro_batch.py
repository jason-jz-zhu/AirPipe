"""Tests for micro-batch streaming processor."""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import time
import threading
from pathlib import Path

from airpipe.core.streaming.micro_batch import (
    StreamConfig, MicroBatchProcessor, StreamingStats
)
from airpipe.core.task import TaskPipeline
from tests.base import AsyncTestCase
from tests.fixtures.mocks import MockDataSource
from tests.fixtures.factories import DataFactory, PipelineFactory


class TestStreamConfig(unittest.TestCase):
    """Test StreamConfig dataclass."""
    
    def test_default_config(self):
        """Test default configuration values."""
        config = StreamConfig()
        
        self.assertEqual(config.batch_size, 1000)
        self.assertEqual(config.batch_interval, 10.0)
        self.assertIsNone(config.max_batches)
        self.assertEqual(config.error_strategy, "continue")
        self.assertEqual(config.retry_attempts, 3)
        
    def test_custom_config(self):
        """Test custom configuration."""
        config = StreamConfig(
            batch_size=500,
            batch_interval=5.0,
            max_batches=100,
            error_strategy="stop",
            enable_monitoring=False
        )
        
        self.assertEqual(config.batch_size, 500)
        self.assertEqual(config.batch_interval, 5.0)
        self.assertEqual(config.max_batches, 100)
        self.assertEqual(config.error_strategy, "stop")
        self.assertFalse(config.enable_monitoring)
        
    def test_config_validation(self):
        """Test configuration validation."""
        # Negative batch size should raise
        with self.assertRaises(ValueError):
            StreamConfig(batch_size=-1)
            
        # Invalid error strategy should raise
        with self.assertRaises(ValueError):
            StreamConfig(error_strategy="invalid")
            
        # Negative retry attempts should raise
        with self.assertRaises(ValueError):
            StreamConfig(retry_attempts=-1)


class TestStreamingStats(unittest.TestCase):
    """Test StreamingStats class."""
    
    def test_stats_initialization(self):
        """Test stats initialization."""
        stats = StreamingStats()
        
        self.assertEqual(stats.total_records, 0)
        self.assertEqual(stats.total_batches, 0)
        self.assertEqual(stats.successful_batches, 0)
        self.assertEqual(stats.failed_batches, 0)
        self.assertIsNone(stats.start_time)
        
    def test_record_batch(self):
        """Test recording batch statistics."""
        stats = StreamingStats()
        stats.start()
        
        # Record successful batch
        stats.record_batch(batch_size=100, success=True, processing_time=0.5)
        
        self.assertEqual(stats.total_batches, 1)
        self.assertEqual(stats.total_records, 100)
        self.assertEqual(stats.successful_batches, 1)
        self.assertEqual(stats.failed_batches, 0)
        self.assertEqual(len(stats.processing_times), 1)
        
        # Record failed batch
        stats.record_batch(batch_size=50, success=False)
        
        self.assertEqual(stats.total_batches, 2)
        self.assertEqual(stats.total_records, 150)
        self.assertEqual(stats.successful_batches, 1)
        self.assertEqual(stats.failed_batches, 1)
        
    def test_throughput_calculation(self):
        """Test throughput calculation."""
        stats = StreamingStats()
        
        # No start time
        self.assertEqual(stats.get_throughput(), 0.0)
        
        stats.start()
        stats.record_batch(batch_size=1000, success=True)
        
        # Sleep to simulate time passing
        time.sleep(0.1)
        
        throughput = stats.get_throughput()
        self.assertGreater(throughput, 0)
        self.assertLess(throughput, 20000)  # Should be less than 20k/sec
        
    def test_average_batch_time(self):
        """Test average batch processing time."""
        stats = StreamingStats()
        
        # No processing times
        self.assertEqual(stats.get_average_batch_time(), 0.0)
        
        # Add processing times
        stats.record_batch(100, True, 0.5)
        stats.record_batch(100, True, 0.7)
        stats.record_batch(100, True, 0.3)
        
        avg_time = stats.get_average_batch_time()
        self.assertAlmostEqual(avg_time, 0.5, places=1)
        
    def test_get_summary(self):
        """Test getting summary statistics."""
        stats = StreamingStats()
        stats.start()
        
        stats.record_batch(100, True, 0.5)
        stats.record_batch(150, True, 0.6)
        stats.record_batch(50, False, 0.3)
        
        summary = stats.get_summary()
        
        self.assertEqual(summary['total_records'], 300)
        self.assertEqual(summary['total_batches'], 3)
        self.assertEqual(summary['successful_batches'], 2)
        self.assertEqual(summary['failed_batches'], 1)
        self.assertIn('throughput_per_sec', summary)
        self.assertIn('avg_batch_time', summary)
        self.assertEqual(summary['avg_batch_size'], 100)


class TestDataBuffer(unittest.TestCase):
    """Test DataBuffer class."""
    
    def test_buffer_initialization(self):
        """Test buffer initialization."""
        buffer = DataBuffer(max_size=1000)
        
        self.assertEqual(buffer.max_size, 1000)
        self.assertEqual(len(buffer), 0)
        self.assertTrue(buffer.is_empty())
        
    def test_add_and_get_batch(self):
        """Test adding and retrieving batches."""
        buffer = DataBuffer(max_size=1000)
        
        # Add data
        df1 = DataFactory.create_sample_dataframe(50)
        buffer.add(df1)
        
        self.assertEqual(len(buffer), 50)
        self.assertFalse(buffer.is_empty())
        
        # Get batch
        batch = buffer.get_batch(30)
        self.assertEqual(len(batch), 30)
        self.assertEqual(len(buffer), 20)
        
        # Get remaining
        batch2 = buffer.get_batch(50)
        self.assertEqual(len(batch2), 20)
        self.assertTrue(buffer.is_empty())
        
    def test_buffer_overflow(self):
        """Test buffer overflow protection."""
        buffer = DataBuffer(max_size=100)
        
        # Try to add more than max size
        df = DataFactory.create_sample_dataframe(150)
        
        with self.assertRaises(ValueError):
            buffer.add(df)
            
    def test_clear_buffer(self):
        """Test clearing buffer."""
        buffer = DataBuffer(max_size=1000)
        
        df = DataFactory.create_sample_dataframe(100)
        buffer.add(df)
        
        self.assertEqual(len(buffer), 100)
        
        buffer.clear()
        
        self.assertEqual(len(buffer), 0)
        self.assertTrue(buffer.is_empty())
        
    def test_concurrent_access(self):
        """Test thread-safe buffer operations."""
        buffer = DataBuffer(max_size=10000)
        errors = []
        
        def add_data():
            try:
                for _ in range(10):
                    df = DataFactory.create_sample_dataframe(50)
                    buffer.add(df)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
                
        def get_data():
            try:
                for _ in range(10):
                    if not buffer.is_empty():
                        batch = buffer.get_batch(25)
                    time.sleep(0.01)
            except Exception as e:
                errors.append(e)
                
        # Run concurrent operations
        threads = []
        for _ in range(3):
            t1 = threading.Thread(target=add_data)
            t2 = threading.Thread(target=get_data)
            threads.extend([t1, t2])
            
        for t in threads:
            t.start()
            
        for t in threads:
            t.join()
            
        # Should complete without errors
        self.assertEqual(len(errors), 0)


class TestMicroBatchProcessor(AsyncTestCase):
    """Test MicroBatchProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.pipeline = self.create_simple_streaming_pipeline()
        self.config = StreamConfig(
            batch_size=50,
            batch_interval=0.1,
            max_batches=5,
            enable_monitoring=True
        )
        
    def create_simple_streaming_pipeline(self):
        """Create a simple pipeline for streaming tests."""
        pipeline = TaskPipeline("streaming_test")
        
        @pipeline.task(consumes="stream_batch", produces="processed_batch")
        def process_batch():
            batch = pipeline.get_artifact("stream_batch")
            df = batch.as_dataframe()
            # Simple processing
            df['processed'] = df.iloc[:, 0] * 2
            return pipeline.create_artifact(df, "processed_batch")
            
        @pipeline.task(
            depends_on=["process_batch"],
            consumes="processed_batch"
        )
        def save_batch():
            batch = pipeline.get_artifact("processed_batch")
            # Simulate saving
            self.log_event("batch_saved", {'size': len(batch.as_dataframe())})
            
        return pipeline
        
    def test_processor_initialization(self):
        """Test processor initialization."""
        processor = MicroBatchProcessor(self.pipeline, self.config)
        
        self.assertEqual(processor.pipeline, self.pipeline)
        self.assertEqual(processor.config, self.config)
        self.assertIsNotNone(processor.stats)
        self.assertIsNotNone(processor.buffer)
        
    def test_process_single_batch(self):
        """Test processing a single batch."""
        processor = MicroBatchProcessor(self.pipeline, self.config)
        
        # Create test batch
        batch_df = DataFactory.create_sample_dataframe(30)
        
        # Process batch
        result = processor._process_batch(batch_df)
        
        self.assertTrue(result)
        self.assert_events_occurred(["batch_saved"])
        
    def test_stream_processing(self):
        """Test processing a stream of data."""
        config = StreamConfig(
            batch_size=20,
            batch_interval=0.05,
            max_batches=3,
            enable_monitoring=False
        )
        processor = MicroBatchProcessor(self.pipeline, config)
        
        # Create mock source
        source = MockDataSource(
            total_records=60,
            batch_size=20,
            delay=0.01
        )
        
        # Process stream
        processor.process_stream(source=source)
        
        # Check stats
        stats = processor.get_stats()
        self.assertEqual(stats['total_batches'], 3)
        self.assertEqual(stats['total_records'], 60)
        self.assertEqual(stats['successful_batches'], 3)
        
    def test_error_handling_continue(self):
        """Test error handling with continue strategy."""
        config = StreamConfig(
            batch_size=10,
            max_batches=3,
            error_strategy="continue"
        )
        
        # Create pipeline with failing task
        pipeline = TaskPipeline("error_test")
        
        @pipeline.task(consumes="stream_batch")
        def failing_task():
            batch = pipeline.get_artifact("stream_batch")
            # Fail on second batch
            if hasattr(failing_task, 'call_count'):
                failing_task.call_count += 1
            else:
                failing_task.call_count = 1
                
            if failing_task.call_count == 2:
                raise ValueError("Task failed!")
                
            return batch
            
        processor = MicroBatchProcessor(pipeline, config)
        
        source = MockDataSource(total_records=30, batch_size=10, delay=0)
        
        # Should continue despite error
        processor.process_stream(source=source)
        
        stats = processor.get_stats()
        self.assertEqual(stats['total_batches'], 3)
        self.assertEqual(stats['failed_batches'], 1)
        self.assertEqual(stats['successful_batches'], 2)
        
    def test_error_handling_stop(self):
        """Test error handling with stop strategy."""
        config = StreamConfig(
            batch_size=10,
            max_batches=5,
            error_strategy="stop"
        )
        
        # Create pipeline with failing task
        pipeline = TaskPipeline("error_stop_test")
        
        @pipeline.task(consumes="stream_batch")
        def failing_task():
            raise ValueError("Task failed!")
            
        processor = MicroBatchProcessor(pipeline, config)
        
        source = MockDataSource(total_records=50, batch_size=10, delay=0)
        
        # Should stop on first error
        with self.assertRaises(ValueError):
            processor.process_stream(source=source)
            
        stats = processor.get_stats()
        self.assertEqual(stats['failed_batches'], 1)
        
    def test_retry_mechanism(self):
        """Test retry mechanism for failed batches."""
        config = StreamConfig(
            batch_size=10,
            max_batches=2,
            error_strategy="retry",
            retry_attempts=3,
            retry_delay=0.01
        )
        
        # Create pipeline with task that fails then succeeds
        pipeline = TaskPipeline("retry_test")
        
        @pipeline.task(consumes="stream_batch")
        def flaky_task():
            batch = pipeline.get_artifact("stream_batch")
            
            if not hasattr(flaky_task, 'attempts'):
                flaky_task.attempts = {}
                
            batch_id = id(batch)
            if batch_id not in flaky_task.attempts:
                flaky_task.attempts[batch_id] = 0
                
            flaky_task.attempts[batch_id] += 1
            
            # Fail first attempt, succeed on retry
            if flaky_task.attempts[batch_id] == 1:
                raise ValueError("Temporary failure")
                
            return batch
            
        processor = MicroBatchProcessor(pipeline, config)
        
        source = MockDataSource(total_records=20, batch_size=10, delay=0)
        
        processor.process_stream(source=source)
        
        stats = processor.get_stats()
        # Should eventually succeed after retries
        self.assertEqual(stats['successful_batches'], 2)
        
    def test_checkpoint_and_recovery(self):
        """Test checkpointing and recovery."""
        checkpoint_dir = self.temp_path / "checkpoints"
        checkpoint_dir.mkdir()
        
        config = StreamConfig(
            batch_size=10,
            max_batches=5,
            checkpoint_interval=2,
            enable_checkpointing=True,
            checkpoint_dir=str(checkpoint_dir)
        )
        
        processor = MicroBatchProcessor(self.pipeline, config)
        
        source = MockDataSource(total_records=50, batch_size=10, delay=0)
        
        # Process some batches
        processor.process_stream(source=source)
        
        # Check checkpoints were created
        checkpoints = list(checkpoint_dir.glob("*.checkpoint"))
        self.assertGreater(len(checkpoints), 0)
        
        # Test recovery from checkpoint
        processor2 = MicroBatchProcessor(self.pipeline, config)
        
        # Load checkpoint
        latest_checkpoint = max(checkpoints, key=lambda p: p.stat().st_mtime)
        state = processor2._load_checkpoint(latest_checkpoint)
        
        self.assertIsNotNone(state)
        self.assertIn('batch_count', state)
        self.assertIn('total_records', state)
        
    def test_backpressure_handling(self):
        """Test backpressure handling."""
        config = StreamConfig(
            batch_size=100,
            backpressure_threshold=200,
            enable_monitoring=False
        )
        
        # Create slow pipeline
        pipeline = TaskPipeline("backpressure_test")
        
        @pipeline.task(consumes="stream_batch")
        def slow_task():
            batch = pipeline.get_artifact("stream_batch")
            time.sleep(0.1)  # Simulate slow processing
            return batch
            
        processor = MicroBatchProcessor(pipeline, config)
        
        # Fast source
        source = MockDataSource(
            total_records=500,
            batch_size=100,
            delay=0.01  # Fast data generation
        )
        
        # Should handle backpressure without buffer overflow
        processor.process_stream(source=source)
        
        # Buffer should never exceed threshold
        self.assertLessEqual(processor.buffer.max_size, 
                           config.backpressure_threshold * 2)


class TestStreamIntegration(AsyncTestCase):
    """Integration tests for streaming components."""
    
    def test_end_to_end_streaming(self):
        """Test complete streaming pipeline."""
        # Create realistic pipeline
        pipeline = TaskPipeline("integration_test")
        
        @pipeline.task(consumes="stream_batch", produces="validated")
        def validate():
            batch = pipeline.get_artifact("stream_batch")
            df = batch.as_dataframe()
            # Remove invalid records
            valid = df[df.iloc[:, 0] > 0]
            return pipeline.create_artifact(valid, "validated")
            
        @pipeline.task(
            depends_on=["validate"],
            consumes="validated",
            produces="enriched"
        )
        def enrich():
            batch = pipeline.get_artifact("validated")
            df = batch.as_dataframe()
            # Add computed column
            df['computed'] = df.iloc[:, 0] * 1.5
            return pipeline.create_artifact(df, "enriched")
            
        @pipeline.task(
            depends_on=["enrich"],
            consumes="enriched"
        )
        def store():
            batch = pipeline.get_artifact("enriched")
            # Simulate storage
            self.log_event("stored", {'records': len(batch.as_dataframe())})
            
        # Configure streaming
        config = StreamConfig(
            batch_size=100,
            batch_interval=0.05,
            max_batches=10,
            enable_monitoring=True
        )
        
        processor = MicroBatchProcessor(pipeline, config)
        
        # Create data source
        source = MockDataSource(
            total_records=1000,
            batch_size=100,
            delay=0.02
        )
        
        # Process stream
        processor.process_stream(source=source)
        
        # Verify results
        stats = processor.get_stats()
        self.assertEqual(stats['total_batches'], 10)
        self.assertEqual(stats['successful_batches'], 10)
        self.assertGreater(stats['throughput_per_sec'], 0)
        
        # Check events
        self.assert_events_occurred(['stored'])
        stored_events = [e for e in self.event_log if e['event'] == 'stored']
        self.assertEqual(len(stored_events), 10)


if __name__ == "__main__":
    unittest.main()