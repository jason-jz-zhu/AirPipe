"""
Micro-batch streaming support for AirPipe pipelines.
Run existing pipelines continuously on streaming data.
"""

import time
import threading
from queue import Queue, Empty
from typing import Optional, Callable, Any, Dict, List, Generator, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
import pandas as pd
from pathlib import Path

from airpipe.core.task import TaskPipeline
from airpipe.artifacts.data_artifact import DataArtifact

logger = logging.getLogger(__name__)


@dataclass
class StreamConfig:
    """Configuration for streaming execution."""
    batch_size: int = 1000
    batch_interval: float = 10.0  # seconds
    max_batches: Optional[int] = None  # None = run forever
    checkpoint_interval: int = 100  # Checkpoint every N batches
    error_strategy: str = "continue"  # "continue", "stop", "retry"
    retry_attempts: int = 3
    retry_delay: float = 1.0  # seconds between retries
    backpressure_threshold: int = 10000  # Max items in buffer
    enable_monitoring: bool = True
    enable_checkpointing: bool = False
    checkpoint_dir: Optional[str] = None


class StreamingStats:
    """Statistics collector for streaming operations."""
    
    def __init__(self):
        self.total_records = 0
        self.total_batches = 0
        self.successful_batches = 0
        self.failed_batches = 0
        self.start_time = None
        self.last_batch_time = None
        self.processing_times = []
        self.batch_sizes = []
        
    def start(self):
        """Start timing."""
        self.start_time = datetime.now()
        self.last_batch_time = self.start_time
        
    def record_batch(self, batch_size: int, success: bool = True, processing_time: float = 0):
        """Record batch statistics."""
        self.total_batches += 1
        self.total_records += batch_size
        self.batch_sizes.append(batch_size)
        
        if success:
            self.successful_batches += 1
        else:
            self.failed_batches += 1
            
        if processing_time > 0:
            self.processing_times.append(processing_time)
            
        self.last_batch_time = datetime.now()
    
    def get_throughput(self) -> float:
        """Calculate records per second."""
        if not self.start_time:
            return 0.0
        
        elapsed = (datetime.now() - self.start_time).total_seconds()
        if elapsed == 0:
            return 0.0
            
        return self.total_records / elapsed
    
    def get_average_batch_time(self) -> float:
        """Get average batch processing time."""
        if not self.processing_times:
            return 0.0
        return sum(self.processing_times) / len(self.processing_times)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics."""
        return {
            'total_records': self.total_records,
            'total_batches': self.total_batches,
            'successful_batches': self.successful_batches,
            'failed_batches': self.failed_batches,
            'throughput_per_sec': self.get_throughput(),
            'avg_batch_time': self.get_average_batch_time(),
            'avg_batch_size': sum(self.batch_sizes) / len(self.batch_sizes) if self.batch_sizes else 0,
            'runtime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        }


class MicroBatchProcessor:
    """Process streaming data through existing pipelines using micro-batches."""
    
    def __init__(self, pipeline: TaskPipeline, config: Optional[StreamConfig] = None):
        """
        Initialize micro-batch processor.
        
        Args:
            pipeline: Existing TaskPipeline to use for processing
            config: Streaming configuration
        """
        self.pipeline = pipeline
        self.config = config or StreamConfig()
        self.buffer = []
        self.buffer_lock = threading.Lock()
        self.is_running = False
        self.processed_batches = 0
        self.stats = StreamingStats()
        self._stop_event = threading.Event()
        self._checkpoint_state = {}
        
        # Backpressure queue
        self.queue = Queue(maxsize=self.config.backpressure_threshold)
        
        logger.info(f"Initialized MicroBatchProcessor for pipeline: {pipeline.name}")
        
    def process_stream(self, 
                      source: 'StreamingSource',
                      transform_batch: Optional[Callable] = None) -> None:
        """
        Process continuous stream through pipeline.
        
        Args:
            source: Streaming data source
            transform_batch: Optional function to transform batch before processing
        """
        self.is_running = True
        self.stats.start()
        self._stop_event.clear()
        
        logger.info("Starting stream processing...")
        
        # Start consumer thread for processing batches
        consumer_thread = threading.Thread(target=self._batch_consumer, args=(transform_batch,))
        consumer_thread.start()
        
        # Start timer thread for time-based batching
        timer_thread = threading.Thread(target=self._batch_timer)
        timer_thread.start()
        
        try:
            # Producer loop - read from source
            for record in source.read():
                if not self.is_running or self._stop_event.is_set():
                    break
                    
                # Check backpressure
                if self.queue.qsize() >= self.config.backpressure_threshold * 0.9:
                    logger.warning("Backpressure detected, slowing down source reading")
                    time.sleep(0.1)
                
                # Add to buffer
                with self.buffer_lock:
                    self.buffer.append(record)
                    
                    # Check if batch is ready (size-based)
                    if len(self.buffer) >= self.config.batch_size:
                        self._flush_buffer()
                        
                # Check max batches limit
                if self.config.max_batches and self.processed_batches >= self.config.max_batches:
                    logger.info(f"Reached max batches limit: {self.config.max_batches}")
                    break
                    
        except KeyboardInterrupt:
            logger.info("Stream processing interrupted by user")
        except Exception as e:
            logger.error(f"Error in stream processing: {e}")
            raise
        finally:
            # Cleanup
            self.stop()
            
            # Wait for threads to finish
            consumer_thread.join(timeout=5)
            timer_thread.join(timeout=1)
            
            # Process remaining buffer
            if self.buffer:
                logger.info(f"Processing remaining {len(self.buffer)} records in buffer")
                self._flush_buffer()
                
            # Final statistics
            logger.info("Stream processing completed")
            self._print_statistics()
    
    def _batch_timer(self):
        """Timer thread for time-based batching."""
        while self.is_running and not self._stop_event.is_set():
            time.sleep(self.config.batch_interval)
            
            with self.buffer_lock:
                if self.buffer:
                    logger.debug(f"Timer triggered batch flush with {len(self.buffer)} records")
                    self._flush_buffer()
    
    def _flush_buffer(self):
        """Flush buffer to processing queue."""
        if not self.buffer:
            return
            
        batch = self.buffer.copy()
        self.buffer.clear()
        
        try:
            self.queue.put(batch, block=False)
            logger.debug(f"Flushed batch of {len(batch)} records to queue")
        except:
            logger.error("Queue is full, dropping batch")
            self.stats.record_batch(len(batch), success=False)
    
    def _batch_consumer(self, transform_batch: Optional[Callable]):
        """Consumer thread that processes batches from queue."""
        while self.is_running or not self.queue.empty():
            try:
                # Get batch from queue with timeout
                batch = self.queue.get(timeout=1.0)
                
                # Process batch
                start_time = time.time()
                success = self._process_batch(batch, transform_batch)
                processing_time = time.time() - start_time
                
                # Record statistics
                self.stats.record_batch(len(batch), success, processing_time)
                self.processed_batches += 1
                
                # Checkpoint if needed
                if (self.config.enable_checkpointing and 
                    self.processed_batches % self.config.checkpoint_interval == 0):
                    self._save_checkpoint()
                    
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Error in batch consumer: {e}")
                if self.config.error_strategy == "stop":
                    self.stop()
                    break
    
    def _process_batch(self, batch: List[Any], transform_batch: Optional[Callable]) -> bool:
        """
        Process a single batch through the pipeline.
        
        Args:
            batch: List of records to process
            transform_batch: Optional transformation function
            
        Returns:
            True if successful, False otherwise
        """
        retry_count = 0
        
        while retry_count <= self.config.retry_attempts:
            try:
                # Transform batch if needed
                if transform_batch:
                    data = transform_batch(batch)
                else:
                    # Default: convert to DataFrame
                    data = pd.DataFrame(batch)
                
                # Create artifact for batch
                batch_artifact = DataArtifact(
                    data=data,
                    name=f"batch_{self.processed_batches}"
                )
                
                # Clear previous artifacts to prevent memory buildup
                self.pipeline.artifacts.clear()
                self.pipeline.named_artifacts.clear()
                
                # Inject batch into pipeline
                self.pipeline.set_artifact("stream_batch", batch_artifact)
                
                # Execute pipeline on batch
                # Note: Pipeline should be designed to work with "stream_batch" artifact
                results = self.pipeline.execute(parallel=False)
                
                logger.info(f"Processed batch {self.processed_batches} with {len(batch)} records")
                return True
                
            except Exception as e:
                retry_count += 1
                logger.error(f"Error processing batch (attempt {retry_count}): {e}")
                
                if retry_count <= self.config.retry_attempts:
                    time.sleep(self.config.retry_delay)
                else:
                    if self.config.error_strategy == "stop":
                        raise
                    elif self.config.error_strategy == "continue":
                        logger.warning("Skipping failed batch and continuing")
                        return False
        
        return False
    
    def stop(self):
        """Stop stream processing."""
        logger.info("Stopping stream processor...")
        self.is_running = False
        self._stop_event.set()
    
    def _save_checkpoint(self):
        """Save checkpoint for recovery."""
        if not self.config.checkpoint_dir:
            return
            
        checkpoint_path = Path(self.config.checkpoint_dir) / f"checkpoint_{self.processed_batches}.json"
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint_data = {
            'processed_batches': self.processed_batches,
            'total_records': self.stats.total_records,
            'timestamp': datetime.now().isoformat(),
            'pipeline_name': self.pipeline.name
        }
        
        import json
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint_data, f)
            
        logger.debug(f"Saved checkpoint at batch {self.processed_batches}")
    
    def _print_statistics(self):
        """Print processing statistics."""
        stats = self.stats.get_summary()
        
        print("\n" + "=" * 60)
        print("Streaming Processing Statistics")
        print("=" * 60)
        print(f"Total Records: {stats['total_records']:,}")
        print(f"Total Batches: {stats['total_batches']:,}")
        print(f"Successful: {stats['successful_batches']:,}")
        print(f"Failed: {stats['failed_batches']:,}")
        print(f"Throughput: {stats['throughput_per_sec']:.2f} records/sec")
        print(f"Avg Batch Time: {stats['avg_batch_time']:.3f} sec")
        print(f"Avg Batch Size: {stats['avg_batch_size']:.1f}")
        print(f"Total Runtime: {stats['runtime_seconds']:.1f} sec")
        print("=" * 60)


class StreamingSource:
    """Abstraction for various streaming data sources."""
    
    def read(self) -> Generator[Any, None, None]:
        """Read from source. Must be implemented by subclasses."""
        raise NotImplementedError
    
    @staticmethod
    def from_generator(generator_func: Callable) -> 'GeneratorSource':
        """Create source from generator function."""
        return GeneratorSource(generator_func)
    
    @staticmethod
    def from_file_watch(directory: str, pattern: str = "*.csv") -> 'FileWatchSource':
        """Watch directory for new files."""
        return FileWatchSource(directory, pattern)
    
    @staticmethod
    def from_csv_stream(file_path: str, chunk_size: int = 1000) -> 'CSVStreamSource':
        """Stream CSV file in chunks."""
        return CSVStreamSource(file_path, chunk_size)


class GeneratorSource(StreamingSource):
    """Source from custom generator function."""
    
    def __init__(self, generator_func: Callable):
        self.generator_func = generator_func
        
    def read(self) -> Generator[Any, None, None]:
        """Read from generator."""
        for item in self.generator_func():
            yield item


class FileWatchSource(StreamingSource):
    """Watch directory for new files."""
    
    def __init__(self, directory: str, pattern: str = "*.csv"):
        self.directory = Path(directory)
        self.pattern = pattern
        self.processed_files = set()
        
    def read(self) -> Generator[pd.DataFrame, None, None]:
        """Watch for new files and yield their contents."""
        logger.info(f"Watching directory: {self.directory} for {self.pattern}")
        
        while True:
            # Find new files
            files = list(self.directory.glob(self.pattern))
            new_files = [f for f in files if f not in self.processed_files]
            
            for file_path in new_files:
                logger.info(f"Processing new file: {file_path}")
                
                try:
                    # Read file based on extension
                    if file_path.suffix.lower() == '.csv':
                        df = pd.read_csv(file_path)
                        # Yield each row as a record
                        for _, row in df.iterrows():
                            yield row.to_dict()
                    
                    self.processed_files.add(file_path)
                    
                except Exception as e:
                    logger.error(f"Error reading file {file_path}: {e}")
            
            # Wait before checking again
            time.sleep(1)


class CSVStreamSource(StreamingSource):
    """Stream CSV file in chunks."""
    
    def __init__(self, file_path: str, chunk_size: int = 1000):
        self.file_path = file_path
        self.chunk_size = chunk_size
        
    def read(self) -> Generator[Dict, None, None]:
        """Read CSV in chunks."""
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=self.chunk_size):
                for _, row in chunk.iterrows():
                    yield row.to_dict()
        except Exception as e:
            logger.error(f"Error reading CSV stream: {e}")
            raise


class WindowedAggregator:
    """Support for time-based windowing operations."""
    
    def __init__(self, processor: MicroBatchProcessor):
        self.processor = processor
        self.windows = {}
        
    def tumbling_window(self, size_seconds: int) -> 'TumblingWindow':
        """Create non-overlapping time windows."""
        return TumblingWindow(size_seconds)
    
    def sliding_window(self, size_seconds: int, slide_seconds: int) -> 'SlidingWindow':
        """Create overlapping time windows."""
        return SlidingWindow(size_seconds, slide_seconds)


class TumblingWindow:
    """Non-overlapping time window."""
    
    def __init__(self, size_seconds: int):
        self.size = timedelta(seconds=size_seconds)
        self.current_window_start = datetime.now()
        self.buffer = []
        
    def add(self, record: Any) -> Optional[List]:
        """Add record to window. Returns window contents if window closes."""
        now = datetime.now()
        
        if now - self.current_window_start >= self.size:
            # Window closed, return contents and start new window
            result = self.buffer.copy()
            self.buffer = [record]
            self.current_window_start = now
            return result
        else:
            # Add to current window
            self.buffer.append(record)
            return None


class SlidingWindow:
    """Overlapping time window."""
    
    def __init__(self, size_seconds: int, slide_seconds: int):
        self.size = timedelta(seconds=size_seconds)
        self.slide = timedelta(seconds=slide_seconds)
        self.buffer = []
        self.last_slide = datetime.now()
        
    def add(self, record: Any) -> Optional[List]:
        """Add record and check if slide interval passed."""
        now = datetime.now()
        self.buffer.append((now, record))
        
        # Remove old records outside window
        cutoff = now - self.size
        self.buffer = [(t, r) for t, r in self.buffer if t > cutoff]
        
        # Check if slide interval passed
        if now - self.last_slide >= self.slide:
            self.last_slide = now
            return [r for t, r in self.buffer]
        
        return None