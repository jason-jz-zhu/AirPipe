"""
Spark Structured Streaming integration for AirPipe.

Provides micro-batch processing using Spark Structured Streaming as an alternative
to the default streaming engine.
"""

import logging
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path

LOG = logging.getLogger(__name__)


@dataclass
class SparkStreamConfig:
    """Configuration for Spark Structured Streaming."""
    
    # Input configuration
    input_path: str  # Path to monitor for new files
    input_format: str = "csv"  # csv, json, parquet
    input_options: Dict[str, Any] = field(default_factory=dict)
    
    # Processing configuration
    trigger_interval: str = "10 seconds"  # Processing trigger interval
    output_mode: str = "append"  # append, complete, update
    
    # Output configuration
    output_path: Optional[str] = None
    output_format: str = "parquet"
    checkpoint_path: str = "/tmp/airpipe_spark_checkpoint"
    
    # Watermark for late data
    watermark_column: Optional[str] = None
    watermark_delay: str = "10 minutes"
    
    # Spark specific
    max_files_per_trigger: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert config to dictionary."""
        return {
            'input_path': self.input_path,
            'input_format': self.input_format,
            'trigger_interval': self.trigger_interval,
            'output_mode': self.output_mode,
            'checkpoint_path': self.checkpoint_path
        }


class SparkMicroBatchProcessor:
    """
    Processor for Spark Structured Streaming micro-batches.
    
    Integrates with AirPipe's TaskPipeline to process streaming data using
    Spark's structured streaming engine.
    """
    
    def __init__(self, pipeline, config: SparkStreamConfig):
        """
        Initialize Spark streaming processor.
        
        Args:
            pipeline: TaskPipeline instance
            config: Spark streaming configuration
        """
        self.pipeline = pipeline
        self.config = config
        self.spark = None
        self.stream_df = None
        self.query = None
        
    def start(self):
        """Start the Spark streaming job."""
        from airpipe.utils.spark import SparkSessionManager
        
        # Get or create Spark session
        self.spark = SparkSessionManager.get_or_create({
            'app_name': f'AirPipe Stream - {self.pipeline.name}',
            'config': {
                'spark.sql.streaming.checkpointLocation': self.config.checkpoint_path,
                'spark.sql.adaptive.enabled': 'false',  # Disable AQE for streaming
            }
        })
        
        LOG.info(f"Starting Spark streaming from: {self.config.input_path}")
        
        # Create streaming DataFrame based on input format
        self._create_stream_dataframe()
        
        # Apply transformations
        processed_df = self._apply_pipeline_transformations(self.stream_df)
        
        # Start the streaming query
        self._start_streaming_query(processed_df)
        
    def _create_stream_dataframe(self):
        """Create streaming DataFrame from input source."""
        reader = self.spark.readStream.format(self.config.input_format)
        
        # Apply input options
        for key, value in self.config.input_options.items():
            reader = reader.option(key, value)
        
        # Set max files per trigger if specified
        if self.config.max_files_per_trigger:
            reader = reader.option("maxFilesPerTrigger", self.config.max_files_per_trigger)
        
        # Common options for different formats
        if self.config.input_format == "csv":
            reader = reader.option("header", "true").option("inferSchema", "true")
        elif self.config.input_format == "json":
            reader = reader.option("multiLine", "true")
        
        self.stream_df = reader.load(self.config.input_path)
        
        # Add watermark if configured
        if self.config.watermark_column:
            from pyspark.sql.functions import col
            self.stream_df = self.stream_df.withWatermark(
                self.config.watermark_column,
                self.config.watermark_delay
            )
        
        LOG.info(f"Created streaming DataFrame with schema: {self.stream_df.schema}")
        
    def _apply_pipeline_transformations(self, stream_df):
        """
        Apply TaskPipeline transformations to streaming DataFrame.
        
        This is where we bridge Spark Streaming with AirPipe's task system.
        """
        # Create artifact from Spark DataFrame
        from airpipe.artifacts.data_artifact import DataArtifact, ArtifactMetadata
        
        # For streaming, we work with the DataFrame directly
        # The pipeline tasks should be designed to work with Spark DataFrames
        stream_artifact = DataArtifact(
            data=stream_df,
            name="spark_stream",
            metadata=ArtifactMetadata(source_component="spark_streaming")
        )
        
        # Store in pipeline artifacts
        self.pipeline.artifacts["spark_stream"] = stream_artifact
        
        # Execute pipeline tasks that are compatible with Spark
        # Note: Tasks should check if input is Spark DataFrame and handle accordingly
        LOG.info("Applying pipeline transformations to stream")
        
        # For now, return the stream as-is
        # In practice, tasks would transform this
        return stream_df
    
    def _start_streaming_query(self, processed_df):
        """Start the streaming query with output sink."""
        writer = processed_df.writeStream \
            .outputMode(self.config.output_mode) \
            .trigger(processingTime=self.config.trigger_interval)
        
        # Configure output based on settings
        if self.config.output_path:
            # File sink
            writer = writer.format(self.config.output_format) \
                .option("path", self.config.output_path) \
                .option("checkpointLocation", self.config.checkpoint_path)
            
            self.query = writer.start()
            LOG.info(f"Started file stream to: {self.config.output_path}")
        else:
            # Console sink for debugging
            writer = writer.format("console") \
                .option("truncate", False)
            
            self.query = writer.start()
            LOG.info("Started console stream output")
        
        LOG.info(f"Stream query ID: {self.query.id}")
        LOG.info(f"Stream query name: {self.query.name}")
        
    def process_batch_function(self, batch_df, batch_id):
        """
        Process each micro-batch using pipeline tasks.
        
        This method can be used with foreachBatch for custom processing.
        
        Args:
            batch_df: DataFrame for this micro-batch
            batch_id: Unique ID for this batch
        """
        LOG.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        # Convert to artifact
        from airpipe.artifacts.data_artifact import DataArtifact, ArtifactMetadata
        
        batch_artifact = DataArtifact(
            data=batch_df,
            name=f"batch_{batch_id}",
            metadata=ArtifactMetadata(
                source_component="spark_streaming",
                tags={"batch_id": batch_id}
            )
        )
        
        # Store in pipeline
        self.pipeline.artifacts["stream_batch"] = batch_artifact
        
        # Execute pipeline tasks
        try:
            results = self.pipeline.execute()
            LOG.info(f"Batch {batch_id} processed successfully")
        except Exception as e:
            LOG.error(f"Error processing batch {batch_id}: {e}")
            raise
    
    def stop(self):
        """Stop the streaming query."""
        if self.query:
            LOG.info("Stopping Spark streaming query")
            self.query.stop()
            self.query = None
    
    def await_termination(self, timeout: Optional[int] = None):
        """
        Wait for streaming query to terminate.
        
        Args:
            timeout: Optional timeout in seconds
        """
        if self.query:
            if timeout:
                self.query.awaitTermination(timeout)
            else:
                self.query.awaitTermination()
    
    def get_status(self) -> Dict[str, Any]:
        """Get current status of streaming query."""
        if not self.query:
            return {"status": "not_started"}
        
        status = self.query.status
        return {
            "status": "running" if self.query.isActive else "stopped",
            "message": status.get("message", ""),
            "is_data_available": status.get("isDataAvailable", False),
            "is_trigger_active": status.get("isTriggerActive", False)
        }
    
    def get_progress(self) -> Dict[str, Any]:
        """Get progress information for the streaming query."""
        if not self.query:
            return {}
        
        progress = self.query.lastProgress
        if progress:
            return {
                "batch_id": progress.get("batchId", 0),
                "input_rows": progress.get("numInputRows", 0),
                "processed_rows": progress.get("processedRowsPerSecond", 0),
                "duration_ms": progress.get("durationMs", {})
            }
        return {}


def create_spark_file_stream(
    input_path: str,
    output_path: str,
    pipeline,
    format: str = "csv",
    trigger_interval: str = "10 seconds"
) -> SparkMicroBatchProcessor:
    """
    Convenience function to create a Spark file stream processor.
    
    Args:
        input_path: Directory to monitor for new files
        output_path: Directory to write processed data
        pipeline: TaskPipeline to apply
        format: File format (csv, json, parquet)
        trigger_interval: How often to process new data
        
    Returns:
        Configured SparkMicroBatchProcessor
    """
    config = SparkStreamConfig(
        input_path=input_path,
        input_format=format,
        output_path=output_path,
        output_format="parquet",
        trigger_interval=trigger_interval
    )
    
    processor = SparkMicroBatchProcessor(pipeline, config)
    return processor