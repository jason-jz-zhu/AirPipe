#!/usr/bin/env python3
"""
Example streaming workflow demonstrating micro-batch processing.
Shows how to run existing pipelines on continuous data streams using
proper pipeline components (extractors, transformers, loaders).
"""

import pandas as pd
import logging
from datetime import datetime
from airpipe.core.task import TaskPipeline
from airpipe.core.streaming import (
    MicroBatchProcessor,
    StreamConfig,
    StreamMonitor,
    AlertRule,
    StateManager,
    WindowedAggregator
)

# Import streaming pipeline components
from pipelines.streaming.extractors.batch_extractor import StreamingBatchExtractor
from pipelines.streaming.transformers.batch_processor import StreamingBatchProcessor
from pipelines.streaming.transformers.alert_processor import StreamingAlertProcessor
from pipelines.streaming.loaders.metrics_loader import StreamingMetricsLoader
from pipelines.streaming.loaders.alert_loader import StreamingAlertLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Create pipeline for processing streaming data
pipeline = TaskPipeline("streaming_analytics")

# State manager for maintaining aggregations
state_manager = StateManager(backend="memory")

# Initialize pipeline components
batch_extractor = StreamingBatchExtractor()
batch_processor = StreamingBatchProcessor(state_manager)
alert_processor = StreamingAlertProcessor()
metrics_loader = StreamingMetricsLoader(state_manager)
alert_loader = StreamingAlertLoader()


@pipeline.task(produces="processed_batch")
def process_batch():
    """Process incoming stream batch using batch processor."""
    # Extract batch data
    df = batch_extractor.extract_stream_batch(pipeline, "stream_batch")
    
    # Apply anomaly detection
    df_with_anomalies = batch_processor.detect_anomalies(df, method='iqr', threshold=1.5)
    
    # Process the batch (add timestamps, etc.)
    processed_df = batch_processor.process_batch_data(df_with_anomalies)
    
    return pipeline.create_artifact(processed_df, "processed_batch")


@pipeline.task(
    depends_on=["process_batch"],
    consumes="processed_batch",
    produces="aggregated_metrics"
)
def aggregate_metrics():
    """Aggregate metrics from processed batch using batch processor."""
    batch = pipeline.get_artifact("processed_batch")
    df = batch.as_dataframe()
    
    # Use batch processor to aggregate metrics
    metrics = batch_processor.aggregate_batch_metrics(df)
    
    return pipeline.create_artifact(metrics, "aggregated_metrics")


@pipeline.task(
    depends_on=["aggregate_metrics"],
    consumes="aggregated_metrics",
    produces="alerts"
)
def check_alerts():
    """Check for alert conditions using alert processor."""
    metrics = pipeline.get_artifact("aggregated_metrics")
    data = metrics.data
    
    # Setup alert rules
    alert_processor.add_alert_rule(
        rule_name="high_anomaly_rate",
        metric_name="anomaly_rate",
        condition=">",
        threshold=5.0,
        severity="WARNING",
        message_template="Anomaly rate {value:.2f}% exceeds threshold (5.0%)"
    )
    
    alert_processor.add_alert_rule(
        rule_name="low_throughput",
        metric_name="record_count",
        condition="<",
        threshold=50,
        severity="ERROR",
        message_template="Low batch size detected: {value} records"
    )
    
    # Process all alert types
    alerts = alert_processor.process_all_alerts(data)
    
    # Convert to list of dictionaries
    alert_dicts = []
    for alert in alerts:
        alert_dicts.append({
            'type': alert.alert_type,
            'message': alert.message,
            'severity': alert.severity,
            'timestamp': alert.timestamp,
            'value': alert.value,
            'threshold': alert.threshold,
            'metric_name': alert.metric_name
        })
    
    return pipeline.create_artifact(alert_dicts, "alerts")


@pipeline.task(
    depends_on=["aggregate_metrics", "check_alerts"],
    consumes=["aggregated_metrics", "alerts"]
)
def save_metrics_and_alerts():
    """Save metrics and alerts using loaders."""
    # Get artifacts
    metrics = pipeline.get_artifact("aggregated_metrics")
    alerts = pipeline.get_artifact("alerts")
    
    # Save metrics using metrics loader
    metrics_loader.save_streaming_metrics(metrics.data)
    
    # Save alerts using alert loader
    if alerts.data:
        alert_loader.save_alerts(alerts.data)
        
        # Process alert notifications
        notification_stats = alert_loader.process_alert_notifications(
            alerts.data,
            {
                'methods': ['log'],
                'severity_thresholds': {
                    'INFO': ['log'],
                    'WARNING': ['log'],
                    'ERROR': ['log'],
                    'CRITICAL': ['log']
                }
            }
        )
        logger.info(f"Processed notifications: {notification_stats}")


def run_streaming():
    """Run the streaming pipeline."""
    
    # Create simulated data source using batch extractor
    # In production, this could be Kafka, API, Database, etc.
    source = batch_extractor.create_sensor_data_source()
    
    # Configure streaming
    config = StreamConfig(
        batch_size=500,  # Process 500 records at a time
        batch_interval=5.0,  # Or every 5 seconds
        max_batches=20,  # Run for 20 batches then stop (for demo)
        checkpoint_interval=5,
        error_strategy="continue",
        retry_attempts=3,
        enable_monitoring=True
    )
    
    # Create processor
    processor = MicroBatchProcessor(pipeline, config)
    
    # Setup monitoring
    monitor = StreamMonitor(
        enable_alerts=True,
        metrics_interval=2.0,
        export_path="streaming_metrics.jsonl"
    )
    
    # Add alert rules
    monitor.add_alert_rule(AlertRule(
        name="high_latency",
        metric_name="avg_batch_time",
        condition=">",
        threshold=2.0,  # 2 seconds
        window_seconds=30,
        severity="WARNING",
        message="Batch processing taking too long: {value:.2f}s"
    ))
    
    monitor.add_alert_rule(AlertRule(
        name="low_throughput",
        metric_name="throughput_per_sec",
        condition="<",
        threshold=50.0,  # Less than 50 records/sec
        window_seconds=60,
        severity="ERROR",
        message="Throughput dropped to {value:.2f} records/sec"
    ))
    
    # Start monitoring
    monitor.start(processor)
    
    try:
        logger.info("Starting streaming pipeline...")
        logger.info(f"Processing with batch_size={config.batch_size}, interval={config.batch_interval}s")
        
        # Execute pipeline tasks for each batch
        def execute_pipeline_tasks():
            """Execute all pipeline tasks in order."""
            process_batch()
            aggregate_metrics()
            check_alerts()
            save_metrics_and_alerts()
            
            # Return pipeline execution results
            return pipeline.execute(parallel=False)
        
        # Custom batch transformer
        def transform_batch(batch):
            """Transform raw batch to DataFrame."""
            return pd.DataFrame(batch)
        
        # Process stream
        processor.process_stream(
            source=source,
            transform_batch=transform_batch
        )
        
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user")
    finally:
        # Stop monitoring
        monitor.stop()
        
        # Print final statistics using metrics loader
        final_metrics = monitor.get_metrics_summary()
        metrics_loader.print_streaming_summary(final_metrics)
        
        # Show alert summary from alert loader
        alert_stats = alert_loader.get_alert_statistics()
        if alert_stats['total_alerts'] > 0:
            print(f"\nAlert Statistics:")
            print(f"  Total Alerts: {alert_stats['total_alerts']}")
            for severity, count in alert_stats['by_severity'].items():
                print(f"  {severity}: {count}")
            if alert_stats['latest_alert_time']:
                print(f"  Latest Alert: {alert_stats['latest_alert_time']}")
        
        # Export final reports
        alert_loader.export_alert_history(hours=24, format='json')
        print(f"\nReports saved to: {metrics_loader.output_dir}")


def run_streaming_with_window():
    """Example with windowed aggregation."""
    from airpipe.core.streaming import WindowedAggregator
    
    # Create pipeline for windowed processing
    window_pipeline = TaskPipeline("windowed_streaming")
    
    # Create window-specific batch processor
    window_batch_processor = StreamingBatchProcessor()
    
    @window_pipeline.task(produces="windowed_stats") 
    def compute_window_stats():
        """Compute statistics for time window using batch processor."""
        batch = window_pipeline.get_artifact("stream_batch")
        df = batch.as_dataframe()
        
        # Use batch processor for windowed statistics
        stats = window_batch_processor.compute_windowed_statistics(df, window_seconds=30.0)
        
        return window_pipeline.create_artifact(stats, "windowed_stats")
    
    # Create source using batch extractor
    window_extractor = StreamingBatchExtractor()
    source = window_extractor.create_simulated_source(
        schema={'value': 'float', 'category': 'string'},
        rate=50.0,
        anomaly_rate=0.01
    )
    
    # Configure with larger batches for windowing
    config = StreamConfig(
        batch_size=1000,
        batch_interval=10.0,
        max_batches=10
    )
    
    processor = MicroBatchProcessor(window_pipeline, config)
    aggregator = WindowedAggregator(processor)
    
    # Create 30-second tumbling window
    window = aggregator.tumbling_window(30)
    
    logger.info("Starting windowed streaming pipeline...")
    
    # Process with window (simplified example)
    processor.process_stream(source=source)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--windowed":
        run_streaming_with_window()
    else:
        run_streaming()