#!/usr/bin/env python3
"""
Example streaming workflow demonstrating micro-batch processing.
Shows how to run existing pipelines on continuous data streams.
"""

import pandas as pd
import logging
import time
from datetime import datetime
from airpipe.core.task import TaskPipeline
from airpipe.core.streaming import (
    MicroBatchProcessor,
    StreamConfig,
    StreamingSource,
    StreamMonitor,
    AlertRule,
    StateManager,
    SimulatedDataSource,
    create_source
)

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


@pipeline.task(produces="processed_batch")
def process_batch():
    """Process incoming stream batch."""
    # Get the batch injected by streaming processor
    batch_artifact = pipeline.get_artifact("stream_batch")
    df = batch_artifact.as_dataframe()
    
    logger.info(f"Processing batch with {len(df)} records")
    
    # Add processing timestamp
    df['processed_at'] = datetime.now()
    
    # Detect anomalies
    if '_is_anomaly' in df.columns:
        anomaly_count = df['_is_anomaly'].sum()
        if anomaly_count > 0:
            logger.warning(f"Found {anomaly_count} anomalies in batch")
    
    return pipeline.create_artifact(df, "processed_batch")


@pipeline.task(
    depends_on=["process_batch"],
    consumes="processed_batch",
    produces="aggregated_metrics"
)
def aggregate_metrics():
    """Aggregate metrics from processed batch."""
    batch = pipeline.get_artifact("processed_batch")
    df = batch.as_dataframe()
    
    # Calculate batch metrics
    metrics = {
        'record_count': len(df),
        'timestamp': datetime.now().isoformat()
    }
    
    # Aggregate numeric columns
    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in numeric_cols:
        if col not in ['_id', '_is_anomaly']:
            metrics[f'{col}_mean'] = df[col].mean()
            metrics[f'{col}_std'] = df[col].std()
            metrics[f'{col}_min'] = df[col].min()
            metrics[f'{col}_max'] = df[col].max()
    
    # Update running statistics in state
    total_records = state_manager.get_state('total_records', 0)
    total_records += len(df)
    state_manager.update_state('total_records', total_records)
    
    # Track anomalies
    if '_is_anomaly' in df.columns:
        total_anomalies = state_manager.get_state('total_anomalies', 0)
        total_anomalies += df['_is_anomaly'].sum()
        state_manager.update_state('total_anomalies', total_anomalies)
        metrics['anomaly_rate'] = (total_anomalies / total_records) * 100
    
    metrics['total_records_processed'] = total_records
    
    return pipeline.create_artifact(metrics, "aggregated_metrics")


@pipeline.task(
    depends_on=["aggregate_metrics"],
    consumes="aggregated_metrics",
    produces="alerts"
)
def check_alerts():
    """Check for alert conditions."""
    metrics = pipeline.get_artifact("aggregated_metrics")
    data = metrics.data
    
    alerts = []
    
    # Check anomaly rate
    if 'anomaly_rate' in data and data['anomaly_rate'] > 5.0:
        alerts.append({
            'type': 'HIGH_ANOMALY_RATE',
            'message': f"Anomaly rate {data['anomaly_rate']:.2f}% exceeds threshold",
            'severity': 'WARNING',
            'timestamp': datetime.now().isoformat()
        })
    
    # Check for extreme values
    for key, value in data.items():
        if '_max' in key and value > 500:
            alerts.append({
                'type': 'EXTREME_VALUE',
                'message': f"{key} = {value:.2f} exceeds threshold",
                'severity': 'INFO',
                'timestamp': datetime.now().isoformat()
            })
    
    if alerts:
        logger.warning(f"Generated {len(alerts)} alerts")
        for alert in alerts:
            logger.warning(f"[{alert['severity']}] {alert['message']}")
    
    return pipeline.create_artifact(alerts, "alerts")


@pipeline.task(
    depends_on=["aggregate_metrics"],
    consumes="aggregated_metrics"
)
def save_metrics():
    """Save metrics to file (simulating data sink)."""
    metrics = pipeline.get_artifact("aggregated_metrics")
    
    # In real scenario, this would write to database, S3, etc.
    # For demo, just log the metrics
    logger.info(f"Metrics: {metrics.data}")
    
    # Periodically save checkpoint
    batch_count = state_manager.get_state('batch_count', 0) + 1
    state_manager.update_state('batch_count', batch_count)
    
    if batch_count % 10 == 0:
        checkpoint_id = state_manager.checkpoint()
        logger.info(f"Created checkpoint: {checkpoint_id}")


def run_streaming():
    """Run the streaming pipeline."""
    
    # Create simulated data source
    # In production, this could be Kafka, API, Database, etc.
    source = SimulatedDataSource(
        schema={
            'temperature': 'float',
            'pressure': 'float',
            'humidity': 'float',
            'sensor_id': 'string',
            'location': 'string'
        },
        rate=100.0,  # 100 records per second
        noise=0.2,
        anomaly_rate=0.02  # 2% anomalies
    )
    
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
            save_metrics()
            
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
        
        # Print final statistics
        print("\n" + "=" * 60)
        print("STREAMING PIPELINE COMPLETE")
        print("=" * 60)
        
        # Get final metrics
        final_metrics = monitor.get_metrics_summary()
        print(f"Total Records: {state_manager.get_state('total_records', 0):,}")
        print(f"Total Anomalies: {state_manager.get_state('total_anomalies', 0):,}")
        print(f"Total Batches: {state_manager.get_state('batch_count', 0)}")
        
        if 'cumulative' in final_metrics:
            print("\nPerformance Metrics:")
            for metric, stats in final_metrics['cumulative'].items():
                if 'throughput' in metric:
                    print(f"  {metric}: {stats['avg']:.2f} records/sec")
                elif 'batch_time' in metric:
                    print(f"  {metric}: {stats['avg']:.3f} seconds")
        
        # Show alert summary
        if monitor.alerts:
            print(f"\nAlerts Generated: {len(monitor.alerts)}")
            severity_counts = {}
            for alert in monitor.alerts:
                severity = alert.severity
                severity_counts[severity] = severity_counts.get(severity, 0) + 1
            for severity, count in severity_counts.items():
                print(f"  {severity}: {count}")
        
        print("=" * 60)


def run_streaming_with_window():
    """Example with windowed aggregation."""
    from airpipe.core.streaming import WindowedAggregator
    
    # Create pipeline for windowed processing
    window_pipeline = TaskPipeline("windowed_streaming")
    
    @window_pipeline.task(produces="windowed_stats")
    def compute_window_stats():
        """Compute statistics for time window."""
        batch = window_pipeline.get_artifact("stream_batch")
        df = batch.as_dataframe()
        
        stats = {
            'window_start': df['_timestamp'].min(),
            'window_end': df['_timestamp'].max(),
            'record_count': len(df),
            'anomaly_count': df['_is_anomaly'].sum() if '_is_anomaly' in df.columns else 0
        }
        
        # Calculate aggregates
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        for col in numeric_cols:
            if col not in ['_id', '_is_anomaly']:
                stats[f'{col}_avg'] = df[col].mean()
                stats[f'{col}_p95'] = df[col].quantile(0.95)
        
        logger.info(f"Window stats: {stats['record_count']} records, "
                   f"{stats.get('anomaly_count', 0)} anomalies")
        
        return window_pipeline.create_artifact(stats, "windowed_stats")
    
    # Create source
    source = SimulatedDataSource(
        schema={'value': 'float', 'category': 'string'},
        rate=50.0
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