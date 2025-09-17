"""
Streaming metrics loading and persistence logic.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional
from airpipe.core.streaming import StateManager
from airpipe.utils.loaders.file_utils import FileUtils

logger = logging.getLogger(__name__)


class StreamingMetricsLoader:
    """Load and persist streaming metrics and state."""
    
    def __init__(self, 
                 state_manager: Optional[StateManager] = None,
                 output_dir: str = "output/streaming"):
        self.logger = logger
        self.state_manager = state_manager or StateManager(backend="memory")
        self.file_utils = FileUtils()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def save_batch_metrics(self, 
                          metrics: Dict[str, Any], 
                          batch_id: Optional[str] = None) -> str:
        """
        Save batch metrics to file.
        
        Args:
            metrics: Dictionary of batch metrics
            batch_id: Optional batch identifier
            
        Returns:
            Path to saved metrics file
        """
        if batch_id is None:
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        
        filename = f"batch_metrics_{batch_id}.json"
        filepath = self.output_dir / filename
        
        # Add metadata
        enhanced_metrics = {
            **metrics,
            'saved_at': datetime.now().isoformat(),
            'batch_id': batch_id
        }
        
        with open(filepath, 'w') as f:
            json.dump(enhanced_metrics, f, indent=2, default=str)
        
        self.logger.info(f"Saved batch metrics to {filepath}")
        return str(filepath)
    
    def save_streaming_metrics(self, metrics: Dict[str, Any]) -> None:
        """
        Save metrics to streaming output (simulating data sink).
        
        Args:
            metrics: Dictionary of metrics to save
        """
        # In real scenario, this would write to database, S3, Kafka, etc.
        # For demo, log the metrics and optionally save to file
        self.logger.info(f"Streaming Metrics: {metrics}")
        
        # Append to streaming metrics log file
        log_file = self.output_dir / "streaming_metrics.jsonl"
        
        with open(log_file, 'a') as f:
            json.dump(metrics, f, default=str)
            f.write('\n')
        
        # Update batch count in state
        batch_count = self.state_manager.get_state('batch_count', 0) + 1
        self.state_manager.update_state('batch_count', batch_count)
        
        # Periodically save checkpoint
        if batch_count % 10 == 0:
            checkpoint_id = self.save_checkpoint()
            self.logger.info(f"Created checkpoint: {checkpoint_id}")
    
    def save_checkpoint(self) -> str:
        """
        Save state checkpoint.
        
        Returns:
            Checkpoint identifier
        """
        checkpoint_id = self.state_manager.checkpoint()
        
        # Also save human-readable checkpoint info
        checkpoint_info = {
            'checkpoint_id': checkpoint_id,
            'timestamp': datetime.now().isoformat(),
            'total_records': self.state_manager.get_state('total_records', 0),
            'total_anomalies': self.state_manager.get_state('total_anomalies', 0),
            'batch_count': self.state_manager.get_state('batch_count', 0)
        }
        
        checkpoint_file = self.output_dir / f"checkpoint_{checkpoint_id}.json"
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_info, f, indent=2, default=str)
        
        self.logger.info(f"Saved checkpoint info to {checkpoint_file}")
        return checkpoint_id
    
    def save_alerts(self, alerts: List[Dict[str, Any]]) -> Optional[str]:
        """
        Save alerts to file.
        
        Args:
            alerts: List of alert dictionaries
            
        Returns:
            Path to saved alerts file, None if no alerts
        """
        if not alerts:
            return None
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"alerts_{timestamp}.json"
        filepath = self.output_dir / filename
        
        alert_data = {
            'timestamp': datetime.now().isoformat(),
            'alert_count': len(alerts),
            'alerts': alerts
        }
        
        with open(filepath, 'w') as f:
            json.dump(alert_data, f, indent=2, default=str)
        
        self.logger.warning(f"Saved {len(alerts)} alerts to {filepath}")
        return str(filepath)
    
    def save_windowed_statistics(self, stats: Dict[str, Any], window_id: str = None) -> str:
        """
        Save windowed statistics.
        
        Args:
            stats: Dictionary of windowed statistics
            window_id: Optional window identifier
            
        Returns:
            Path to saved statistics file
        """
        if window_id is None:
            window_id = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        
        filename = f"window_stats_{window_id}.json"
        filepath = self.output_dir / filename
        
        # Add metadata
        enhanced_stats = {
            **stats,
            'saved_at': datetime.now().isoformat(),
            'window_id': window_id
        }
        
        with open(filepath, 'w') as f:
            json.dump(enhanced_stats, f, indent=2, default=str)
        
        self.logger.info(f"Saved windowed statistics to {filepath}")
        return str(filepath)
    
    def export_processed_data(self, 
                             df, 
                             format: str = 'csv',
                             filename: str = None) -> str:
        """
        Export processed streaming data to file.
        
        Args:
            df: DataFrame to export
            format: Export format ('csv', 'json', 'parquet')
            filename: Optional filename (auto-generated if None)
            
        Returns:
            Path to exported file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"processed_data_{timestamp}"
        
        if format == 'csv':
            filepath = self.output_dir / f"{filename}.csv"
            self.file_utils.save_to_csv(df, str(filepath))
        elif format == 'json':
            filepath = self.output_dir / f"{filename}.json"
            self.file_utils.save_to_json(df, str(filepath), orient='records')
        elif format == 'parquet':
            filepath = self.output_dir / f"{filename}.parquet"
            df.to_parquet(filepath, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        self.logger.info(f"Exported {len(df)} records to {filepath} ({format} format)")
        return str(filepath)
    
    def get_streaming_summary(self) -> Dict[str, Any]:
        """
        Get summary of streaming processing state.
        
        Returns:
            Dictionary with streaming summary
        """
        total_records = self.state_manager.get_state('total_records', 0)
        total_anomalies = self.state_manager.get_state('total_anomalies', 0)
        batch_count = self.state_manager.get_state('batch_count', 0)
        
        summary = {
            'total_records_processed': total_records,
            'total_anomalies_detected': total_anomalies,
            'total_batches_processed': batch_count,
            'anomaly_rate_overall': (total_anomalies / total_records) * 100 if total_records > 0 else 0.0,
            'average_records_per_batch': total_records / batch_count if batch_count > 0 else 0,
            'processing_start_time': self.state_manager.get_state('start_time'),
            'last_checkpoint': self.state_manager.get_state('last_checkpoint')
        }
        
        return summary
    
    def print_streaming_summary(self, performance_metrics: Dict[str, Any] = None) -> None:
        """
        Print comprehensive streaming processing summary.
        
        Args:
            performance_metrics: Optional performance metrics from monitoring
        """
        summary = self.get_streaming_summary()
        
        print("\n" + "=" * 60)
        print("STREAMING PIPELINE SUMMARY")
        print("=" * 60)
        
        # Processing statistics
        print(f"Total Records Processed: {summary['total_records_processed']:,}")
        print(f"Total Anomalies Detected: {summary['total_anomalies_detected']:,}")
        print(f"Total Batches Processed: {summary['total_batches_processed']}")
        print(f"Overall Anomaly Rate: {summary['anomaly_rate_overall']:.2f}%")
        
        if summary['total_batches_processed'] > 0:
            print(f"Average Records per Batch: {summary['average_records_per_batch']:.1f}")
        
        # Performance metrics
        if performance_metrics and 'cumulative' in performance_metrics:
            print("\nPerformance Metrics:")
            for metric, stats in performance_metrics['cumulative'].items():
                if 'throughput' in metric:
                    print(f"  Average {metric}: {stats['avg']:.2f} records/sec")
                elif 'batch_time' in metric:
                    print(f"  Average {metric}: {stats['avg']:.3f} seconds")
        
        print("=" * 60)
        
        self.logger.info("Printed streaming processing summary")