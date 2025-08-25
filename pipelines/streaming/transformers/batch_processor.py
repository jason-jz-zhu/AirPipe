"""
Streaming batch processing and transformation logic.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from airpipe.core.streaming import StateManager

logger = logging.getLogger(__name__)


class StreamingBatchProcessor:
    """Process and transform streaming data batches."""
    
    def __init__(self, state_manager: Optional[StateManager] = None):
        self.logger = logger
        self.state_manager = state_manager or StateManager(backend="memory")
        
    def process_batch_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Process incoming stream batch with timestamp and anomaly detection.
        
        Args:
            df: Input batch DataFrame
            
        Returns:
            Processed DataFrame with additional metadata
        """
        if df.empty:
            self.logger.warning("Received empty batch")
            return df
        
        processed_df = df.copy()
        
        # Add processing timestamp
        processed_df['processed_at'] = datetime.now()
        
        # Detect anomalies if present
        if '_is_anomaly' in processed_df.columns:
            anomaly_count = processed_df['_is_anomaly'].sum()
            if anomaly_count > 0:
                self.logger.warning(f"Found {anomaly_count} anomalies in batch")
        
        self.logger.info(f"Processed batch with {len(processed_df)} records")
        return processed_df
    
    def detect_anomalies(self, df: pd.DataFrame, 
                        columns: Optional[List[str]] = None,
                        method: str = 'iqr',
                        threshold: float = 1.5) -> pd.DataFrame:
        """
        Detect anomalies in streaming data using statistical methods.
        
        Args:
            df: Input DataFrame
            columns: Columns to check for anomalies (defaults to numeric columns)
            method: Anomaly detection method ('iqr', 'zscore', 'isolation')
            threshold: Threshold for anomaly detection
            
        Returns:
            DataFrame with anomaly flags
        """
        result_df = df.copy()
        
        if columns is None:
            columns = df.select_dtypes(include=[np.number]).columns.tolist()
        
        # Initialize anomaly flag
        result_df['_is_anomaly'] = False
        
        for column in columns:
            if column not in df.columns:
                continue
                
            if method == 'iqr':
                Q1 = df[column].quantile(0.25)
                Q3 = df[column].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                
                anomalies = (df[column] < lower_bound) | (df[column] > upper_bound)
                result_df['_is_anomaly'] = result_df['_is_anomaly'] | anomalies
                
            elif method == 'zscore':
                z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
                anomalies = z_scores > threshold
                result_df['_is_anomaly'] = result_df['_is_anomaly'] | anomalies
        
        anomaly_count = result_df['_is_anomaly'].sum()
        if anomaly_count > 0:
            self.logger.info(f"Detected {anomaly_count} anomalies using {method} method")
        
        return result_df
    
    def aggregate_batch_metrics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Aggregate metrics from processed batch.
        
        Args:
            df: Processed batch DataFrame
            
        Returns:
            Dictionary with batch metrics
        """
        metrics = {
            'record_count': len(df),
            'timestamp': datetime.now().isoformat(),
            'batch_id': str(hash(df.to_string()))[:8]  # Simple batch identifier
        }
        
        # Aggregate numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            if col not in ['_id', '_is_anomaly']:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    metrics[f'{col}_mean'] = float(col_data.mean())
                    metrics[f'{col}_std'] = float(col_data.std()) if len(col_data) > 1 else 0.0
                    metrics[f'{col}_min'] = float(col_data.min())
                    metrics[f'{col}_max'] = float(col_data.max())
                    metrics[f'{col}_median'] = float(col_data.median())
        
        # Update running statistics in state
        total_records = self.state_manager.get_state('total_records', 0)
        total_records += len(df)
        self.state_manager.update_state('total_records', total_records)
        
        # Track anomalies
        if '_is_anomaly' in df.columns:
            total_anomalies = self.state_manager.get_state('total_anomalies', 0)
            batch_anomalies = int(df['_is_anomaly'].sum())
            total_anomalies += batch_anomalies
            self.state_manager.update_state('total_anomalies', total_anomalies)
            
            metrics['batch_anomaly_count'] = batch_anomalies
            metrics['total_anomaly_count'] = total_anomalies
            metrics['anomaly_rate'] = (total_anomalies / total_records) * 100 if total_records > 0 else 0.0
        
        metrics['total_records_processed'] = total_records
        
        self.logger.info(f"Aggregated metrics for batch: {metrics['record_count']} records")
        return metrics
    
    def compute_windowed_statistics(self, df: pd.DataFrame,
                                   window_seconds: float = 30.0) -> Dict[str, Any]:
        """
        Compute statistics for time window.
        
        Args:
            df: DataFrame with timestamp column
            window_seconds: Window size in seconds
            
        Returns:
            Dictionary with windowed statistics
        """
        if '_timestamp' not in df.columns:
            # Add timestamp if not present
            df = df.copy()
            df['_timestamp'] = datetime.now()
        
        stats = {
            'window_start': df['_timestamp'].min().isoformat() if not df.empty else None,
            'window_end': df['_timestamp'].max().isoformat() if not df.empty else None,
            'record_count': len(df),
            'window_duration_seconds': window_seconds
        }
        
        # Anomaly count
        if '_is_anomaly' in df.columns:
            stats['anomaly_count'] = int(df['_is_anomaly'].sum())
            stats['anomaly_rate'] = (stats['anomaly_count'] / len(df)) * 100 if len(df) > 0 else 0.0
        
        # Calculate aggregates for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            if col not in ['_id', '_is_anomaly']:
                col_data = df[col].dropna()
                if len(col_data) > 0:
                    stats[f'{col}_avg'] = float(col_data.mean())
                    stats[f'{col}_p95'] = float(col_data.quantile(0.95))
                    stats[f'{col}_p99'] = float(col_data.quantile(0.99))
                    stats[f'{col}_count'] = len(col_data)
        
        self.logger.info(f"Computed windowed stats: {stats['record_count']} records, "
                        f"{stats.get('anomaly_count', 0)} anomalies")
        
        return stats
    
    def apply_business_rules(self, df: pd.DataFrame, rules: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply business-specific rules to streaming data.
        
        Args:
            df: Input DataFrame
            rules: Dictionary containing business rules
            
        Returns:
            DataFrame with business rules applied
        """
        result_df = df.copy()
        
        # Example rule: Filter by value thresholds
        if 'value_thresholds' in rules:
            thresholds = rules['value_thresholds']
            for column, (min_val, max_val) in thresholds.items():
                if column in result_df.columns:
                    mask = (result_df[column] >= min_val) & (result_df[column] <= max_val)
                    result_df = result_df[mask]
                    self.logger.info(f"Applied threshold filter to {column}: {len(result_df)} records remain")
        
        # Example rule: Enrich with categories
        if 'categorization_rules' in rules:
            cat_rules = rules['categorization_rules']
            for column, categories in cat_rules.items():
                if column in result_df.columns:
                    def categorize_value(val):
                        for category, (min_val, max_val) in categories.items():
                            if min_val <= val <= max_val:
                                return category
                        return 'other'
                    
                    result_df[f'{column}_category'] = result_df[column].apply(categorize_value)
                    self.logger.info(f"Applied categorization to {column}")
        
        return result_df