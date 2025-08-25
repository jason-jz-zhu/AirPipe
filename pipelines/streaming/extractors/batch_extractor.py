"""
Streaming batch extraction logic.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from airpipe.core.streaming import SimulatedDataSource, create_source

logger = logging.getLogger(__name__)


class StreamingBatchExtractor:
    """Extract and process streaming data batches."""
    
    def __init__(self):
        self.logger = logger
        
    def extract_stream_batch(self, pipeline, batch_key: str = "stream_batch") -> pd.DataFrame:
        """
        Extract the current streaming batch from pipeline artifacts.
        
        Args:
            pipeline: Pipeline instance containing artifacts
            batch_key: Key for the batch artifact
            
        Returns:
            DataFrame with batch data
        """
        try:
            batch_artifact = pipeline.get_artifact(batch_key)
            df = batch_artifact.as_dataframe()
            
            self.logger.info(f"Extracted batch with {len(df)} records")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract batch: {e}")
            return pd.DataFrame()
    
    def create_simulated_source(self,
                               schema: Dict[str, str],
                               rate: float = 100.0,
                               anomaly_rate: float = 0.02,
                               noise: float = 0.2) -> SimulatedDataSource:
        """
        Create a simulated data source for streaming data.
        
        Args:
            schema: Data schema definition
            rate: Records per second generation rate
            anomaly_rate: Fraction of records that are anomalies
            noise: Noise level for data generation
            
        Returns:
            Configured SimulatedDataSource instance
        """
        source = SimulatedDataSource(
            schema=schema,
            rate=rate,
            noise=noise,
            anomaly_rate=anomaly_rate
        )
        
        self.logger.info(f"Created simulated source with rate={rate} records/sec, anomaly_rate={anomaly_rate*100}%")
        return source
    
    def create_sensor_data_source(self) -> SimulatedDataSource:
        """
        Create a sensor data source for IoT/streaming scenarios.
        
        Returns:
            Configured sensor data source
        """
        schema = {
            'temperature': 'float',
            'pressure': 'float', 
            'humidity': 'float',
            'sensor_id': 'string',
            'location': 'string'
        }
        
        return self.create_simulated_source(
            schema=schema,
            rate=100.0,
            anomaly_rate=0.02,
            noise=0.2
        )
    
    def create_financial_data_source(self) -> SimulatedDataSource:
        """
        Create a financial data source for trading/market scenarios.
        
        Returns:
            Configured financial data source
        """
        schema = {
            'price': 'float',
            'volume': 'float',
            'symbol': 'string',
            'exchange': 'string'
        }
        
        return self.create_simulated_source(
            schema=schema,
            rate=200.0,  # Higher rate for financial data
            anomaly_rate=0.01,  # Lower anomaly rate
            noise=0.1
        )
    
    def create_web_analytics_source(self) -> SimulatedDataSource:
        """
        Create a web analytics data source for clickstream scenarios.
        
        Returns:
            Configured web analytics data source
        """
        schema = {
            'user_id': 'string',
            'page_url': 'string', 
            'session_duration': 'float',
            'bounce_rate': 'float',
            'conversion_value': 'float'
        }
        
        return self.create_simulated_source(
            schema=schema,
            rate=50.0,
            anomaly_rate=0.05,  # Higher anomaly rate for web data
            noise=0.3
        )