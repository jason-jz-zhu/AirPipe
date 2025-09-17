"""Tests for streaming data sources."""

import unittest
from unittest.mock import Mock, patch, mock_open
import pandas as pd
import json
import time
from pathlib import Path

from airpipe.core.streaming.sources import (
    SimulatedDataSource, KafkaSource, APIPollingSource
)
from tests.base import BaseTestCase
from tests.fixtures.factories import DataFactory


class TestSimulatedDataSource(BaseTestCase):
    """Test SimulatedDataSource class."""
    
    def test_initialization(self):
        """Test source initialization."""
        schema = {
            'id': 'int',
            'value': 'float',
            'name': 'string'
        }
        
        source = SimulatedDataSource(
            schema=schema,
            rate=100.0,
            anomaly_rate=0.1
        )
        
        self.assertEqual(source.schema, schema)
        self.assertEqual(source.rate, 100.0)
        self.assertEqual(source.anomaly_rate, 0.1)
        
    def test_read_batch(self):
        """Test reading batches from simulated source."""
        source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=1000.0
        )
        
        # Read first batch
        batch1 = source.read_batch(batch_size=100)
        
        self.assertIsNotNone(batch1)
        self.assertIsInstance(batch1, pd.DataFrame)
        self.assertEqual(len(batch1), 100)
        self.assertIn('value', batch1.columns)
        self.assertIn('timestamp', batch1.columns)
        
        # Read second batch
        batch2 = source.read_batch(batch_size=50)
        
        self.assertEqual(len(batch2), 50)
        
        # Timestamps should be different
        self.assertGreater(
            batch2['timestamp'].min(),
            batch1['timestamp'].max()
        )
        
    def test_data_generation(self):
        """Test data generation based on schema."""
        schema = {
            'int_col': 'int',
            'float_col': 'float',
            'string_col': 'string',
            'bool_col': 'bool'
        }
        
        source = SimulatedDataSource(schema=schema, rate=100.0)
        
        batch = source.read_batch(batch_size=10)
        
        # Check column types
        self.assertTrue(batch['int_col'].dtype.kind in ['i', 'f'])
        self.assertTrue(batch['float_col'].dtype.kind == 'f')
        self.assertTrue(batch['string_col'].dtype == 'object')
        self.assertTrue(batch['bool_col'].dtype == 'bool')
        
        # Check value ranges
        self.assertTrue(all(0 <= v <= 1000 for v in batch['int_col']))
        self.assertTrue(all(0 <= v <= 1000 for v in batch['float_col']))
        
    def test_anomaly_injection(self):
        """Test anomaly injection."""
        source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=100.0,
            anomaly_rate=0.5  # 50% anomalies
        )
        
        batch = source.read_batch(batch_size=100)
        
        # Check for anomaly column
        self.assertIn('is_anomaly', batch.columns)
        
        # Should have some anomalies (roughly 50%)
        anomaly_count = batch['is_anomaly'].sum()
        self.assertGreater(anomaly_count, 30)
        self.assertLess(anomaly_count, 70)
        
        # Anomalous values should be different
        anomalies = batch[batch['is_anomaly']]
        normal = batch[~batch['is_anomaly']]
        
        if len(anomalies) > 0 and len(normal) > 0:
            # Anomalies should have extreme values
            self.assertGreater(
                anomalies['value'].std(),
                normal['value'].std() * 1.5
            )
            
    def test_rate_limiting(self):
        """Test data generation rate limiting."""
        source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=1000.0  # 1000 records per second
        )
        
        start_time = time.time()
        
        # Read multiple batches
        total_records = 0
        for _ in range(5):
            batch = source.read_batch(batch_size=100)
            total_records += len(batch)
            
        elapsed = time.time() - start_time
        
        # Should take roughly 0.5 seconds for 500 records at 1000/sec
        expected_time = total_records / 1000.0
        
        # Allow some tolerance
        self.assertGreater(elapsed, expected_time * 0.8)
        self.assertLess(elapsed, expected_time * 1.5)
        
    def test_reset(self):
        """Test resetting the source."""
        source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=100.0
        )
        
        # Read some data
        batch1 = source.read_batch(batch_size=50)
        first_timestamp = batch1['timestamp'].min()
        
        # Reset
        source.reset()
        
        # Read again
        batch2 = source.read_batch(batch_size=50)
        second_timestamp = batch2['timestamp'].min()
        
        # Timestamps should be close (both starting fresh)
        time_diff = abs((second_timestamp - first_timestamp).total_seconds())
        self.assertLess(time_diff, 1.0)


class TestFileDataSource(BaseTestCase):
    """Test FileDataSource class."""
    
    def test_csv_source(self):
        """Test reading from CSV file."""
        # Create test CSV
        df = DataFactory.create_sample_dataframe(200)
        csv_path = self.create_temp_csv("test_data.csv", df)
        
        source = FileDataSource(
            file_path=str(csv_path),
            file_format='csv'
        )
        
        # Read batches
        batch1 = source.read_batch(batch_size=50)
        self.assertEqual(len(batch1), 50)
        
        batch2 = source.read_batch(batch_size=100)
        self.assertEqual(len(batch2), 100)
        
        batch3 = source.read_batch(batch_size=100)
        self.assertEqual(len(batch3), 50)  # Remaining records
        
        batch4 = source.read_batch(batch_size=100)
        self.assertIsNone(batch4)  # End of file
        
    def test_json_source(self):
        """Test reading from JSON file."""
        # Create test JSON
        data = [
            {'id': i, 'value': i * 10}
            for i in range(100)
        ]
        json_path = self.create_temp_json("test_data.json", data)
        
        source = FileDataSource(
            file_path=str(json_path),
            file_format='json'
        )
        
        # Read all data
        batch = source.read_batch(batch_size=200)
        self.assertEqual(len(batch), 100)
        self.assertIn('id', batch.columns)
        self.assertIn('value', batch.columns)
        
    def test_parquet_source(self):
        """Test reading from Parquet file."""
        # Create test Parquet
        df = DataFactory.create_sample_dataframe(150)
        parquet_path = self.temp_path / "test_data.parquet"
        df.to_parquet(parquet_path)
        
        source = FileDataSource(
            file_path=str(parquet_path),
            file_format='parquet'
        )
        
        # Read batches
        total_read = 0
        while True:
            batch = source.read_batch(batch_size=40)
            if batch is None:
                break
            total_read += len(batch)
            
        self.assertEqual(total_read, 150)
        
    def test_chunked_reading(self):
        """Test chunked reading for large files."""
        # Create large CSV
        df = DataFactory.create_sample_dataframe(10000)
        csv_path = self.create_temp_csv("large_data.csv", df)
        
        source = FileDataSource(
            file_path=str(csv_path),
            file_format='csv',
            chunk_size=1000  # Read in chunks
        )
        
        # Read multiple batches
        batches = []
        for _ in range(5):
            batch = source.read_batch(batch_size=500)
            if batch is not None:
                batches.append(batch)
                
        total_records = sum(len(b) for b in batches)
        self.assertEqual(total_records, 2500)
        
    def test_reset_file_source(self):
        """Test resetting file source."""
        df = DataFactory.create_sample_dataframe(100)
        csv_path = self.create_temp_csv("reset_test.csv", df)
        
        source = FileDataSource(
            file_path=str(csv_path),
            file_format='csv'
        )
        
        # Read some data
        batch1 = source.read_batch(batch_size=30)
        self.assertEqual(len(batch1), 30)
        
        # Reset
        source.reset()
        
        # Should start from beginning
        batch2 = source.read_batch(batch_size=30)
        self.assertEqual(len(batch2), 30)
        
        # Should be the same data
        pd.testing.assert_frame_equal(batch1, batch2)
        
    def test_invalid_file(self):
        """Test handling invalid file path."""
        source = FileDataSource(
            file_path="/nonexistent/file.csv",
            file_format='csv'
        )
        
        with self.assertRaises(FileNotFoundError):
            source.read_batch(batch_size=10)
            
    def test_compression_support(self):
        """Test reading compressed files."""
        # Create compressed CSV
        df = DataFactory.create_sample_dataframe(100)
        gz_path = self.temp_path / "compressed.csv.gz"
        df.to_csv(gz_path, compression='gzip', index=False)
        
        source = FileDataSource(
            file_path=str(gz_path),
            file_format='csv',
            compression='gzip'
        )
        
        batch = source.read_batch(batch_size=50)
        self.assertEqual(len(batch), 50)


class TestKafkaDataSource(unittest.TestCase):
    """Test KafkaDataSource class."""
    
    @patch('airpipe.core.streaming.sources.KafkaConsumer')
    def test_kafka_source_initialization(self, mock_consumer_class):
        """Test Kafka source initialization."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        source = KafkaDataSource(
            topic='test_topic',
            bootstrap_servers='localhost:9092',
            group_id='test_group'
        )
        
        mock_consumer_class.assert_called_once_with(
            'test_topic',
            bootstrap_servers='localhost:9092',
            group_id='test_group',
            value_deserializer=source._deserialize_json,
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
    @patch('airpipe.core.streaming.sources.KafkaConsumer')
    def test_kafka_read_batch(self, mock_consumer_class):
        """Test reading batches from Kafka."""
        # Setup mock consumer
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        
        # Mock messages
        mock_messages = [
            Mock(value={'id': i, 'value': i * 10})
            for i in range(5)
        ]
        mock_consumer.poll.return_value = {
            'partition': mock_messages
        }
        
        source = KafkaDataSource(
            topic='test_topic',
            bootstrap_servers='localhost:9092'
        )
        
        # Read batch
        batch = source.read_batch(batch_size=10, timeout=1.0)
        
        self.assertIsNotNone(batch)
        self.assertEqual(len(batch), 5)
        self.assertIn('id', batch.columns)
        self.assertIn('value', batch.columns)
        
    @patch('airpipe.core.streaming.sources.KafkaConsumer')
    def test_kafka_no_messages(self, mock_consumer_class):
        """Test handling no messages from Kafka."""
        mock_consumer = Mock()
        mock_consumer_class.return_value = mock_consumer
        mock_consumer.poll.return_value = {}
        
        source = KafkaDataSource(
            topic='test_topic',
            bootstrap_servers='localhost:9092'
        )
        
        batch = source.read_batch(batch_size=10, timeout=0.1)
        
        self.assertIsNone(batch)


class TestAPIDataSource(unittest.TestCase):
    """Test APIDataSource class."""
    
    @patch('requests.get')
    def test_api_source_basic(self, mock_get):
        """Test basic API source functionality."""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [
                {'id': 1, 'value': 100},
                {'id': 2, 'value': 200}
            ]
        }
        mock_get.return_value = mock_response
        
        source = APIDataSource(
            url='https://api.example.com/data',
            data_path='data'
        )
        
        batch = source.read_batch(batch_size=10)
        
        self.assertIsNotNone(batch)
        self.assertEqual(len(batch), 2)
        self.assertEqual(batch.iloc[0]['id'], 1)
        self.assertEqual(batch.iloc[1]['value'], 200)
        
    @patch('requests.get')
    def test_api_pagination(self, mock_get):
        """Test API source with pagination."""
        # Mock paginated responses
        responses = [
            {'data': [{'id': i} for i in range(10)], 'next': 'page2'},
            {'data': [{'id': i} for i in range(10, 20)], 'next': 'page3'},
            {'data': [{'id': i} for i in range(20, 25)], 'next': None}
        ]
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.side_effect = responses
        mock_get.return_value = mock_response
        
        source = APIDataSource(
            url='https://api.example.com/data',
            data_path='data',
            pagination_type='cursor',
            next_page_path='next'
        )
        
        # Read first batch
        batch1 = source.read_batch(batch_size=15)
        self.assertEqual(len(batch1), 15)
        
        # Read second batch
        batch2 = source.read_batch(batch_size=15)
        self.assertEqual(len(batch2), 10)  # Only 10 remaining
        
    @patch('requests.get')
    def test_api_rate_limiting(self, mock_get):
        """Test API source with rate limiting."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': [{'id': 1}]}
        mock_get.return_value = mock_response
        
        source = APIDataSource(
            url='https://api.example.com/data',
            data_path='data',
            rate_limit=2  # 2 requests per second
        )
        
        start_time = time.time()
        
        # Make multiple requests
        for _ in range(3):
            source.read_batch(batch_size=1)
            
        elapsed = time.time() - start_time
        
        # Should take at least 1 second for 3 requests at 2/sec
        self.assertGreater(elapsed, 1.0)
        
    @patch('requests.get')
    def test_api_authentication(self, mock_get):
        """Test API source with authentication."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': []}
        mock_get.return_value = mock_response
        
        source = APIDataSource(
            url='https://api.example.com/data',
            data_path='data',
            auth_type='bearer',
            auth_token='test_token'
        )
        
        source.read_batch(batch_size=10)
        
        # Check authorization header was set
        mock_get.assert_called()
        call_args = mock_get.call_args
        headers = call_args[1].get('headers', {})
        self.assertEqual(headers.get('Authorization'), 'Bearer test_token')
        
    @patch('requests.get')
    def test_api_error_handling(self, mock_get):
        """Test API error handling."""
        # Mock error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = Exception("Server error")
        mock_get.return_value = mock_response
        
        source = APIDataSource(
            url='https://api.example.com/data',
            data_path='data'
        )
        
        with self.assertRaises(Exception):
            source.read_batch(batch_size=10)


class TestDataSourceIntegration(BaseTestCase):
    """Integration tests for data sources."""
    
    def test_source_switching(self):
        """Test switching between different data sources."""
        # Create multiple sources
        sim_source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=100.0
        )
        
        df = DataFactory.create_sample_dataframe(100)
        csv_path = self.create_temp_csv("switch_test.csv", df)
        file_source = FileDataSource(
            file_path=str(csv_path),
            file_format='csv'
        )
        
        # Read from different sources
        sim_batch = sim_source.read_batch(batch_size=10)
        file_batch = file_source.read_batch(batch_size=10)
        
        self.assertEqual(len(sim_batch), 10)
        self.assertEqual(len(file_batch), 10)
        
        # Different data
        self.assertFalse(sim_batch.equals(file_batch))
        
    def test_source_as_generator(self):
        """Test using source as a generator."""
        source = SimulatedDataSource(
            schema={'value': 'float'},
            rate=1000.0
        )
        
        def batch_generator(source, batch_size=10, max_batches=5):
            """Generate batches from source."""
            count = 0
            while count < max_batches:
                batch = source.read_batch(batch_size)
                if batch is None:
                    break
                yield batch
                count += 1
                
        # Consume generator
        batches = list(batch_generator(source))
        
        self.assertEqual(len(batches), 5)
        for batch in batches:
            self.assertEqual(len(batch), 10)


if __name__ == "__main__":
    unittest.main()