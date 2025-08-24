"""
Additional streaming data sources for AirPipe.
"""

import time
import json
import random
from typing import Generator, Any, Dict, List, Optional, Callable
from pathlib import Path
from datetime import datetime, timedelta
import logging
import pandas as pd
from abc import ABC, abstractmethod
import requests
from queue import Queue, Empty
import threading

logger = logging.getLogger(__name__)


class KafkaSource:
    """
    Stream data from Apache Kafka topics.
    Requires kafka-python to be installed.
    """
    
    def __init__(self, 
                 bootstrap_servers: str,
                 topic: str,
                 group_id: str = "airpipe_consumer",
                 auto_offset_reset: str = "latest"):
        """
        Initialize Kafka source.
        
        Args:
            bootstrap_servers: Kafka broker addresses
            topic: Topic to consume from
            group_id: Consumer group ID
            auto_offset_reset: Where to start reading ("earliest" or "latest")
        """
        try:
            from kafka import KafkaConsumer
        except ImportError:
            raise ImportError("kafka-python required. Install with: pip install kafka-python")
        
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        logger.info(f"Connected to Kafka topic: {topic}")
    
    def read(self) -> Generator[Dict, None, None]:
        """Read messages from Kafka."""
        try:
            for message in self.consumer:
                yield message.value
        finally:
            self.consumer.close()


class APIPollingSource:
    """Poll REST API endpoints for data."""
    
    def __init__(self,
                 url: str,
                 poll_interval: float = 30.0,
                 headers: Optional[Dict] = None,
                 params: Optional[Dict] = None,
                 data_extractor: Optional[Callable] = None):
        """
        Initialize API polling source.
        
        Args:
            url: API endpoint URL
            poll_interval: Seconds between polls
            headers: Optional HTTP headers
            params: Optional query parameters
            data_extractor: Function to extract data from response
        """
        self.url = url
        self.poll_interval = poll_interval
        self.headers = headers or {}
        self.params = params or {}
        self.data_extractor = data_extractor or self._default_extractor
        self.last_poll = None
        
    def _default_extractor(self, response: Dict) -> List[Dict]:
        """Default data extraction from API response."""
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            # Try common patterns
            for key in ['data', 'results', 'items', 'records']:
                if key in response and isinstance(response[key], list):
                    return response[key]
            return [response]
        return []
    
    def read(self) -> Generator[Dict, None, None]:
        """Poll API and yield records."""
        while True:
            try:
                # Make API request
                response = requests.get(
                    self.url,
                    headers=self.headers,
                    params=self.params
                )
                response.raise_for_status()
                
                # Extract data
                data = response.json()
                records = self.data_extractor(data)
                
                # Yield each record
                for record in records:
                    yield record
                
                logger.debug(f"Polled {len(records)} records from {self.url}")
                
            except Exception as e:
                logger.error(f"Error polling API: {e}")
            
            # Wait before next poll
            time.sleep(self.poll_interval)


class DatabaseChangeSource:
    """
    Stream changes from database using CDC (Change Data Capture).
    This is a simplified example - real CDC requires database-specific setup.
    """
    
    def __init__(self,
                 connection_string: str,
                 table: str,
                 timestamp_column: str = "modified_at",
                 poll_interval: float = 5.0):
        """
        Initialize database change source.
        
        Args:
            connection_string: Database connection string
            table: Table to monitor
            timestamp_column: Column tracking modification time
            poll_interval: Seconds between polls
        """
        self.connection_string = connection_string
        self.table = table
        self.timestamp_column = timestamp_column
        self.poll_interval = poll_interval
        self.last_timestamp = datetime.now() - timedelta(days=1)
        
    def read(self) -> Generator[Dict, None, None]:
        """Poll database for changes."""
        import sqlalchemy as sa
        
        engine = sa.create_engine(self.connection_string)
        
        while True:
            try:
                # Query for new/updated records
                query = f"""
                    SELECT * FROM {self.table}
                    WHERE {self.timestamp_column} > '{self.last_timestamp}'
                    ORDER BY {self.timestamp_column}
                """
                
                df = pd.read_sql(query, engine)
                
                if not df.empty:
                    # Update last timestamp
                    self.last_timestamp = df[self.timestamp_column].max()
                    
                    # Yield each row
                    for _, row in df.iterrows():
                        yield row.to_dict()
                    
                    logger.debug(f"Found {len(df)} changes in {self.table}")
                
            except Exception as e:
                logger.error(f"Error polling database: {e}")
            
            time.sleep(self.poll_interval)


class SocketSource:
    """Stream data from TCP/UDP sockets."""
    
    def __init__(self,
                 host: str = "localhost",
                 port: int = 9999,
                 protocol: str = "tcp",
                 buffer_size: int = 4096):
        """
        Initialize socket source.
        
        Args:
            host: Host to bind to
            port: Port to listen on
            protocol: "tcp" or "udp"
            buffer_size: Buffer size for receiving data
        """
        import socket
        
        self.host = host
        self.port = port
        self.protocol = protocol.lower()
        self.buffer_size = buffer_size
        
        if self.protocol == "tcp":
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        elif self.protocol == "udp":
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            raise ValueError(f"Unsupported protocol: {protocol}")
        
        self.socket.bind((self.host, self.port))
        
        if self.protocol == "tcp":
            self.socket.listen(5)
            logger.info(f"Listening for TCP connections on {host}:{port}")
        else:
            logger.info(f"Listening for UDP packets on {host}:{port}")
    
    def read(self) -> Generator[Dict, None, None]:
        """Read data from socket."""
        try:
            if self.protocol == "tcp":
                # Accept connections
                while True:
                    conn, addr = self.socket.accept()
                    logger.info(f"Connection from {addr}")
                    
                    while True:
                        data = conn.recv(self.buffer_size)
                        if not data:
                            break
                        
                        # Parse JSON data
                        try:
                            record = json.loads(data.decode('utf-8'))
                            yield record
                        except json.JSONDecodeError:
                            # Try line-based parsing
                            for line in data.decode('utf-8').split('\n'):
                                if line.strip():
                                    try:
                                        yield json.loads(line)
                                    except:
                                        yield {"raw": line}
                    
                    conn.close()
            
            else:  # UDP
                while True:
                    data, addr = self.socket.recvfrom(self.buffer_size)
                    try:
                        record = json.loads(data.decode('utf-8'))
                        yield record
                    except:
                        yield {"raw": data.decode('utf-8'), "from": addr}
        
        finally:
            self.socket.close()


class S3EventSource:
    """
    Stream S3 events (new file uploads).
    Requires boto3 to be installed.
    """
    
    def __init__(self,
                 bucket: str,
                 prefix: str = "",
                 poll_interval: float = 30.0,
                 process_existing: bool = False):
        """
        Initialize S3 event source.
        
        Args:
            bucket: S3 bucket name
            prefix: Object key prefix to filter
            poll_interval: Seconds between polls
            process_existing: Whether to process existing files
        """
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 required. Install with: pip install boto3")
        
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.prefix = prefix
        self.poll_interval = poll_interval
        self.processed_keys = set()
        
        if not process_existing:
            # Mark existing files as processed
            response = self.s3.list_objects_v2(
                Bucket=self.bucket,
                Prefix=self.prefix
            )
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.processed_keys.add(obj['Key'])
    
    def read(self) -> Generator[Dict, None, None]:
        """Poll S3 for new objects."""
        import boto3
        
        while True:
            try:
                # List objects
                response = self.s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=self.prefix
                )
                
                if 'Contents' in response:
                    for obj in response['Contents']:
                        key = obj['Key']
                        
                        if key not in self.processed_keys:
                            # New object found
                            logger.info(f"New S3 object: {key}")
                            
                            # Get object content
                            obj_response = self.s3.get_object(
                                Bucket=self.bucket,
                                Key=key
                            )
                            
                            # Parse based on file type
                            if key.endswith('.json'):
                                content = json.loads(obj_response['Body'].read())
                                if isinstance(content, list):
                                    for item in content:
                                        yield item
                                else:
                                    yield content
                            
                            elif key.endswith('.csv'):
                                df = pd.read_csv(obj_response['Body'])
                                for _, row in df.iterrows():
                                    yield row.to_dict()
                            
                            else:
                                # Return metadata for other file types
                                yield {
                                    'bucket': self.bucket,
                                    'key': key,
                                    'size': obj['Size'],
                                    'last_modified': obj['LastModified'].isoformat()
                                }
                            
                            self.processed_keys.add(key)
                
            except Exception as e:
                logger.error(f"Error polling S3: {e}")
            
            time.sleep(self.poll_interval)


class QueueSource:
    """Stream data from Python Queue (for inter-thread communication)."""
    
    def __init__(self, queue: Optional[Queue] = None):
        """
        Initialize queue source.
        
        Args:
            queue: Queue instance (creates new if not provided)
        """
        self.queue = queue or Queue()
    
    def put(self, item: Any) -> None:
        """Add item to queue."""
        self.queue.put(item)
    
    def read(self) -> Generator[Any, None, None]:
        """Read from queue."""
        while True:
            try:
                item = self.queue.get(timeout=1.0)
                yield item
            except Empty:
                continue


class SimulatedDataSource:
    """Generate simulated streaming data for testing."""
    
    def __init__(self,
                 schema: Dict[str, str],
                 rate: float = 10.0,
                 noise: float = 0.1,
                 anomaly_rate: float = 0.01):
        """
        Initialize simulated data source.
        
        Args:
            schema: Data schema (field_name: type)
            rate: Records per second
            noise: Noise level (0-1)
            anomaly_rate: Probability of anomalies
        """
        self.schema = schema
        self.rate = rate
        self.noise = noise
        self.anomaly_rate = anomaly_rate
        self.record_count = 0
    
    def _generate_value(self, field_type: str, is_anomaly: bool = False):
        """Generate value based on type."""
        if field_type == "int":
            base = random.randint(1, 100)
            if is_anomaly:
                return base * random.randint(10, 100)
            return base + random.randint(-5, 5)
        
        elif field_type == "float":
            base = random.uniform(0, 100)
            if is_anomaly:
                return base * random.uniform(10, 100)
            return base + random.uniform(-5, 5) * self.noise
        
        elif field_type == "string":
            options = ["A", "B", "C", "D", "E"]
            if is_anomaly:
                return "ANOMALY_" + random.choice(options)
            return random.choice(options)
        
        elif field_type == "timestamp":
            now = datetime.now()
            if is_anomaly:
                # Time far in past or future
                offset = random.randint(-365, 365)
                return (now + timedelta(days=offset)).isoformat()
            return now.isoformat()
        
        elif field_type == "boolean":
            if is_anomaly:
                return None  # Unexpected null
            return random.choice([True, False])
        
        else:
            return None
    
    def read(self) -> Generator[Dict, None, None]:
        """Generate simulated records."""
        while True:
            # Generate record
            is_anomaly = random.random() < self.anomaly_rate
            
            record = {
                "_id": self.record_count,
                "_timestamp": datetime.now().isoformat(),
                "_is_anomaly": is_anomaly
            }
            
            for field, field_type in self.schema.items():
                record[field] = self._generate_value(field_type, is_anomaly)
            
            yield record
            
            self.record_count += 1
            
            # Control rate
            time.sleep(1.0 / self.rate)


class WebSocketSource:
    """
    Stream data from WebSocket connections.
    Requires websocket-client to be installed.
    """
    
    def __init__(self,
                 url: str,
                 on_connect: Optional[Callable] = None,
                 heartbeat_interval: float = 30.0):
        """
        Initialize WebSocket source.
        
        Args:
            url: WebSocket URL (ws:// or wss://)
            on_connect: Optional callback after connection
            heartbeat_interval: Interval for sending heartbeat
        """
        try:
            import websocket
        except ImportError:
            raise ImportError("websocket-client required. Install with: pip install websocket-client")
        
        self.url = url
        self.on_connect = on_connect
        self.heartbeat_interval = heartbeat_interval
        self.ws = None
        self.queue = Queue()
        self.is_running = False
    
    def _on_message(self, ws, message):
        """Handle incoming message."""
        try:
            data = json.loads(message)
            self.queue.put(data)
        except json.JSONDecodeError:
            self.queue.put({"raw": message})
    
    def _on_error(self, ws, error):
        """Handle WebSocket error."""
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws):
        """Handle WebSocket close."""
        logger.info("WebSocket connection closed")
        self.is_running = False
    
    def _on_open(self, ws):
        """Handle WebSocket open."""
        logger.info(f"Connected to WebSocket: {self.url}")
        if self.on_connect:
            self.on_connect(ws)
        
        # Start heartbeat thread
        def heartbeat():
            while self.is_running:
                time.sleep(self.heartbeat_interval)
                if self.ws:
                    self.ws.send(json.dumps({"type": "heartbeat"}))
        
        threading.Thread(target=heartbeat, daemon=True).start()
    
    def read(self) -> Generator[Dict, None, None]:
        """Read from WebSocket."""
        import websocket
        
        self.is_running = True
        
        # Start WebSocket in thread
        self.ws = websocket.WebSocketApp(
            self.url,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        
        ws_thread = threading.Thread(
            target=self.ws.run_forever,
            daemon=True
        )
        ws_thread.start()
        
        # Read from queue
        while self.is_running:
            try:
                data = self.queue.get(timeout=1.0)
                yield data
            except Empty:
                continue
        
        # Cleanup
        if self.ws:
            self.ws.close()


# Factory function for creating sources
def create_source(source_type: str, **kwargs) -> Any:
    """
    Factory function to create streaming sources.
    
    Args:
        source_type: Type of source to create
        **kwargs: Source-specific configuration
        
    Returns:
        StreamingSource instance
    """
    sources = {
        'kafka': KafkaSource,
        'api': APIPollingSource,
        'database': DatabaseChangeSource,
        'socket': SocketSource,
        's3': S3EventSource,
        'queue': QueueSource,
        'simulated': SimulatedDataSource,
        'websocket': WebSocketSource
    }
    
    if source_type not in sources:
        raise ValueError(f"Unknown source type: {source_type}. Available: {list(sources.keys())}")
    
    return sources[source_type](**kwargs)