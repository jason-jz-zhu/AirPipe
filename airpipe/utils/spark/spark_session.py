"""
Singleton Spark Session Manager for AirPipe.

Provides a reusable Spark session that can be configured once and used throughout
the application lifecycle.
"""

from typing import Dict, Optional, Any
import logging
from threading import Lock

LOG = logging.getLogger(__name__)


class SparkSessionManager:
    """
    Singleton manager for PySpark sessions.
    
    Ensures only one Spark session exists per application and provides
    easy configuration and lifecycle management.
    
    Usage:
        # Get or create session with default config
        spark = SparkSessionManager.get_or_create()
        
        # Get or create with custom config
        spark = SparkSessionManager.get_or_create({
            'app_name': 'MyApp',
            'master': 'local[4]',
            'config': {
                'spark.sql.shuffle.partitions': '200'
            }
        })
        
        # Get existing session (returns None if not created)
        spark = SparkSessionManager.get_current()
        
        # Stop session
        SparkSessionManager.stop()
    """
    
    _instance = None
    _spark_session = None
    _lock = Lock()
    _default_config = {
        'app_name': 'AirPipe Application',
        'master': 'local[*]',
        'config': {
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.shuffle.partitions': '200',
            'spark.driver.memory': '2g',
            'spark.executor.memory': '2g',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',  # Enable Arrow optimization
            'spark.sql.execution.arrow.pyspark.fallback.enabled': 'true'
        }
    }
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    @classmethod
    def get_or_create(cls, config: Optional[Dict[str, Any]] = None):
        """
        Get existing Spark session or create new one with config.
        
        Args:
            config: Optional configuration dict with keys:
                - app_name: Spark application name
                - master: Spark master URL (e.g., 'local[*]', 'spark://...')
                - config: Dict of Spark configuration properties
                - memory: Dict with 'driver' and 'executor' memory settings
                
        Returns:
            SparkSession instance
        """
        instance = cls()
        
        if cls._spark_session is not None:
            LOG.info("Returning existing Spark session")
            return cls._spark_session
        
        with cls._lock:
            if cls._spark_session is None:
                try:
                    from pyspark.sql import SparkSession
                except ImportError:
                    raise ImportError(
                        "PySpark is not installed. Install it with: pip install pyspark"
                    )
                
                # Merge with default config
                final_config = cls._default_config.copy()
                if config:
                    if 'app_name' in config:
                        final_config['app_name'] = config['app_name']
                    if 'master' in config:
                        final_config['master'] = config['master']
                    if 'config' in config:
                        final_config['config'].update(config['config'])
                    if 'memory' in config:
                        if 'driver' in config['memory']:
                            final_config['config']['spark.driver.memory'] = config['memory']['driver']
                        if 'executor' in config['memory']:
                            final_config['config']['spark.executor.memory'] = config['memory']['executor']
                
                LOG.info(f"Creating new Spark session: {final_config['app_name']}")
                
                # Build Spark session
                builder = SparkSession.builder \
                    .appName(final_config['app_name']) \
                    .master(final_config['master'])
                
                # Apply all config settings
                for key, value in final_config['config'].items():
                    builder = builder.config(key, value)
                
                cls._spark_session = builder.getOrCreate()
                
                # Set log level
                cls._spark_session.sparkContext.setLogLevel("WARN")
                
                LOG.info(f"Spark session created successfully")
                LOG.info(f"Spark UI available at: {cls._spark_session.sparkContext.uiWebUrl}")
                
        return cls._spark_session
    
    @classmethod
    def get_current(cls):
        """
        Get current Spark session if it exists.
        
        Returns:
            SparkSession or None if not created
        """
        return cls._spark_session
    
    @classmethod
    def stop(cls):
        """Stop the current Spark session if it exists."""
        if cls._spark_session is not None:
            LOG.info("Stopping Spark session")
            cls._spark_session.stop()
            cls._spark_session = None
            LOG.info("Spark session stopped")
    
    @classmethod
    def reset(cls, config: Optional[Dict[str, Any]] = None):
        """
        Reset Spark session with new configuration.
        
        Args:
            config: New configuration for Spark session
            
        Returns:
            New SparkSession instance
        """
        cls.stop()
        return cls.get_or_create(config)
    
    @classmethod
    def configure_for_environment(cls, environment: str = 'local'):
        """
        Configure Spark session for specific environment.
        
        Args:
            environment: One of 'local', 'cluster', 'yarn', 'k8s'
            
        Returns:
            SparkSession configured for environment
        """
        configs = {
            'local': {
                'app_name': 'AirPipe Local',
                'master': 'local[*]',
                'config': {
                    'spark.driver.memory': '2g',
                    'spark.sql.shuffle.partitions': '50'
                }
            },
            'cluster': {
                'app_name': 'AirPipe Cluster',
                'master': 'spark://master:7077',
                'config': {
                    'spark.driver.memory': '4g',
                    'spark.executor.memory': '4g',
                    'spark.executor.instances': '4',
                    'spark.sql.shuffle.partitions': '200'
                }
            },
            'yarn': {
                'app_name': 'AirPipe YARN',
                'master': 'yarn',
                'config': {
                    'spark.submit.deployMode': 'client',
                    'spark.driver.memory': '4g',
                    'spark.executor.memory': '4g',
                    'spark.executor.instances': '10',
                    'spark.sql.shuffle.partitions': '400'
                }
            },
            'k8s': {
                'app_name': 'AirPipe K8s',
                'master': 'k8s://https://kubernetes.default.svc',
                'config': {
                    'spark.executor.instances': '5',
                    'spark.kubernetes.container.image': 'spark:latest',
                    'spark.driver.memory': '4g',
                    'spark.executor.memory': '4g'
                }
            }
        }
        
        if environment not in configs:
            raise ValueError(f"Unknown environment: {environment}. Choose from {list(configs.keys())}")
        
        return cls.get_or_create(configs[environment])
    
    @classmethod
    def get_spark_context(cls):
        """Get SparkContext from current session."""
        if cls._spark_session is None:
            raise RuntimeError("No Spark session exists. Call get_or_create() first")
        return cls._spark_session.sparkContext
    
    @classmethod
    def get_sql_context(cls):
        """Get SQLContext from current session."""
        if cls._spark_session is None:
            raise RuntimeError("No Spark session exists. Call get_or_create() first")
        return cls._spark_session.sql
    
    @classmethod
    def set_log_level(cls, level: str = "WARN"):
        """
        Set Spark log level.
        
        Args:
            level: One of "ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"
        """
        if cls._spark_session is None:
            raise RuntimeError("No Spark session exists. Call get_or_create() first")
        cls._spark_session.sparkContext.setLogLevel(level)
    
    @classmethod
    def get_config(cls) -> Dict[str, str]:
        """Get all Spark configuration as dict."""
        if cls._spark_session is None:
            raise RuntimeError("No Spark session exists. Call get_or_create() first")
        return dict(cls._spark_session.sparkContext.getConf().getAll())