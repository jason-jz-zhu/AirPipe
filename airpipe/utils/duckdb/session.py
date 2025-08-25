"""
DuckDB Session Manager for AirPipe.

Provides a singleton DuckDB session that can be configured once and used throughout
the application lifecycle. Similar to SparkSessionManager but for DuckDB.
"""

import duckdb
from typing import Dict, Optional, Any, List
import logging
from threading import Lock
from pathlib import Path

LOG = logging.getLogger(__name__)


class DuckDBSession:
    """
    Singleton manager for DuckDB connections.
    
    Ensures only one DuckDB connection exists per database and provides
    easy configuration and lifecycle management.
    
    Usage:
        # Get or create in-memory session
        db = DuckDBSession.get_or_create()
        
        # Get or create with persistent database
        db = DuckDBSession.get_or_create({
            'database': 'analytics.db',
            'read_only': False,
            'config': {
                'memory_limit': '4GB',
                'threads': 4
            }
        })
        
        # Execute queries
        result = db.execute("SELECT * FROM 'data.parquet'")
        
        # Get existing session
        db = DuckDBSession.get_current()
        
        # Stop session
        DuckDBSession.stop()
    """
    
    _instances: Dict[str, 'DuckDBSession'] = {}
    _connections: Dict[str, duckdb.DuckDBPyConnection] = {}
    _lock = Lock()
    _default_config = {
        'database': ':memory:',
        'read_only': False,
        'config': {
            'memory_limit': '2GB',
            'threads': -1,  # Use all available threads
            'default_order': 'ASC',
            'enable_profiling': False,
            'enable_progress_bar': False,
            'max_expression_depth': 1000
        }
    }
    
    def __new__(cls, database: str = ':memory:'):
        with cls._lock:
            if database not in cls._instances:
                cls._instances[database] = super().__new__(cls)
        return cls._instances[database]
    
    def __init__(self, database: str = ':memory:'):
        """Initialize DuckDB session for specific database."""
        self.database = database
    
    @classmethod
    def get_or_create(cls, config: Optional[Dict[str, Any]] = None) -> duckdb.DuckDBPyConnection:
        """
        Get existing DuckDB connection or create new one with config.
        
        Args:
            config: Optional configuration dict with keys:
                - database: Path to database file or ':memory:'
                - read_only: Open in read-only mode
                - config: Dict of DuckDB configuration options
                - memory_limit: Memory limit (e.g., '4GB')
                - threads: Number of threads to use
                
        Returns:
            DuckDB connection instance
        """
        # Merge with default config
        final_config = cls._default_config.copy()
        if config:
            if 'database' in config:
                final_config['database'] = config['database']
            if 'read_only' in config:
                final_config['read_only'] = config['read_only']
            if 'config' in config:
                final_config['config'].update(config['config'])
            # Convenience options
            if 'memory_limit' in config:
                final_config['config']['memory_limit'] = config['memory_limit']
            if 'threads' in config:
                final_config['config']['threads'] = config['threads']
        
        database = final_config['database']
        
        with cls._lock:
            if database not in cls._connections:
                LOG.info(f"Creating new DuckDB connection: {database}")
                
                # Create connection
                conn = duckdb.connect(
                    database=database,
                    read_only=final_config['read_only']
                )
                
                # Apply configuration
                for key, value in final_config['config'].items():
                    try:
                        conn.execute(f"SET {key} = '{value}'")
                    except Exception as e:
                        LOG.warning(f"Could not set {key} = {value}: {e}")
                
                cls._connections[database] = conn
                
                # Install and load common extensions
                cls._setup_extensions(conn)
                
                LOG.info(f"DuckDB connection created successfully")
                if database != ':memory:':
                    LOG.info(f"Database file: {database}")
            else:
                LOG.info(f"Returning existing DuckDB connection: {database}")
        
        return cls._connections[database]
    
    @classmethod
    def _setup_extensions(cls, conn: duckdb.DuckDBPyConnection):
        """Install and load useful DuckDB extensions."""
        extensions = ['httpfs', 'parquet', 'json']
        
        for ext in extensions:
            try:
                conn.install_extension(ext)
                conn.load_extension(ext)
                LOG.debug(f"Loaded extension: {ext}")
            except Exception as e:
                LOG.debug(f"Could not load extension {ext}: {e}")
    
    @classmethod
    def get_current(cls, database: str = ':memory:') -> Optional[duckdb.DuckDBPyConnection]:
        """
        Get current DuckDB connection if it exists.
        
        Args:
            database: Database to get connection for
            
        Returns:
            DuckDB connection or None if not created
        """
        return cls._connections.get(database)
    
    @classmethod
    def execute(cls, query: str, database: str = ':memory:') -> Any:
        """
        Execute a query on the specified database.
        
        Args:
            query: SQL query to execute
            database: Database to execute on
            
        Returns:
            Query result
        """
        conn = cls.get_or_create({'database': database})
        return conn.execute(query).fetchall()
    
    @classmethod
    def execute_many(cls, queries: List[str], database: str = ':memory:') -> List[Any]:
        """
        Execute multiple queries in sequence.
        
        Args:
            queries: List of SQL queries
            database: Database to execute on
            
        Returns:
            List of query results
        """
        conn = cls.get_or_create({'database': database})
        results = []
        for query in queries:
            try:
                result = conn.execute(query).fetchall()
                results.append(result)
            except Exception as e:
                LOG.error(f"Error executing query: {e}")
                results.append(None)
        return results
    
    @classmethod
    def read_parquet(cls, path: str, database: str = ':memory:') -> duckdb.DuckDBPyRelation:
        """
        Read Parquet file(s) into DuckDB.
        
        Args:
            path: Path to Parquet file(s), supports wildcards
            database: Database to use
            
        Returns:
            DuckDB relation object
        """
        conn = cls.get_or_create({'database': database})
        return conn.read_parquet(path)
    
    @classmethod
    def read_csv(cls, path: str, database: str = ':memory:', **kwargs) -> duckdb.DuckDBPyRelation:
        """
        Read CSV file(s) into DuckDB.
        
        Args:
            path: Path to CSV file(s)
            database: Database to use
            **kwargs: Additional CSV reading options
            
        Returns:
            DuckDB relation object
        """
        conn = cls.get_or_create({'database': database})
        return conn.read_csv(path, **kwargs)
    
    @classmethod
    def read_json(cls, path: str, database: str = ':memory:', **kwargs) -> duckdb.DuckDBPyRelation:
        """
        Read JSON file(s) into DuckDB.
        
        Args:
            path: Path to JSON file(s)
            database: Database to use
            **kwargs: Additional JSON reading options
            
        Returns:
            DuckDB relation object
        """
        conn = cls.get_or_create({'database': database})
        return conn.read_json(path, **kwargs)
    
    @classmethod
    def from_pandas(cls, df, database: str = ':memory:') -> duckdb.DuckDBPyRelation:
        """
        Create DuckDB relation from Pandas DataFrame.
        
        Args:
            df: Pandas DataFrame
            database: Database to use
            
        Returns:
            DuckDB relation object
        """
        conn = cls.get_or_create({'database': database})
        return conn.from_df(df)
    
    @classmethod
    def stop(cls, database: Optional[str] = None):
        """
        Stop DuckDB connection(s).
        
        Args:
            database: Specific database to stop, or None for all
        """
        with cls._lock:
            if database:
                if database in cls._connections:
                    LOG.info(f"Closing DuckDB connection: {database}")
                    cls._connections[database].close()
                    del cls._connections[database]
                    if database in cls._instances:
                        del cls._instances[database]
            else:
                # Close all connections
                LOG.info("Closing all DuckDB connections")
                for db, conn in cls._connections.items():
                    conn.close()
                cls._connections.clear()
                cls._instances.clear()
    
    @classmethod
    def reset(cls, config: Optional[Dict[str, Any]] = None) -> duckdb.DuckDBPyConnection:
        """
        Reset DuckDB connection with new configuration.
        
        Args:
            config: New configuration
            
        Returns:
            New DuckDB connection instance
        """
        database = config.get('database', ':memory:') if config else ':memory:'
        cls.stop(database)
        return cls.get_or_create(config)
    
    @classmethod
    def get_tables(cls, database: str = ':memory:') -> List[str]:
        """
        Get list of tables in the database.
        
        Args:
            database: Database to query
            
        Returns:
            List of table names
        """
        conn = cls.get_or_create({'database': database})
        result = conn.execute("SHOW TABLES").fetchall()
        return [row[0] for row in result]
    
    @classmethod
    def get_table_info(cls, table: str, database: str = ':memory:') -> List[tuple]:
        """
        Get information about a table.
        
        Args:
            table: Table name
            database: Database to query
            
        Returns:
            Table schema information
        """
        conn = cls.get_or_create({'database': database})
        return conn.execute(f"DESCRIBE {table}").fetchall()
    
    @classmethod
    def profile_query(cls, query: str, database: str = ':memory:') -> str:
        """
        Profile a query to understand performance.
        
        Args:
            query: SQL query to profile
            database: Database to use
            
        Returns:
            Query profile as string
        """
        conn = cls.get_or_create({'database': database})
        
        # Enable profiling
        conn.execute("SET enable_profiling = true")
        conn.execute("SET profiling_mode = 'detailed'")
        
        # Execute query
        conn.execute(query)
        
        # Get profile
        profile = conn.execute("SELECT * FROM duckdb_profiling_info()").fetchall()
        
        # Disable profiling
        conn.execute("SET enable_profiling = false")
        
        return str(profile)
    
    @classmethod
    def get_config(cls, database: str = ':memory:') -> Dict[str, str]:
        """
        Get all DuckDB configuration as dict.
        
        Args:
            database: Database to query
            
        Returns:
            Configuration dictionary
        """
        conn = cls.get_or_create({'database': database})
        result = conn.execute("SELECT * FROM duckdb_settings()").fetchall()
        return {row[0]: row[1] for row in result}