"""
SQL Task Decorator for DuckDB Integration.

Provides @pipeline.sql_task() decorator for defining SQL-based
pipeline tasks that execute directly in DuckDB.
"""

import functools
from typing import Optional, Dict, Any, List, Union, Callable
import pandas as pd
import logging

from airpipe.core.task import TaskPipeline
from .session import DuckDBSession
from .artifact import DuckDBArtifact

LOG = logging.getLogger(__name__)


def sql_task(
    pipeline: TaskPipeline,
    sql: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    consumes: Optional[Union[str, List[str]]] = None,
    produces: Optional[str] = None,
    database: str = ':memory:',
    **kwargs
):
    """
    Decorator for SQL-based pipeline tasks using DuckDB.
    
    This decorator allows you to define pipeline tasks using SQL queries
    that execute in DuckDB. The SQL can reference artifacts as tables.
    
    Args:
        pipeline: TaskPipeline instance
        sql: SQL query to execute (can use {artifact_name} placeholders)
        depends_on: Task dependencies
        consumes: Artifact(s) to consume
        produces: Artifact to produce
        database: DuckDB database to use
        **kwargs: Additional task decorator arguments
    
    Usage:
        @sql_task(pipeline, sql="SELECT * FROM {raw_data} WHERE value > 100", 
                  consumes="raw_data", produces="filtered_data")
        def filter_high_values():
            pass  # SQL is executed automatically
        
        # Or with dynamic SQL:
        @sql_task(pipeline, consumes="raw_data", produces="aggregated")
        def aggregate_data():
            return "SELECT category, SUM(value) as total FROM {raw_data} GROUP BY category"
    """
    def decorator(func: Callable) -> Callable:
        
        @functools.wraps(func)
        def wrapper(*args, **func_kwargs):
            # Get SQL query - either from decorator param or function return
            query = sql
            if query is None:
                # Function should return SQL string
                result = func(*args, **func_kwargs)
                if isinstance(result, str):
                    query = result
                else:
                    raise ValueError(
                        f"SQL task {func.__name__} must either have 'sql' parameter "
                        "or return SQL string"
                    )
            
            # Get DuckDB connection
            conn = DuckDBSession.get_or_create({'database': database})
            
            # Register consumed artifacts as tables
            if consumes:
                artifacts_to_consume = [consumes] if isinstance(consumes, str) else consumes
                
                for artifact_name in artifacts_to_consume:
                    artifact = pipeline.get_artifact(artifact_name)
                    if artifact:
                        df = artifact.as_dataframe()
                        conn.register(artifact_name, df)
                        LOG.debug(f"Registered artifact '{artifact_name}' as DuckDB table")
                        
                        # Replace placeholders in query
                        query = query.replace(f"{{{artifact_name}}}", artifact_name)
            
            # Execute SQL query
            LOG.info(f"Executing SQL task: {func.__name__}")
            LOG.debug(f"Query: {query[:200]}...")
            
            try:
                result_df = conn.execute(query).fetchdf()
                LOG.info(f"Query returned {len(result_df)} rows")
                
                # Create output artifact if produces is specified
                if produces:
                    artifact = DuckDBArtifact(
                        result_df,
                        name=produces,
                        metadata={
                            'source': 'sql_task',
                            'task': func.__name__,
                            'query': query[:500]
                        }
                    )
                    pipeline.store_artifact(artifact)
                    return artifact
                
                return result_df
                
            except Exception as e:
                LOG.error(f"SQL task {func.__name__} failed: {e}")
                raise
            
            finally:
                # Unregister temporary tables
                if consumes:
                    artifacts_to_consume = [consumes] if isinstance(consumes, str) else consumes
                    for artifact_name in artifacts_to_consume:
                        try:
                            conn.unregister(artifact_name)
                        except:
                            pass  # Table might not exist
        
        # Register with pipeline
        task_decorator = pipeline.task(
            depends_on=depends_on,
            consumes=consumes,
            produces=produces,
            **kwargs
        )
        
        # Apply task decorator
        wrapper = task_decorator(wrapper)
        wrapper._is_sql_task = True
        wrapper._sql = sql
        wrapper._database = database
        
        return wrapper
    
    return decorator


# Extend TaskPipeline with sql_task method
def add_sql_task_to_pipeline():
    """Add sql_task method to TaskPipeline class."""
    
    def sql_task_method(
        self,
        sql: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        consumes: Optional[Union[str, List[str]]] = None,
        produces: Optional[str] = None,
        database: str = ':memory:',
        **kwargs
    ):
        """
        SQL task decorator method for TaskPipeline.
        
        See sql_task function for full documentation.
        """
        return sql_task(
            pipeline=self,
            sql=sql,
            depends_on=depends_on,
            consumes=consumes,
            produces=produces,
            database=database,
            **kwargs
        )
    
    # Add method to TaskPipeline if not already present
    if not hasattr(TaskPipeline, 'sql_task'):
        TaskPipeline.sql_task = sql_task_method
        LOG.debug("Added sql_task method to TaskPipeline")


# Auto-add sql_task to TaskPipeline when module is imported
add_sql_task_to_pipeline()


class SQLPipeline(TaskPipeline):
    """
    Extended TaskPipeline with built-in SQL support.
    
    This class provides additional SQL-specific functionality
    for pipelines that primarily work with SQL queries.
    """
    
    def __init__(self, name: str, database: str = ':memory:', **kwargs):
        """
        Initialize SQL pipeline.
        
        Args:
            name: Pipeline name
            database: Default DuckDB database
            **kwargs: Additional TaskPipeline arguments
        """
        super().__init__(name, **kwargs)
        self.database = database
        self._conn = None
    
    def get_connection(self) -> Any:
        """Get DuckDB connection for this pipeline."""
        if self._conn is None:
            self._conn = DuckDBSession.get_or_create({'database': self.database})
        return self._conn
    
    def execute_sql(
        self,
        query: str,
        params: Optional[Dict] = None,
        return_artifact: bool = True,
        artifact_name: Optional[str] = None
    ) -> Union[DuckDBArtifact, pd.DataFrame]:
        """
        Execute SQL query directly.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            return_artifact: Return as DuckDBArtifact vs DataFrame
            artifact_name: Name for artifact if return_artifact=True
            
        Returns:
            Query result as DuckDBArtifact or DataFrame
        """
        conn = self.get_connection()
        
        # Execute query
        if params:
            result_df = conn.execute(query, params).fetchdf()
        else:
            result_df = conn.execute(query).fetchdf()
        
        if return_artifact:
            artifact = DuckDBArtifact(
                result_df,
                name=artifact_name or "sql_result",
                metadata={'query': query[:500]}
            )
            return artifact
        
        return result_df
    
    def load_table(
        self,
        table_name: str,
        data: Union[pd.DataFrame, str],
        if_exists: str = 'replace'
    ) -> None:
        """
        Load data into DuckDB table.
        
        Args:
            table_name: Table name to create/replace
            data: DataFrame or file path to load
            if_exists: How to handle existing table (replace, append, fail)
        """
        conn = self.get_connection()
        
        if isinstance(data, str):
            # Load from file
            if data.endswith('.parquet'):
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM '{data}'")
            elif data.endswith('.csv'):
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{data}')")
            elif data.endswith('.json'):
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{data}')")
            else:
                raise ValueError(f"Unsupported file format: {data}")
        else:
            # Load from DataFrame
            if if_exists == 'replace':
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data")
            elif if_exists == 'append':
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM data")
            elif if_exists == 'fail':
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data")
        
        LOG.info(f"Loaded data into table: {table_name}")
    
    def create_view(self, view_name: str, query: str) -> None:
        """
        Create a view in DuckDB.
        
        Args:
            view_name: View name
            query: SQL query for view
        """
        conn = self.get_connection()
        conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
        LOG.info(f"Created view: {view_name}")
    
    def cleanup(self) -> None:
        """Clean up resources and close connections."""
        if self._conn:
            DuckDBSession.stop(self.database)
            self._conn = None
        super().cleanup()