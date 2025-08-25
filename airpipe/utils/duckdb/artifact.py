"""
DuckDB Artifact Support for AirPipe.

Extends DataArtifact to support DuckDB-specific operations and
provides seamless integration between AirPipe artifacts and DuckDB.
"""

import duckdb
import pandas as pd
from typing import Optional, Any, Dict, List
import logging
from pathlib import Path

from airpipe.artifacts.data_artifact import DataArtifact
from .session import DuckDBSession

LOG = logging.getLogger(__name__)


class DuckDBArtifact(DataArtifact):
    """
    Extended DataArtifact with DuckDB-specific capabilities.
    
    Allows artifacts to be directly queried with SQL, converted to
    DuckDB relations, and efficiently processed using DuckDB's
    columnar engine.
    """
    
    def __init__(self, data: Any, name: str = "unnamed", metadata: Optional[Dict] = None):
        """Initialize DuckDB artifact."""
        super().__init__(data, name, metadata)
        self._duckdb_table = None
        self._database = ':memory:'
    
    def to_duckdb(
        self,
        table_name: Optional[str] = None,
        database: str = ':memory:',
        persist: bool = False
    ) -> duckdb.DuckDBPyRelation:
        """
        Convert artifact to DuckDB relation.
        
        Args:
            table_name: Optional table name to create
            database: Database to use
            persist: Whether to persist as table
            
        Returns:
            DuckDB relation object
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Convert data to DataFrame if needed
        df = self.as_dataframe()
        
        # Create relation from DataFrame
        relation = conn.from_df(df)
        
        if persist and table_name:
            # Create permanent table
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM relation")
            self._duckdb_table = table_name
            self._database = database
            LOG.info(f"Created DuckDB table: {table_name}")
            return conn.table(table_name)
        
        return relation
    
    def query(self, sql: str, database: str = ':memory:') -> pd.DataFrame:
        """
        Execute SQL query directly on artifact data.
        
        Args:
            sql: SQL query (use 'data' as table name)
            database: Database to use
            
        Returns:
            Query result as DataFrame
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Register DataFrame as temporary view
        df = self.as_dataframe()
        conn.register('data', df)
        
        try:
            # Execute query
            result = conn.execute(sql).fetchdf()
            LOG.debug(f"Executed query on artifact: {sql[:100]}...")
            return result
        finally:
            # Unregister temporary view
            conn.unregister('data')
    
    def aggregate(
        self,
        group_by: List[str],
        aggregations: Dict[str, str],
        database: str = ':memory:'
    ) -> pd.DataFrame:
        """
        Perform aggregations using DuckDB.
        
        Args:
            group_by: Columns to group by
            aggregations: Dict of {column: agg_function}
            database: Database to use
            
        Returns:
            Aggregated DataFrame
        """
        # Build aggregation expressions
        agg_exprs = []
        for col, func in aggregations.items():
            agg_exprs.append(f"{func}({col}) AS {col}_{func.lower()}")
        
        # Build query
        query = f"""
        SELECT 
            {', '.join(group_by)},
            {', '.join(agg_exprs)}
        FROM data
        GROUP BY {', '.join(group_by)}
        ORDER BY {', '.join(group_by)}
        """
        
        return self.query(query, database)
    
    def filter(self, condition: str, database: str = ':memory:') -> 'DuckDBArtifact':
        """
        Filter artifact data using SQL condition.
        
        Args:
            condition: SQL WHERE clause condition
            database: Database to use
            
        Returns:
            New filtered DuckDBArtifact
        """
        query = f"SELECT * FROM data WHERE {condition}"
        filtered_df = self.query(query, database)
        
        return DuckDBArtifact(
            filtered_df,
            name=f"{self.name}_filtered",
            metadata={**self.metadata, 'filter': condition}
        )
    
    def join(
        self,
        other: 'DuckDBArtifact',
        on: str,
        how: str = 'inner',
        database: str = ':memory:'
    ) -> 'DuckDBArtifact':
        """
        Join with another DuckDBArtifact.
        
        Args:
            other: Other artifact to join with
            on: Join condition
            how: Join type (inner, left, right, full)
            database: Database to use
            
        Returns:
            New joined DuckDBArtifact
        """
        conn = DuckDBSession.get_or_create({'database': database})
        
        # Register both DataFrames
        df1 = self.as_dataframe()
        df2 = other.as_dataframe()
        conn.register('left_table', df1)
        conn.register('right_table', df2)
        
        # Map join type
        join_map = {
            'inner': 'INNER JOIN',
            'left': 'LEFT JOIN',
            'right': 'RIGHT JOIN',
            'full': 'FULL OUTER JOIN'
        }
        join_type = join_map.get(how, 'INNER JOIN')
        
        query = f"""
        SELECT *
        FROM left_table
        {join_type} right_table
        ON {on}
        """
        
        try:
            result = conn.execute(query).fetchdf()
            return DuckDBArtifact(
                result,
                name=f"{self.name}_joined_{other.name}",
                metadata={**self.metadata, 'joined_with': other.name}
            )
        finally:
            conn.unregister('left_table')
            conn.unregister('right_table')
    
    def to_parquet(self, path: str, database: str = ':memory:') -> str:
        """
        Save artifact to Parquet file using DuckDB.
        
        Args:
            path: Output file path
            database: Database to use
            
        Returns:
            Output file path
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = self.as_dataframe()
        conn.register('data', df)
        
        try:
            conn.execute(f"COPY data TO '{path}' (FORMAT PARQUET)")
            LOG.info(f"Saved artifact to Parquet: {path}")
            return path
        finally:
            conn.unregister('data')
    
    def to_csv(self, path: str, database: str = ':memory:', **kwargs) -> str:
        """
        Save artifact to CSV file using DuckDB.
        
        Args:
            path: Output file path
            database: Database to use
            **kwargs: Additional CSV options
            
        Returns:
            Output file path
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = self.as_dataframe()
        conn.register('data', df)
        
        # Build options
        options = []
        if kwargs.get('header', True):
            options.append('HEADER')
        if kwargs.get('delimiter'):
            options.append(f"DELIMITER '{kwargs['delimiter']}'")
        
        options_str = f"({', '.join(options)})" if options else ""
        
        try:
            conn.execute(f"COPY data TO '{path}' {options_str}")
            LOG.info(f"Saved artifact to CSV: {path}")
            return path
        finally:
            conn.unregister('data')
    
    @classmethod
    def from_parquet(
        cls,
        path: str,
        name: Optional[str] = None,
        database: str = ':memory:'
    ) -> 'DuckDBArtifact':
        """
        Create artifact from Parquet file using DuckDB.
        
        Args:
            path: Parquet file path
            name: Artifact name
            database: Database to use
            
        Returns:
            New DuckDBArtifact
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = conn.read_parquet(path).df()
        
        artifact_name = name or Path(path).stem
        return cls(
            df,
            name=artifact_name,
            metadata={'source': path, 'format': 'parquet'}
        )
    
    @classmethod
    def from_csv(
        cls,
        path: str,
        name: Optional[str] = None,
        database: str = ':memory:',
        **kwargs
    ) -> 'DuckDBArtifact':
        """
        Create artifact from CSV file using DuckDB.
        
        Args:
            path: CSV file path
            name: Artifact name
            database: Database to use
            **kwargs: Additional CSV reading options
            
        Returns:
            New DuckDBArtifact
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = conn.read_csv(path, **kwargs).df()
        
        artifact_name = name or Path(path).stem
        return cls(
            df,
            name=artifact_name,
            metadata={'source': path, 'format': 'csv'}
        )
    
    @classmethod
    def from_query(
        cls,
        query: str,
        name: str,
        database: str = ':memory:'
    ) -> 'DuckDBArtifact':
        """
        Create artifact from SQL query result.
        
        Args:
            query: SQL query
            name: Artifact name
            database: Database to use
            
        Returns:
            New DuckDBArtifact
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = conn.execute(query).fetchdf()
        
        return cls(
            df,
            name=name,
            metadata={'source': 'query', 'query': query[:200]}
        )
    
    def profile(self, database: str = ':memory:') -> Dict[str, Any]:
        """
        Generate data profile using DuckDB.
        
        Args:
            database: Database to use
            
        Returns:
            Profile dictionary
        """
        from .operations import DuckDBOperations
        
        # Register data temporarily
        conn = DuckDBSession.get_or_create({'database': database})
        df = self.as_dataframe()
        temp_table = f"temp_{self.name}_{id(self)}"
        conn.register(temp_table, df)
        
        try:
            profile = DuckDBOperations.profile_data(
                temp_table,
                database=database,
                include_advanced=True
            )
            profile['artifact_name'] = self.name
            return profile
        finally:
            conn.unregister(temp_table)
    
    def sample(
        self,
        n: Optional[int] = None,
        fraction: Optional[float] = None,
        database: str = ':memory:'
    ) -> 'DuckDBArtifact':
        """
        Sample data from artifact.
        
        Args:
            n: Number of rows to sample
            fraction: Fraction of data to sample
            database: Database to use
            
        Returns:
            New sampled DuckDBArtifact
        """
        conn = DuckDBSession.get_or_create({'database': database})
        df = self.as_dataframe()
        conn.register('data', df)
        
        try:
            if n:
                query = f"SELECT * FROM data USING SAMPLE {n}"
            elif fraction:
                query = f"SELECT * FROM data USING SAMPLE {int(fraction * 100)} PERCENT"
            else:
                query = "SELECT * FROM data USING SAMPLE 1000"
            
            sampled_df = conn.execute(query).fetchdf()
            
            return DuckDBArtifact(
                sampled_df,
                name=f"{self.name}_sampled",
                metadata={**self.metadata, 'sampled': True, 'sample_size': len(sampled_df)}
            )
        finally:
            conn.unregister('data')