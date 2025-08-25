"""
Tests for DuckDB Integration.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import tempfile
import os

from airpipe.core.task import TaskPipeline
from airpipe.utils.duckdb import (
    DuckDBSession,
    DuckDBOperations,
    DuckDBArtifact,
    SQLPipeline
)


class TestDuckDBSession:
    """Test DuckDB session management."""
    
    def test_singleton_pattern(self):
        """Test that DuckDBSession follows singleton pattern."""
        session1 = DuckDBSession.get_or_create()
        session2 = DuckDBSession.get_or_create()
        assert session1 is session2
    
    def test_different_databases(self):
        """Test managing different database connections."""
        memory_db = DuckDBSession.get_or_create({'database': ':memory:'})
        
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            file_db = DuckDBSession.get_or_create({'database': f.name})
            
            # Should be different connections
            assert memory_db is not file_db
            
            # Clean up
            DuckDBSession.stop(f.name)
            os.unlink(f.name)
    
    def test_execute_query(self):
        """Test executing queries."""
        result = DuckDBSession.execute("SELECT 1 as value, 'test' as name")
        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][1] == 'test'
    
    def test_read_formats(self):
        """Test reading different file formats."""
        # Create test data
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10.5, 20.5, 30.5]
        })
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test Parquet
            parquet_path = os.path.join(tmpdir, 'test.parquet')
            df.to_parquet(parquet_path)
            parquet_rel = DuckDBSession.read_parquet(parquet_path)
            assert len(parquet_rel.fetchall()) == 3
            
            # Test CSV
            csv_path = os.path.join(tmpdir, 'test.csv')
            df.to_csv(csv_path, index=False)
            csv_rel = DuckDBSession.read_csv(csv_path)
            assert len(csv_rel.fetchall()) == 3
            
            # Test JSON
            json_path = os.path.join(tmpdir, 'test.json')
            df.to_json(json_path, orient='records')
            json_rel = DuckDBSession.read_json(json_path)
            assert len(json_rel.fetchall()) == 3
    
    def test_from_pandas(self):
        """Test creating DuckDB relation from pandas."""
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['x', 'y', 'z']
        })
        
        rel = DuckDBSession.from_pandas(df)
        result = rel.fetchall()
        assert len(result) == 3
        assert result[0][0] == 1
        assert result[0][1] == 'x'
    
    def test_configuration(self):
        """Test DuckDB configuration."""
        config = {
            'database': ':memory:',
            'memory_limit': '1GB',
            'threads': 2
        }
        
        conn = DuckDBSession.get_or_create(config)
        db_config = DuckDBSession.get_config()
        
        # Config should be applied
        assert 'memory_limit' in db_config
        assert 'threads' in db_config


class TestDuckDBOperations:
    """Test DuckDB operations utilities."""
    
    def setup_method(self):
        """Set up test data."""
        self.df = pd.DataFrame({
            'id': range(1, 101),
            'value': np.random.uniform(0, 100, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100),
            'date': pd.date_range('2023-01-01', periods=100, freq='D')
        })
        
        # Register with DuckDB
        conn = DuckDBSession.get_or_create()
        conn.register('test_table', self.df)
    
    def test_profile_data(self):
        """Test data profiling."""
        profile = DuckDBOperations.profile_data('test_table', include_advanced=True)
        
        assert profile['row_count'] == 100
        assert len(profile['columns']) == 4
        assert 'numeric_statistics' in profile
        assert 'string_statistics' in profile
    
    def test_convert_format(self):
        """Test format conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Save test data as CSV
            csv_path = os.path.join(tmpdir, 'input.csv')
            self.df.to_csv(csv_path, index=False)
            
            # Convert to Parquet
            parquet_path = os.path.join(tmpdir, 'output.parquet')
            success = DuckDBOperations.convert_format(
                csv_path,
                parquet_path,
                source_format='csv',
                target_format='parquet'
            )
            
            assert success
            assert os.path.exists(parquet_path)
            
            # Verify data
            result_df = pd.read_parquet(parquet_path)
            assert len(result_df) == 100
    
    def test_optimize_query(self):
        """Test query optimization."""
        query = "SELECT * FROM test_table WHERE value > 50"
        result = DuckDBOperations.optimize_query(query)
        
        assert 'original_query' in result
        assert 'execution_plan' in result
        assert 'execution_time_seconds' in result
        assert result['execution_time_seconds'] >= 0
    
    def test_pivot_data(self):
        """Test data pivoting."""
        pivoted = DuckDBOperations.pivot_data(
            'test_table',
            index=['date'],
            columns='category',
            values='value',
            agg_func='SUM'
        )
        
        assert 'A' in pivoted.columns or 'B' in pivoted.columns or 'C' in pivoted.columns
        assert len(pivoted) <= 100  # One row per unique date
    
    def test_sample_data(self):
        """Test data sampling."""
        # Sample by count
        sample_n = DuckDBOperations.sample_data('test_table', n=10)
        assert len(sample_n) == 10
        
        # Sample by fraction
        sample_frac = DuckDBOperations.sample_data('test_table', fraction=0.1)
        assert 5 <= len(sample_frac) <= 15  # Approximate 10%
    
    def test_detect_outliers(self):
        """Test outlier detection."""
        outliers = DuckDBOperations.detect_outliers(
            'test_table',
            'value',
            method='iqr',
            threshold=1.5
        )
        
        assert 'is_outlier' in outliers.columns
        assert 'outlier_type' in outliers.columns
        assert len(outliers) == 100


class TestDuckDBArtifact:
    """Test DuckDB artifact functionality."""
    
    def setup_method(self):
        """Set up test artifact."""
        self.df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'category': ['A', 'B', 'A', 'B', 'A']
        })
        self.artifact = DuckDBArtifact(self.df, name='test_artifact')
    
    def test_to_duckdb(self):
        """Test converting artifact to DuckDB relation."""
        relation = self.artifact.to_duckdb()
        result = relation.fetchall()
        assert len(result) == 5
    
    def test_query(self):
        """Test querying artifact with SQL."""
        result = self.artifact.query("SELECT * FROM data WHERE value > 20")
        assert len(result) == 3
        assert all(row > 20 for row in result['value'])
    
    def test_aggregate(self):
        """Test aggregation on artifact."""
        aggregated = self.artifact.aggregate(
            group_by=['category'],
            aggregations={'value': 'SUM', 'id': 'COUNT'}
        )
        
        assert len(aggregated) == 2
        assert 'category' in aggregated.columns
        assert 'value_sum' in aggregated.columns
        assert 'id_count' in aggregated.columns
    
    def test_filter(self):
        """Test filtering artifact."""
        filtered = self.artifact.filter("value >= 30")
        assert len(filtered.as_dataframe()) == 3
        assert filtered.name == 'test_artifact_filtered'
    
    def test_join(self):
        """Test joining artifacts."""
        other_df = pd.DataFrame({
            'category': ['A', 'B'],
            'description': ['Category A', 'Category B']
        })
        other = DuckDBArtifact(other_df, name='categories')
        
        joined = self.artifact.join(
            other,
            on="left_table.category = right_table.category",
            how='inner'
        )
        
        assert len(joined.as_dataframe()) == 5
        assert 'description' in joined.as_dataframe().columns
    
    def test_file_operations(self):
        """Test saving and loading artifacts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Test Parquet
            parquet_path = os.path.join(tmpdir, 'test.parquet')
            self.artifact.to_parquet(parquet_path)
            loaded_parquet = DuckDBArtifact.from_parquet(parquet_path, name='loaded')
            assert len(loaded_parquet.as_dataframe()) == 5
            
            # Test CSV
            csv_path = os.path.join(tmpdir, 'test.csv')
            self.artifact.to_csv(csv_path)
            loaded_csv = DuckDBArtifact.from_csv(csv_path, name='loaded')
            assert len(loaded_csv.as_dataframe()) == 5
    
    def test_profile(self):
        """Test artifact profiling."""
        profile = self.artifact.profile()
        assert profile['row_count'] == 5
        assert profile['artifact_name'] == 'test_artifact'
        assert len(profile['columns']) == 3


class TestSQLTask:
    """Test SQL task decorator."""
    
    def test_sql_task_basic(self):
        """Test basic SQL task execution."""
        pipeline = TaskPipeline("test_sql")
        
        @pipeline.task(produces="data")
        def create_data():
            df = pd.DataFrame({
                'id': [1, 2, 3],
                'value': [10, 20, 30]
            })
            return DuckDBArtifact(df, name="data")
        
        @pipeline.sql_task(
            sql="SELECT * FROM {data} WHERE value > 15",
            consumes="data",
            produces="filtered"
        )
        def filter_data():
            pass
        
        # Execute pipeline
        pipeline.execute()
        
        # Check result
        filtered = pipeline.get_artifact("filtered")
        assert filtered is not None
        assert len(filtered.as_dataframe()) == 2
    
    def test_sql_task_dynamic(self):
        """Test SQL task with dynamic query."""
        pipeline = TaskPipeline("test_dynamic_sql")
        
        @pipeline.task(produces="data")
        def create_data():
            df = pd.DataFrame({
                'category': ['A', 'B', 'A', 'B'],
                'value': [10, 20, 30, 40]
            })
            return DuckDBArtifact(df, name="data")
        
        @pipeline.sql_task(consumes="data", produces="summary")
        def summarize():
            return """
            SELECT 
                category,
                SUM(value) as total,
                AVG(value) as average
            FROM {data}
            GROUP BY category
            """
        
        # Execute pipeline
        pipeline.execute()
        
        # Check result
        summary = pipeline.get_artifact("summary")
        assert summary is not None
        assert len(summary.as_dataframe()) == 2
        assert 'total' in summary.as_dataframe().columns
        assert 'average' in summary.as_dataframe().columns


class TestSQLPipeline:
    """Test SQLPipeline class."""
    
    def test_sql_pipeline_creation(self):
        """Test creating SQL pipeline."""
        pipeline = SQLPipeline("test_pipeline", database=':memory:')
        assert pipeline.name == "test_pipeline"
        assert pipeline.database == ':memory:'
    
    def test_execute_sql(self):
        """Test executing SQL directly."""
        pipeline = SQLPipeline("test")
        
        # Create test table
        conn = pipeline.get_connection()
        conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
        conn.execute("INSERT INTO test VALUES (1, 'Alice'), (2, 'Bob')")
        
        # Execute query
        result = pipeline.execute_sql("SELECT * FROM test")
        assert isinstance(result, DuckDBArtifact)
        assert len(result.as_dataframe()) == 2
        
        # Execute as DataFrame
        df = pipeline.execute_sql("SELECT * FROM test", return_artifact=False)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
    
    def test_load_table(self):
        """Test loading data into table."""
        pipeline = SQLPipeline("test")
        
        # Load DataFrame
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'value': [10, 20, 30]
        })
        pipeline.load_table('my_table', df)
        
        # Verify table exists
        result = pipeline.execute_sql("SELECT * FROM my_table", return_artifact=False)
        assert len(result) == 3
    
    def test_create_view(self):
        """Test creating views."""
        pipeline = SQLPipeline("test")
        
        # Create table
        df = pd.DataFrame({'x': [1, 2, 3], 'y': [4, 5, 6]})
        pipeline.load_table('source', df)
        
        # Create view
        pipeline.create_view('my_view', 'SELECT x, y, x + y as sum FROM source')
        
        # Query view
        result = pipeline.execute_sql("SELECT * FROM my_view", return_artifact=False)
        assert len(result) == 3
        assert 'sum' in result.columns


if __name__ == "__main__":
    pytest.main([__file__, "-v"])