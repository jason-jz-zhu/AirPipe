"""Test data factories for creating test objects."""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import random
import string

from airpipe.artifacts.data_artifact import DataArtifact
from airpipe.core.task import TaskPipeline


class DataFactory:
    """Factory for creating test data."""
    
    @staticmethod
    def create_sample_dataframe(rows: int = 100, 
                               columns: Optional[List[str]] = None,
                               dtypes: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        Create a sample DataFrame for testing.
        
        Args:
            rows: Number of rows
            columns: Column names (default: ['a', 'b', 'c'])
            dtypes: Column data types
            
        Returns:
            Sample DataFrame
        """
        if columns is None:
            columns = ['a', 'b', 'c']
            
        data = {}
        for col in columns:
            if dtypes and col in dtypes:
                dtype = dtypes[col]
                if dtype == 'int':
                    data[col] = np.random.randint(0, 100, rows)
                elif dtype == 'float':
                    data[col] = np.random.random(rows) * 100
                elif dtype == 'string':
                    data[col] = [''.join(random.choices(string.ascii_letters, k=5)) 
                                 for _ in range(rows)]
                elif dtype == 'datetime':
                    base = datetime.now()
                    data[col] = [base + timedelta(days=i) for i in range(rows)]
                elif dtype == 'bool':
                    data[col] = np.random.choice([True, False], rows)
                else:
                    data[col] = range(rows)
            else:
                data[col] = np.random.random(rows) * 100
                
        return pd.DataFrame(data)
    
    @staticmethod
    def create_employee_dataframe(rows: int = 50) -> pd.DataFrame:
        """Create a sample employee DataFrame."""
        departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance']
        
        return pd.DataFrame({
            'employee_id': range(1, rows + 1),
            'name': [f'Employee_{i}' for i in range(1, rows + 1)],
            'department': np.random.choice(departments, rows),
            'salary': np.random.randint(40000, 150000, rows),
            'hire_date': pd.date_range(start='2020-01-01', periods=rows, freq='W'),
            'is_active': np.random.choice([True, False], rows, p=[0.9, 0.1])
        })
    
    @staticmethod
    def create_sales_dataframe(rows: int = 100) -> pd.DataFrame:
        """Create a sample sales DataFrame."""
        regions = ['North', 'South', 'East', 'West']
        products = ['Product_A', 'Product_B', 'Product_C', 'Product_D']
        
        return pd.DataFrame({
            'sale_id': range(1, rows + 1),
            'product': np.random.choice(products, rows),
            'region': np.random.choice(regions, rows),
            'amount': np.random.uniform(10, 1000, rows),
            'quantity': np.random.randint(1, 50, rows),
            'date': pd.date_range(start='2023-01-01', periods=rows, freq='D')
        })
    
    @staticmethod
    def create_dict_data(size: int = 10) -> Dict[str, Any]:
        """Create sample dictionary data."""
        return {
            f'key_{i}': {
                'value': np.random.random() * 100,
                'label': f'label_{i}',
                'active': random.choice([True, False])
            }
            for i in range(size)
        }
    
    @staticmethod
    def create_list_data(size: int = 20) -> List[Dict[str, Any]]:
        """Create sample list of dictionaries."""
        return [
            {
                'id': i,
                'value': np.random.random() * 100,
                'category': random.choice(['A', 'B', 'C']),
                'timestamp': datetime.now() + timedelta(hours=i)
            }
            for i in range(size)
        ]


class ArtifactFactory:
    """Factory for creating test artifacts."""
    
    @staticmethod
    def create_dataframe_artifact(name: str = "test_artifact",
                                 rows: int = 100,
                                 columns: Optional[List[str]] = None) -> DataArtifact:
        """Create a DataArtifact with DataFrame."""
        df = DataFactory.create_sample_dataframe(rows, columns)
        artifact = DataArtifact(data=df, name=name)
        return artifact
    
    @staticmethod
    def create_dict_artifact(name: str = "test_dict",
                            size: int = 10) -> DataArtifact:
        """Create a DataArtifact with dictionary data."""
        data = DataFactory.create_dict_data(size)
        return DataArtifact(data=data, name=name)
    
    @staticmethod
    def create_list_artifact(name: str = "test_list",
                            size: int = 20) -> DataArtifact:
        """Create a DataArtifact with list data."""
        data = DataFactory.create_list_data(size)
        return DataArtifact(data=data, name=name)


class PipelineFactory:
    """Factory for creating test pipelines."""
    
    @staticmethod
    def create_simple_pipeline(name: str = "test_pipeline") -> TaskPipeline:
        """Create a simple test pipeline with basic tasks."""
        pipeline = TaskPipeline(name)
        
        @pipeline.task(produces="raw_data")
        def extract():
            df = DataFactory.create_sample_dataframe(50)
            return pipeline.create_artifact(df, "raw_data")
        
        @pipeline.task(
            depends_on=["extract"],
            consumes="raw_data",
            produces="transformed_data"
        )
        def transform():
            raw_data = pipeline.get_artifact("raw_data")
            df = raw_data.as_dataframe()
            df['new_col'] = df.iloc[:, 0] * 2
            return pipeline.create_artifact(df, "transformed_data")
        
        @pipeline.task(
            depends_on=["transform"],
            consumes="transformed_data"
        )
        def load():
            data = pipeline.get_artifact("transformed_data")
            # Simulate loading
            return None
        
        return pipeline
    
    @staticmethod
    def create_parallel_pipeline(name: str = "parallel_pipeline") -> TaskPipeline:
        """Create a pipeline with parallel tasks."""
        pipeline = TaskPipeline(name)
        
        @pipeline.task(produces="data1")
        def extract1():
            df = DataFactory.create_sample_dataframe(30)
            return pipeline.create_artifact(df, "data1")
        
        @pipeline.task(produces="data2")
        def extract2():
            df = DataFactory.create_sample_dataframe(40)
            return pipeline.create_artifact(df, "data2")
        
        @pipeline.task(
            depends_on=["extract1", "extract2"],
            consumes=["data1", "data2"],
            produces="merged_data"
        )
        def merge():
            data1 = pipeline.get_artifact("data1")
            data2 = pipeline.get_artifact("data2")
            # Simulate merge
            merged = pd.concat([data1.as_dataframe(), data2.as_dataframe()])
            return pipeline.create_artifact(merged, "merged_data")
        
        return pipeline
    
    @staticmethod
    def create_complex_pipeline(name: str = "complex_pipeline") -> TaskPipeline:
        """Create a complex pipeline with multiple dependency levels."""
        pipeline = TaskPipeline(name)
        
        # Layer 1: Multiple extractors
        @pipeline.task(produces="source1")
        def extract_source1():
            return pipeline.create_artifact(
                DataFactory.create_employee_dataframe(20), "source1"
            )
        
        @pipeline.task(produces="source2")
        def extract_source2():
            return pipeline.create_artifact(
                DataFactory.create_sales_dataframe(30), "source2"
            )
        
        # Layer 2: Transform each source
        @pipeline.task(depends_on=["extract_source1"], consumes="source1", produces="filtered1")
        def filter_source1():
            data = pipeline.get_artifact("source1")
            df = data.as_dataframe()
            filtered = df[df['salary'] > 50000]
            return pipeline.create_artifact(filtered, "filtered1")
        
        @pipeline.task(depends_on=["extract_source2"], consumes="source2", produces="filtered2")
        def filter_source2():
            data = pipeline.get_artifact("source2")
            df = data.as_dataframe()
            filtered = df[df['amount'] > 100]
            return pipeline.create_artifact(filtered, "filtered2")
        
        # Layer 3: Aggregate
        @pipeline.task(
            depends_on=["filter_source1"], 
            consumes="filtered1",
            produces="agg1"
        )
        def aggregate1():
            data = pipeline.get_artifact("filtered1")
            df = data.as_dataframe()
            agg = df.groupby('department')['salary'].mean().reset_index()
            return pipeline.create_artifact(agg, "agg1")
        
        @pipeline.task(
            depends_on=["filter_source2"],
            consumes="filtered2", 
            produces="agg2"
        )
        def aggregate2():
            data = pipeline.get_artifact("filtered2")
            df = data.as_dataframe()
            agg = df.groupby('region')['amount'].sum().reset_index()
            return pipeline.create_artifact(agg, "agg2")
        
        # Layer 4: Final report
        @pipeline.task(
            depends_on=["aggregate1", "aggregate2"],
            consumes=["agg1", "agg2"],
            produces="final_report"
        )
        def create_report():
            agg1 = pipeline.get_artifact("agg1")
            agg2 = pipeline.get_artifact("agg2")
            # Simulate report creation
            report = {
                'employee_stats': agg1.as_dataframe().to_dict(),
                'sales_stats': agg2.as_dataframe().to_dict()
            }
            return pipeline.create_artifact(report, "final_report")
        
        return pipeline