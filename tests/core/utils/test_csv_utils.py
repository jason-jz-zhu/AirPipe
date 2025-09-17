"""Tests for CSV utility functions."""

import unittest
import pandas as pd
import numpy as np
from pathlib import Path

from airpipe.utils.extractors.csv_utils import CSVUtils
from tests.base import BaseTestCase
from tests.fixtures.factories import DataFactory


class TestCSVUtils(BaseTestCase):
    """Test CSVUtils class."""
    
    def test_read_csv_success(self):
        """Test successful CSV reading."""
        # Create test CSV
        df = DataFactory.create_sample_dataframe(100)
        csv_path = self.create_temp_csv("test.csv", df)
        
        # Read CSV
        result = CSVUtils.read_csv(str(csv_path))
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 100)
        pd.testing.assert_frame_equal(result, df)
        
    def test_read_csv_with_encoding(self):
        """Test reading CSV with specific encoding."""
        # Create CSV with special characters
        df = pd.DataFrame({
            'name': ['Café', 'Naïve', 'Résumé'],
            'value': [1, 2, 3]
        })
        csv_path = self.temp_path / "encoded.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        # Read with UTF-8
        result = CSVUtils.read_csv(str(csv_path), encoding='utf-8')
        
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]['name'], 'Café')
        
    def test_read_csv_with_delimiter(self):
        """Test reading CSV with custom delimiter."""
        # Create TSV (tab-separated)
        df = DataFactory.create_sample_dataframe(50)
        tsv_path = self.temp_path / "test.tsv"
        df.to_csv(tsv_path, sep='\t', index=False)
        
        # Read with tab delimiter
        result = CSVUtils.read_csv(str(tsv_path), delimiter='\t')
        
        pd.testing.assert_frame_equal(result, df)
        
    def test_read_csv_file_not_found(self):
        """Test handling of missing file."""
        with self.assertRaises(FileNotFoundError) as ctx:
            CSVUtils.read_csv("/nonexistent/file.csv")
            
        self.assertIn("not found", str(ctx.exception))
        
    def test_read_csv_empty_file(self):
        """Test handling of empty CSV file."""
        # Create empty file
        empty_path = self.temp_path / "empty.csv"
        empty_path.write_text("")
        
        with self.assertRaises(ValueError) as ctx:
            CSVUtils.read_csv(str(empty_path))
            
        self.assertIn("empty", str(ctx.exception).lower())
        
    def test_validate_columns_success(self):
        """Test successful column validation."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c'],
            'col3': [True, False, True]
        })
        
        # All columns exist
        result = CSVUtils.validate_columns(
            df, 
            required_columns=['col1', 'col2']
        )
        
        self.assertTrue(result)
        
    def test_validate_columns_missing(self):
        """Test validation with missing columns."""
        df = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a', 'b', 'c']
        })
        
        # Missing column with raise
        with self.assertRaises(ValueError) as ctx:
            CSVUtils.validate_columns(
                df,
                required_columns=['col1', 'col3'],
                raise_on_missing=True
            )
            
        self.assertIn("Missing required columns", str(ctx.exception))
        self.assertIn("col3", str(ctx.exception))
        
        # Missing column without raise
        result = CSVUtils.validate_columns(
            df,
            required_columns=['col1', 'col3'],
            raise_on_missing=False
        )
        
        self.assertFalse(result)
        
    def test_infer_dtypes(self):
        """Test data type inference."""
        # Create DataFrame with mixed types
        df = pd.DataFrame({
            'int_col': ['1', '2', '3'],
            'float_col': ['1.5', '2.5', '3.5'],
            'date_col': ['2023-01-01', '2023-01-02', '2023-01-03'],
            'string_col': ['a', 'b', 'c']
        })
        
        # Infer types
        result = CSVUtils.infer_dtypes(
            df,
            date_columns=['date_col']
        )
        
        # Check date conversion
        self.assertEqual(result['date_col'].dtype, 'datetime64[ns]')
        
        # Other columns should be inferred
        self.assertTrue(pd.api.types.is_object_dtype(result['string_col']))
        
    def test_infer_dtypes_invalid_date(self):
        """Test handling invalid date conversion."""
        df = pd.DataFrame({
            'bad_date': ['not a date', '2023-01-01', '2023-01-02']
        })
        
        # Should not raise, just warn
        result = CSVUtils.infer_dtypes(
            df,
            date_columns=['bad_date']
        )
        
        # Column should remain as object
        self.assertEqual(result['bad_date'].dtype, 'object')
        
    def test_handle_missing_values_drop(self):
        """Test dropping missing values."""
        df = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['a', None, 'c', 'd'],
            'col3': [10, 20, 30, 40]
        })
        
        # Drop all rows with any missing
        result = CSVUtils.handle_missing_values(df, strategy='drop')
        
        self.assertEqual(len(result), 2)
        self.assertFalse(result.isnull().any().any())
        
        # Drop only from specific columns
        result2 = CSVUtils.handle_missing_values(
            df,
            strategy='drop',
            columns=['col1']
        )
        
        self.assertEqual(len(result2), 3)
        self.assertFalse(result2['col1'].isnull().any())
        
    def test_handle_missing_values_fill(self):
        """Test filling missing values."""
        df = pd.DataFrame({
            'col1': [1, 2, None, 4],
            'col2': ['a', None, 'c', 'd']
        })
        
        # Fill with specific value
        result = CSVUtils.handle_missing_values(
            df,
            strategy='fill',
            fill_value=0
        )
        
        self.assertFalse(result.isnull().any().any())
        self.assertEqual(result.iloc[2]['col1'], 0)
        self.assertEqual(result.iloc[1]['col2'], 0)
        
        # Fill specific columns
        result2 = CSVUtils.handle_missing_values(
            df,
            strategy='fill',
            fill_value=-1,
            columns=['col1']
        )
        
        self.assertEqual(result2.iloc[2]['col1'], -1)
        self.assertTrue(pd.isna(result2.iloc[1]['col2']))
        
    def test_handle_missing_values_forward_fill(self):
        """Test forward filling missing values."""
        df = pd.DataFrame({
            'col1': [1, None, None, 4],
            'col2': ['a', 'b', None, 'd']
        })
        
        result = CSVUtils.handle_missing_values(
            df,
            strategy='forward'
        )
        
        # Forward filled values
        self.assertEqual(result.iloc[1]['col1'], 1)
        self.assertEqual(result.iloc[2]['col1'], 1)
        self.assertEqual(result.iloc[2]['col2'], 'b')
        
    def test_handle_missing_values_backward_fill(self):
        """Test backward filling missing values."""
        df = pd.DataFrame({
            'col1': [None, None, 3, 4],
            'col2': ['a', None, 'c', 'd']
        })
        
        result = CSVUtils.handle_missing_values(
            df,
            strategy='backward'
        )
        
        # Backward filled values
        self.assertEqual(result.iloc[0]['col1'], 3)
        self.assertEqual(result.iloc[1]['col1'], 3)
        self.assertEqual(result.iloc[1]['col2'], 'c')
        
    def test_csv_utils_integration(self):
        """Test complete CSV processing workflow."""
        # Create CSV with issues
        df = pd.DataFrame({
            'id': ['1', '2', '3', '4'],
            'value': ['10.5', None, '30.7', '40.2'],
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'category': ['A', 'B', None, 'D']
        })
        csv_path = self.create_temp_csv("integration.csv", df)
        
        # Read CSV
        data = CSVUtils.read_csv(str(csv_path))
        
        # Validate columns
        CSVUtils.validate_columns(
            data,
            required_columns=['id', 'value', 'date']
        )
        
        # Handle missing values
        data = CSVUtils.handle_missing_values(
            data,
            strategy='fill',
            fill_value=0,
            columns=['value', 'category']
        )
        
        # Infer types
        data = CSVUtils.infer_dtypes(
            data,
            date_columns=['date']
        )
        
        # Verify results
        self.assertEqual(len(data), 4)
        self.assertFalse(data[['value', 'category']].isnull().any().any())
        self.assertEqual(data['date'].dtype, 'datetime64[ns]')
        self.assertEqual(data.iloc[1]['value'], '0')  # Filled value


if __name__ == "__main__":
    unittest.main()