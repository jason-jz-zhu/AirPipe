"""Tests for filter utility functions."""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from airpipe.utils.transformers.filter_utils import FilterUtils
from tests.base import BaseTestCase
from tests.fixtures.factories import DataFactory


class TestFilterUtils(BaseTestCase):
    """Test FilterUtils class."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        
        # Create sample DataFrames for testing
        self.numeric_df = pd.DataFrame({
            'id': range(1, 101),
            'value': np.random.uniform(0, 100, 100),
            'score': np.random.randint(0, 10, 100),
            'amount': np.random.uniform(100, 1000, 100)
        })
        
        self.categorical_df = pd.DataFrame({
            'id': range(1, 51),
            'category': np.random.choice(['A', 'B', 'C', 'D'], 50),
            'status': np.random.choice(['active', 'inactive', 'pending'], 50),
            'region': np.random.choice(['North', 'South', 'East', 'West'], 50)
        })
        
        self.datetime_df = pd.DataFrame({
            'id': range(1, 31),
            'date': pd.date_range('2023-01-01', periods=30, freq='D'),
            'timestamp': pd.date_range('2023-01-01', periods=30, freq='H')
        })
        
    def test_filter_by_value_greater(self):
        """Test filtering by value (greater than)."""
        result = FilterUtils.filter_by_value(
            self.numeric_df,
            column='value',
            operator='>',
            value=50
        )
        
        self.assertTrue(all(result['value'] > 50))
        self.assertLess(len(result), len(self.numeric_df))
        
    def test_filter_by_value_less_equal(self):
        """Test filtering by value (less than or equal)."""
        result = FilterUtils.filter_by_value(
            self.numeric_df,
            column='score',
            operator='<=',
            value=5
        )
        
        self.assertTrue(all(result['score'] <= 5))
        
    def test_filter_by_value_equal(self):
        """Test filtering by value (equal)."""
        result = FilterUtils.filter_by_value(
            self.categorical_df,
            column='category',
            operator='==',
            value='A'
        )
        
        self.assertTrue(all(result['category'] == 'A'))
        
    def test_filter_by_value_not_equal(self):
        """Test filtering by value (not equal)."""
        result = FilterUtils.filter_by_value(
            self.categorical_df,
            column='status',
            operator='!=',
            value='inactive'
        )
        
        self.assertTrue(all(result['status'] != 'inactive'))
        
    def test_filter_by_value_invalid_operator(self):
        """Test invalid operator handling."""
        with self.assertRaises(ValueError):
            FilterUtils.filter_by_value(
                self.numeric_df,
                column='value',
                operator='invalid',
                value=50
            )
            
    def test_filter_by_value_missing_column(self):
        """Test filtering with missing column."""
        with self.assertRaises(KeyError):
            FilterUtils.filter_by_value(
                self.numeric_df,
                column='nonexistent',
                operator='>',
                value=50
            )
            
    def test_filter_by_range(self):
        """Test filtering by range."""
        result = FilterUtils.filter_by_range(
            self.numeric_df,
            column='value',
            min_value=20,
            max_value=80
        )
        
        self.assertTrue(all((result['value'] >= 20) & (result['value'] <= 80)))
        
    def test_filter_by_range_inclusive(self):
        """Test inclusive range filtering."""
        result = FilterUtils.filter_by_range(
            self.numeric_df,
            column='score',
            min_value=3,
            max_value=7,
            inclusive=True
        )
        
        self.assertTrue(all((result['score'] >= 3) & (result['score'] <= 7)))
        
    def test_filter_by_range_exclusive(self):
        """Test exclusive range filtering."""
        result = FilterUtils.filter_by_range(
            self.numeric_df,
            column='score',
            min_value=3,
            max_value=7,
            inclusive=False
        )
        
        self.assertTrue(all((result['score'] > 3) & (result['score'] < 7)))
        
    def test_filter_by_range_open_ended(self):
        """Test open-ended range filtering."""
        # Only min value
        result = FilterUtils.filter_by_range(
            self.numeric_df,
            column='value',
            min_value=50
        )
        self.assertTrue(all(result['value'] >= 50))
        
        # Only max value
        result = FilterUtils.filter_by_range(
            self.numeric_df,
            column='value',
            max_value=50
        )
        self.assertTrue(all(result['value'] <= 50))
        
    def test_filter_by_list(self):
        """Test filtering by list of values."""
        result = FilterUtils.filter_by_list(
            self.categorical_df,
            column='category',
            values=['A', 'B']
        )
        
        self.assertTrue(all(result['category'].isin(['A', 'B'])))
        self.assertFalse(any(result['category'].isin(['C', 'D'])))
        
    def test_filter_by_list_exclude(self):
        """Test excluding values in list."""
        result = FilterUtils.filter_by_list(
            self.categorical_df,
            column='status',
            values=['inactive'],
            exclude=True
        )
        
        self.assertFalse(any(result['status'] == 'inactive'))
        
    def test_filter_by_pattern(self):
        """Test filtering by pattern/regex."""
        # Add string column with patterns
        self.categorical_df['email'] = [
            f"user{i}@{'gmail' if i % 2 == 0 else 'yahoo'}.com"
            for i in range(len(self.categorical_df))
        ]
        
        # Filter Gmail addresses
        result = FilterUtils.filter_by_pattern(
            self.categorical_df,
            column='email',
            pattern='.*@gmail\\.com'
        )
        
        self.assertTrue(all(result['email'].str.contains('@gmail.com')))
        
    def test_filter_by_pattern_case_insensitive(self):
        """Test case-insensitive pattern matching."""
        df = pd.DataFrame({
            'name': ['Alice', 'alice', 'ALICE', 'Bob', 'Charlie']
        })
        
        result = FilterUtils.filter_by_pattern(
            df,
            column='name',
            pattern='alice',
            case=False
        )
        
        self.assertEqual(len(result), 3)
        
    def test_filter_nulls(self):
        """Test filtering null values."""
        # Add nulls to DataFrame
        df = self.numeric_df.copy()
        df.loc[df['id'] % 5 == 0, 'value'] = None
        
        # Remove nulls
        result = FilterUtils.filter_nulls(
            df,
            columns=['value'],
            keep_nulls=False
        )
        
        self.assertFalse(result['value'].isnull().any())
        
        # Keep only nulls
        result = FilterUtils.filter_nulls(
            df,
            columns=['value'],
            keep_nulls=True
        )
        
        self.assertTrue(result['value'].isnull().all())
        
    def test_filter_nulls_multiple_columns(self):
        """Test filtering nulls across multiple columns."""
        df = pd.DataFrame({
            'col1': [1, None, 3, None],
            'col2': ['a', 'b', None, None],
            'col3': [10, 20, 30, 40]
        })
        
        # Remove rows with any null in specified columns
        result = FilterUtils.filter_nulls(
            df,
            columns=['col1', 'col2'],
            keep_nulls=False
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['col1'], 1)
        
    def test_filter_duplicates(self):
        """Test filtering duplicate values."""
        # Create DataFrame with duplicates
        df = pd.DataFrame({
            'id': [1, 2, 2, 3, 3, 3, 4],
            'value': ['a', 'b', 'b', 'c', 'c', 'c', 'd']
        })
        
        # Keep first occurrence
        result = FilterUtils.filter_duplicates(
            df,
            subset=['id'],
            keep='first'
        )
        
        self.assertEqual(len(result), 4)
        self.assertEqual(list(result['id']), [1, 2, 3, 4])
        
        # Keep last occurrence
        result = FilterUtils.filter_duplicates(
            df,
            subset=['id'],
            keep='last'
        )
        
        self.assertEqual(len(result), 4)
        
        # Remove all duplicates
        result = FilterUtils.filter_duplicates(
            df,
            subset=['id'],
            keep=False
        )
        
        self.assertEqual(len(result), 2)  # Only unique values
        self.assertEqual(list(result['id']), [1, 4])
        
    def test_filter_by_date_range(self):
        """Test filtering by date range."""
        start_date = pd.Timestamp('2023-01-10')
        end_date = pd.Timestamp('2023-01-20')
        
        result = FilterUtils.filter_by_date_range(
            self.datetime_df,
            date_column='date',
            start_date=start_date,
            end_date=end_date
        )
        
        self.assertTrue(all(
            (result['date'] >= start_date) & 
            (result['date'] <= end_date)
        ))
        
    def test_filter_by_date_range_timezone(self):
        """Test filtering with timezone-aware dates."""
        # Add timezone
        df = self.datetime_df.copy()
        df['date'] = df['date'].dt.tz_localize('UTC')
        
        start_date = pd.Timestamp('2023-01-10', tz='UTC')
        end_date = pd.Timestamp('2023-01-20', tz='UTC')
        
        result = FilterUtils.filter_by_date_range(
            df,
            date_column='date',
            start_date=start_date,
            end_date=end_date
        )
        
        self.assertGreater(len(result), 0)
        
    def test_filter_top_n(self):
        """Test filtering top N values."""
        result = FilterUtils.filter_top_n(
            self.numeric_df,
            column='value',
            n=10,
            ascending=False
        )
        
        self.assertEqual(len(result), 10)
        
        # Check values are the highest
        top_values = self.numeric_df.nlargest(10, 'value')['value'].values
        np.testing.assert_array_almost_equal(
            sorted(result['value'].values, reverse=True),
            sorted(top_values, reverse=True)
        )
        
    def test_filter_bottom_n(self):
        """Test filtering bottom N values."""
        result = FilterUtils.filter_top_n(
            self.numeric_df,
            column='value',
            n=10,
            ascending=True
        )
        
        self.assertEqual(len(result), 10)
        
        # Check values are the lowest
        bottom_values = self.numeric_df.nsmallest(10, 'value')['value'].values
        np.testing.assert_array_almost_equal(
            sorted(result['value'].values),
            sorted(bottom_values)
        )
        
    def test_filter_by_condition(self):
        """Test filtering by custom condition."""
        # Complex condition
        condition = lambda row: (row['value'] > 50) & (row['score'] >= 5)
        
        result = FilterUtils.filter_by_condition(
            self.numeric_df,
            condition=condition
        )
        
        self.assertTrue(all((result['value'] > 50) & (result['score'] >= 5)))
        
    def test_filter_by_percentile(self):
        """Test filtering by percentile."""
        # Keep values above 75th percentile
        result = FilterUtils.filter_by_percentile(
            self.numeric_df,
            column='value',
            min_percentile=75
        )
        
        threshold = self.numeric_df['value'].quantile(0.75)
        self.assertTrue(all(result['value'] >= threshold))
        
        # Keep values between 25th and 75th percentile
        result = FilterUtils.filter_by_percentile(
            self.numeric_df,
            column='value',
            min_percentile=25,
            max_percentile=75
        )
        
        lower = self.numeric_df['value'].quantile(0.25)
        upper = self.numeric_df['value'].quantile(0.75)
        self.assertTrue(all(
            (result['value'] >= lower) & 
            (result['value'] <= upper)
        ))
        
    def test_chain_filters(self):
        """Test chaining multiple filters."""
        # Start with original DataFrame
        result = self.numeric_df.copy()
        
        # Apply multiple filters
        result = FilterUtils.filter_by_range(
            result,
            column='value',
            min_value=30,
            max_value=70
        )
        
        result = FilterUtils.filter_by_value(
            result,
            column='score',
            operator='>=',
            value=5
        )
        
        result = FilterUtils.filter_top_n(
            result,
            column='amount',
            n=5
        )
        
        # Verify all conditions
        self.assertEqual(len(result), 5)
        self.assertTrue(all((result['value'] >= 30) & (result['value'] <= 70)))
        self.assertTrue(all(result['score'] >= 5))


if __name__ == "__main__":
    unittest.main()