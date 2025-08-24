"""Tests for simple value transformer business logic."""

import unittest
import pandas as pd
import numpy as np

from pipelines.examples.simple.transformers.value_transformer import ValueTransformer
from tests.base import BaseTestCase


class TestValueTransformer(BaseTestCase):
    """Test ValueTransformer business logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.transformer = ValueTransformer()
        
        # Create test data
        self.test_df = pd.DataFrame({
            'id': range(1, 101),
            'value': np.random.uniform(0, 1000, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100),
            'score': np.random.randint(0, 100, 100)
        })
        
    def test_filter_and_transform_basic(self):
        """Test basic filtering and transformation."""
        result = self.transformer.filter_and_transform(
            self.test_df,
            threshold=500,
            transformation='square'
        )
        
        # Should filter values above threshold
        self.assertTrue(all(result['value'] > 500))
        
        # Should have transformed column
        self.assertIn('transformed_value', result.columns)
        
        # Check square transformation
        expected = result['value'] ** 2
        np.testing.assert_array_almost_equal(
            result['transformed_value'],
            expected,
            decimal=5
        )
        
    def test_transformation_types(self):
        """Test different transformation types."""
        # Test square transformation
        result = self.transformer.filter_and_transform(
            self.test_df,
            threshold=0,
            transformation='square'
        )
        expected = self.test_df['value'] ** 2
        np.testing.assert_array_almost_equal(
            result['transformed_value'],
            expected,
            decimal=5
        )
        
        # Test sqrt transformation
        result = self.transformer.filter_and_transform(
            self.test_df,
            threshold=0,
            transformation='sqrt'
        )
        expected = np.sqrt(self.test_df['value'])
        np.testing.assert_array_almost_equal(
            result['transformed_value'],
            expected,
            decimal=5
        )
        
        # Test log transformation
        result = self.transformer.filter_and_transform(
            self.test_df,
            threshold=0,
            transformation='log'
        )
        # Log of values > 0
        valid_mask = self.test_df['value'] > 0
        expected = np.log(self.test_df.loc[valid_mask, 'value'])
        np.testing.assert_array_almost_equal(
            result['transformed_value'],
            expected,
            decimal=5
        )
        
    def test_scale_values(self):
        """Test value scaling operations."""
        # Test min-max scaling
        result = self.transformer.scale_values(
            self.test_df,
            column='value',
            method='minmax'
        )
        
        self.assertIn('value_scaled', result.columns)
        self.assertAlmostEqual(result['value_scaled'].min(), 0, places=5)
        self.assertAlmostEqual(result['value_scaled'].max(), 1, places=5)
        
        # Test standard scaling
        result = self.transformer.scale_values(
            self.test_df,
            column='value',
            method='standard'
        )
        
        self.assertAlmostEqual(result['value_scaled'].mean(), 0, places=5)
        self.assertAlmostEqual(result['value_scaled'].std(), 1, places=5)
        
    def test_apply_custom_function(self):
        """Test applying custom transformation function."""
        def custom_transform(x):
            return x * 2 + 10
            
        result = self.transformer.apply_custom_transform(
            self.test_df,
            column='value',
            func=custom_transform
        )
        
        expected = self.test_df['value'] * 2 + 10
        np.testing.assert_array_almost_equal(
            result['value_transformed'],
            expected,
            decimal=5
        )
        
    def test_categorize_values(self):
        """Test value categorization."""
        bins = [0, 250, 500, 750, 1000]
        labels = ['Low', 'Medium', 'High', 'Very High']
        
        result = self.transformer.categorize_values(
            self.test_df,
            column='value',
            bins=bins,
            labels=labels
        )
        
        self.assertIn('value_category', result.columns)
        
        # Check categorization
        for _, row in result.iterrows():
            val = row['value']
            cat = row['value_category']
            if val <= 250:
                self.assertEqual(cat, 'Low')
            elif val <= 500:
                self.assertEqual(cat, 'Medium')
            elif val <= 750:
                self.assertEqual(cat, 'High')
            else:
                self.assertEqual(cat, 'Very High')
                
    def test_aggregate_by_category(self):
        """Test aggregation by category."""
        result = self.transformer.aggregate_by_category(
            self.test_df,
            group_col='category',
            value_col='value',
            agg_func='mean'
        )
        
        # Should have one row per category
        categories = self.test_df['category'].unique()
        self.assertEqual(len(result), len(categories))
        
        # Verify aggregations
        for cat in categories:
            cat_data = self.test_df[self.test_df['category'] == cat]
            cat_result = result[result['category'] == cat]
            
            self.assertAlmostEqual(
                cat_result['value_mean'].iloc[0],
                cat_data['value'].mean(),
                places=5
            )
            
    def test_calculate_rolling_statistics(self):
        """Test rolling window statistics."""
        # Sort by id for consistent rolling
        df = self.test_df.sort_values('id')
        
        result = self.transformer.calculate_rolling_stats(
            df,
            column='value',
            window=10
        )
        
        # Should have rolling stats columns
        self.assertIn('value_rolling_mean', result.columns)
        self.assertIn('value_rolling_std', result.columns)
        self.assertIn('value_rolling_min', result.columns)
        self.assertIn('value_rolling_max', result.columns)
        
        # First window-1 values should be NaN
        self.assertTrue(result['value_rolling_mean'].iloc[:9].isnull().all())
        
        # Check a specific rolling mean
        expected_mean = df['value'].iloc[0:10].mean()
        self.assertAlmostEqual(
            result['value_rolling_mean'].iloc[9],
            expected_mean,
            places=5
        )
        
    def test_detect_anomalies(self):
        """Test anomaly detection in values."""
        # Add some anomalies
        df = self.test_df.copy()
        df.loc[0, 'value'] = 10000  # Very high
        df.loc[1, 'value'] = -100    # Negative
        
        result = self.transformer.detect_anomalies(
            df,
            column='value',
            method='zscore',
            threshold=3
        )
        
        self.assertIn('is_anomaly', result.columns)
        self.assertIn('anomaly_score', result.columns)
        
        # Known anomalies should be detected
        self.assertTrue(result.loc[0, 'is_anomaly'])
        
    def test_normalize_by_group(self):
        """Test normalization within groups."""
        result = self.transformer.normalize_by_group(
            self.test_df,
            value_col='value',
            group_col='category'
        )
        
        self.assertIn('value_normalized', result.columns)
        
        # Check normalization within each group
        for cat in result['category'].unique():
            cat_data = result[result['category'] == cat]
            # Should be normalized within group
            self.assertAlmostEqual(cat_data['value_normalized'].mean(), 0, places=5)
            self.assertAlmostEqual(cat_data['value_normalized'].std(), 1, places=5)
            
    def test_calculate_percentile_ranks(self):
        """Test percentile rank calculation."""
        result = self.transformer.calculate_percentile_ranks(
            self.test_df,
            column='value'
        )
        
        self.assertIn('value_percentile', result.columns)
        
        # Percentiles should be between 0 and 100
        self.assertTrue(all(result['value_percentile'] >= 0))
        self.assertTrue(all(result['value_percentile'] <= 100))
        
        # Highest value should have highest percentile
        max_idx = result['value'].idxmax()
        min_idx = result['value'].idxmin()
        self.assertGreater(
            result.loc[max_idx, 'value_percentile'],
            result.loc[min_idx, 'value_percentile']
        )
        
    def test_handle_missing_values(self):
        """Test handling of missing values in transformation."""
        # Add missing values
        df = self.test_df.copy()
        df.loc[0:10, 'value'] = None
        
        # Should handle missing values gracefully
        result = self.transformer.filter_and_transform(
            df,
            threshold=0,
            transformation='square',
            handle_missing='drop'
        )
        
        # No missing values in result
        self.assertFalse(result['value'].isnull().any())
        
        # Test with fill strategy
        result = self.transformer.filter_and_transform(
            df,
            threshold=0,
            transformation='square',
            handle_missing='fill',
            fill_value=0
        )
        
        # Missing values should be filled
        self.assertFalse(result['value'].isnull().any())


class TestValueTransformerEdgeCases(BaseTestCase):
    """Test edge cases for value transformer."""
    
    def test_empty_dataframe(self):
        """Test handling of empty DataFrame."""
        empty_df = pd.DataFrame()
        transformer = ValueTransformer()
        
        result = transformer.filter_and_transform(
            empty_df,
            threshold=0,
            transformation='square'
        )
        
        self.assertTrue(result.empty)
        
    def test_single_row_dataframe(self):
        """Test handling of single row DataFrame."""
        single_df = pd.DataFrame({'value': [100]})
        transformer = ValueTransformer()
        
        result = transformer.filter_and_transform(
            single_df,
            threshold=0,
            transformation='square'
        )
        
        self.assertEqual(len(result), 1)
        self.assertEqual(result['transformed_value'].iloc[0], 10000)
        
    def test_all_filtered_out(self):
        """Test when all values are filtered out."""
        df = pd.DataFrame({'value': [1, 2, 3, 4, 5]})
        transformer = ValueTransformer()
        
        result = transformer.filter_and_transform(
            df,
            threshold=1000,  # Very high threshold
            transformation='square'
        )
        
        self.assertTrue(result.empty)
        
    def test_invalid_transformation(self):
        """Test handling of invalid transformation type."""
        df = pd.DataFrame({'value': [1, 2, 3]})
        transformer = ValueTransformer()
        
        with self.assertRaises(ValueError):
            transformer.filter_and_transform(
                df,
                threshold=0,
                transformation='invalid'
            )


if __name__ == "__main__":
    unittest.main()