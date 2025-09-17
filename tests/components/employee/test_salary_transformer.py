"""Tests for salary transformation business logic."""

import unittest
import pandas as pd
import numpy as np

from pipelines.employee.transformers.salary_transformer import SalaryTransformer
from tests.base import BaseTestCase
from tests.fixtures.factories import DataFactory


class TestSalaryTransformer(BaseTestCase):
    """Test SalaryTransformer business logic."""
    
    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.transformer = SalaryTransformer()
        
        # Create test data with known salary distribution
        self.employee_df = pd.DataFrame({
            'employee_id': range(1, 101),
            'name': [f'Employee_{i}' for i in range(1, 101)],
            'department': np.random.choice(['Engineering', 'Sales', 'HR', 'Finance'], 100),
            'salary': np.random.randint(40000, 150000, 100),
            'hire_date': pd.date_range('2020-01-01', periods=100, freq='W'),
            'is_active': [True] * 100
        })
        
    def test_filter_high_earners(self):
        """Test filtering employees with high salaries."""
        threshold = 80000
        result = self.transformer.filter_high_earners(
            self.employee_df,
            threshold=threshold
        )
        
        # All salaries should be above threshold
        self.assertTrue(all(result['salary'] > threshold))
        
        # Should preserve all columns
        self.assertEqual(set(result.columns), set(self.employee_df.columns))
        
    def test_calculate_salary_bands(self):
        """Test categorizing employees into salary bands."""
        result = self.transformer.calculate_salary_bands(self.employee_df)
        
        # Should have salary_band column
        self.assertIn('salary_band', result.columns)
        
        # Check band assignments
        for _, row in result.iterrows():
            if row['salary'] < 50000:
                self.assertEqual(row['salary_band'], 'Junior')
            elif row['salary'] < 80000:
                self.assertEqual(row['salary_band'], 'Mid')
            elif row['salary'] < 120000:
                self.assertEqual(row['salary_band'], 'Senior')
            else:
                self.assertEqual(row['salary_band'], 'Executive')
                
    def test_apply_salary_adjustment(self):
        """Test applying salary adjustments."""
        # Test percentage increase
        result = self.transformer.apply_salary_adjustment(
            self.employee_df.copy(),
            adjustment_type='percentage',
            value=10
        )
        
        # Salaries should be 10% higher
        expected = self.employee_df['salary'] * 1.1
        np.testing.assert_array_almost_equal(result['salary'], expected)
        
        # Test flat increase
        result = self.transformer.apply_salary_adjustment(
            self.employee_df.copy(),
            adjustment_type='flat',
            value=5000
        )
        
        expected = self.employee_df['salary'] + 5000
        np.testing.assert_array_equal(result['salary'], expected)
        
    def test_calculate_department_salary_stats(self):
        """Test calculating salary statistics by department."""
        result = self.transformer.calculate_department_stats(self.employee_df)
        
        # Should have one row per department
        departments = self.employee_df['department'].unique()
        self.assertEqual(len(result), len(departments))
        
        # Should have statistical columns
        expected_cols = ['department', 'avg_salary', 'min_salary', 
                        'max_salary', 'median_salary', 'employee_count']
        for col in expected_cols:
            self.assertIn(col, result.columns)
            
        # Verify calculations
        for dept in departments:
            dept_data = self.employee_df[self.employee_df['department'] == dept]
            dept_stats = result[result['department'] == dept].iloc[0]
            
            self.assertAlmostEqual(
                dept_stats['avg_salary'],
                dept_data['salary'].mean(),
                places=2
            )
            self.assertEqual(
                dept_stats['employee_count'],
                len(dept_data)
            )
            
    def test_identify_salary_outliers(self):
        """Test identifying salary outliers."""
        # Add some outliers
        df = self.employee_df.copy()
        df.loc[0, 'salary'] = 500000  # Very high
        df.loc[1, 'salary'] = 10000   # Very low
        
        result = self.transformer.identify_salary_outliers(
            df,
            method='iqr',
            threshold=1.5
        )
        
        # Should have outlier flag
        self.assertIn('is_outlier', result.columns)
        
        # Known outliers should be flagged
        self.assertTrue(result.loc[0, 'is_outlier'])
        self.assertTrue(result.loc[1, 'is_outlier'])
        
        # Test z-score method
        result = self.transformer.identify_salary_outliers(
            df,
            method='zscore',
            threshold=3
        )
        
        self.assertIn('is_outlier', result.columns)
        self.assertIn('outlier_score', result.columns)
        
    def test_calculate_salary_percentiles(self):
        """Test calculating salary percentiles."""
        result = self.transformer.calculate_salary_percentiles(self.employee_df)
        
        # Should have percentile column
        self.assertIn('salary_percentile', result.columns)
        
        # Percentiles should be between 0 and 100
        self.assertTrue(all(result['salary_percentile'] >= 0))
        self.assertTrue(all(result['salary_percentile'] <= 100))
        
        # Highest salary should have high percentile
        max_salary_idx = result['salary'].idxmax()
        self.assertGreater(result.loc[max_salary_idx, 'salary_percentile'], 95)
        
    def test_normalize_salaries(self):
        """Test salary normalization for comparison."""
        result = self.transformer.normalize_salaries(
            self.employee_df,
            method='minmax'
        )
        
        # Should have normalized column
        self.assertIn('salary_normalized', result.columns)
        
        # Min-max normalization should be between 0 and 1
        self.assertAlmostEqual(result['salary_normalized'].min(), 0, places=5)
        self.assertAlmostEqual(result['salary_normalized'].max(), 1, places=5)
        
        # Test z-score normalization
        result = self.transformer.normalize_salaries(
            self.employee_df,
            method='zscore'
        )
        
        # Z-score should have mean ~0 and std ~1
        self.assertAlmostEqual(result['salary_normalized'].mean(), 0, places=5)
        self.assertAlmostEqual(result['salary_normalized'].std(), 1, places=5)
        
    def test_calculate_compensation_ratio(self):
        """Test calculating compensation ratios."""
        # Add market rate data
        market_rates = {
            'Engineering': 95000,
            'Sales': 75000,
            'HR': 65000,
            'Finance': 85000
        }
        
        result = self.transformer.calculate_comp_ratio(
            self.employee_df,
            market_rates=market_rates
        )
        
        # Should have comp_ratio column
        self.assertIn('comp_ratio', result.columns)
        
        # Verify calculations
        for _, row in result.iterrows():
            expected_ratio = row['salary'] / market_rates[row['department']]
            self.assertAlmostEqual(row['comp_ratio'], expected_ratio, places=4)
            
    def test_project_salary_growth(self):
        """Test projecting future salary growth."""
        result = self.transformer.project_salary_growth(
            self.employee_df,
            years=3,
            annual_increase=0.05
        )
        
        # Should have projection columns
        for year in range(1, 4):
            self.assertIn(f'salary_year_{year}', result.columns)
            
        # Verify projections
        for year in range(1, 4):
            expected = self.employee_df['salary'] * (1.05 ** year)
            np.testing.assert_array_almost_equal(
                result[f'salary_year_{year}'],
                expected,
                decimal=2
            )
            
    def test_calculate_salary_budget_impact(self):
        """Test calculating budget impact of salary changes."""
        # Apply 10% increase
        adjusted_df = self.transformer.apply_salary_adjustment(
            self.employee_df.copy(),
            adjustment_type='percentage',
            value=10
        )
        
        impact = self.transformer.calculate_budget_impact(
            original_df=self.employee_df,
            adjusted_df=adjusted_df
        )
        
        # Should have impact metrics
        self.assertIn('total_increase', impact)
        self.assertIn('percentage_increase', impact)
        self.assertIn('department_breakdown', impact)
        
        # Verify calculations
        expected_increase = (adjusted_df['salary'].sum() - 
                           self.employee_df['salary'].sum())
        self.assertAlmostEqual(impact['total_increase'], expected_increase, places=2)
        self.assertAlmostEqual(impact['percentage_increase'], 10.0, places=1)
        
    def test_handle_invalid_salaries(self):
        """Test handling of invalid salary data."""
        # Create data with invalid salaries
        df = self.employee_df.copy()
        df.loc[0, 'salary'] = -1000  # Negative
        df.loc[1, 'salary'] = None    # Missing
        df.loc[2, 'salary'] = 0       # Zero
        
        # Should handle gracefully
        result = self.transformer.clean_salary_data(df)
        
        # No negative salaries
        self.assertTrue(all(result['salary'] >= 0))
        
        # No null salaries
        self.assertFalse(result['salary'].isnull().any())


class TestSalaryTransformerPerformance(BaseTestCase):
    """Performance tests for salary transformer."""
    
    def test_large_dataset_performance(self):
        """Test transformer performance with large dataset."""
        # Create large dataset
        large_df = DataFactory.create_employee_dataframe(100000)
        transformer = SalaryTransformer()
        
        import time
        
        # Test filtering performance
        start = time.time()
        result = transformer.filter_high_earners(large_df, threshold=80000)
        elapsed = time.time() - start
        
        self.assertLess(elapsed, 1.0)  # Should complete in under 1 second
        
        # Test statistics calculation performance
        start = time.time()
        stats = transformer.calculate_department_stats(large_df)
        elapsed = time.time() - start
        
        self.assertLess(elapsed, 2.0)  # Should complete in under 2 seconds


if __name__ == "__main__":
    unittest.main()