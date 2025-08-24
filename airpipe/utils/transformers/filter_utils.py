"""
Filtering utilities for data transformation.
"""

import pandas as pd
from typing import Any, List, Optional, Union, Callable, Dict
import operator
import logging

logger = logging.getLogger(__name__)


class FilterUtils:
    """Reusable filtering operations for DataFrames."""
    
    # Operator mapping
    OPERATORS = {
        '=': operator.eq,
        '==': operator.eq,
        '!=': operator.ne,
        '<>': operator.ne,
        '>': operator.gt,
        '>=': operator.ge,
        '<': operator.lt,
        '<=': operator.le,
        'in': lambda x, y: x.isin(y) if isinstance(y, (list, tuple)) else x == y,
        'not in': lambda x, y: ~x.isin(y) if isinstance(y, (list, tuple)) else x != y,
        'contains': lambda x, y: x.str.contains(y, na=False),
        'startswith': lambda x, y: x.str.startswith(y, na=False),
        'endswith': lambda x, y: x.str.endswith(y, na=False),
    }
    
    @staticmethod
    def filter_by_column(df: pd.DataFrame,
                        column: str,
                        operator_str: str,
                        value: Any) -> pd.DataFrame:
        """
        Filter DataFrame by a single column condition.
        
        Args:
            df: DataFrame to filter
            column: Column name to filter on
            operator_str: Operator as string ('=', '>', '<', etc.)
            value: Value to compare against
            
        Returns:
            Filtered DataFrame
            
        Raises:
            ValueError: If column doesn't exist or operator is invalid
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        if operator_str not in FilterUtils.OPERATORS:
            raise ValueError(f"Invalid operator: {operator_str}. "
                           f"Valid operators: {list(FilterUtils.OPERATORS.keys())}")
        
        op_func = FilterUtils.OPERATORS[operator_str]
        
        try:
            mask = op_func(df[column], value)
            filtered = df[mask].copy()
            logger.info(f"Filtered {len(df)} rows to {len(filtered)} rows "
                       f"using condition: {column} {operator_str} {value}")
            return filtered
        except Exception as e:
            logger.error(f"Error applying filter: {e}")
            raise
    
    @staticmethod
    def filter_by_multiple(df: pd.DataFrame,
                          conditions: List[Dict[str, Any]],
                          logic: str = 'and') -> pd.DataFrame:
        """
        Filter DataFrame by multiple conditions.
        
        Args:
            df: DataFrame to filter
            conditions: List of condition dicts with keys: column, operator, value
            logic: How to combine conditions ('and' or 'or')
            
        Returns:
            Filtered DataFrame
        """
        if not conditions:
            return df.copy()
        
        masks = []
        for cond in conditions:
            column = cond.get('column')
            op = cond.get('operator', '=')
            value = cond.get('value')
            
            if column not in df.columns:
                logger.warning(f"Column '{column}' not found, skipping condition")
                continue
            
            if op not in FilterUtils.OPERATORS:
                logger.warning(f"Invalid operator '{op}', skipping condition")
                continue
            
            op_func = FilterUtils.OPERATORS[op]
            masks.append(op_func(df[column], value))
        
        if not masks:
            return df.copy()
        
        if logic.lower() == 'and':
            combined_mask = masks[0]
            for mask in masks[1:]:
                combined_mask = combined_mask & mask
        elif logic.lower() == 'or':
            combined_mask = masks[0]
            for mask in masks[1:]:
                combined_mask = combined_mask | mask
        else:
            raise ValueError(f"Invalid logic: {logic}. Must be 'and' or 'or'")
        
        filtered = df[combined_mask].copy()
        logger.info(f"Applied {len(conditions)} conditions with {logic} logic: "
                   f"{len(df)} rows -> {len(filtered)} rows")
        return filtered
    
    @staticmethod
    def filter_by_range(df: pd.DataFrame,
                       column: str,
                       min_value: Optional[Any] = None,
                       max_value: Optional[Any] = None,
                       inclusive: str = 'both') -> pd.DataFrame:
        """
        Filter DataFrame by range on a column.
        
        Args:
            df: DataFrame to filter
            column: Column name to filter on
            min_value: Minimum value (inclusive by default)
            max_value: Maximum value (inclusive by default)
            inclusive: Include boundaries ('both', 'neither', 'left', 'right')
            
        Returns:
            Filtered DataFrame
        """
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        
        df_copy = df.copy()
        
        if min_value is not None and max_value is not None:
            if inclusive == 'both':
                mask = (df_copy[column] >= min_value) & (df_copy[column] <= max_value)
            elif inclusive == 'neither':
                mask = (df_copy[column] > min_value) & (df_copy[column] < max_value)
            elif inclusive == 'left':
                mask = (df_copy[column] >= min_value) & (df_copy[column] < max_value)
            elif inclusive == 'right':
                mask = (df_copy[column] > min_value) & (df_copy[column] <= max_value)
            else:
                raise ValueError(f"Invalid inclusive value: {inclusive}")
        elif min_value is not None:
            mask = df_copy[column] >= min_value if inclusive in ['both', 'left'] else df_copy[column] > min_value
        elif max_value is not None:
            mask = df_copy[column] <= max_value if inclusive in ['both', 'right'] else df_copy[column] < max_value
        else:
            return df_copy
        
        filtered = df_copy[mask]
        logger.info(f"Filtered by range on {column}: {len(df)} rows -> {len(filtered)} rows")
        return filtered
    
    @staticmethod
    def filter_by_custom(df: pd.DataFrame,
                        filter_func: Callable[[pd.DataFrame], pd.Series]) -> pd.DataFrame:
        """
        Filter DataFrame using a custom function.
        
        Args:
            df: DataFrame to filter
            filter_func: Function that takes DataFrame and returns boolean Series
            
        Returns:
            Filtered DataFrame
        """
        try:
            mask = filter_func(df)
            if not isinstance(mask, pd.Series) or mask.dtype != bool:
                raise ValueError("Filter function must return boolean Series")
            
            filtered = df[mask].copy()
            logger.info(f"Applied custom filter: {len(df)} rows -> {len(filtered)} rows")
            return filtered
        except Exception as e:
            logger.error(f"Error applying custom filter: {e}")
            raise
    
    @staticmethod
    def remove_duplicates(df: pd.DataFrame,
                         subset: Optional[List[str]] = None,
                         keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows from DataFrame.
        
        Args:
            df: DataFrame to process
            subset: Columns to consider for duplicates
            keep: Which duplicates to keep ('first', 'last', False)
            
        Returns:
            DataFrame without duplicates
        """
        original_len = len(df)
        deduped = df.drop_duplicates(subset=subset, keep=keep).copy()
        removed = original_len - len(deduped)
        
        if removed > 0:
            logger.info(f"Removed {removed} duplicate rows")
        
        return deduped