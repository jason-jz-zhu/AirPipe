"""
Data lineage tracking integration for AirPipe.

This module provides integration with Apache Spline for capturing and visualizing
data lineage from AirPipe workflows.
"""

from .spline_tracker import SplineLineageTracker
from .config import SplineConfig

__all__ = ['SplineLineageTracker', 'SplineConfig']