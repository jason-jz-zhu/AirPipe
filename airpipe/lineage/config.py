"""
Configuration management for Spline integration.
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import json
from pathlib import Path


@dataclass
class SplineConfig:
    """Configuration for Spline lineage tracking."""
    
    # Spline Server settings
    spline_url: str = "http://localhost:8080"
    producer_api_path: str = "/producer/v1/lineage"
    
    # Authentication (if needed)
    auth_enabled: bool = False
    auth_token: Optional[str] = None
    
    # Capture settings
    enabled: bool = True
    capture_schemas: bool = True
    capture_row_counts: bool = True
    capture_execution_time: bool = True
    batch_size: int = 1  # Number of events to batch before sending
    
    # Metadata enrichment
    application_name: str = "AirPipe"
    application_version: str = "1.0.0"
    environment: str = "development"
    
    # Advanced settings
    timeout_seconds: int = 30
    retry_count: int = 3
    verify_ssl: bool = True
    
    # Custom metadata to include with all events
    custom_metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_env(cls) -> 'SplineConfig':
        """Create configuration from environment variables."""
        config = cls()
        
        # Override with environment variables if present
        if os.getenv('SPLINE_URL'):
            config.spline_url = os.getenv('SPLINE_URL')
        
        if os.getenv('SPLINE_AUTH_TOKEN'):
            config.auth_enabled = True
            config.auth_token = os.getenv('SPLINE_AUTH_TOKEN')
        
        if os.getenv('SPLINE_ENABLED'):
            config.enabled = os.getenv('SPLINE_ENABLED').lower() == 'true'
        
        if os.getenv('SPLINE_ENVIRONMENT'):
            config.environment = os.getenv('SPLINE_ENVIRONMENT')
        
        if os.getenv('SPLINE_APP_NAME'):
            config.application_name = os.getenv('SPLINE_APP_NAME')
        
        return config
    
    @classmethod
    def from_file(cls, config_path: str) -> 'SplineConfig':
        """Load configuration from a JSON file."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(path, 'r') as f:
            config_data = json.load(f)
        
        return cls(**config_data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'spline_url': self.spline_url,
            'producer_api_path': self.producer_api_path,
            'auth_enabled': self.auth_enabled,
            'enabled': self.enabled,
            'capture_schemas': self.capture_schemas,
            'capture_row_counts': self.capture_row_counts,
            'capture_execution_time': self.capture_execution_time,
            'batch_size': self.batch_size,
            'application_name': self.application_name,
            'application_version': self.application_version,
            'environment': self.environment,
            'timeout_seconds': self.timeout_seconds,
            'retry_count': self.retry_count,
            'verify_ssl': self.verify_ssl,
            'custom_metadata': self.custom_metadata
        }
    
    def save_to_file(self, config_path: str) -> None:
        """Save configuration to a JSON file."""
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
    
    @property
    def producer_url(self) -> str:
        """Get the full URL for the Spline Producer API."""
        return f"{self.spline_url.rstrip('/')}{self.producer_api_path}"
    
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for Spline API requests."""
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        if self.auth_enabled and self.auth_token:
            headers['Authorization'] = f"Bearer {self.auth_token}"
        
        return headers