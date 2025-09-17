"""
Logging utilities for the AirPipe framework.
"""

import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime


def setup_logger(
    name: str,
    level: str = "INFO",
    log_file: Optional[Path] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Setup a logger with consistent formatting.
    
    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file to write logs to
        format_string: Optional custom format string
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    logger.handlers = []
    
    # Default format
    if not format_string:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    formatter = logging.Formatter(format_string)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        log_file = Path(log_file)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


class PipelineLogger:
    """Context manager for pipeline execution logging."""
    
    def __init__(self, pipeline_name: str, log_dir: Optional[Path] = None):
        """
        Initialize pipeline logger.
        
        Args:
            pipeline_name: Name of the pipeline
            log_dir: Directory for log files
        """
        self.pipeline_name = pipeline_name
        self.log_dir = log_dir or Path("logs")
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamped log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = self.log_dir / f"{pipeline_name}_{timestamp}.log"
        
        self.logger = setup_logger(
            name=f"Pipeline.{pipeline_name}",
            log_file=self.log_file
        )
    
    def __enter__(self):
        self.logger.info(f"Starting pipeline: {self.pipeline_name}")
        return self.logger
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.logger.error(
                f"Pipeline failed with error: {exc_val}",
                exc_info=True
            )
        else:
            self.logger.info(f"Pipeline completed successfully: {self.pipeline_name}")
        return False