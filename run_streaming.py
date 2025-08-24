#!/usr/bin/env python3
"""
CLI for running AirPipe workflows in streaming mode.
Allows any existing workflow to process continuous data streams.
"""

import argparse
import sys
import importlib.util
import logging
from pathlib import Path
from typing import Optional
import json

from airpipe.core.task import TaskPipeline
from airpipe.core.streaming import (
    MicroBatchProcessor,
    StreamConfig,
    StreamMonitor,
    AlertRule,
    StateManager,
    create_source
)


def load_workflow(workflow_path: str) -> TaskPipeline:
    """Load a workflow module and return its pipeline."""
    path = Path(workflow_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Workflow not found: {workflow_path}")
    
    # Load module
    spec = importlib.util.spec_from_file_location("workflow", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Find pipeline instance
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, TaskPipeline):
            return obj
    
    raise ValueError(f"No TaskPipeline found in {workflow_path}")


def setup_logging(level: str = "INFO"):
    """Configure logging."""
    log_level = getattr(logging, level.upper())
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def parse_source_config(config_str: str) -> dict:
    """Parse source configuration from string or file."""
    if config_str.startswith('@'):
        # Load from file
        with open(config_str[1:], 'r') as f:
            return json.load(f)
    else:
        # Parse as JSON
        return json.loads(config_str)


def main():
    parser = argparse.ArgumentParser(
        description="Run AirPipe workflows in streaming mode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with simulated data
  python run_streaming.py workflows/employee_task_workflow.py \\
    --source simulated \\
    --source-config '{"schema": {"value": "float"}, "rate": 100}'
  
  # Run with CSV file streaming
  python run_streaming.py workflows/transform_workflow.py \\
    --source csv \\
    --source-config '{"file_path": "data.csv", "chunk_size": 1000}'
  
  # Run with API polling
  python run_streaming.py workflows/api_workflow.py \\
    --source api \\
    --source-config '{"url": "http://api.example.com/data", "poll_interval": 30}'
  
  # Run with custom configuration file
  python run_streaming.py workflows/complex_workflow.py \\
    --source kafka \\
    --source-config @kafka_config.json \\
    --batch-size 5000 \\
    --checkpoint-dir ./checkpoints
        """
    )
    
    # Required arguments
    parser.add_argument(
        'workflow',
        help='Path to workflow Python file'
    )
    
    # Source configuration
    parser.add_argument(
        '--source',
        default='simulated',
        choices=['simulated', 'csv', 'api', 'kafka', 'database', 's3', 'socket', 'websocket'],
        help='Type of streaming source (default: simulated)'
    )
    
    parser.add_argument(
        '--source-config',
        default='{}',
        help='Source configuration as JSON string or @file.json'
    )
    
    # Streaming configuration
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Number of records per batch (default: 1000)'
    )
    
    parser.add_argument(
        '--batch-interval',
        type=float,
        default=10.0,
        help='Max seconds between batches (default: 10.0)'
    )
    
    parser.add_argument(
        '--max-batches',
        type=int,
        help='Maximum number of batches to process (default: unlimited)'
    )
    
    parser.add_argument(
        '--checkpoint-interval',
        type=int,
        default=100,
        help='Checkpoint every N batches (default: 100)'
    )
    
    parser.add_argument(
        '--checkpoint-dir',
        help='Directory for checkpoints'
    )
    
    parser.add_argument(
        '--error-strategy',
        choices=['continue', 'stop', 'retry'],
        default='continue',
        help='Error handling strategy (default: continue)'
    )
    
    parser.add_argument(
        '--retry-attempts',
        type=int,
        default=3,
        help='Number of retry attempts (default: 3)'
    )
    
    # Monitoring configuration
    parser.add_argument(
        '--enable-monitoring',
        action='store_true',
        default=True,
        help='Enable performance monitoring'
    )
    
    parser.add_argument(
        '--metrics-export',
        help='Path to export metrics'
    )
    
    parser.add_argument(
        '--alert-rules',
        help='Alert rules configuration file (JSON)'
    )
    
    # State management
    parser.add_argument(
        '--state-backend',
        choices=['memory', 'file', 'redis'],
        default='memory',
        help='State backend (default: memory)'
    )
    
    parser.add_argument(
        '--state-config',
        default='{}',
        help='State backend configuration as JSON'
    )
    
    # Other options
    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel task execution within batches'
    )
    
    parser.add_argument(
        '--workers',
        type=int,
        default=4,
        help='Number of parallel workers (default: 4)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate configuration without running'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    logger = logging.getLogger(__name__)
    
    try:
        # Load workflow
        logger.info(f"Loading workflow: {args.workflow}")
        pipeline = load_workflow(args.workflow)
        logger.info(f"Loaded pipeline: {pipeline.name}")
        
        # Parse configurations
        source_config = parse_source_config(args.source_config)
        state_config = json.loads(args.state_config)
        
        # Create streaming configuration
        stream_config = StreamConfig(
            batch_size=args.batch_size,
            batch_interval=args.batch_interval,
            max_batches=args.max_batches,
            checkpoint_interval=args.checkpoint_interval,
            error_strategy=args.error_strategy,
            retry_attempts=args.retry_attempts,
            enable_monitoring=args.enable_monitoring,
            enable_checkpointing=bool(args.checkpoint_dir),
            checkpoint_dir=args.checkpoint_dir
        )
        
        # Create state manager
        state_manager = StateManager(
            backend=args.state_backend,
            config=state_config
        )
        
        # Create source
        logger.info(f"Creating {args.source} source with config: {source_config}")
        
        # Handle special source types
        if args.source == 'csv':
            from airpipe.core.streaming.micro_batch import CSVStreamSource
            source = CSVStreamSource(**source_config)
        else:
            source = create_source(args.source, **source_config)
        
        # Create processor
        processor = MicroBatchProcessor(pipeline, stream_config)
        
        # Setup monitoring if enabled
        monitor = None
        if args.enable_monitoring:
            monitor = StreamMonitor(
                enable_alerts=bool(args.alert_rules),
                metrics_interval=5.0,
                export_path=args.metrics_export
            )
            
            # Load alert rules if provided
            if args.alert_rules:
                with open(args.alert_rules, 'r') as f:
                    rules = json.load(f)
                    for rule_config in rules:
                        rule = AlertRule(**rule_config)
                        monitor.add_alert_rule(rule)
                        logger.info(f"Added alert rule: {rule.name}")
            
            # Add default alert rules
            monitor.add_alert_rule(AlertRule(
                name="high_failure_rate",
                metric_name="success_rate",
                condition="<",
                threshold=90.0,
                window_seconds=60,
                severity="ERROR",
                message="Success rate dropped to {value:.1f}%"
            ))
            
            monitor.start(processor)
        
        if args.dry_run:
            logger.info("Dry run mode - configuration validated successfully")
            print("\nConfiguration Summary:")
            print(f"  Workflow: {args.workflow}")
            print(f"  Pipeline: {pipeline.name}")
            print(f"  Source: {args.source}")
            print(f"  Batch Size: {args.batch_size}")
            print(f"  Batch Interval: {args.batch_interval}s")
            print(f"  State Backend: {args.state_backend}")
            print(f"  Parallel Execution: {args.parallel}")
            print(f"  Workers: {args.workers}")
            
            # Show pipeline tasks
            print(f"\nPipeline Tasks ({len(pipeline.tasks)}):")
            for task_name, task in pipeline.tasks.items():
                print(f"  - {task_name} ({task.task_type.value})")
                if task.dependencies:
                    print(f"    Dependencies: {', '.join(task.dependencies)}")
                if task.produces:
                    print(f"    Produces: {task.produces}")
                if task.consumes:
                    print(f"    Consumes: {task.consumes}")
            
            return 0
        
        # Run streaming pipeline
        logger.info("Starting streaming pipeline...")
        logger.info(f"Configuration: batch_size={args.batch_size}, "
                   f"interval={args.batch_interval}s, "
                   f"max_batches={args.max_batches or 'unlimited'}")
        
        # Define batch transformation
        def transform_batch(batch):
            """Transform batch for pipeline processing."""
            import pandas as pd
            
            # Convert to DataFrame if needed
            if isinstance(batch, list):
                return pd.DataFrame(batch)
            return batch
        
        # Process stream
        processor.process_stream(
            source=source,
            transform_batch=transform_batch
        )
        
    except KeyboardInterrupt:
        logger.info("Streaming interrupted by user")
        return 0
    
    except Exception as e:
        logger.error(f"Error running streaming pipeline: {e}", exc_info=True)
        return 1
    
    finally:
        # Cleanup
        if monitor:
            monitor.stop()
            
            # Print final dashboard
            if args.enable_monitoring:
                monitor.print_dashboard()
        
        logger.info("Streaming pipeline stopped")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())