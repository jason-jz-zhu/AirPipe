"""
Real-time monitoring and metrics for streaming operations.
"""

import threading
import time
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import logging
from collections import deque
import json
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Single metric measurement point."""
    timestamp: datetime
    metric_name: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Alert:
    """Alert triggered by monitoring rules."""
    alert_id: str
    timestamp: datetime
    severity: str  # "INFO", "WARNING", "ERROR", "CRITICAL"
    metric_name: str
    condition: str
    current_value: float
    threshold: float
    message: str


class MetricsBuffer:
    """Ring buffer for storing recent metrics."""
    
    def __init__(self, max_size: int = 10000):
        """
        Initialize metrics buffer.
        
        Args:
            max_size: Maximum number of metric points to store
        """
        self.buffer = deque(maxlen=max_size)
        self.lock = threading.Lock()
    
    def add(self, metric: MetricPoint) -> None:
        """Add metric point to buffer."""
        with self.lock:
            self.buffer.append(metric)
    
    def get_recent(self, seconds: int = 60) -> List[MetricPoint]:
        """Get metrics from last N seconds."""
        cutoff = datetime.now() - timedelta(seconds=seconds)
        with self.lock:
            return [m for m in self.buffer if m.timestamp >= cutoff]
    
    def get_by_name(self, metric_name: str, seconds: int = 60) -> List[MetricPoint]:
        """Get specific metric from last N seconds."""
        cutoff = datetime.now() - timedelta(seconds=seconds)
        with self.lock:
            return [m for m in self.buffer 
                   if m.timestamp >= cutoff and m.metric_name == metric_name]
    
    def clear(self) -> None:
        """Clear all metrics."""
        with self.lock:
            self.buffer.clear()


class StreamMonitor:
    """Monitor streaming pipeline performance and health."""
    
    def __init__(self, 
                 enable_alerts: bool = True,
                 metrics_interval: float = 5.0,
                 export_path: Optional[str] = None):
        """
        Initialize stream monitor.
        
        Args:
            enable_alerts: Whether to enable alerting
            metrics_interval: Interval for metrics collection (seconds)
            export_path: Optional path to export metrics
        """
        self.enable_alerts = enable_alerts
        self.metrics_interval = metrics_interval
        self.export_path = export_path
        
        # Metrics storage
        self.metrics_buffer = MetricsBuffer()
        self.cumulative_metrics = {}
        
        # Alert management
        self.alerts: List[Alert] = []
        self.alert_rules: List[AlertRule] = []
        
        # Monitoring state
        self.is_running = False
        self._monitor_thread = None
        self._stop_event = threading.Event()
        
        # Performance counters
        self.counters = {
            'records_processed': 0,
            'batches_processed': 0,
            'errors': 0,
            'backpressure_events': 0,
            'checkpoint_saves': 0
        }
        
        # Latency tracking
        self.latency_tracker = LatencyTracker()
        
        logger.info("Initialized StreamMonitor")
    
    def start(self, processor: 'MicroBatchProcessor' = None) -> None:
        """
        Start monitoring.
        
        Args:
            processor: Optional MicroBatchProcessor to monitor
        """
        if self.is_running:
            logger.warning("Monitor already running")
            return
        
        self.is_running = True
        self._stop_event.clear()
        self.processor = processor
        
        # Start monitoring thread
        self._monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=True
        )
        self._monitor_thread.start()
        
        logger.info("Started stream monitoring")
    
    def stop(self) -> None:
        """Stop monitoring."""
        if not self.is_running:
            return
        
        self.is_running = False
        self._stop_event.set()
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        
        # Export final metrics if configured
        if self.export_path:
            self.export_metrics()
        
        logger.info("Stopped stream monitoring")
    
    def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_running and not self._stop_event.is_set():
            try:
                # Collect metrics
                self._collect_metrics()
                
                # Check alert rules
                if self.enable_alerts:
                    self._check_alerts()
                
                # Export metrics if configured
                if self.export_path and len(self.metrics_buffer.buffer) % 100 == 0:
                    self.export_metrics()
                
                # Sleep until next collection
                time.sleep(self.metrics_interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
    
    def _collect_metrics(self) -> None:
        """Collect current metrics."""
        now = datetime.now()
        
        # Collect processor metrics if available
        if hasattr(self, 'processor') and self.processor:
            stats = self.processor.stats.get_summary()
            
            # Record throughput
            self.record_metric(
                "throughput_per_sec",
                stats['throughput_per_sec'],
                {"pipeline": self.processor.pipeline.name}
            )
            
            # Record batch processing time
            self.record_metric(
                "avg_batch_time",
                stats['avg_batch_time'],
                {"pipeline": self.processor.pipeline.name}
            )
            
            # Record success rate
            total = stats['total_batches']
            if total > 0:
                success_rate = stats['successful_batches'] / total * 100
                self.record_metric(
                    "success_rate",
                    success_rate,
                    {"pipeline": self.processor.pipeline.name}
                )
            
            # Record queue size (backpressure indicator)
            if hasattr(self.processor, 'queue'):
                queue_size = self.processor.queue.qsize()
                self.record_metric(
                    "queue_size",
                    queue_size,
                    {"pipeline": self.processor.pipeline.name}
                )
        
        # Record system metrics
        import psutil
        
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.1)
        self.record_metric("cpu_percent", cpu_percent, {"type": "system"})
        
        # Memory usage
        memory = psutil.virtual_memory()
        self.record_metric("memory_percent", memory.percent, {"type": "system"})
        
        # Record custom counters
        for counter_name, value in self.counters.items():
            self.record_metric(f"counter_{counter_name}", value, {"type": "counter"})
    
    def record_metric(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """
        Record a metric value.
        
        Args:
            name: Metric name
            value: Metric value
            tags: Optional tags
        """
        metric = MetricPoint(
            timestamp=datetime.now(),
            metric_name=name,
            value=value,
            tags=tags or {}
        )
        
        self.metrics_buffer.add(metric)
        
        # Update cumulative metrics
        if name not in self.cumulative_metrics:
            self.cumulative_metrics[name] = {
                'count': 0,
                'sum': 0,
                'min': float('inf'),
                'max': float('-inf')
            }
        
        stats = self.cumulative_metrics[name]
        stats['count'] += 1
        stats['sum'] += value
        stats['min'] = min(stats['min'], value)
        stats['max'] = max(stats['max'], value)
    
    def increment_counter(self, name: str, value: int = 1) -> None:
        """Increment a counter metric."""
        if name in self.counters:
            self.counters[name] += value
        else:
            self.counters[name] = value
    
    def record_latency(self, operation: str, latency_ms: float) -> None:
        """Record operation latency."""
        self.latency_tracker.record(operation, latency_ms)
        self.record_metric(f"latency_{operation}", latency_ms, {"unit": "ms"})
    
    def add_alert_rule(self, rule: 'AlertRule') -> None:
        """Add alert rule."""
        self.alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name}")
    
    def _check_alerts(self) -> None:
        """Check alert rules against current metrics."""
        for rule in self.alert_rules:
            # Get recent metrics for this rule
            metrics = self.metrics_buffer.get_by_name(
                rule.metric_name,
                rule.window_seconds
            )
            
            if not metrics:
                continue
            
            # Calculate aggregate value
            values = [m.value for m in metrics]
            if rule.aggregation == "avg":
                agg_value = sum(values) / len(values)
            elif rule.aggregation == "min":
                agg_value = min(values)
            elif rule.aggregation == "max":
                agg_value = max(values)
            elif rule.aggregation == "sum":
                agg_value = sum(values)
            else:
                agg_value = values[-1]  # latest value
            
            # Check condition
            triggered = False
            if rule.condition == ">" and agg_value > rule.threshold:
                triggered = True
            elif rule.condition == "<" and agg_value < rule.threshold:
                triggered = True
            elif rule.condition == ">=" and agg_value >= rule.threshold:
                triggered = True
            elif rule.condition == "<=" and agg_value <= rule.threshold:
                triggered = True
            elif rule.condition == "==" and agg_value == rule.threshold:
                triggered = True
            
            if triggered:
                self._trigger_alert(rule, agg_value)
    
    def _trigger_alert(self, rule: 'AlertRule', current_value: float) -> None:
        """Trigger an alert."""
        alert = Alert(
            alert_id=f"{rule.name}_{datetime.now().timestamp()}",
            timestamp=datetime.now(),
            severity=rule.severity,
            metric_name=rule.metric_name,
            condition=f"{rule.condition} {rule.threshold}",
            current_value=current_value,
            threshold=rule.threshold,
            message=rule.message.format(
                metric=rule.metric_name,
                value=current_value,
                threshold=rule.threshold
            )
        )
        
        self.alerts.append(alert)
        
        # Log alert
        if rule.severity == "CRITICAL":
            logger.critical(alert.message)
        elif rule.severity == "ERROR":
            logger.error(alert.message)
        elif rule.severity == "WARNING":
            logger.warning(alert.message)
        else:
            logger.info(alert.message)
        
        # Execute callback if provided
        if rule.callback:
            try:
                rule.callback(alert)
            except Exception as e:
                logger.error(f"Error in alert callback: {e}")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics."""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'counters': self.counters.copy(),
            'cumulative': {},
            'recent': {},
            'latencies': self.latency_tracker.get_summary(),
            'alerts': len(self.alerts)
        }
        
        # Add cumulative metrics
        for name, stats in self.cumulative_metrics.items():
            if stats['count'] > 0:
                summary['cumulative'][name] = {
                    'avg': stats['sum'] / stats['count'],
                    'min': stats['min'],
                    'max': stats['max'],
                    'count': stats['count']
                }
        
        # Add recent metrics (last 60 seconds)
        recent_metrics = self.metrics_buffer.get_recent(60)
        metric_groups = {}
        for metric in recent_metrics:
            if metric.metric_name not in metric_groups:
                metric_groups[metric.metric_name] = []
            metric_groups[metric.metric_name].append(metric.value)
        
        for name, values in metric_groups.items():
            if values:
                summary['recent'][name] = {
                    'latest': values[-1],
                    'avg': sum(values) / len(values),
                    'min': min(values),
                    'max': max(values)
                }
        
        return summary
    
    def export_metrics(self, filepath: Optional[str] = None) -> None:
        """Export metrics to file."""
        if not filepath:
            filepath = self.export_path
        
        if not filepath:
            return
        
        path = Path(filepath)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        # Export as JSON lines format
        with open(path, 'a') as f:
            for metric in self.metrics_buffer.buffer:
                metric_dict = {
                    'timestamp': metric.timestamp.isoformat(),
                    'metric': metric.metric_name,
                    'value': metric.value,
                    'tags': metric.tags
                }
                f.write(json.dumps(metric_dict) + '\n')
        
        logger.debug(f"Exported {len(self.metrics_buffer.buffer)} metrics to {filepath}")
    
    def print_dashboard(self) -> None:
        """Print monitoring dashboard to console."""
        summary = self.get_metrics_summary()
        
        print("\n" + "=" * 80)
        print(" STREAMING MONITOR DASHBOARD ".center(80))
        print("=" * 80)
        
        # Counters
        print("\nCOUNTERS:")
        for name, value in summary['counters'].items():
            print(f"  {name:30} {value:>10,}")
        
        # Recent metrics
        if summary['recent']:
            print("\nRECENT METRICS (last 60s):")
            for name, stats in summary['recent'].items():
                print(f"  {name:30} Latest: {stats['latest']:>8.2f}  "
                      f"Avg: {stats['avg']:>8.2f}  "
                      f"Min: {stats['min']:>8.2f}  "
                      f"Max: {stats['max']:>8.2f}")
        
        # Latencies
        if summary['latencies']:
            print("\nLATENCIES (ms):")
            for op, stats in summary['latencies'].items():
                print(f"  {op:30} P50: {stats['p50']:>8.2f}  "
                      f"P95: {stats['p95']:>8.2f}  "
                      f"P99: {stats['p99']:>8.2f}")
        
        # Alerts
        if self.alerts:
            print(f"\nRECENT ALERTS ({len(self.alerts)} total):")
            for alert in self.alerts[-5:]:  # Show last 5 alerts
                print(f"  [{alert.severity:8}] {alert.timestamp.strftime('%H:%M:%S')} "
                      f"{alert.message}")
        
        print("=" * 80)


class LatencyTracker:
    """Track latency percentiles for operations."""
    
    def __init__(self, window_size: int = 1000):
        """
        Initialize latency tracker.
        
        Args:
            window_size: Number of samples to keep per operation
        """
        self.window_size = window_size
        self.latencies = {}
        self.lock = threading.Lock()
    
    def record(self, operation: str, latency_ms: float) -> None:
        """Record operation latency."""
        with self.lock:
            if operation not in self.latencies:
                self.latencies[operation] = deque(maxlen=self.window_size)
            self.latencies[operation].append(latency_ms)
    
    def get_percentile(self, operation: str, percentile: float) -> float:
        """Get latency percentile for operation."""
        with self.lock:
            if operation not in self.latencies or not self.latencies[operation]:
                return 0.0
            
            sorted_latencies = sorted(self.latencies[operation])
            index = int(len(sorted_latencies) * percentile / 100)
            return sorted_latencies[min(index, len(sorted_latencies) - 1)]
    
    def get_summary(self) -> Dict[str, Dict[str, float]]:
        """Get latency summary for all operations."""
        summary = {}
        with self.lock:
            for operation in self.latencies:
                if self.latencies[operation]:
                    summary[operation] = {
                        'p50': self.get_percentile(operation, 50),
                        'p95': self.get_percentile(operation, 95),
                        'p99': self.get_percentile(operation, 99),
                        'count': len(self.latencies[operation])
                    }
        return summary


@dataclass
class AlertRule:
    """Rule for triggering alerts."""
    name: str
    metric_name: str
    condition: str  # ">", "<", ">=", "<=", "=="
    threshold: float
    window_seconds: int = 60
    aggregation: str = "avg"  # "avg", "min", "max", "sum", "latest"
    severity: str = "WARNING"  # "INFO", "WARNING", "ERROR", "CRITICAL"
    message: str = "Alert: {metric} {value} exceeds threshold {threshold}"
    callback: Optional[Callable[[Alert], None]] = None


class HealthChecker:
    """Health checking for streaming pipelines."""
    
    def __init__(self, monitor: StreamMonitor):
        """
        Initialize health checker.
        
        Args:
            monitor: StreamMonitor instance
        """
        self.monitor = monitor
        self.health_checks = []
    
    def add_check(self, name: str, check_func: Callable[[], bool], 
                  critical: bool = False) -> None:
        """
        Add health check.
        
        Args:
            name: Check name
            check_func: Function that returns True if healthy
            critical: Whether this is a critical check
        """
        self.health_checks.append({
            'name': name,
            'check': check_func,
            'critical': critical
        })
    
    def check_health(self) -> Dict[str, Any]:
        """
        Run all health checks.
        
        Returns:
            Health status dictionary
        """
        results = {
            'timestamp': datetime.now().isoformat(),
            'healthy': True,
            'checks': []
        }
        
        for check in self.health_checks:
            try:
                is_healthy = check['check']()
                results['checks'].append({
                    'name': check['name'],
                    'healthy': is_healthy,
                    'critical': check['critical']
                })
                
                if not is_healthy and check['critical']:
                    results['healthy'] = False
                    
            except Exception as e:
                results['checks'].append({
                    'name': check['name'],
                    'healthy': False,
                    'critical': check['critical'],
                    'error': str(e)
                })
                
                if check['critical']:
                    results['healthy'] = False
        
        return results


# Re-export StreamingStats from micro_batch
from .micro_batch import StreamingStats

__all__ = ['StreamMonitor', 'StreamingStats', 'AlertRule', 'HealthChecker']