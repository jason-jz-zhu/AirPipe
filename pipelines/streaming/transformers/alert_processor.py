"""
Streaming alert processing and rule evaluation logic.
"""

import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    """Data class for streaming alerts."""
    alert_type: str
    message: str
    severity: str
    timestamp: str
    value: Optional[float] = None
    threshold: Optional[float] = None
    metric_name: Optional[str] = None


class StreamingAlertProcessor:
    """Process alerts and notifications for streaming data."""
    
    def __init__(self):
        self.logger = logger
        self.alert_rules = {}
        
    def add_alert_rule(self, 
                      rule_name: str,
                      metric_name: str,
                      condition: str,
                      threshold: float,
                      severity: str = "WARNING",
                      message_template: str = None) -> None:
        """
        Add an alert rule for monitoring streaming metrics.
        
        Args:
            rule_name: Name of the alert rule
            metric_name: Name of the metric to monitor
            condition: Condition operator ('>', '<', '>=', '<=', '==', '!=')
            threshold: Threshold value for the condition
            severity: Alert severity level
            message_template: Custom message template with {value} placeholder
        """
        if message_template is None:
            message_template = f"{metric_name} {condition} {threshold}: {{value}}"
        
        self.alert_rules[rule_name] = {
            'metric_name': metric_name,
            'condition': condition,
            'threshold': threshold,
            'severity': severity,
            'message_template': message_template
        }
        
        self.logger.info(f"Added alert rule '{rule_name}' for {metric_name} {condition} {threshold}")
    
    def check_alert_conditions(self, metrics: Dict[str, Any]) -> List[Alert]:
        """
        Check metrics against defined alert rules.
        
        Args:
            metrics: Dictionary of metrics to check
            
        Returns:
            List of triggered alerts
        """
        alerts = []
        
        for rule_name, rule in self.alert_rules.items():
            metric_name = rule['metric_name']
            
            if metric_name not in metrics:
                continue
                
            metric_value = metrics[metric_name]
            threshold = rule['threshold']
            condition = rule['condition']
            
            # Evaluate condition
            triggered = False
            try:
                if condition == '>':
                    triggered = metric_value > threshold
                elif condition == '<':
                    triggered = metric_value < threshold
                elif condition == '>=':
                    triggered = metric_value >= threshold
                elif condition == '<=':
                    triggered = metric_value <= threshold
                elif condition == '==':
                    triggered = metric_value == threshold
                elif condition == '!=':
                    triggered = metric_value != threshold
                    
            except (TypeError, ValueError) as e:
                self.logger.error(f"Error evaluating rule '{rule_name}': {e}")
                continue
            
            if triggered:
                message = rule['message_template'].format(value=metric_value)
                alert = Alert(
                    alert_type=rule_name.upper(),
                    message=message,
                    severity=rule['severity'],
                    timestamp=datetime.now().isoformat(),
                    value=metric_value,
                    threshold=threshold,
                    metric_name=metric_name
                )
                alerts.append(alert)
                
                self.logger.warning(f"[{alert.severity}] {alert.message}")
        
        return alerts
    
    def check_anomaly_alerts(self, metrics: Dict[str, Any]) -> List[Alert]:
        """
        Check for anomaly-based alerts.
        
        Args:
            metrics: Dictionary of metrics containing anomaly information
            
        Returns:
            List of anomaly alerts
        """
        alerts = []
        
        # High anomaly rate alert
        if 'anomaly_rate' in metrics:
            anomaly_rate = metrics['anomaly_rate']
            if anomaly_rate > 5.0:  # 5% threshold
                alert = Alert(
                    alert_type='HIGH_ANOMALY_RATE',
                    message=f"Anomaly rate {anomaly_rate:.2f}% exceeds threshold (5.0%)",
                    severity='WARNING',
                    timestamp=datetime.now().isoformat(),
                    value=anomaly_rate,
                    threshold=5.0,
                    metric_name='anomaly_rate'
                )
                alerts.append(alert)
        
        # Batch anomaly count alert
        if 'batch_anomaly_count' in metrics:
            batch_anomalies = metrics['batch_anomaly_count']
            if batch_anomalies > 10:  # More than 10 anomalies in a batch
                alert = Alert(
                    alert_type='HIGH_BATCH_ANOMALY_COUNT',
                    message=f"Batch contains {batch_anomalies} anomalies (threshold: 10)",
                    severity='ERROR',
                    timestamp=datetime.now().isoformat(),
                    value=batch_anomalies,
                    threshold=10,
                    metric_name='batch_anomaly_count'
                )
                alerts.append(alert)
        
        return alerts
    
    def check_extreme_value_alerts(self, metrics: Dict[str, Any], 
                                  extreme_thresholds: Dict[str, float] = None) -> List[Alert]:
        """
        Check for extreme values in metrics.
        
        Args:
            metrics: Dictionary of metrics
            extreme_thresholds: Custom thresholds for extreme value detection
            
        Returns:
            List of extreme value alerts
        """
        alerts = []
        
        if extreme_thresholds is None:
            extreme_thresholds = {
                '_max': 500,    # Default max threshold
                '_min': -100,   # Default min threshold
                '_mean': 1000   # Default mean threshold
            }
        
        for metric_name, value in metrics.items():
            if not isinstance(value, (int, float)):
                continue
                
            # Check against extreme thresholds
            for suffix, threshold in extreme_thresholds.items():
                if suffix in metric_name:
                    if suffix == '_max' and value > threshold:
                        alert = Alert(
                            alert_type='EXTREME_VALUE',
                            message=f"{metric_name} = {value:.2f} exceeds maximum threshold ({threshold})",
                            severity='INFO',
                            timestamp=datetime.now().isoformat(),
                            value=value,
                            threshold=threshold,
                            metric_name=metric_name
                        )
                        alerts.append(alert)
                        
                    elif suffix == '_min' and value < threshold:
                        alert = Alert(
                            alert_type='EXTREME_VALUE',
                            message=f"{metric_name} = {value:.2f} below minimum threshold ({threshold})",
                            severity='INFO',
                            timestamp=datetime.now().isoformat(),
                            value=value,
                            threshold=threshold,
                            metric_name=metric_name
                        )
                        alerts.append(alert)
        
        return alerts
    
    def process_all_alerts(self, metrics: Dict[str, Any]) -> List[Alert]:
        """
        Process all types of alerts for the given metrics.
        
        Args:
            metrics: Dictionary of metrics to check
            
        Returns:
            List of all triggered alerts
        """
        all_alerts = []
        
        # Check rule-based alerts
        rule_alerts = self.check_alert_conditions(metrics)
        all_alerts.extend(rule_alerts)
        
        # Check anomaly alerts
        anomaly_alerts = self.check_anomaly_alerts(metrics)
        all_alerts.extend(anomaly_alerts)
        
        # Check extreme value alerts
        extreme_alerts = self.check_extreme_value_alerts(metrics)
        all_alerts.extend(extreme_alerts)
        
        if all_alerts:
            self.logger.warning(f"Generated {len(all_alerts)} alerts")
            
            # Log alert summary by severity
            severity_counts = {}
            for alert in all_alerts:
                severity_counts[alert.severity] = severity_counts.get(alert.severity, 0) + 1
            
            for severity, count in severity_counts.items():
                self.logger.info(f"  {severity}: {count} alerts")
        
        return all_alerts
    
    def alerts_to_dataframe(self, alerts: List[Alert]) -> pd.DataFrame:
        """
        Convert list of alerts to DataFrame for easier handling.
        
        Args:
            alerts: List of Alert objects
            
        Returns:
            DataFrame with alert data
        """
        if not alerts:
            return pd.DataFrame()
        
        alert_data = []
        for alert in alerts:
            alert_data.append({
                'type': alert.alert_type,
                'message': alert.message,
                'severity': alert.severity,
                'timestamp': alert.timestamp,
                'value': alert.value,
                'threshold': alert.threshold,
                'metric_name': alert.metric_name
            })
        
        return pd.DataFrame(alert_data)
    
    def get_alert_summary(self, alerts: List[Alert]) -> Dict[str, Any]:
        """
        Get summary statistics for alerts.
        
        Args:
            alerts: List of alerts
            
        Returns:
            Dictionary with alert summary
        """
        if not alerts:
            return {'total_alerts': 0}
        
        severity_counts = {}
        type_counts = {}
        
        for alert in alerts:
            # Count by severity
            severity_counts[alert.severity] = severity_counts.get(alert.severity, 0) + 1
            
            # Count by type
            type_counts[alert.alert_type] = type_counts.get(alert.alert_type, 0) + 1
        
        return {
            'total_alerts': len(alerts),
            'severity_breakdown': severity_counts,
            'type_breakdown': type_counts,
            'first_alert_time': alerts[0].timestamp if alerts else None,
            'last_alert_time': alerts[-1].timestamp if alerts else None
        }