"""
Streaming alert loading and notification logic.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from airpipe.utils.loaders.file_utils import FileUtils

logger = logging.getLogger(__name__)


class StreamingAlertLoader:
    """Load and persist streaming alerts and notifications."""
    
    def __init__(self, output_dir: str = "output/streaming/alerts"):
        self.logger = logger
        self.file_utils = FileUtils()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.alert_log_file = self.output_dir / "alerts.jsonl"
        
    def save_alerts(self, alerts: List[Dict[str, Any]]) -> Optional[str]:
        """
        Save alerts to persistent storage.
        
        Args:
            alerts: List of alert dictionaries
            
        Returns:
            Path to saved alerts file, None if no alerts
        """
        if not alerts:
            self.logger.info("No alerts to save")
            return None
        
        # Create batch file for this set of alerts
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        batch_filename = f"alert_batch_{timestamp}.json"
        batch_filepath = self.output_dir / batch_filename
        
        alert_batch = {
            'timestamp': datetime.now().isoformat(),
            'alert_count': len(alerts),
            'alerts': alerts
        }
        
        with open(batch_filepath, 'w') as f:
            json.dump(alert_batch, f, indent=2, default=str)
        
        # Also append to continuous log
        self._append_to_alert_log(alerts)
        
        # Log alert details
        severity_counts = {}
        for alert in alerts:
            severity = alert.get('severity', 'UNKNOWN')
            severity_counts[severity] = severity_counts.get(severity, 0) + 1
            
            # Log individual severe alerts
            if severity in ['ERROR', 'CRITICAL']:
                self.logger.error(f"[{severity}] {alert.get('message', 'No message')}")
            elif severity == 'WARNING':
                self.logger.warning(f"[{severity}] {alert.get('message', 'No message')}")
        
        self.logger.warning(f"Saved {len(alerts)} alerts to {batch_filepath}")
        
        # Log severity breakdown
        for severity, count in severity_counts.items():
            self.logger.info(f"  {severity}: {count} alerts")
        
        return str(batch_filepath)
    
    def _append_to_alert_log(self, alerts: List[Dict[str, Any]]) -> None:
        """
        Append alerts to continuous log file.
        
        Args:
            alerts: List of alert dictionaries
        """
        with open(self.alert_log_file, 'a') as f:
            for alert in alerts:
                # Add processing timestamp if not present
                if 'logged_at' not in alert:
                    alert['logged_at'] = datetime.now().isoformat()
                json.dump(alert, f, default=str)
                f.write('\n')
    
    def save_alert_summary(self, 
                          alert_summary: Dict[str, Any],
                          filename: str = None) -> str:
        """
        Save alert summary statistics.
        
        Args:
            alert_summary: Dictionary with alert summary
            filename: Optional filename (auto-generated if None)
            
        Returns:
            Path to saved summary file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"alert_summary_{timestamp}.json"
        
        filepath = self.output_dir / filename
        
        # Add metadata
        enhanced_summary = {
            **alert_summary,
            'generated_at': datetime.now().isoformat(),
            'summary_type': 'streaming_alerts'
        }
        
        with open(filepath, 'w') as f:
            json.dump(enhanced_summary, f, indent=2, default=str)
        
        self.logger.info(f"Saved alert summary to {filepath}")
        return str(filepath)
    
    def send_notification(self, 
                         alert: Dict[str, Any], 
                         notification_type: str = "log") -> bool:
        """
        Send notification for critical alerts.
        
        Args:
            alert: Alert dictionary
            notification_type: Type of notification ('log', 'email', 'slack')
            
        Returns:
            True if notification sent successfully
        """
        try:
            severity = alert.get('severity', 'INFO')
            message = alert.get('message', 'No message')
            alert_type = alert.get('type', 'UNKNOWN')
            
            if notification_type == "log":
                # Log-based notification (always available)
                log_message = f"ALERT NOTIFICATION [{alert_type}] {message}"
                
                if severity in ['CRITICAL', 'ERROR']:
                    self.logger.error(log_message)
                elif severity == 'WARNING':
                    self.logger.warning(log_message)
                else:
                    self.logger.info(log_message)
                    
                return True
            
            elif notification_type == "email":
                # Placeholder for email notification
                # In production, integrate with email service
                self.logger.info(f"EMAIL NOTIFICATION: Would send alert '{alert_type}' to configured recipients")
                return True
            
            elif notification_type == "slack":
                # Placeholder for Slack notification
                # In production, integrate with Slack API
                self.logger.info(f"SLACK NOTIFICATION: Would post alert '{alert_type}' to configured channel")
                return True
            
            else:
                self.logger.error(f"Unknown notification type: {notification_type}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to send notification: {e}")
            return False
    
    def process_alert_notifications(self, 
                                   alerts: List[Dict[str, Any]],
                                   notification_config: Dict[str, Any] = None) -> Dict[str, int]:
        """
        Process notifications for a batch of alerts.
        
        Args:
            alerts: List of alert dictionaries
            notification_config: Configuration for notifications
            
        Returns:
            Dictionary with notification statistics
        """
        if not alerts:
            return {'total_alerts': 0, 'notifications_sent': 0}
        
        # Default notification config
        if notification_config is None:
            notification_config = {
                'methods': ['log'],
                'severity_thresholds': {
                    'INFO': [],
                    'WARNING': ['log'],
                    'ERROR': ['log'],
                    'CRITICAL': ['log']
                }
            }
        
        stats = {
            'total_alerts': len(alerts),
            'notifications_sent': 0,
            'notification_failures': 0,
            'by_severity': {}
        }
        
        for alert in alerts:
            severity = alert.get('severity', 'INFO')
            stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
            
            # Get notification methods for this severity
            methods = notification_config.get('severity_thresholds', {}).get(severity, [])
            
            for method in methods:
                success = self.send_notification(alert, method)
                if success:
                    stats['notifications_sent'] += 1
                else:
                    stats['notification_failures'] += 1
        
        self.logger.info(f"Processed notifications for {stats['total_alerts']} alerts: "
                        f"{stats['notifications_sent']} sent, {stats['notification_failures']} failed")
        
        return stats
    
    def export_alert_history(self, 
                            hours: int = 24,
                            format: str = 'json',
                            filename: str = None) -> str:
        """
        Export alert history for specified time period.
        
        Args:
            hours: Number of hours of history to export
            format: Export format ('json', 'csv')
            filename: Optional filename (auto-generated if None)
            
        Returns:
            Path to exported file
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"alert_history_{hours}h_{timestamp}"
        
        # Read alerts from log file
        alerts = []
        if self.alert_log_file.exists():
            with open(self.alert_log_file, 'r') as f:
                for line in f:
                    try:
                        alert = json.loads(line.strip())
                        alerts.append(alert)
                    except json.JSONDecodeError:
                        continue
        
        # Filter by time period if needed
        # (For demo, we'll export all alerts)
        
        if format == 'json':
            filepath = self.output_dir / f"{filename}.json"
            with open(filepath, 'w') as f:
                json.dump({
                    'export_time': datetime.now().isoformat(),
                    'time_period_hours': hours,
                    'total_alerts': len(alerts),
                    'alerts': alerts
                }, f, indent=2, default=str)
        
        elif format == 'csv':
            import pandas as pd
            filepath = self.output_dir / f"{filename}.csv"
            
            if alerts:
                df = pd.DataFrame(alerts)
                self.file_utils.save_to_csv(df, str(filepath))
            else:
                # Create empty CSV with headers
                df = pd.DataFrame(columns=['type', 'message', 'severity', 'timestamp'])
                self.file_utils.save_to_csv(df, str(filepath))
        
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        self.logger.info(f"Exported {len(alerts)} alerts to {filepath} ({format} format)")
        return str(filepath)
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about saved alerts.
        
        Returns:
            Dictionary with alert statistics
        """
        stats = {
            'total_alerts': 0,
            'by_severity': {},
            'by_type': {},
            'alert_files_count': 0,
            'latest_alert_time': None
        }
        
        # Count alert batch files
        alert_files = list(self.output_dir.glob("alert_batch_*.json"))
        stats['alert_files_count'] = len(alert_files)
        
        # Read from continuous log if available
        if self.alert_log_file.exists():
            with open(self.alert_log_file, 'r') as f:
                for line in f:
                    try:
                        alert = json.loads(line.strip())
                        stats['total_alerts'] += 1
                        
                        # Count by severity
                        severity = alert.get('severity', 'UNKNOWN')
                        stats['by_severity'][severity] = stats['by_severity'].get(severity, 0) + 1
                        
                        # Count by type
                        alert_type = alert.get('type', 'UNKNOWN')
                        stats['by_type'][alert_type] = stats['by_type'].get(alert_type, 0) + 1
                        
                        # Track latest timestamp
                        timestamp = alert.get('timestamp')
                        if timestamp and (stats['latest_alert_time'] is None or timestamp > stats['latest_alert_time']):
                            stats['latest_alert_time'] = timestamp
                            
                    except json.JSONDecodeError:
                        continue
        
        return stats