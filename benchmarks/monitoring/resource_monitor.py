"""Resource monitoring for benchmarks"""

import psutil
import time
import threading
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import numpy as np


class ResourceMonitor:
    """Monitor system resources during benchmark execution"""

    def __init__(self, sampling_interval: float = 0.1):
        """Initialize monitor with sampling interval in seconds"""
        self.sampling_interval = sampling_interval
        self.monitoring = False
        self.monitor_thread = None
        self.metrics = {
            "memory": [],
            "cpu": [],
            "io_read": [],
            "io_write": [],
            "timestamps": []
        }
        self.process = psutil.Process()
        self.start_time = None

    def start(self):
        """Start monitoring in background thread"""
        self.monitoring = True
        self.metrics = {
            "memory": [],
            "cpu": [],
            "io_read": [],
            "io_write": [],
            "timestamps": []
        }
        self.start_time = time.time()
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()

    def stop(self) -> Dict:
        """Stop monitoring and return collected metrics"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=1)

        return self.get_summary()

    def _monitor_loop(self):
        """Background monitoring loop"""
        io_counters_start = self.process.io_counters() if hasattr(self.process, 'io_counters') else None

        while self.monitoring:
            try:
                # Memory usage
                mem_info = self.process.memory_info()
                memory_mb = mem_info.rss / (1024 * 1024)
                self.metrics["memory"].append(memory_mb)

                # CPU usage
                cpu_percent = self.process.cpu_percent(interval=None)
                self.metrics["cpu"].append(cpu_percent)

                # I/O counters (if available)
                if hasattr(self.process, 'io_counters'):
                    io_counters = self.process.io_counters()
                    if io_counters_start:
                        read_bytes = (io_counters.read_bytes - io_counters_start.read_bytes) / (1024 * 1024)
                        write_bytes = (io_counters.write_bytes - io_counters_start.write_bytes) / (1024 * 1024)
                        self.metrics["io_read"].append(read_bytes)
                        self.metrics["io_write"].append(write_bytes)

                # Timestamp
                self.metrics["timestamps"].append(time.time() - self.start_time)

                time.sleep(self.sampling_interval)

            except Exception as e:
                print(f"Monitor error: {e}")
                break

    def get_summary(self) -> Dict:
        """Get summary statistics of collected metrics"""
        summary = {}

        # Memory statistics
        if self.metrics["memory"]:
            memory_array = np.array(self.metrics["memory"])
            summary["memory"] = {
                "peak_mb": float(np.max(memory_array)),
                "mean_mb": float(np.mean(memory_array)),
                "min_mb": float(np.min(memory_array)),
                "final_mb": float(memory_array[-1]) if len(memory_array) > 0 else 0
            }

        # CPU statistics
        if self.metrics["cpu"]:
            cpu_array = np.array(self.metrics["cpu"])
            summary["cpu"] = {
                "peak_percent": float(np.max(cpu_array)),
                "mean_percent": float(np.mean(cpu_array)),
                "total_cpu_seconds": float(np.sum(cpu_array) * self.sampling_interval / 100)
            }

        # I/O statistics
        if self.metrics["io_read"]:
            summary["io"] = {
                "total_read_mb": float(np.sum(self.metrics["io_read"])),
                "total_write_mb": float(np.sum(self.metrics["io_write"])),
                "read_throughput_mbps": float(np.mean(self.metrics["io_read"]) / self.sampling_interval),
                "write_throughput_mbps": float(np.mean(self.metrics["io_write"]) / self.sampling_interval)
            }

        # Duration
        if self.metrics["timestamps"]:
            summary["duration_seconds"] = float(self.metrics["timestamps"][-1])

        return summary

    def get_current_memory(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / (1024 * 1024)

    def get_system_info(self) -> Dict:
        """Get system information"""
        return {
            "cpu_count": psutil.cpu_count(),
            "total_memory_gb": psutil.virtual_memory().total / (1024 ** 3),
            "available_memory_gb": psutil.virtual_memory().available / (1024 ** 3),
        }