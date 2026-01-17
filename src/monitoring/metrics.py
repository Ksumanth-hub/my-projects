"""
Pipeline metrics collection and monitoring.
Tracks performance, throughput, and health metrics.
"""
import logging
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field, asdict
from threading import Lock
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class MetricPoint:
    """Single metric measurement."""
    name: str
    value: float
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    tags: Dict[str, str] = field(default_factory=dict)
    unit: str = ""


@dataclass
class PipelineMetrics:
    """Container for pipeline metrics."""
    # Throughput metrics
    records_processed: int = 0
    records_per_second: float = 0.0
    bytes_processed: int = 0

    # Latency metrics
    processing_latency_ms: float = 0.0
    end_to_end_latency_ms: float = 0.0

    # Error metrics
    errors_total: int = 0
    error_rate: float = 0.0

    # Resource metrics
    memory_used_mb: float = 0.0
    cpu_percent: float = 0.0

    # Timestamps
    start_time: Optional[str] = None
    last_update: Optional[str] = None


class MetricsCollector:
    """Collects and manages pipeline metrics."""

    def __init__(self, metrics_dir: str = None):
        """
        Initialize metrics collector.

        Args:
            metrics_dir: Directory for storing metrics files
        """
        self.metrics_dir = Path(metrics_dir or os.getenv(
            'METRICS_DIR',
            '/opt/spark-apps/data/output/metrics'
        ))
        self.metrics_dir.mkdir(parents=True, exist_ok=True)

        self._metrics = PipelineMetrics()
        self._metric_points: List[MetricPoint] = []
        self._lock = Lock()
        self._start_time = time.time()

    def record_metric(
        self,
        name: str,
        value: float,
        tags: Dict[str, str] = None,
        unit: str = ""
    ) -> None:
        """
        Record a single metric point.

        Args:
            name: Metric name
            value: Metric value
            tags: Optional tags for the metric
            unit: Unit of measurement
        """
        with self._lock:
            point = MetricPoint(
                name=name,
                value=value,
                tags=tags or {},
                unit=unit
            )
            self._metric_points.append(point)

            # Update running metrics
            self._update_running_metrics(name, value)

        logger.debug(f"Recorded metric: {name}={value} {unit}")

    def _update_running_metrics(self, name: str, value: float) -> None:
        """Update running metrics based on recorded point."""
        self._metrics.last_update = datetime.utcnow().isoformat()

        if name == 'records_processed':
            self._metrics.records_processed = int(value)
            elapsed = time.time() - self._start_time
            self._metrics.records_per_second = value / elapsed if elapsed > 0 else 0

        elif name == 'processing_latency_ms':
            self._metrics.processing_latency_ms = value

        elif name == 'error_count':
            self._metrics.errors_total = int(value)
            if self._metrics.records_processed > 0:
                self._metrics.error_rate = value / self._metrics.records_processed

    def increment(self, name: str, delta: float = 1.0, tags: Dict[str, str] = None) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name
            delta: Amount to increment
            tags: Optional tags
        """
        with self._lock:
            # Find existing value or start at 0
            current = 0
            for point in reversed(self._metric_points):
                if point.name == name:
                    current = point.value
                    break

            self.record_metric(name, current + delta, tags)

    def gauge(self, name: str, value: float, tags: Dict[str, str] = None, unit: str = "") -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name
            value: Current value
            tags: Optional tags
            unit: Unit of measurement
        """
        self.record_metric(name, value, tags, unit)

    def timer(self, name: str):
        """
        Context manager for timing operations.

        Args:
            name: Timer metric name

        Yields:
            Timer context
        """
        return TimerContext(self, name)

    def get_metrics(self) -> PipelineMetrics:
        """Get current metrics snapshot."""
        with self._lock:
            return PipelineMetrics(**asdict(self._metrics))

    def get_metric_points(
        self,
        name: str = None,
        since: datetime = None
    ) -> List[MetricPoint]:
        """
        Get recorded metric points.

        Args:
            name: Filter by metric name
            since: Filter by timestamp

        Returns:
            List of MetricPoint objects
        """
        with self._lock:
            points = self._metric_points.copy()

        if name:
            points = [p for p in points if p.name == name]

        if since:
            since_str = since.isoformat()
            points = [p for p in points if p.timestamp >= since_str]

        return points

    def save_metrics(self, filename: str = None) -> str:
        """
        Save metrics to JSON file.

        Args:
            filename: Optional filename (auto-generated if not provided)

        Returns:
            Path to saved file
        """
        if not filename:
            timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
            filename = f"metrics_{timestamp}.json"

        filepath = self.metrics_dir / filename

        with self._lock:
            data = {
                'summary': asdict(self._metrics),
                'points': [asdict(p) for p in self._metric_points[-1000:]],  # Last 1000 points
                'exported_at': datetime.utcnow().isoformat()
            }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)

        logger.info(f"Metrics saved to {filepath}")
        return str(filepath)

    def log_summary(self) -> None:
        """Log current metrics summary."""
        metrics = self.get_metrics()

        logger.info(
            f"Pipeline Metrics Summary:\n"
            f"  Records Processed: {metrics.records_processed:,}\n"
            f"  Throughput: {metrics.records_per_second:.1f} records/sec\n"
            f"  Processing Latency: {metrics.processing_latency_ms:.1f} ms\n"
            f"  Errors: {metrics.errors_total} ({metrics.error_rate:.2%})\n"
            f"  Last Update: {metrics.last_update}"
        )

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics = PipelineMetrics()
            self._metric_points.clear()
            self._start_time = time.time()

        logger.info("Metrics reset")


class TimerContext:
    """Context manager for timing operations."""

    def __init__(self, collector: MetricsCollector, name: str):
        self.collector = collector
        self.name = name
        self.start_time: Optional[float] = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            elapsed_ms = (time.time() - self.start_time) * 1000
            self.collector.gauge(f"{self.name}_ms", elapsed_ms, unit="ms")
        return False


class PipelineMonitor:
    """High-level pipeline monitoring."""

    def __init__(self, pipeline_name: str = "ecommerce_pipeline"):
        """
        Initialize pipeline monitor.

        Args:
            pipeline_name: Name of the pipeline
        """
        self.pipeline_name = pipeline_name
        self.metrics = MetricsCollector()
        self._batch_start: Optional[float] = None
        self._batch_records: int = 0

    def start_batch(self) -> None:
        """Mark start of a processing batch."""
        self._batch_start = time.time()
        self._batch_records = 0
        self.metrics.gauge('batch_started', 1)

    def record_processed(self, count: int = 1) -> None:
        """Record processed records."""
        self._batch_records += count
        self.metrics.increment('records_processed', count)

    def record_error(self, error_type: str = 'unknown') -> None:
        """Record an error."""
        self.metrics.increment('error_count')
        self.metrics.increment(f'error_{error_type}')

    def end_batch(self) -> Dict[str, Any]:
        """
        Mark end of processing batch.

        Returns:
            Batch metrics dictionary
        """
        if self._batch_start is None:
            return {}

        elapsed = time.time() - self._batch_start
        throughput = self._batch_records / elapsed if elapsed > 0 else 0

        batch_metrics = {
            'records': self._batch_records,
            'duration_seconds': elapsed,
            'throughput_per_second': throughput
        }

        self.metrics.gauge('batch_duration_ms', elapsed * 1000, unit='ms')
        self.metrics.gauge('batch_throughput', throughput, unit='records/sec')

        self._batch_start = None
        self._batch_records = 0

        return batch_metrics

    def record_latency(self, latency_ms: float, latency_type: str = 'processing') -> None:
        """Record latency measurement."""
        self.metrics.gauge(f'{latency_type}_latency_ms', latency_ms, unit='ms')

    def get_health_status(self) -> Dict[str, Any]:
        """
        Get pipeline health status.

        Returns:
            Health status dictionary
        """
        metrics = self.metrics.get_metrics()

        # Determine health status
        status = 'healthy'
        issues = []

        if metrics.error_rate > 0.05:
            status = 'degraded'
            issues.append(f"High error rate: {metrics.error_rate:.2%}")

        if metrics.error_rate > 0.10:
            status = 'unhealthy'

        if metrics.processing_latency_ms > 5000:
            status = 'degraded' if status == 'healthy' else status
            issues.append(f"High latency: {metrics.processing_latency_ms:.0f}ms")

        return {
            'pipeline': self.pipeline_name,
            'status': status,
            'issues': issues,
            'metrics': {
                'records_processed': metrics.records_processed,
                'throughput': metrics.records_per_second,
                'error_rate': metrics.error_rate,
                'latency_ms': metrics.processing_latency_ms
            },
            'timestamp': datetime.utcnow().isoformat()
        }

    def save_report(self) -> str:
        """Save monitoring report."""
        health = self.get_health_status()
        self.metrics.save_metrics()

        report_path = self.metrics.metrics_dir / f"health_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(health, f, indent=2)

        return str(report_path)


# Global monitor instance
_monitor: Optional[PipelineMonitor] = None


def get_monitor(pipeline_name: str = "ecommerce_pipeline") -> PipelineMonitor:
    """Get or create global monitor instance."""
    global _monitor
    if _monitor is None:
        _monitor = PipelineMonitor(pipeline_name)
    return _monitor
