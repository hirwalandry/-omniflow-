from __future__ import annotations
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


class MetricsCollector:
    def __init__(self):
        self._counters: Dict[str, int] = defaultdict(int)
        self._gauges: Dict[str, float] = {}
        self._timings: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}

    def increment(self, metric: str, value: int = 1, labels: Optional[Dict[str, str]] = None) -> None:
        self._counters[metric] += value
        if labels:
            self._labels[metric] = labels

    def gauge(self, metric: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        self._gauges[metric] = value
        if labels:
            self._labels[metric] = labels

    def timing(self, metric: str, duration_ms: float, labels: Optional[Dict[str, str]] = None) -> None:
        self._timings[metric].append(duration_ms)
        if labels:
            self._labels[metric] = labels

    def timing_context(self, metric: str, labels: Optional[Dict[str, str]] = None):
        return _TimingContext(self, metric, labels)

    def get_count(self, metric: str) -> int:
        return self._counters.get(metric, 0)

    def get_gauge(self, metric: str) -> Optional[float]:
        return self._gauges.get(metric)

    def get_timing_stats(self, metric: str) -> Optional[Dict[str, float]]:
        values = self._timings.get(metric)
        if not values:
            return None
        return {
            "count": len(values),
            "sum": sum(values),
            "avg": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
        }

    def get_all_metrics(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for metric, value in self._counters.items():
            result[metric] = {"type": "counter", "value": value}
        for metric, value in self._gauges.items():
            result[metric] = {"type": "gauge", "value": value}
        for metric in self._timings:
            stats = self.get_timing_stats(metric)
            if stats:
                result[metric] = {"type": "timing", **stats}
        return result

    def export_json(self) -> str:
        return json.dumps(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "metrics": self.get_all_metrics(),
            },
            indent=2,
        )

    def reset(self) -> None:
        self._counters.clear()
        self._gauges.clear()
        self._timings.clear()
        self._labels.clear()


class _TimingContext:
    def __init__(self, collector: MetricsCollector, metric: str, labels: Optional[Dict[str, str]] = None):
        self.collector = collector
        self.metric = metric
        self.labels = labels
        self.start: Optional[float] = None

    def __enter__(self):
        self.start = time.monotonic()
        return self

    def __exit__(self, *args):
        if self.start is not None:
            elapsed = (time.monotonic() - self.start) * 1000
            self.collector.timing(self.metric, elapsed, self.labels)
