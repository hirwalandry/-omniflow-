from __future__ import annotations
import pytest

from src.models.metrics import MetricType, MetricSample, MetricBucket, PipelineRunMetrics


class TestMetricModels:
    def test_metric_sample_defaults(self):
        s = MetricSample(name="test", metric_type=MetricType.COUNTER, value=1)
        assert s.name == "test"
        assert s.tags == {}
        assert s.host is None

    def test_metric_bucket_defaults(self):
        b = MetricBucket(name="test", metric_type=MetricType.TIMING)
        assert b.count == 0
        assert b.sum == 0.0
        assert b.min is None

    def test_pipeline_run_metrics_defaults(self):
        m = PipelineRunMetrics(pipeline_name="p1")
        assert m.documents_processed == 0
        assert m.stage_metrics == {}
        assert m.errors == []
