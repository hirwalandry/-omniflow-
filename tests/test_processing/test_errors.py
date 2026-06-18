from __future__ import annotations
import pytest

from src.utils.errors import (
    OmniFlowError,
    ConfigurationError,
    IngestionError,
    ProcessingError,
    ValidationError,
    TransformationError,
    EnrichmentError,
    RoutingError,
    StorageError,
    RetryExhaustedError,
)


class TestErrorHierarchy:
    def test_base_error(self):
        e = OmniFlowError("test error", {"key": "val"})
        assert str(e) == "test error"
        assert e.code == "OMNIFLOW_ERROR"
        assert e.context == {"key": "val"}

    def test_configuration_error(self):
        e = ConfigurationError("config missing")
        assert e.code == "CONFIGURATION_ERROR"
        assert isinstance(e, OmniFlowError)

    def test_ingestion_error(self):
        e = IngestionError("cannot read source")
        assert e.code == "INGESTION_ERROR"

    def test_processing_error(self):
        e = ProcessingError("processing failed")
        assert e.code == "PROCESSING_ERROR"

    def test_validation_error(self):
        e = ValidationError("invalid field")
        assert e.code == "VALIDATION_ERROR"
        assert isinstance(e, ProcessingError)

    def test_transformation_error(self):
        e = TransformationError("bad type")
        assert e.code == "TRANSFORMATION_ERROR"
        assert isinstance(e, ProcessingError)

    def test_enrichment_error(self):
        e = EnrichmentError("enrichment failed")
        assert e.code == "ENRICHMENT_ERROR"
        assert isinstance(e, ProcessingError)

    def test_routing_error(self):
        e = RoutingError("no route found")
        assert e.code == "ROUTING_ERROR"
        assert isinstance(e, ProcessingError)

    def test_storage_error(self):
        e = StorageError("disk full")
        assert e.code == "STORAGE_ERROR"

    def test_retry_exhausted_error(self):
        e = RetryExhaustedError("all attempts failed")
        assert e.code == "RETRY_EXHAUSTED"

    def test_default_context(self):
        e = OmniFlowError("simple")
        assert e.context == {}
