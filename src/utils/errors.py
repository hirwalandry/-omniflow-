from typing import Optional


class OmniFlowError(Exception):
    code = "OMNIFLOW_ERROR"

    def __init__(self, message: str, context: Optional[dict] = None):
        self.message = message
        self.context = context or {}
        super().__init__(self.message)


class IngestionError(OmniFlowError):
    code = "INGESTION_ERROR"


class ProcessingError(OmniFlowError):
    code = "PROCESSING_ERROR"


class ValidationError(ProcessingError):
    code = "VALIDATION_ERROR"


class TransformationError(ProcessingError):
    code = "TRANSFORMATION_ERROR"


class RoutingError(ProcessingError):
    code = "ROUTING_ERROR"


class EnrichmentError(ProcessingError):
    code = "ENRICHMENT_ERROR"


class StorageError(OmniFlowError):
    code = "STORAGE_ERROR"


class ConfigurationError(OmniFlowError):
    code = "CONFIGURATION_ERROR"


class RetryExhaustedError(OmniFlowError):
    code = "RETRY_EXHAUSTED"
