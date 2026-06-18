from __future__ import annotations
import os
import json
from typing import Any, Dict, Optional

from .utils.errors import ConfigurationError


class Settings:
    _instance: Optional[Settings] = None

    def __new__(cls) -> Settings:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load()
        return cls._instance

    def _load(self):
        self.log_level = os.getenv("OMNIFLOW_LOG_LEVEL", "INFO")
        self.max_concurrent_documents = int(os.getenv("OMNIFLOW_MAX_CONCURRENT", "10"))
        self.default_retry_attempts = int(os.getenv("OMNIFLOW_RETRY_ATTEMPTS", "3"))
        self.pipeline_config_path = os.getenv("OMNIFLOW_PIPELINE_CONFIG", "config/pipelines")
        self.data_input_dir = os.getenv("OMNIFLOW_INPUT_DIR", "data/input")
        self.data_output_dir = os.getenv("OMNIFLOW_OUTPUT_DIR", "data/output")
        self.storage_backend = os.getenv("OMNIFLOW_STORAGE", "file")
        self.metrics_enabled = os.getenv("OMNIFLOW_METRICS_ENABLED", "true").lower() == "true"
        self.metrics_flush_interval = int(os.getenv("OMNIFLOW_METRICS_FLUSH_INTERVAL", "10"))
        self.default_encoding = os.getenv("OMNIFLOW_DEFAULT_ENCODING", "utf-8")
        self.max_payload_size_bytes = int(os.getenv("OMNIFLOW_MAX_PAYLOAD_BYTES", str(100 * 1024 * 1024)))
        self.tracing_enabled = os.getenv("OMNIFLOW_TRACING_ENABLED", "false").lower() == "true"

    def get_pipeline_config(self, pipeline_name: str) -> Dict[str, Any]:
        path = os.path.join(self.pipeline_config_path, f"{pipeline_name}.json")
        if not os.path.exists(path):
            raise ConfigurationError(f"Pipeline config not found: {path}")
        with open(path, "r") as f:
            return json.load(f)

    @property
    def as_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


settings = Settings()
