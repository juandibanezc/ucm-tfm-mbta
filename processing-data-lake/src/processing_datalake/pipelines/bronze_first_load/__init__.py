"""Full load pipeline to ingest raw data into the bronze layer."""

from .pipeline import create_pipeline

__all__ = ["create_pipeline"]
