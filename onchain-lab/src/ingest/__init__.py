"""ONCHAIN LAB ingest module."""

from .config import IngestConfig, load_config
from .pipeline import sync_from_tip, sync_range

__all__ = [
    "IngestConfig",
    "load_config",
    "sync_from_tip",
    "sync_range",
]
