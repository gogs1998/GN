from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import Optional

import yaml
from pydantic import (
    BaseModel,
    Field,
    NonNegativeFloat,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    field_validator,
)
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_CONFIG_DEFAULT_PATH = Path("config/utxo.yaml")


class ConfigError(RuntimeError):
    """Raised when lifecycle configuration is invalid."""


class IngestPaths(BaseModel):
    blocks: str
    transactions: str
    txin: str
    txout: str


class PriceConfig(BaseModel):
    parquet: str
    symbol: str
    freq: str


class EntitiesConfig(BaseModel):
    lookup: Optional[Path] = Field(default=None)

    @field_validator("lookup", mode="before")
    @classmethod
    def _validate_lookup(cls, value: Optional[str | Path]) -> Optional[Path]:
        if value in (None, "", "null"):
            return None
        return Path(value).resolve()


class DataConfig(BaseModel):
    ingest: IngestPaths
    price: PriceConfig
    entities: Optional[EntitiesConfig] = None
    lifecycle_root: Path

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("lifecycle_root", mode="before")
    @classmethod
    def _validate_root(cls, value: str | Path) -> Path:
        return Path(value).resolve()


class SnapshotConfig(BaseModel):
    timezone: str
    daily_close_hhmm: str

    @field_validator("timezone")
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ConfigError(f"Unknown timezone '{value}'") from exc
        return value

    @field_validator("daily_close_hhmm")
    @classmethod
    def _validate_hhmm(cls, value: str) -> str:
        if len(value) != 5 or value[2] != ":":
            raise ConfigError("daily_close_hhmm must be formatted as HH:MM")
        hour, minute = value.split(":")
        try:
            hour_i = int(hour)
            minute_i = int(minute)
        except ValueError as exc:
            raise ConfigError("daily_close_hhmm must contain numeric values") from exc
        if not (0 <= hour_i < 24 and 0 <= minute_i < 60):
            raise ConfigError("daily_close_hhmm must be a valid time")
        return value

    def close_time(self) -> time:
        hour, minute = (int(item) for item in self.daily_close_hhmm.split(":"))
        return time(hour=hour, minute=minute)

    def zoneinfo(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)


class WriterConfig(BaseModel):
    compression: str = Field(default="zstd")
    zstd_level: PositiveInt = Field(default=9)

    @field_validator("compression")
    @classmethod
    def _validate_compression(cls, value: str) -> str:
        supported = {"zstd", "snappy"}
        if value not in supported:
            raise ConfigError(f"Unsupported compression '{value}' (choose from {sorted(supported)})")
        return value


class QAConfig(BaseModel):
    price_coverage_min_pct: PositiveFloat
    supply_tolerance_sats: PositiveInt
    lifespan_max_days: PositiveInt
    max_snapshot_gap_pct: NonNegativeFloat

    @field_validator("price_coverage_min_pct")
    @classmethod
    def _validate_pct(cls, value: float) -> float:
        if value > 100.0:
            raise ConfigError("price_coverage_min_pct cannot exceed 100")
        return value


class LifecycleConfig(BaseModel):
    data: DataConfig
    snapshot: SnapshotConfig
    writer: WriterConfig
    qa: QAConfig


def load_config(path: Optional[Path] = None) -> LifecycleConfig:
    config_path = path or _CONFIG_DEFAULT_PATH
    if not config_path.exists():
        raise ConfigError(f"Lifecycle config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    try:
        return LifecycleConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Lifecycle configuration invalid: {exc}") from exc


__all__ = ["ConfigError", "LifecycleConfig", "load_config"]
