from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

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

_CONFIG_DEFAULT_PATH = Path("config/metrics.yaml")
_REGISTRY_DEFAULT_PATH = Path("config/metrics_registry.yaml")


class ConfigError(RuntimeError):
    """Raised when metrics configuration is invalid."""


class LifecyclePaths(BaseModel):
    created: Path
    spent: Path
    snapshots_glob: str

    @field_validator("created", "spent", mode="before")
    @classmethod
    def _to_path(cls, value: str | Path) -> Path:
        return Path(value).resolve()


class DataConfig(BaseModel):
    price_glob: str
    lifecycle: LifecyclePaths
    output_root: Path
    symbol: str = Field(min_length=1)
    frequency: str = Field(min_length=1)

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("output_root", mode="before")
    @classmethod
    def _root_path(cls, value: str | Path) -> Path:
        return Path(value).resolve()


class EngineConfig(BaseModel):
    mvrv_window_days: PositiveInt
    dormancy_window_days: PositiveInt
    drawdown_window_days: PositiveInt


class GoldenDay(BaseModel):
    date: date
    metrics: Dict[str, float]
    tolerance_pct: PositiveFloat = Field(default=5.0)


class QAConfig(BaseModel):
    golden_days: List[GoldenDay] = Field(default_factory=list)
    max_drawdown_pct: PositiveFloat
    min_price: NonNegativeFloat
    lookahead_tolerance_days: int = Field(default=0, ge=0)


class WriterConfig(BaseModel):
    compression: str = Field(default="zstd", min_length=1)
    compression_level: PositiveInt = Field(default=7)


class MetricsConfig(BaseModel):
    data: DataConfig
    engine: EngineConfig
    qa: QAConfig
    writer: WriterConfig = Field(default_factory=WriterConfig)


class RegistryMetadata(BaseModel):
    generated_at: datetime
    generator: Optional[str]


class MetricBadge(BaseModel):
    version: str
    status: str
    coverage_pct: float = Field(ge=0.0, le=100.0)
    null_ratio: float = Field(ge=0.0)
    golden_checks_passed: bool
    deflated_sharpe_score: float
    no_lookahead: bool
    reproducible: bool
    utxo_snapshot_commit: str = Field(min_length=1)
    price_root_commit: str = Field(min_length=1)
    formulas_version: str = Field(min_length=1)


class MetricRegistryEntry(BaseModel):
    description: str
    dependencies: List[str]
    badge: MetricBadge
    docs_path: Optional[Path]

    @field_validator("docs_path", mode="before")
    @classmethod
    def _to_path(cls, value: Optional[str | Path]) -> Optional[Path]:
        if value is None:
            return None
        return Path(value)


class MetricsRegistry(BaseModel):
    schema_version: str
    metadata: Optional[RegistryMetadata] = None
    metrics: Dict[str, MetricRegistryEntry]


def load_config(path: Optional[Path] = None) -> MetricsConfig:
    config_path = (path or _CONFIG_DEFAULT_PATH).resolve()
    if not config_path.exists():
        raise ConfigError(f"Metrics config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if raw is None:
        raise ConfigError(f"Metrics configuration file {config_path} is empty")
    base_dir = config_path.parent.resolve()

    def _as_path(value: str | Path) -> str:
        path_obj = Path(value)
        if not path_obj.is_absolute():
            path_obj = base_dir / path_obj
        return str(path_obj.resolve())

    def _as_pattern(value: str) -> str:
        pattern_path = Path(value)
        if not pattern_path.is_absolute():
            pattern_path = base_dir / pattern_path
        return str(pattern_path)

    data_section = raw.get("data", {})
    if "price_glob" in data_section:
        data_section["price_glob"] = _as_pattern(data_section["price_glob"])
    if "output_root" in data_section:
        data_section["output_root"] = _as_path(data_section["output_root"])
    lifecycle_section = data_section.get("lifecycle", {})
    if "created" in lifecycle_section:
        lifecycle_section["created"] = _as_path(lifecycle_section["created"])
    if "spent" in lifecycle_section:
        lifecycle_section["spent"] = _as_path(lifecycle_section["spent"])
    if "snapshots_glob" in lifecycle_section:
        lifecycle_section["snapshots_glob"] = _as_pattern(lifecycle_section["snapshots_glob"])
    raw["data"] = data_section

    try:
        return MetricsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Metrics configuration invalid: {exc}") from exc


def load_registry(path: Optional[Path] = None) -> MetricsRegistry:
    registry_path = (path or _REGISTRY_DEFAULT_PATH).resolve()
    if not registry_path.exists():
        raise ConfigError(f"Metrics registry not found: {registry_path}")
    with registry_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if raw is None:
        raise ConfigError(f"Metrics registry file {registry_path} is empty")
    base_dir = registry_path.parent.resolve()
    metrics_section = raw.get("metrics", {})
    if not isinstance(metrics_section, dict):
        raise ConfigError("Metrics registry invalid: 'metrics' must be a mapping")
    processed_metrics: Dict[str, Dict[str, object]] = {}
    for name, entry in metrics_section.items():
        if not isinstance(entry, dict):
            raise ConfigError(f"Metrics registry invalid: entry for '{name}' must be a mapping")
        entry_copy = dict(entry)
        docs_path = entry_copy.get("docs_path")
        if docs_path:
            resolved = Path(docs_path)
            if not resolved.is_absolute():
                entry_copy["docs_path"] = str((base_dir / resolved).resolve())
        processed_metrics[name] = entry_copy
    raw["metrics"] = processed_metrics
    try:
        return MetricsRegistry.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Metrics registry invalid: {exc}") from exc


__all__ = [
    "ConfigError",
    "LifecyclePaths",
    "DataConfig",
    "EngineConfig",
    "QAConfig",
    "MetricsConfig",
    "MetricsRegistry",
    "MetricRegistryEntry",
    "MetricBadge",
    "RegistryMetadata",
    "GoldenDay",
    "WriterConfig",
    "load_config",
    "load_registry",
]
