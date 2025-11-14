from __future__ import annotations

import os
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import (
    BaseModel,
    Field,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    field_validator,
)
from dotenv import load_dotenv

_CONFIG_DEFAULT_PATH = Path("config/ingest.yaml")
_ENV_LOADED = False


class ConfigError(RuntimeError):
    """Raised when configuration cannot be loaded."""


def _ensure_env_loaded(dotenv_path: Optional[Path] = None) -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    load_dotenv(dotenv_path)
    _ENV_LOADED = True


class RPCConfig(BaseModel):
    host: str
    port: PositiveInt
    user_env: str = Field(alias="user_env")
    pass_env: str = Field(alias="pass_env")
    timeout_seconds: PositiveFloat = Field(default=120.0)

    model_config = {"populate_by_name": True}

    def credentials(self) -> tuple[str, str]:
        user = os.environ.get(self.user_env)
        password = os.environ.get(self.pass_env)
        if user is None or password is None:
            raise ConfigError(
                f"Missing RPC credentials. Populate environment variables {self.user_env} and {self.pass_env}."
            )
        return user, password


class LimitsConfig(BaseModel):
    max_blocks_per_run: PositiveInt
    io_batch_size: PositiveInt


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ConfigError(f"Invalid date value '{value}' in golden_days. Expected YYYY-MM-DD.") from exc


GoldenDayList = List[object]


def _validate_golden_days(values: GoldenDayList) -> List[date]:
    normalized: List[date] = []
    for item in values:
        if isinstance(item, date):
            normalized.append(item)
        elif isinstance(item, str):
            normalized.append(_parse_date(item))
        else:
            raise ConfigError(
                "golden_days entries must be dates or YYYY-MM-DD strings."
            )
    return normalized


class QAConfig(BaseModel):
    golden_days: List[date] = Field(default_factory=list)
    tolerance_pct: float

    _convert_days = field_validator("golden_days", mode="before")(_validate_golden_days)

    @field_validator("tolerance_pct")
    @classmethod
    def _validate_tolerance(cls, value: float) -> float:
        if value <= 0:
            raise ConfigError("tolerance_pct must be positive.")
        return value


class IngestConfig(BaseModel):
    data_root: Path
    partitions: Dict[str, str]
    height_bucket_size: PositiveInt
    compression: str
    zstd_level: int
    rpc: RPCConfig
    limits: LimitsConfig
    qa: QAConfig

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("compression")
    @classmethod
    def _validate_compression(cls, value: str) -> str:
        permitted = {"zstd"}
        lowered = value.lower()
        if lowered not in permitted:
            raise ConfigError(f"Unsupported compression '{value}'. Expected one of {sorted(permitted)}.")
        return lowered

    @field_validator("zstd_level")
    @classmethod
    def _validate_zstd_level(cls, value: int) -> int:
        if value < 1 or value > 22:
            raise ConfigError("zstd_level must be within [1, 22].")
        return value

    @field_validator("partitions")
    @classmethod
    def _validate_partitions(cls, mapping: Dict[str, str]) -> Dict[str, str]:
        required = {"blocks", "transactions", "txin", "txout"}
        missing = required.difference(mapping.keys())
        if missing:
            raise ConfigError(f"Missing partition paths for: {', '.join(sorted(missing))}")
        return mapping

    @field_validator("data_root", mode="before")
    @classmethod
    def _expand_data_root(cls, value: str | Path) -> Path:
        return Path(value).resolve()


def load_config(path: Optional[Path] = None, dotenv_path: Optional[Path] = None) -> IngestConfig:
    """Load ingest configuration from YAML and environment variables."""
    _ensure_env_loaded(dotenv_path)
    config_path = path or _CONFIG_DEFAULT_PATH
    if not config_path.exists():
        raise ConfigError(f"Configuration file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        raw = yaml.safe_load(file)

    if raw is None:
        raise ConfigError(f"Configuration file {config_path} is empty.")

    try:
        return IngestConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Configuration invalid: {exc}") from exc
