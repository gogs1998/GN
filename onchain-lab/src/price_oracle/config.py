from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, PositiveFloat, PositiveInt, ValidationError, field_validator
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

_CONFIG_DEFAULT_PATH = Path("config/price_oracle.yaml")


class ConfigError(RuntimeError):
    """Raised when price oracle configuration is invalid."""


class AlignmentConfig(BaseModel):
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
            raise ConfigError("daily_close_hhmm must be in HH:MM format")
        hour, minute = value.split(":")
        try:
            hour_i = int(hour)
            minute_i = int(minute)
        except ValueError as exc:
            raise ConfigError("daily_close_hhmm must be numeric HH:MM") from exc
        if not (0 <= hour_i < 24 and 0 <= minute_i < 60):
            raise ConfigError("daily_close_hhmm must be valid time")
        return value

    def boundary_time(self) -> time:
        hour, minute = (int(item) for item in self.daily_close_hhmm.split(":"))
        return time(hour=hour, minute=minute)


class QAConfig(BaseModel):
    max_gap_hours: PositiveInt
    max_basis_diff_pct: PositiveFloat


class PriceOracleConfig(BaseModel):
    data_root: Path
    symbols: List[str]
    freqs: List[str]
    primary: str
    fallback: str
    alignment: AlignmentConfig
    qa: QAConfig

    model_config = {"arbitrary_types_allowed": True}

    @field_validator("data_root", mode="before")
    @classmethod
    def _expand_data_root(cls, value: str | Path) -> Path:
        return Path(value).resolve()

    @field_validator("freqs")
    @classmethod
    def _validate_freqs(cls, freqs: List[str]) -> List[str]:
        supported = {"1h", "1d"}
        unknown = set(freqs) - supported
        if unknown:
            raise ConfigError(f"Unsupported frequency values: {', '.join(sorted(unknown))}")
        return freqs


def load_config(path: Optional[Path] = None) -> PriceOracleConfig:
    config_path = path or _CONFIG_DEFAULT_PATH
    if not config_path.exists():
        raise ConfigError(f"Price oracle config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    try:
        return PriceOracleConfig.model_validate(raw)
    except ValidationError as exc:
        raise ConfigError(f"Price oracle configuration invalid: {exc}") from exc
