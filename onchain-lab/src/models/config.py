from __future__ import annotations

from pathlib import Path
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator


class DataConfig(BaseModel):
    metrics_parquet: Path
    out_root: Path
    artifacts_root: Path

    @field_validator("metrics_parquet", "out_root", "artifacts_root", mode="before")
    @classmethod
    def _as_path(cls, value: str | Path) -> Path:
        return Path(value)


class TargetConfig(BaseModel):
    label_horizon_days: int = Field(..., gt=0)
    lookback_days: int = Field(..., gt=0)
    min_history_days: int = Field(..., ge=0)


class FeatureTransformsConfig(BaseModel):
    scale: Literal["standard", "none"] = "standard"
    diffs: List[str] = Field(default_factory=list)
    lags: List[int] = Field(default_factory=list)
    clip_pct: float = Field(0.0, ge=0.0, le=0.5)

    @field_validator("lags", mode="after")
    @classmethod
    def _ensure_positive_lags(cls, value: List[int]) -> List[int]:
        for lag in value:
            if lag <= 0:
                raise ValueError("feature lags must be positive integers")
        return value


class FeaturesConfig(BaseModel):
    include: List[str]
    hodl_pattern: str = ""
    transforms: FeatureTransformsConfig = Field(default_factory=FeatureTransformsConfig)

    @field_validator("include")
    @classmethod
    def _require_features(cls, value: List[str]) -> List[str]:
        if not value:
            raise ValueError("at least one feature must be specified")
        return value


class SplitAnchorsConfig(BaseModel):
    train_end: Optional[str]
    val_end: Optional[str]
    test_start: Optional[str]


class SplitConfig(BaseModel):
    scheme: Literal["forward_chaining"] = "forward_chaining"
    anchors: SplitAnchorsConfig
    n_splits: int = Field(1, ge=1)
    seed: int = 42


class BorutaConfig(BaseModel):
    enabled: bool = True
    max_iter: int = Field(100, gt=0)
    perc: int = Field(85, gt=0, le=100)
    estimator: Literal["xgboost", "random_forest"] = "xgboost"


class LogRegConfig(BaseModel):
    C: float = Field(1.0, gt=0)
    penalty: Literal["l2"] = "l2"
    max_iter: int = Field(500, gt=0)
    class_weight: Optional[Literal["balanced"]] = None


class XGBoostConfig(BaseModel):
    n_estimators: int = Field(500, gt=0)
    learning_rate: float = Field(0.03, gt=0)
    max_depth: int = Field(4, gt=0)
    subsample: float = Field(0.9, gt=0, le=1)
    colsample_bytree: float = Field(0.9, gt=0, le=1)
    reg_lambda: float = Field(1.0, ge=0)
    early_stopping_rounds: int = Field(50, gt=0)


class CNNLSTMConfig(BaseModel):
    epochs: int = Field(50, gt=0)
    batch_size: int = Field(64, gt=0)
    lr: float = Field(0.001, gt=0)
    conv_filters: int = Field(16, gt=0)
    conv_kernel: int = Field(3, gt=1)
    lstm_units: int = Field(32, gt=0)
    dropout: float = Field(0.2, ge=0, lt=1)
    patience: int = Field(6, gt=0)
    framework: Literal["pytorch"] = "pytorch"


class ModelsConfig(BaseModel):
    enabled: List[str]
    logreg: LogRegConfig = Field(default_factory=LogRegConfig)
    xgboost: XGBoostConfig = Field(default_factory=XGBoostConfig)
    cnn_lstm: CNNLSTMConfig = Field(default_factory=CNNLSTMConfig)

    @field_validator("enabled", mode="after")
    @classmethod
    def _validate_enabled(cls, value: List[str]) -> List[str]:
        allowed = {"logreg", "xgboost", "cnn_lstm"}
        seen: set[str] = set()
        for name in value:
            if name not in allowed:
                raise ValueError(f"unknown model '{name}' in models.enabled")
            if name in seen:
                raise ValueError(f"duplicate model '{name}' in models.enabled")
            seen.add(name)
        return value


class DecisionConfig(BaseModel):
    prob_threshold: float = Field(0.55, gt=0, lt=1)
    side: Literal["long_flat", "long_short"] = "long_flat"


class CostsConfig(BaseModel):
    fee_bps: float = Field(5.0, ge=0)
    slippage_bps: float = Field(5.0, ge=0)
    execution_timing: Literal["next_close"] = "next_close"


class SizingConfig(BaseModel):
    mode: Literal["fixed", "kelly_cap"] = "fixed"
    fixed_weight: float = Field(1.0, ge=0, le=1)
    kelly_cap: float = Field(0.25, ge=0, le=1)


class QAConfig(BaseModel):
    forbid_future_lookahead: bool = True
    min_oos_start: str = "2018-01-01"
    tolerance_pct: float = Field(0.5, ge=0)


class RegistryConfig(BaseModel):
    path: Path

    @field_validator("path", mode="before")
    @classmethod
    def _as_path(cls, value: str | Path) -> Path:
        return Path(value)


class ModelConfig(BaseModel):
    data: DataConfig
    target: TargetConfig
    features: FeaturesConfig
    splits: SplitConfig
    boruta: BorutaConfig
    models: ModelsConfig
    decision: DecisionConfig
    costs: CostsConfig
    sizing: SizingConfig
    qa: QAConfig
    registry: RegistryConfig


def load_model_config(path: Path | str) -> ModelConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"model config missing at {config_path}")
    config_dir = config_path.resolve().parent
    content = yaml.safe_load(config_path.read_text())
    if not isinstance(content, dict):
        raise ValueError("model config must be a mapping")
    data_section = content.get("data")
    if isinstance(data_section, dict):
        for key in ("metrics_parquet", "out_root", "artifacts_root"):
            value = data_section.get(key)
            if value is None:
                continue
            candidate = Path(value)
            if not candidate.is_absolute():
                data_section[key] = str((config_dir / candidate).resolve())
    registry_section = content.get("registry")
    if isinstance(registry_section, dict):
        path_value = registry_section.get("path")
        if path_value is not None:
            candidate = Path(path_value)
            if not candidate.is_absolute():
                registry_section["path"] = str((config_dir / candidate).resolve())
    try:
        return ModelConfig.model_validate(content)
    except ValidationError as exc:
        raise ValueError(f"invalid model config: {exc}") from exc


__all__ = [
    "ModelConfig",
    "load_model_config",
]
