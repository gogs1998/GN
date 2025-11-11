from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from boruta import BorutaPy
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier

from .config import BorutaConfig, ModelConfig
from .frame import FrameBundle
from .utils import ensure_directory, feature_hash, save_json

BORUTA_FILE = "selected_features.json"


@dataclass
class BorutaResult:
    selected_features: List[str]
    support: Dict[str, bool]
    ranking: Dict[str, int]
    feature_hash: str


class BorutaRunner:
    def __init__(self, config: ModelConfig, frame: FrameBundle):
        self.config = config
        self.frame = frame
        self.output_path = config.data.out_root / BORUTA_FILE

    def run(self, features: Optional[Sequence[str]] = None) -> BorutaResult:
        if not self.config.boruta.enabled:
            raise ValueError("Boruta disabled in config; enable boruta.enabled to run selection")
        feature_list = list(features) if features is not None else list(self.frame.feature_columns)
        if not feature_list:
            raise ValueError("no features available for Boruta selection")
        x_train, y_train = self.frame.features("train", feature_list)
        if x_train.size == 0:
            raise ValueError("training split empty; cannot run Boruta")
        estimator = self._make_estimator(self.config.boruta)
        selector = BorutaPy(
            estimator=estimator,
            n_estimators="auto",
            two_step=True,
            perc=self.config.boruta.perc,
            max_iter=self.config.boruta.max_iter,
            random_state=self.config.splits.seed,
            verbose=0,
        )
        selector.fit(x_train, y_train.astype(int))
        support = selector.support_.tolist()
        ranking = selector.ranking_.tolist()
        selected = [feat for feat, keep in zip(feature_list, support) if keep]
        if not selected:
            # Fallback to top-ranked features if Boruta rejects all
            min_rank = min(ranking)
            selected = [feat for feat, rank in zip(feature_list, ranking) if rank == min_rank]
        support_map = {feat: bool(flag) for feat, flag in zip(feature_list, support)}
        ranking_map = {feat: int(rank) for feat, rank in zip(feature_list, ranking)}
        result = BorutaResult(
            selected_features=selected,
            support=support_map,
            ranking=ranking_map,
            feature_hash=feature_hash(selected),
        )
        self._persist_result(result)
        return result

    def _make_estimator(self, config: BorutaConfig):
        if config.estimator == "xgboost":
            return XGBClassifier(
                objective="binary:logistic",
                eval_metric="logloss",
                n_estimators=200,
                learning_rate=0.05,
                max_depth=5,
                subsample=0.9,
                colsample_bytree=0.9,
                tree_method="hist",
                random_state=self.config.splits.seed,
                n_jobs=-1,
            )
        return RandomForestClassifier(
            n_estimators=500,
            max_depth=5,
            class_weight="balanced_subsample",
            random_state=self.config.splits.seed,
            n_jobs=-1,
        )

    def _persist_result(self, result: BorutaResult) -> None:
        ensure_directory(self.output_path.parent)
        payload = {
            "selected_features": result.selected_features,
            "support": result.support,
            "ranking": result.ranking,
            "feature_hash": result.feature_hash,
            "max_iter": self.config.boruta.max_iter,
            "perc": self.config.boruta.perc,
        }
        save_json(self.output_path, payload)


def load_selected_features(config: ModelConfig) -> Optional[List[str]]:
    path = config.data.out_root / BORUTA_FILE
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    return list(data.get("selected_features", [])) or None


__all__ = ["BorutaRunner", "BorutaResult", "load_selected_features"]
