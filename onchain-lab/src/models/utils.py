from __future__ import annotations

import json
import logging
import random
from dataclasses import asdict, is_dataclass
from fnmatch import fnmatch
from hashlib import sha256
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import numpy as np

logger = logging.getLogger(__name__)


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, payload: Any) -> None:
    ensure_directory(path.parent)
    if is_dataclass(payload):
        payload = asdict(payload)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"missing json artifact at {path}")
    return json.loads(path.read_text())


def set_random_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    try:
        import torch

        torch.manual_seed(seed)
        if torch.cuda.is_available():  # pragma: no cover - gpu optional
            torch.cuda.manual_seed_all(seed)
    except ModuleNotFoundError:
        logger.debug("torch unavailable; skipping torch seeding")


def resolve_feature_list(columns: Iterable[str], include: Sequence[str], pattern: str) -> List[str]:
    base = list(dict.fromkeys(include))
    if pattern:
        dynamic = [name for name in columns if fnmatch(name, pattern)]
        for name in dynamic:
            if name not in base:
                base.append(name)
    return base


def feature_hash(features: Sequence[str]) -> str:
    joined = ",".join(sorted(features))
    return sha256(joined.encode("utf-8")).hexdigest()[:16]


def save_numpy(path: Path, array: np.ndarray) -> None:
    ensure_directory(path.parent)
    np.save(path, array)


def load_numpy(path: Path) -> np.ndarray:
    if not path.exists():
        raise FileNotFoundError(f"missing numpy artifact at {path}")
    return np.load(path)


def compute_trade_signal(probabilities: Sequence[float], *, threshold: float, side: str) -> np.ndarray:
    values = np.asarray(probabilities, dtype=float)
    if side == "long_flat":
        return (values >= threshold).astype(float)
    if side == "long_short":
        result = np.zeros_like(values, dtype=float)
        result[values >= threshold] = 1.0
        result[values <= (1.0 - threshold)] = -1.0
        return result
    raise ValueError(f"unsupported decision side '{side}'")


def append_registry_entry(path: Path, entry: Dict[str, Any]) -> None:
    ensure_directory(path.parent)
    if path.exists():
        registry = json.loads(path.read_text())
        if not isinstance(registry, list):
            raise ValueError("registry file must contain a JSON list")
    else:
        registry = []
    registry.append(entry)
    path.write_text(json.dumps(registry, indent=2, sort_keys=True))


__all__ = [
    "append_registry_entry",
    "ensure_directory",
    "feature_hash",
    "load_json",
    "load_numpy",
    "resolve_feature_list",
    "save_json",
    "save_numpy",
    "set_random_seed",
]
