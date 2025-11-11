"""Legacy compatibility shim for Stage 5 baselines.

This file is intentionally thin because an external generator still rewrites
`baselines.py`. The actual implementations live in `baselines_core.py`, and we
re-export everything here to avoid breakage if older imports linger.
"""

from importlib import import_module

_baselines_core = import_module(".baselines_core", package=__package__)

__all__ = list(getattr(_baselines_core, "__all__", []))
globals().update({name: getattr(_baselines_core, name) for name in __all__})
