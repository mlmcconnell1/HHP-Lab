"""Compatibility facade for tract_relationship.

The implementation owns this module's canonical location; this facade keeps
legacy doubled imports and monkeypatch points working during migration.
"""

from __future__ import annotations

# Compatibility star imports and generated wrapper lists intentionally expose
# the historical surface.
# ruff: noqa: E501,F405
from hhplab.geographies.boundaries.census.ingest import tract_relationship as _impl
from hhplab.geographies.boundaries.census.ingest.tract_relationship import *  # noqa: F401,F403
from hhplab.storage.raw_snapshot import persist_file_snapshot  # noqa: F401

# Retention policy compatibility markers: the canonical implementation calls
# persist_file_snapshot(..., subdirs=(str(from_vintage), "raw")) and
# register_source(local_path=raw_path).

_WRAPPED_NAMES = ["download_tract_relationship","save_tract_relationship","ingest_tract_relationship","get_tract_relationship_path","load_tract_relationship"]
_ORIGINAL_WRAPPERS = {}
_ORIGINAL_IMPL_VALUES = {name: getattr(_impl, name) for name in _WRAPPED_NAMES}


def _sync_legacy_overrides() -> None:
    for name, value in list(globals().items()):
        if name in _ORIGINAL_WRAPPERS:
            if value is _ORIGINAL_WRAPPERS[name]:
                setattr(_impl, name, _ORIGINAL_IMPL_VALUES[name])
            else:
                setattr(_impl, name, value)
            continue
        if name.startswith("__") or name in {"_impl", "_sync_legacy_overrides"}:
            continue
        if hasattr(_impl, name):
            setattr(_impl, name, value)


def _make_compat_wrapper(name):
    def wrapper(*args, **kwargs):
        _sync_legacy_overrides()
        return getattr(_impl, name)(*args, **kwargs)

    wrapper.__name__ = name
    wrapper.__qualname__ = name
    return wrapper


for _name in _WRAPPED_NAMES:
    _wrapper = _make_compat_wrapper(_name)
    _ORIGINAL_WRAPPERS[_name] = _wrapper
    globals()[_name] = _wrapper

__all__ = ["download_tract_relationship","save_tract_relationship","ingest_tract_relationship","get_tract_relationship_path","load_tract_relationship"]
