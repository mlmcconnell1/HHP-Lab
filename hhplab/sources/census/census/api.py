"""Compatibility facade for api.

The implementation owns this module's canonical location; this facade keeps
legacy doubled imports and monkeypatch points working during migration.
"""

from __future__ import annotations

# Compatibility star imports and generated wrapper lists intentionally expose
# the historical surface.
# ruff: noqa: E501,F405
from hhplab.sources.census import api as _impl
from hhplab.sources.census.api import *  # noqa: F401,F403

_WRAPPED_NAMES = ["get_census_api_key","census_api_credentials_status","is_census_missing_key_response","raise_for_census_api_status","probe_census_api_reachability"]
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

__all__ = ["get_census_api_key","census_api_credentials_status","is_census_missing_key_response","raise_for_census_api_status","probe_census_api_reachability"]
