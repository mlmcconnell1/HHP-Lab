"""ACS (American Community Survey) public entrypoints.

The root package exposes a stable public surface while lazily importing the
owning submodules to avoid parent/child package cycles in architecture tools.
"""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "aggregate_acs_to_metro": ("coclab.acs.metro", "aggregate_acs_to_metro"),
    "aggregate_to_coc": ("coclab.acs.aggregate", "aggregate_to_coc"),
    "aggregate_to_geo": ("coclab.acs.aggregate", "aggregate_to_geo"),
    "build_metro_tract_crosswalk": ("coclab.acs.metro", "build_metro_tract_crosswalk"),
    "TranslationStats": ("coclab.acs.translate", "TranslationStats"),
    "fetch_state_tract_data": ("coclab.acs.ingest.tract_population", "fetch_state_tract_data"),
    "fetch_tract_data": ("coclab.acs.ingest.tract_population", "fetch_tract_data"),
    "get_output_path": ("coclab.acs.ingest.tract_population", "get_output_path"),
    "get_source_tract_vintage": ("coclab.acs.translate", "get_source_tract_vintage"),
    "ingest_tract_data": ("coclab.acs.ingest.tract_population", "ingest_tract_data"),
    "needs_translation": ("coclab.acs.translate", "needs_translation"),
    "translate_acs_to_target_vintage": (
        "coclab.acs.translate",
        "translate_acs_to_target_vintage",
    ),
    "translate_tracts_2010_to_2020": (
        "coclab.acs.translate",
        "translate_tracts_2010_to_2020",
    ),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> object:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr_name = _EXPORTS[name]
    value = getattr(import_module(module_name), attr_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS))
