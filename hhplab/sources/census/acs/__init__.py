"""ACS (American Community Survey) public entrypoints.

The root package exposes a stable public surface while lazily importing the
owning submodules to avoid parent/child package cycles in architecture tools.
"""

from __future__ import annotations

from importlib import import_module

_EXPORTS = {
    "aggregate_acs_to_metro": ("hhplab.sources.census.acs.acs_metro", "aggregate_acs_to_metro"),
    "aggregate_to_coc": ("hhplab.sources.census.acs.acs_aggregate", "aggregate_to_coc"),
    "aggregate_to_geo": ("hhplab.sources.census.acs.acs_aggregate", "aggregate_to_geo"),
    "allocate_acs1_county_to_tracts": (
        "hhplab.sources.census.acs.sae",
        "allocate_acs1_county_to_tracts",
    ),
    "ACS1HybridControlSelectionPolicy": (
        "hhplab.sources.census.acs.sae",
        "ACS1HybridControlSelectionPolicy",
    ),
    "build_metro_tract_crosswalk": (
        "hhplab.sources.census.acs.acs_metro",
        "build_metro_tract_crosswalk",
    ),
    "build_hybrid_acs1_poverty_tract_artifact": (
        "hhplab.sources.census.acs.sae",
        "build_hybrid_acs1_poverty_tract_artifact",
    ),
    "build_sae_provenance": ("hhplab.sources.census.acs.sae", "build_sae_provenance"),
    "compare_sae_to_direct_counties": (
        "hhplab.sources.census.acs.sae",
        "compare_sae_to_direct_counties",
    ),
    "diagnose_acs1_imputation": ("hhplab.sources.census.acs.sae", "diagnose_acs1_imputation"),
    "diagnose_hybrid_acs1_poverty_tract_artifact": (
        "hhplab.sources.census.acs.sae",
        "diagnose_hybrid_acs1_poverty_tract_artifact",
    ),
    "derive_sae_burden_measures": ("hhplab.sources.census.acs.sae", "derive_sae_burden_measures"),
    "derive_sae_distribution_measures": (
        "hhplab.sources.census.acs.sae",
        "derive_sae_distribution_measures",
    ),
    "TranslationStats": ("hhplab.sources.census.acs.translate", "TranslationStats"),
    "fetch_state_tract_data": (
        "hhplab.sources.census.acs.ingest.tract_population",
        "fetch_state_tract_data",
    ),
    "fetch_tract_data": ("hhplab.sources.census.acs.ingest.tract_population", "fetch_tract_data"),
    "get_output_path": ("hhplab.sources.census.acs.ingest.tract_population", "get_output_path"),
    "get_source_tract_vintage": ("hhplab.sources.census.acs.translate", "get_source_tract_vintage"),
    "ingest_tract_data": ("hhplab.sources.census.acs.ingest.tract_population", "ingest_tract_data"),
    "needs_translation": ("hhplab.sources.census.acs.translate", "needs_translation"),
    "rollup_sae_tracts_to_geos": ("hhplab.sources.census.acs.sae", "rollup_sae_tracts_to_geos"),
    "select_hybrid_acs1_controls": ("hhplab.sources.census.acs.sae", "select_hybrid_acs1_controls"),
    "write_sae_parquet_with_provenance": (
        "hhplab.sources.census.acs.sae",
        "write_sae_parquet_with_provenance",
    ),
    "translate_acs_to_target_vintage": (
        "hhplab.sources.census.acs.translate",
        "translate_acs_to_target_vintage",
    ),
    "translate_tracts_2010_to_2020": (
        "hhplab.sources.census.acs.translate",
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
