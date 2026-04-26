"""ZORI (Zillow Observed Rent Index) data ingestion and aggregation.

This package provides tools for:
- Downloading and ingesting ZORI data from Zillow Economic Research
- Building county-level weights from ACS data for aggregation
- Aggregating ZORI from base geography (county) to CoC geography
- Computing coverage and diagnostic metrics

Usage
-----
    from coclab.rents.ingest import ingest_zori
    from coclab.rents.weights import build_county_weights
    from coclab.rents.aggregate import aggregate_zori_to_coc

    # Ingest county-level ZORI data
    zori_path = ingest_zori(geography="county")

    # Build county weights for aggregation
    weights_df = build_county_weights(acs_vintage="2019-2023", method="renter_households")

    # Aggregate ZORI to CoC geography
    coc_zori_path = aggregate_zori_to_coc(
        boundary="2025",
        counties="2023",
        acs_vintage="2019-2023",
        weighting="renter_households",
    )
"""

from coclab.rents.aggregate import (
    aggregate_monthly,
    aggregate_zori_to_coc,
    collapse_to_yearly,
    compute_coc_county_weights,
    compute_geo_county_weights,
    get_coc_zori_path,
    get_coc_zori_yearly_path,
    load_crosswalk,
    load_weights,
    load_zori,
)
from coclab.rents.diagnostics import (
    compute_coc_diagnostics,
    generate_text_summary,
    identify_problem_cocs,
    run_zori_diagnostics,
    summarize_coc_zori,
)
from coclab.rents.ingest import ingest_zori
from coclab.rents.metro import (
    aggregate_yearly_zori_to_metro,
    aggregate_zori_to_metro,
    collapse_zori_to_yearly,
)
from coclab.rents.weights import (
    build_county_weights,
    fetch_county_acs_totals,
    get_county_weights_path,
    load_county_weights,
)

__all__ = [
    # Ingestion
    "ingest_zori",
    # Weights
    "build_county_weights",
    "fetch_county_acs_totals",
    "get_county_weights_path",
    "load_county_weights",
    # Aggregation
    "aggregate_zori_to_coc",
    "aggregate_monthly",
    "collapse_to_yearly",
    "aggregate_zori_to_metro",
    "aggregate_yearly_zori_to_metro",
    "collapse_zori_to_yearly",
    "compute_coc_county_weights",
    "compute_geo_county_weights",
    "load_zori",
    "load_crosswalk",
    "load_weights",
    "get_coc_zori_path",
    "get_coc_zori_yearly_path",
    # Diagnostics
    "summarize_coc_zori",
    "compute_coc_diagnostics",
    "generate_text_summary",
    "identify_problem_cocs",
    "run_zori_diagnostics",
]
