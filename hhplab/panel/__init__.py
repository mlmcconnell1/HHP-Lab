"""Panel assembly module for HHP-Lab Phase 3.

This module provides tools for constructing CoC x year panels by aligning
PIT years with boundary vintages and ACS vintages according to explicit
policies, plus diagnostics for validating panel integrity.

ZORI Integration
----------------
When building panels with `include_zori=True`, the following columns are added:
- zori_coc: CoC-level ZORI (yearly)
- zori_coverage_ratio: Coverage of base geography weights
- zori_is_eligible: Boolean eligibility flag
- zori_excluded_reason: Reason for exclusion if ineligible
- rent_to_income: ZORI / (median_household_income / 12)
- rent_metric, rent_alignment, zori_min_coverage: Provenance fields
"""

from hhplab.panel.assemble import (
    METRO_PANEL_COLUMNS,
    PANEL_COLUMNS,
    ZORI_COLUMNS,
    ZORI_PROVENANCE_COLUMNS,
    build_panel,
    save_panel,
)
from hhplab.panel.conformance import (
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    ConformanceReport,
    ConformanceResult,
    PanelRequest,
    register_check,
    run_conformance,
)
from hhplab.panel.finalize import (
    RECIPE_COLUMN_ALIASES,
    detect_boundary_changes,
    determine_alignment_type,
    finalize_panel,
)
from hhplab.panel.panel_diagnostics import (
    DiagnosticsReport,
    boundary_change_summary,
    coverage_summary,
    generate_diagnostics_report,
    missingness_report,
    weighting_sensitivity,
)
from hhplab.panel.policies import (
    DEFAULT_POLICY,
    AlignmentPolicy,
    default_acs_vintage,
    default_boundary_vintage,
)
from hhplab.panel.zori_eligibility import (
    DEFAULT_ZORI_MIN_COVERAGE,
    EXCLUDED_LOW_COVERAGE,
    EXCLUDED_MISSING,
    EXCLUDED_ZERO_COVERAGE,
    ZoriProvenance,
    add_provenance_columns,
    apply_zori_eligibility,
    compute_rent_to_income,
    determine_exclusion_reason,
    get_zori_panel_columns,
    summarize_zori_eligibility,
)

__all__ = [
    # Policies
    "AlignmentPolicy",
    "DEFAULT_POLICY",
    "default_acs_vintage",
    "default_boundary_vintage",
    # Panel building
    "build_panel",
    "save_panel",
    "PANEL_COLUMNS",
    "METRO_PANEL_COLUMNS",
    "ZORI_COLUMNS",
    "ZORI_PROVENANCE_COLUMNS",
    # Finalization
    "RECIPE_COLUMN_ALIASES",
    "detect_boundary_changes",
    "determine_alignment_type",
    "finalize_panel",
    # Diagnostics
    "DiagnosticsReport",
    "boundary_change_summary",
    "coverage_summary",
    "generate_diagnostics_report",
    "missingness_report",
    "weighting_sensitivity",
    # Conformance
    "ACS_MEASURE_COLUMNS",
    "ACS1_MEASURE_COLUMNS",
    "ConformanceReport",
    "ConformanceResult",
    "PanelRequest",
    "register_check",
    "run_conformance",
    # ZORI eligibility
    "DEFAULT_ZORI_MIN_COVERAGE",
    "EXCLUDED_LOW_COVERAGE",
    "EXCLUDED_MISSING",
    "EXCLUDED_ZERO_COVERAGE",
    "ZoriProvenance",
    "add_provenance_columns",
    "apply_zori_eligibility",
    "compute_rent_to_income",
    "determine_exclusion_reason",
    "get_zori_panel_columns",
    "summarize_zori_eligibility",
]
