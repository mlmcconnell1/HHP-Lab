"""Compatibility facade for panel analysis APIs.

Implementations live in focused modules. Keep imports here stable while callers
migrate away from the historical monolithic module.
"""

# Re-exported private helpers remain available for established downstream tests
# and callers while implementation ownership moves to focused modules.
# ruff: noqa: F401

from __future__ import annotations

from .contracts import AnalysisError, AnalysisResult, InferenceMethod, _json_safe, _RegressionFit
from .correlation import correlate_panel
from .estimation import (
    _first_stage_f_statistic,
    _fit_2sls,
    _fit_ols,
    _parse_inference_terms,
    _restricted_iv_fitted_and_residuals,
    _restricted_ols_fitted_and_residuals,
)
from .inspection import describe_panel
from .iv import anderson_rubin_confidence_set
from .lagged import lagged_associations_panel
from .model import (
    _fixed_effect_dummies,
    _is_binary_indicator,
    _model_frame_for_regression,
    _regression_design,
    _standardize_model_columns,
)
from .persistence import (
    _analysis_provenance,
    _persist_result,
    _read_panel,
    _require_columns,
    _sha256,
    _write_analysis_manifest,
    list_analysis_manifests,
    read_analysis_manifest,
)
from .regression import regress_panel
from .resampling import (
    _permutation_p_values,
    _warn_if_permutation_model_has_correlated_controls,
    _wild_cluster_bootstrap_p_values,
)
from .stats import (
    _cluster_denominator_dof,
    _clustered_covariance,
    _clustered_standard_errors,
    _require_clustered_inference_clusters,
    _two_sided_p_value,
)

__all__ = [
    "AnalysisError",
    "AnalysisResult",
    "InferenceMethod",
    "anderson_rubin_confidence_set",
    "correlate_panel",
    "describe_panel",
    "lagged_associations_panel",
    "list_analysis_manifests",
    "read_analysis_manifest",
    "regress_panel",
]
