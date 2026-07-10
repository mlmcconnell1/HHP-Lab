"""Tests for the tracked core rent-shock state-year FE workflow."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.results.workflows import analyze_core_rent_shock_state_year_fe as analysis

MSA_FIXTURES = (
    ("10000", "Alpha, AA"),
    ("11000", "Beta, AA"),
    ("20000", "Gamma, BB"),
    ("21000", "Delta, BB"),
    ("30000", "Epsilon, CC"),
    ("31000", "Zeta, CC"),
    ("40000", "Eta, DD"),
    ("41000", "Theta, DD"),
)
YEARS = (2016, 2017, 2018, 2020)


def _cohort_frame(msas: tuple[tuple[str, str], ...]) -> pd.DataFrame:
    rows = []
    for msa_index, (msa_id, msa_name) in enumerate(msas):
        for year_index, year in enumerate(YEARS):
            rent_change = 0.01 * (msa_index + year_index + 1)
            rows.append(
                {
                    "msa_id": msa_id,
                    "msa_name": msa_name,
                    "year": year,
                    "year_gap": 1,
                    "population": 1_000_000 - 10_000 * msa_index,
                    "d_log_zori": rent_change,
                    "d_log_unshelt_rate": 1.8 * rent_change + 0.002 * msa_index,
                }
            )
    return pd.DataFrame(rows)


def test_build_pooled_fd_panel_normalizes_schema_and_labels_cohorts() -> None:
    top150 = _cohort_frame(MSA_FIXTURES)
    original_size = analysis.TOP50_SIZE
    analysis.TOP50_SIZE = 4
    try:
        result = analysis.build_pooled_fd_panel(top150)
    finally:
        analysis.TOP50_SIZE = original_size

    assert "d_log_unsheltered_rate" in result.columns
    assert set(result["cohort"]) == {"top50", "rank51_150"}
    assert set(result["primary_state"]) == {"AA", "BB", "CC", "DD"}


@pytest.mark.parametrize("spec", analysis.REGRESSION_SPECS, ids=lambda spec: spec.model)
def test_fit_spec_reports_clustered_focal_estimate(spec: analysis.RegressionSpec) -> None:
    original_size = analysis.TOP50_SIZE
    analysis.TOP50_SIZE = 4
    try:
        frame = analysis.build_pooled_fd_panel(_cohort_frame(MSA_FIXTURES))
    finally:
        analysis.TOP50_SIZE = original_size

    result = analysis.fit_spec(frame, spec)

    assert result["model"] == spec.model
    assert result["term"] == "d_log_zori"
    assert result["nobs"] == len(frame)
    assert result["clusters"] == len(MSA_FIXTURES)
    assert result["std_error_type"] == "clustered:msa_id"


def test_build_pooled_fd_panel_requires_reference_population() -> None:
    frame = _cohort_frame(MSA_FIXTURES[:2])

    with pytest.raises(ValueError, match="requires at least 50 populated MSAs"):
        analysis.build_pooled_fd_panel(frame)


def test_load_required_parquet_gives_actionable_command(tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="Run `uv run hhplab build result example"):
        analysis.load_required_parquet(
            tmp_path / "missing.parquet", "uv run hhplab build result example --json"
        )
