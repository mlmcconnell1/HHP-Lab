"""Tests for tracked overdose-PSH state-year robustness models."""

from __future__ import annotations

import pandas as pd

from hhplab.results.workflows import analyze_overdose_psh_state_year_robustness as analysis


def _fixture() -> pd.DataFrame:
    rows = []
    msas = (
        ("10000", "Alpha, CT"),
        ("11000", "Beta, CT"),
        ("20000", "Gamma, NY"),
        ("21000", "Delta, NY"),
    )
    for msa_index, (msa_id, msa_name) in enumerate(msas):
        for year_index, year in enumerate((2023, 2024, 2025)):
            rows.append(
                {
                    "msa_id": msa_id,
                    "msa_name": msa_name,
                    "year": year,
                    "overdose_coverage_ratio": 1.0,
                    "year_gap": 1,
                    "lag1_year_gap": 1,
                    "log_overdose_rate": 0.2 * msa_index + 0.1 * year_index,
                    "log_psh_rate_lag1": 0.05 * msa_index + 0.02 * year_index,
                    "log_zori": 7 + 0.01 * msa_index + 0.03 * year_index,
                    "d_log_overdose_rate": 0.1 + 0.01 * msa_index,
                    "d_log_psh_rate_lag1": 0.02 + 0.005 * msa_index,
                    "d_log_psh_rate": 0.03 + 0.002 * year_index,
                    "d_log_zori": 0.04 + 0.001 * msa_index,
                }
            )
    return pd.DataFrame(rows)


def test_fit_specs_report_expected_fixed_effects_and_samples() -> None:
    results = pd.DataFrame([analysis.fit_spec(_fixture(), spec) for spec in analysis.SPECS])

    assert results["model"].tolist() == [spec.model for spec in analysis.SPECS]
    assert set(results["fixed_effects"]) == {
        "msa_id+year",
        "msa_id+primary_state_year",
        "year",
        "primary_state_year",
    }
    assert results["nobs"].unique().tolist() == [12]
    assert results["std_error_type"].str.contains("small_sample_correction=false").all()
