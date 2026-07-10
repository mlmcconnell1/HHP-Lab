"""Tests for tracked sanctuary long-difference robustness checks."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from hhplab.results.workflows import analyze_sanctuary_longdiff_robustness as analysis


def _frame() -> pd.DataFrame:
    rows = []
    states = ("CA", "CA", "NY", "NY", "TX", "TX", "PA", "PA")
    outcomes = (0.42, 0.01, 0.50, 0.04, 0.37, 0.08, 0.55, -0.02)
    for index, state in enumerate(states):
        sanctuary = int(index % 2 == 0)
        rows.append(
            {
                "msa_id": f"{index:05d}",
                "primary_state": state,
                "sanctuary": sanctuary,
                "outcome": outcomes[index],
            }
        )
    return pd.DataFrame(rows)


def test_fit_outcome_runs_all_robustness_specifications() -> None:
    outcome = analysis.OutcomeSpec("test", "outcome", "homelessness")

    result = analysis.fit_outcome(_frame(), outcome)

    assert result["specification"].tolist() == list(analysis.SPECIFICATIONS)
    assert result.set_index("specification").loc["exclude_california_hc1", "nobs"] == 6
    assert result.set_index("specification").loc["state_clustered", "state_count"] == 4


def test_add_primary_state_uses_unique_msa_mapping() -> None:
    frame = pd.DataFrame({"msa_id": ["10000"], "sanctuary": [1]})
    reference = pd.DataFrame(
        {"msa_id": ["10000", "10000"], "msa_name": ["Example, PA", "Example, PA"]}
    )

    result = analysis.add_primary_state(frame, reference)

    assert result["primary_state"].tolist() == ["PA"]


def test_add_primary_state_rejects_missing_mapping() -> None:
    frame = pd.DataFrame({"msa_id": ["10000"]})
    reference = pd.DataFrame({"msa_id": ["20000"], "msa_name": ["Example, PA"]})

    with pytest.raises(ValueError, match="missing msa_id"):
        analysis.add_primary_state(frame, reference)


def test_build_longdiff_inputs_uses_canonical_panel_and_hic_rollups(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    base = pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "20000", "20000"],
            "msa_name": ["Alpha, AA", "Alpha, AA", "Beta, BB", "Beta, BB"],
            "year": [2015, 2025, 2015, 2025],
            "cohort": ["top50"] * 4,
            "population": [1000, 1100, 2000, 2200],
            "pit_sheltered": [10, 22, 40, 44],
            "log_unshelt_rate": [0.0, 0.2, 0.1, 0.4],
        }
    )
    sanctuary_path = tmp_path / "sanctuary.parquet"
    pd.DataFrame(
        {"msa_id": ["10000", "20000"], "doj_sanctuary_msa": [1, 0]}
    ).to_parquet(sanctuary_path, index=False)
    hic_path = tmp_path / "panel__msa-rollup-hic__Y2015-2025.parquet"
    pd.DataFrame(
        {
            "msa_id": ["10000", "10000", "20000", "20000"],
            "year": [2015, 2025, 2015, 2025],
            "hic_total_beds": [20, 44, 80, 88],
        }
    ).to_parquet(hic_path, index=False)
    monkeypatch.setattr(analysis, "load_pooled_base_panel", lambda: base)
    monkeypatch.setattr(analysis, "SANCTUARY_INPUT", sanctuary_path)
    monkeypatch.setattr(analysis, "HIC_ROLLUP_GLOB", str(tmp_path / "panel__*.parquet"))

    homelessness, beds, sources = analysis.build_longdiff_inputs()

    assert homelessness.set_index("msa_id").loc[
        "10000", "d_log_unshelt_rate_15_25"
    ] == pytest.approx(0.2)
    assert beds.set_index("msa_id").loc["10000", "d_log_beds_15_25"] == pytest.approx(
        np.log(2.0)
    )
    assert str(hic_path) in sources
