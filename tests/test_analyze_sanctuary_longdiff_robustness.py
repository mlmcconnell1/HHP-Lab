"""Tests for tracked sanctuary long-difference robustness checks."""

from __future__ import annotations

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
