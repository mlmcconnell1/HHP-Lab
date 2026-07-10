"""Tests for the tracked rent-growth R-squared decomposition."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.results.workflows import analyze_rent_growth_r2_decomposition as analysis


def _frames() -> dict[str, pd.DataFrame]:
    keys = pd.DataFrame(
        {
            "msa_id": [f"{msa:05d}" for msa in range(12) for _ in range(3)],
            "year": [2016, 2017, 2018] * 12,
        }
    )
    frames: dict[str, pd.DataFrame] = {}
    for source_index, spec in enumerate(analysis.SOURCES):
        frame = keys.copy()
        row_index = pd.Series(range(len(frame)), dtype="float64")
        for column_index, column in enumerate(spec.columns):
            if column == analysis.OUTCOME:
                frame[column] = 0.01 * row_index + frame["year"].map(
                    {2016: 0.2, 2017: 0.1, 2018: 0.3}
                )
            else:
                frame[column] = (source_index + 1) * 0.001 * row_index + column_index * 0.01
        frames[spec.name] = frame
    return frames


def test_merge_sources_reports_cumulative_coverage() -> None:
    merged, coverage = analysis.merge_sources(_frames())

    assert len(merged) == 36
    assert coverage["source"].tolist() == [spec.name for spec in analysis.SOURCES]
    assert coverage["complete_rows_after_source"].tolist() == [36] * len(analysis.SOURCES)


def test_fit_models_uses_one_common_sample_for_every_block() -> None:
    merged, _ = analysis.merge_sources(_frames())
    sample = analysis.common_sample(merged)

    result = analysis.fit_models(sample)

    assert result["model"].tolist() == [name for name, _ in analysis.MODEL_BLOCKS]
    assert result["nobs"].unique().tolist() == [len(sample)]
    assert result["msa_count"].unique().tolist() == [12]
    assert result["r_squared"].is_monotonic_increasing
    assert result.iloc[0]["delta_vs_year_fe"] == pytest.approx(0.0)


def test_common_sample_drops_rows_missing_any_channel() -> None:
    merged, _ = analysis.merge_sources(_frames())
    merged.loc[0, analysis.ALL_PREDICTORS[-1]] = pd.NA

    sample = analysis.common_sample(merged)

    assert len(sample) == len(merged) - 1


def test_merge_sources_rejects_missing_source() -> None:
    frames = _frames()
    frames.pop(analysis.SOURCES[-1].name)

    with pytest.raises(ValueError, match="Missing source frames"):
        analysis.merge_sources(frames)
