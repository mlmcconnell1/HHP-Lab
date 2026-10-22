"""Count Types truth table: numbers never establish count occurrence.

Full counts: observed, not carried, not partial (including genuine zeroes).
Sheltered-only/no-count: unobserved; numeric AHAR estimates are carried except
2021, whose carry-forward status remains unknown. Partial counts: observed but
partial and ineligible for complete-count analyses. Missing/unrecognized types:
all flags unknown. Every reported number and raw declaration is preserved.
"""

from __future__ import annotations

import json

import pandas as pd
import pytest

from hhplab.sources.hud.pit.ingest.parser import parse_pit_file, write_pit_parquet
from hhplab.sources.hud.pit.observation import (
    COUNT_TYPES_FULL,
    COUNT_TYPES_PARTIAL,
    COUNT_TYPES_SHELTERED_ONLY,
    PIT_BOOLEAN_OBSERVATION_COLUMNS,
    aggregate_pit_count_metadata,
    observed_pit_mask,
    pit_count_metadata,
    with_pit_count_metadata,
)
from hhplab.storage.provenance import read_provenance

COUNT_CASES = [
    pytest.param(COUNT_TYPES_FULL, 2024, 5, (True, False, False), id="full-positive"),
    pytest.param(COUNT_TYPES_FULL, 2024, 0, (True, False, False), id="full-genuine-zero"),
    pytest.param(COUNT_TYPES_FULL, 2024, None, (True, False, False), id="full-value-missing"),
    pytest.param(COUNT_TYPES_SHELTERED_ONLY, 2024, 5, (False, True, False), id="carried"),
    pytest.param(COUNT_TYPES_SHELTERED_ONLY, 2024, 0, (False, True, False), id="carried-zero"),
    pytest.param(COUNT_TYPES_SHELTERED_ONLY, 2024, None, (False, False, False), id="no-estimate"),
    pytest.param(COUNT_TYPES_SHELTERED_ONLY, 2021, 0, (False, None, False), id="2021-placeholder"),
    pytest.param(COUNT_TYPES_PARTIAL, 2021, 5, (True, False, True), id="partial"),
    pytest.param("No 2010 PIT Count", 2010, 5, (False, True, False), id="no-pit-count"),
    pytest.param("No 2011 PIT Count", 2010, 5, (None, None, None), id="wrong-count-year"),
    pytest.param(None, 2024, 5, (None, None, None), id="missing-metadata"),
    pytest.param("", 2024, 0, (None, None, None), id="blank-metadata-zero"),
    pytest.param("Unrecognized", 2024, 5, (None, None, None), id="unknown-label"),
    pytest.param(
        "  Sheltered\u2011Only   Count ", 2024, 5, (False, True, False), id="unicode-hyphen"
    ),
]


@pytest.mark.parametrize(("label", "year", "value", "expected"), COUNT_CASES)
def test_count_type_truth_table(label, year, value, expected):
    result = pit_count_metadata(label, year=year, has_unsheltered_value=value is not None)
    for column, flag in zip(PIT_BOOLEAN_OBSERVATION_COLUMNS, expected, strict=True):
        if flag is None:
            assert pd.isna(result[column])
        else:
            assert result[column] is flag
    if label is None:
        assert pd.isna(result["count_types"])
    else:
        assert result["count_types"] == label


@pytest.mark.parametrize("header", ["Count Types", "count_types"])
def test_parser_retains_declaration_and_reported_count(tmp_path, header):
    path = tmp_path / "pit.csv"
    pd.DataFrame({
        "CoC Number": ["CA-500", "CA-501"],
        "Overall Homeless": [10, 10],
        "Sheltered Total Homeless": [5, 5],
        "Unsheltered Homeless": [5, 5],
        header: [COUNT_TYPES_FULL, COUNT_TYPES_SHELTERED_ONLY],
    }).to_csv(path, index=False)
    frame = parse_pit_file(path, 2024).df
    assert frame.pit_unsheltered.tolist() == [5, 5]
    assert frame.unsheltered_count_observed.tolist() == [True, False]
    assert frame.unsheltered_count_carried_forward.tolist() == [False, True]
    assert frame.count_types.tolist() == [COUNT_TYPES_FULL, COUNT_TYPES_SHELTERED_ONLY]
    assert str(frame.unsheltered_count_observed.dtype) == "boolean"


@pytest.mark.parametrize("measure", ["pit_unsheltered", "pit_total", "pit_sheltered"])
def test_observed_mask_excludes_carried_partial_unknown_and_missing_values(measure):
    labels = [
        COUNT_TYPES_FULL, COUNT_TYPES_SHELTERED_ONLY, COUNT_TYPES_PARTIAL,
        "No 2024 PIT Count", None, COUNT_TYPES_FULL,
    ]
    frame = with_pit_count_metadata(pd.DataFrame({
        "pit_year": [2024] * len(labels), "count_types": labels,
        "pit_unsheltered": [0, 5, 5, 5, 5, None],
        "pit_total": [5, 10, 10, 10, 10, None],
        "pit_sheltered": [5, 5, 5, 5, 5, None],
    }))
    expected = (
        [True, True, True, False, False, False]
        if measure == "pit_sheltered" else [True, False, False, False, False, False]
    )
    assert observed_pit_mask(frame, measure).tolist() == expected


def test_missing_metadata_does_not_infer_observed_from_repeated_numbers():
    frame = with_pit_count_metadata(pd.DataFrame({
        "pit_year": [2023, 2024], "pit_unsheltered": [7, 7]
    }))
    assert frame.unsheltered_count_observed.isna().all()
    assert frame.unsheltered_count_carried_forward.isna().all()
    assert not observed_pit_mask(frame).any()


@pytest.mark.parametrize(
    ("labels", "expected_count", "observed", "carried", "partial"),
    [
        ([COUNT_TYPES_FULL, COUNT_TYPES_FULL], 2, True, False, False),
        ([COUNT_TYPES_FULL, COUNT_TYPES_SHELTERED_ONLY], 2, False, True, False),
        ([COUNT_TYPES_FULL, None], 2, None, None, None),
        ([COUNT_TYPES_FULL, COUNT_TYPES_PARTIAL], 2, True, False, True),
        ([COUNT_TYPES_FULL], 2, None, None, None),
    ],
    ids=["all-observed", "one-carried", "one-unknown", "one-partial", "missing-member"],
)
def test_aggregate_observation_metadata(labels, expected_count, observed, carried, partial):
    frame = with_pit_count_metadata(pd.DataFrame({
        "coc_id": [f"CA-{500 + i}" for i in range(len(labels))],
        "pit_year": [2024] * len(labels), "count_types": labels,
        "pit_unsheltered": [5] * len(labels),
    }))
    result = aggregate_pit_count_metadata(frame, expected_coc_count=expected_count)
    expected = [observed, carried, partial]
    for column, value in zip(PIT_BOOLEAN_OBSERVATION_COLUMNS, expected, strict=True):
        if value is None:
            assert pd.isna(result[column])
        else:
            assert result[column] is value
    assert json.loads(result["count_types"]) == dict(zip(frame.coc_id, labels, strict=True))


def test_parquet_roundtrip_retains_nullable_status_and_provenance(tmp_path):
    frame = with_pit_count_metadata(pd.DataFrame({
        "pit_year": [2024, 2024], "coc_id": ["CA-500", "CA-501"],
        "pit_total": [10, 10], "pit_sheltered": [5, 5], "pit_unsheltered": [5, 5],
        "count_types": [COUNT_TYPES_SHELTERED_ONLY, None],
        "data_source": ["hud_user"] * 2, "source_ref": ["official workbook"] * 2,
        "ingested_at": pd.to_datetime(["2024-12-01"] * 2, utc=True),
    }))
    path = write_pit_parquet(frame, tmp_path / "pit.parquet")
    actual = pd.read_parquet(path)
    assert actual.pit_unsheltered.tolist() == [5, 5]
    assert actual.unsheltered_count_carried_forward.iloc[0]
    assert pd.isna(actual.unsheltered_count_carried_forward.iloc[1])
    assert str(actual.unsheltered_count_observed.dtype) == "boolean"
    policy = read_provenance(path).extra["count_types_policy"]
    assert policy["reported_counts_preserved"] is True
    assert policy["flag_counts"]["unsheltered_count_observed"] == {
        "true": 0, "false": 1, "unknown": 1,
    }
