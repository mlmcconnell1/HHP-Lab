"""Contributor-aware PIT declaration propagation through recipe rollups."""

from __future__ import annotations

import pandas as pd
import pytest

from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.resample import _resample_aggregate, _resample_allocate
from hhplab.recipe.planner import ResampleTask
from hhplab.recipe.schema_common import GeometryRef
from hhplab.sources.hud.pit.observation import (
    COUNT_TYPES_FULL,
    COUNT_TYPES_PARTIAL,
    COUNT_TYPES_SHELTERED_ONLY,
    observed_pit_mask,
    with_pit_count_metadata,
)


def _task() -> ResampleTask:
    return ResampleTask(
        dataset_id="pit",
        year=2024,
        input_path=None,
        effective_geometry=GeometryRef(type="coc"),
        method="aggregate",
        transform_id="coc_to_metro",
        to_geometry=GeometryRef(type="metro"),
        measures=["pit_total", "pit_sheltered", "pit_unsheltered"],
        measure_aggregations={
            "pit_total": "sum",
            "pit_sheltered": "sum",
            "pit_unsheltered": "sum",
        },
        geo_column="coc_id",
    )


def _crosswalk() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "metro_id": ["GF01"] * 4,
            "coc_id": ["CA-500", "CA-501", "CA-502", "CA-503"],
            "area_share": [1.0, 1.0, 1.0, 0.0],
        }
    )


def _source(labels: list[str | None], cocs: list[str]) -> pd.DataFrame:
    return with_pit_count_metadata(
        pd.DataFrame(
            {
                "coc_id": cocs,
                "pit_year": [2024] * len(cocs),
                "pit_total": [10.0] * len(cocs),
                "pit_sheltered": [5.0] * len(cocs),
                "pit_unsheltered": [5.0] * len(cocs),
                "count_types": labels,
            }
        )
    )


@pytest.mark.parametrize(
    ("labels", "cocs", "observed", "carried", "partial", "complete_sample"),
    [
        pytest.param(
            [COUNT_TYPES_FULL, COUNT_TYPES_SHELTERED_ONLY, COUNT_TYPES_FULL],
            ["CA-500", "CA-501", "CA-502"],
            False,
            True,
            False,
            False,
            id="mixed-observed-carried",
        ),
        pytest.param(
            [COUNT_TYPES_FULL, COUNT_TYPES_PARTIAL, COUNT_TYPES_FULL],
            ["CA-500", "CA-501", "CA-502"],
            True,
            False,
            True,
            False,
            id="partial-member",
        ),
        pytest.param(
            [COUNT_TYPES_FULL, COUNT_TYPES_FULL],
            ["CA-500", "CA-501"],
            None,
            None,
            None,
            False,
            id="missing-expected-member",
        ),
        pytest.param(
            [COUNT_TYPES_FULL, COUNT_TYPES_FULL, COUNT_TYPES_FULL, "Unknown declaration"],
            ["CA-500", "CA-501", "CA-502", "CA-503"],
            True,
            False,
            False,
            True,
            id="zero-weight-unknown-excluded",
        ),
    ],
)
def test_pit_resample_aggregates_metadata_from_positive_expected_members(
    labels, cocs, observed, carried, partial, complete_sample
):
    result = _resample_aggregate(_source(labels, cocs), _crosswalk(), _task())
    row = result.iloc[0]
    assert str(result.count_types.dtype) == "string"
    for column in (
        "unsheltered_count_observed",
        "unsheltered_count_carried_forward",
        "unsheltered_count_partial",
        "pit_sheltered_count_observed",
    ):
        assert str(result[column].dtype) == "boolean"
    for column, expected in (
        ("unsheltered_count_observed", observed),
        ("unsheltered_count_carried_forward", carried),
        ("unsheltered_count_partial", partial),
    ):
        if expected is None:
            assert pd.isna(row[column])
        else:
            assert bool(row[column]) is expected
    assert bool(observed_pit_mask(result).iloc[0]) is complete_sample
    if "CA-502" not in cocs:
        assert not bool(observed_pit_mask(result, "pit_sheltered").iloc[0])
    included_count = sum(coc != "CA-503" for coc in cocs)
    assert row.pit_unsheltered == pytest.approx(5.0 * included_count)


@pytest.mark.parametrize("invalid_weight", [-0.25, float("inf"), float("nan")])
def test_pit_resample_rejects_invalid_crosswalk_weights(invalid_weight):
    crosswalk = _crosswalk()
    crosswalk.loc[0, "area_share"] = invalid_weight
    with pytest.raises(ExecutorError, match="finite, nonnegative crosswalk weights"):
        _resample_aggregate(
            _source([COUNT_TYPES_FULL] * 3, ["CA-500", "CA-501", "CA-502"]),
            crosswalk,
            _task(),
        )


def test_pit_resample_rejects_invalid_weight_for_missing_expected_source():
    crosswalk = _crosswalk()
    crosswalk.loc[0, "area_share"] = float("nan")
    source = _source(
        [COUNT_TYPES_FULL, COUNT_TYPES_FULL],
        ["CA-501", "CA-502"],
    )
    with pytest.raises(ExecutorError, match="finite, nonnegative crosswalk weights"):
        _resample_aggregate(source, crosswalk, _task())


def test_pit_resample_rejects_nonadditive_or_numeric_metadata_aggregation():
    source = _source([COUNT_TYPES_FULL] * 3, ["CA-500", "CA-501", "CA-502"])
    nonadditive = _task()
    nonadditive.measure_aggregations["pit_unsheltered"] = "mean"
    with pytest.raises(ExecutorError, match="only accompany additive sum rollups"):
        _resample_aggregate(source, _crosswalk(), nonadditive)

    numeric_metadata = _task()
    numeric_metadata.measures = ["unsheltered_count_observed"]
    with pytest.raises(ExecutorError, match="cannot be configured as numeric measures"):
        _resample_aggregate(source, _crosswalk(), numeric_metadata)

    with pytest.raises(ExecutorError, match="does not support contributor-aware PIT metadata"):
        _resample_allocate(source, pd.DataFrame(), _task())
