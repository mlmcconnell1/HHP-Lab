"""Tests for reusable population crosswalk validation diagnostics.

Truth table:

| tract       | state | population | crosswalk area_share sum | expected condition |
|-------------|-------|------------|--------------------------|--------------------|
| 01001000100 | 01    | 100        | 1.00                     | balanced           |
| 01001000200 | 01    | 300        | 0.50                     | undercounted       |
| 02001000100 | 02    | 200        | 1.20                     | overcounted        |
| 02001000200 | 02    | 400        | missing                  | uncovered          |

National total is 1,000. CoC total is 100 + 150 + 240 = 490.
"""

import pandas as pd
import pytest

from hhplab.xwalks.diagnostics import validate_population_crosswalk

TRACT_ROWS = (
    ("01001000100", 100),
    ("01001000200", 300),
    ("02001000100", 200),
    ("02001000200", 400),
)

CROSSWALK_ROWS = (
    ("AL-500", "01001000100", 1.0),
    ("AL-501", "01001000200", 0.5),
    ("AK-500", "02001000100", 0.7),
    ("AK-501", "02001000100", 0.5),
)

EXPECTED_NATIONAL_TOTAL = sum(population for _, population in TRACT_ROWS)
EXPECTED_COC_TOTAL = 100.0 + 150.0 + 240.0
EXPECTED_RATIO = EXPECTED_COC_TOTAL / EXPECTED_NATIONAL_TOTAL


@pytest.fixture
def tract_population() -> pd.DataFrame:
    return pd.DataFrame(TRACT_ROWS, columns=["tract_geoid", "total_population"])


@pytest.fixture
def crosswalk() -> pd.DataFrame:
    return pd.DataFrame(CROSSWALK_ROWS, columns=["coc_id", "tract_geoid", "area_share"])


def test_population_validation_reports_national_and_coc_totals(
    crosswalk: pd.DataFrame,
    tract_population: pd.DataFrame,
) -> None:
    result = validate_population_crosswalk(crosswalk, tract_population, warn_threshold=0.05)

    assert result.national_total == EXPECTED_NATIONAL_TOTAL
    assert result.total_coc_population == EXPECTED_COC_TOTAL
    assert result.diff == EXPECTED_COC_TOTAL - EXPECTED_NATIONAL_TOTAL
    assert result.ratio == pytest.approx(EXPECTED_RATIO)
    assert result.within_threshold is False


def test_population_validation_reports_area_share_coverage(
    crosswalk: pd.DataFrame,
    tract_population: pd.DataFrame,
) -> None:
    result = validate_population_crosswalk(crosswalk, tract_population)

    assert result.relationship_count == len(CROSSWALK_ROWS)
    assert result.unique_crosswalk_tracts == 3
    assert result.unique_population_tracts == 4
    assert result.unique_geographies == 4
    assert result.missing_tract_count == 1
    assert result.extra_tract_count == 0
    assert result.missing_population == 400
    assert result.area_share.overcounted_count == 1
    assert result.area_share.undercounted_count == 1
    assert result.area_share.balanced_count == 1
    assert result.area_share.overcounted_samples == (("02001000100", 1.2),)


def test_population_validation_can_include_state_breakdown(
    crosswalk: pd.DataFrame,
    tract_population: pd.DataFrame,
) -> None:
    result = validate_population_crosswalk(crosswalk, tract_population, include_state=True)

    states = {state.state: state for state in result.state_comparison}
    assert set(states) == {"01", "02"}
    assert states["01"].acs_total == 400
    assert states["01"].coc_total == 250
    assert states["01"].ratio == pytest.approx(0.625)
    assert states["02"].acs_total == 600
    assert states["02"].coc_total == 240
    assert states["02"].ratio == pytest.approx(0.4)


def test_population_validation_omits_state_breakdown_by_default(
    crosswalk: pd.DataFrame,
    tract_population: pd.DataFrame,
) -> None:
    result = validate_population_crosswalk(crosswalk, tract_population)

    assert result.state_comparison == ()


@pytest.mark.parametrize(
    ("frame_name", "crosswalk_columns", "population_columns", "expected_message"),
    [
        ("crosswalk", ["tract_geoid", "area_share"], ["tract_geoid", "total_population"], "coc_id"),
        ("crosswalk", ["coc_id", "area_share"], ["tract_geoid", "total_population"], "tract_geoid"),
        (
            "population",
            ["coc_id", "tract_geoid", "area_share"],
            ["tract_geoid"],
            "total_population",
        ),
    ],
    ids=["missing-coc-id", "missing-crosswalk-tract", "missing-population-total"],
)
def test_population_validation_requires_expected_columns(
    frame_name: str,
    crosswalk_columns: list[str],
    population_columns: list[str],
    expected_message: str,
) -> None:
    crosswalk = pd.DataFrame(columns=crosswalk_columns)
    population = pd.DataFrame(columns=population_columns)

    with pytest.raises(ValueError, match=f"{frame_name}.*{expected_message}"):
        validate_population_crosswalk(crosswalk, population)
