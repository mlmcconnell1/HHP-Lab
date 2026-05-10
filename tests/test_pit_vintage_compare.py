"""Tests for reusable PIT vintage comparison logic.

Truth table:

| year | CoC    | vintage 1 total | vintage 2 total | expected status |
|------|--------|-----------------|-----------------|-----------------|
| 2020 | CO-500 | 100             | 100             | unchanged       |
| 2020 | CO-501 | 200             | 250             | changed         |
| 2020 | CO-502 | missing         | 300             | added           |
| 2020 | CO-503 | 400             | missing         | removed         |
| 2021 | CO-500 | 500             | 500             | year-filtered   |
"""

import pandas as pd
import pytest

from hhplab.pit.vintage_compare import compare_pit_vintages

VINTAGE1_ROWS = (
    (2020, "CO-500", 100, 70, 30),
    (2020, "CO-501", 200, 150, 50),
    (2020, "CO-503", 400, 300, 100),
    (2021, "CO-500", 500, 350, 150),
)

VINTAGE2_ROWS = (
    (2020, "CO-500", 100, 70, 30),
    (2020, "CO-501", 250, 150, 100),
    (2020, "CO-502", 300, 200, 100),
    (2021, "CO-500", 500, 350, 150),
)

PIT_COLUMNS = ("pit_year", "coc_id", "pit_total", "pit_sheltered", "pit_unsheltered")


@pytest.fixture
def vintage1() -> pd.DataFrame:
    return pd.DataFrame(VINTAGE1_ROWS, columns=PIT_COLUMNS)


@pytest.fixture
def vintage2() -> pd.DataFrame:
    return pd.DataFrame(VINTAGE2_ROWS, columns=PIT_COLUMNS)


def test_compare_pit_vintages_classifies_record_statuses(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
) -> None:
    result = compare_pit_vintages(vintage1, vintage2, year=2020)

    statuses = {
        row.coc_id: row.status
        for row in result.comparison[["coc_id", "status"]].itertuples(index=False)
    }
    assert statuses == {
        "CO-500": "unchanged",
        "CO-501": "changed",
        "CO-502": "added",
        "CO-503": "removed",
    }
    assert result.added_count == 1
    assert result.removed_count == 1
    assert result.changed_count == 1
    assert result.unchanged_count == 1
    assert result.has_differences is True


def test_compare_pit_vintages_reports_changed_deltas(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
) -> None:
    result = compare_pit_vintages(vintage1, vintage2, year=2020)

    changed = result.records_with_status("changed").iloc[0]
    assert changed["coc_id"] == "CO-501"
    assert changed["total_delta"] == 50
    assert changed["sheltered_delta"] == 0
    assert changed["unsheltered_delta"] == 50


def test_compare_pit_vintages_builds_tab_totals(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
) -> None:
    result = compare_pit_vintages(vintage1, vintage2, year=2020)

    row = result.tab_totals.loc[2020]
    assert row["pit_total_v1"] == 700
    assert row["pit_total_v2"] == 650
    assert row["total_delta"] == -50
    assert row["pit_sheltered_v1"] == 520
    assert row["pit_sheltered_v2"] == 420
    assert row["sheltered_delta"] == -100
    assert row["pit_unsheltered_v1"] == 180
    assert row["pit_unsheltered_v2"] == 230
    assert row["unsheltered_delta"] == 50
    assert result.has_tab_total_differences is True


def test_compare_pit_vintages_filters_to_requested_year(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
) -> None:
    result = compare_pit_vintages(vintage1, vintage2, year=2021)

    assert result.common_years == (2021,)
    assert result.vintage1_record_count == 1
    assert result.vintage2_record_count == 1
    assert result.unchanged_count == 1
    assert result.has_differences is False


def test_compare_pit_vintages_prepares_csv_frame(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
) -> None:
    result = compare_pit_vintages(vintage1, vintage2, year=2020)

    csv_frame = result.csv_frame("2023", "2024")
    assert list(csv_frame.columns[:5]) == [
        "vintage1",
        "vintage2",
        "pit_year",
        "coc_id",
        "status",
    ]
    assert set(csv_frame["vintage1"]) == {"2023"}
    assert set(csv_frame["vintage2"]) == {"2024"}


@pytest.mark.parametrize(
    ("v1_rows", "v2_rows", "expected_message"),
    [
        (((2020, "CO-500", 100, 70, 30),), ((2021, "CO-500", 100, 70, 30),), "No common"),
        (VINTAGE1_ROWS, VINTAGE2_ROWS, "Year 2019 not found"),
    ],
    ids=["no-common-years", "missing-filter-year"],
)
def test_compare_pit_vintages_rejects_missing_year_coverage(
    v1_rows: tuple[tuple[int, str, int, int, int], ...],
    v2_rows: tuple[tuple[int, str, int, int, int], ...],
    expected_message: str,
) -> None:
    year = None if expected_message == "No common" else 2019
    with pytest.raises(ValueError, match=expected_message):
        compare_pit_vintages(
            pd.DataFrame(v1_rows, columns=PIT_COLUMNS),
            pd.DataFrame(v2_rows, columns=PIT_COLUMNS),
            year=year,
        )


def test_compare_pit_vintages_requires_expected_columns(vintage2: pd.DataFrame) -> None:
    vintage1 = pd.DataFrame({"pit_year": [2020], "coc_id": ["CO-500"]})

    with pytest.raises(ValueError, match="vintage1.*pit_sheltered.*pit_total.*pit_unsheltered"):
        compare_pit_vintages(vintage1, vintage2)
