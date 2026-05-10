"""Reusable PIT vintage comparison logic."""

from dataclasses import dataclass

import pandas as pd

REQUIRED_PIT_VINTAGE_COLUMNS = frozenset(
    {"pit_year", "coc_id", "pit_total", "pit_sheltered", "pit_unsheltered"}
)
PIT_COUNT_COLUMNS = ("pit_total", "pit_sheltered", "pit_unsheltered")
PIT_DELTA_COLUMNS = ("total_delta", "sheltered_delta", "unsheltered_delta")
PIT_COMPARISON_COLUMNS = (
    "pit_year",
    "coc_id",
    "status",
    "pit_total_v1",
    "pit_total_v2",
    "total_delta",
    "pit_sheltered_v1",
    "pit_sheltered_v2",
    "sheltered_delta",
    "pit_unsheltered_v1",
    "pit_unsheltered_v2",
    "unsheltered_delta",
)


@dataclass(frozen=True)
class PitVintageComparisonResult:
    """Structured comparison between two PIT vintage DataFrames."""

    common_years: tuple[int, ...]
    vintage1_record_count: int
    vintage2_record_count: int
    comparison: pd.DataFrame
    tab_totals: pd.DataFrame

    @property
    def added_count(self) -> int:
        return self.status_count("added")

    @property
    def removed_count(self) -> int:
        return self.status_count("removed")

    @property
    def changed_count(self) -> int:
        return self.status_count("changed")

    @property
    def unchanged_count(self) -> int:
        return self.status_count("unchanged")

    @property
    def has_differences(self) -> bool:
        return self.added_count > 0 or self.removed_count > 0 or self.changed_count > 0

    @property
    def has_tab_total_differences(self) -> bool:
        deltas = self.tab_totals[list(PIT_DELTA_COLUMNS)].fillna(0)
        return bool(deltas.ne(0).any(axis=None))

    def status_count(self, status: str) -> int:
        return int((self.comparison["status"] == status).sum())

    def records_with_status(self, status: str) -> pd.DataFrame:
        return self.comparison[self.comparison["status"] == status].copy()

    def csv_frame(self, vintage1: str, vintage2: str) -> pd.DataFrame:
        output = self.comparison[list(PIT_COMPARISON_COLUMNS)].copy()
        output.insert(0, "vintage1", vintage1)
        output.insert(1, "vintage2", vintage2)
        return output.sort_values(["pit_year", "coc_id"]).reset_index(drop=True)


def compare_pit_vintages(
    vintage1: pd.DataFrame,
    vintage2: pd.DataFrame,
    *,
    year: int | None = None,
) -> PitVintageComparisonResult:
    """Compare PIT count records across two vintage DataFrames."""
    _require_columns(vintage1, "vintage1")
    _require_columns(vintage2, "vintage2")

    df1 = vintage1.copy()
    df2 = vintage2.copy()
    df1["pit_year"] = pd.to_numeric(df1["pit_year"], errors="raise").astype(int)
    df2["pit_year"] = pd.to_numeric(df2["pit_year"], errors="raise").astype(int)
    df1["coc_id"] = df1["coc_id"].astype("string")
    df2["coc_id"] = df2["coc_id"].astype("string")

    years1 = set(df1["pit_year"].unique())
    years2 = set(df2["pit_year"].unique())
    common_years = tuple(sorted(years1 & years2))
    if not common_years:
        raise ValueError(
            "No common years between vintages. "
            f"Vintage 1 has years {sorted(years1)}, vintage 2 has years {sorted(years2)}"
        )

    if year is not None:
        if year not in common_years:
            raise ValueError(
                f"Year {year} not found in both vintages. Common years: {common_years}"
            )
        common_years = (year,)

    df1_filtered = df1[df1["pit_year"].isin(common_years)].copy()
    df2_filtered = df2[df2["pit_year"].isin(common_years)].copy()

    comparison = _build_record_comparison(df1_filtered, df2_filtered)
    tab_totals = _build_tab_totals(df1_filtered, df2_filtered)

    return PitVintageComparisonResult(
        common_years=common_years,
        vintage1_record_count=int(len(df1_filtered)),
        vintage2_record_count=int(len(df2_filtered)),
        comparison=comparison,
        tab_totals=tab_totals,
    )


def _build_record_comparison(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    merged = pd.merge(
        df1[["pit_year", "coc_id", *PIT_COUNT_COLUMNS]],
        df2[["pit_year", "coc_id", *PIT_COUNT_COLUMNS]],
        on=["pit_year", "coc_id"],
        how="outer",
        suffixes=("_v1", "_v2"),
    )
    _add_delta_columns(merged)
    merged["status"] = merged.apply(_classify_change, axis=1)
    return merged[list(PIT_COMPARISON_COLUMNS)].sort_values(["pit_year", "coc_id"]).reset_index(
        drop=True
    )


def _build_tab_totals(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
) -> pd.DataFrame:
    totals_v1 = df1.groupby("pit_year")[list(PIT_COUNT_COLUMNS)].sum()
    totals_v2 = df2.groupby("pit_year")[list(PIT_COUNT_COLUMNS)].sum()
    totals = totals_v1.join(totals_v2, lsuffix="_v1", rsuffix="_v2", how="outer")
    _add_delta_columns(totals)
    return totals.sort_index()


def _add_delta_columns(df: pd.DataFrame) -> None:
    df["total_delta"] = df["pit_total_v2"] - df["pit_total_v1"]
    df["sheltered_delta"] = df["pit_sheltered_v2"] - df["pit_sheltered_v1"]
    df["unsheltered_delta"] = df["pit_unsheltered_v2"] - df["pit_unsheltered_v1"]


def _classify_change(row: pd.Series) -> str:
    if pd.isna(row["pit_total_v1"]):
        return "added"
    if pd.isna(row["pit_total_v2"]):
        return "removed"
    if _delta_changed(row["total_delta"]):
        return "changed"
    if _delta_changed(row["sheltered_delta"]):
        return "changed"
    if _delta_changed(row["unsheltered_delta"]):
        return "changed"
    return "unchanged"


def _delta_changed(value: object) -> bool:
    return bool(not pd.isna(value) and value != 0)


def _require_columns(df: pd.DataFrame, frame_name: str) -> None:
    missing = sorted(REQUIRED_PIT_VINTAGE_COLUMNS - set(df.columns))
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"{frame_name} missing required column(s): {joined}")
