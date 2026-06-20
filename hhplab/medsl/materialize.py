"""Materialize MEDSL county-year political leaning measures."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.medsl.ingest import (
    EXPECTED_PRESIDENTIAL_YEARS,
    default_raw_path,
    parse_county_presidential_returns,
)
from hhplab.naming import medsl_president_county_filename, medsl_president_county_path
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import MEDSL_PRESIDENT_COUNTY_MEASURE_COLUMNS


def materialize_county_political_leaning(
    raw_path: Path | str | None = None,
    *,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    county_vintage: int = 2020,
    force: bool = False,
    expected_years: tuple[int, ...] | None = EXPECTED_PRESIDENTIAL_YEARS,
) -> Path:
    """Build county-year presidential political leaning measures.

    Ratio policy: any ratio with a zero or missing denominator is written as
    null. Vote shares use ``totalvotes`` as the denominator. The Democratic and
    Republican cross-party ratios use the opposite party vote count as the
    denominator.
    """
    destination = (
        Path(output_path)
        if output_path is not None
        else _default_output_path(output_dir, county_vintage, expected_years)
    )
    if destination.exists() and not force:
        return destination

    source_path = default_raw_path() if raw_path is None else Path(raw_path)
    candidate_rows = parse_county_presidential_returns(
        source_path,
        expected_years=expected_years,
    )
    measures = build_county_political_leaning_measures(
        candidate_rows,
        county_vintage=county_vintage,
    )

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_versions = sorted(str(value) for value in candidate_rows["source_version"].dropna().unique())
    raw_sha256 = str(candidate_rows["raw_sha256"].iloc[0]) if not candidate_rows.empty else ""
    provenance = ProvenanceBlock(
        extra={
            "dataset": "medsl_president_county_political_leaning",
            "source": "MIT Election Data and Science Lab",
            "source_url": str(candidate_rows["source_url"].iloc[0]) if not candidate_rows.empty else "",
            "source_doi": str(candidate_rows["source_doi"].iloc[0]) if not candidate_rows.empty else "",
            "source_versions": source_versions,
            "source_file_path": str(source_path),
            "raw_sha256": raw_sha256,
            "row_count": len(measures),
            "input_row_count": len(candidate_rows),
            "county_count": int(measures["county_fips"].nunique()),
            "year_range": [int(measures["year"].min()), int(measures["year"].max())],
            "years": sorted(int(year) for year in measures["year"].unique()),
            "county_vintage": county_vintage,
            "output_schema": MEDSL_PRESIDENT_COUNTY_MEASURE_COLUMNS,
            "ratio_policy": "Ratios are null when the denominator is zero or missing.",
        },
    )
    write_parquet_with_provenance(measures, destination, provenance)
    return destination


def build_county_political_leaning_measures(
    candidate_rows: pd.DataFrame,
    *,
    county_vintage: int = 2020,
) -> pd.DataFrame:
    """Aggregate normalized MEDSL candidate rows to county-year measures."""
    required = {
        "county_fips",
        "geo_type",
        "geo_id",
        "year",
        "party_simplified",
        "candidatevotes",
        "totalvotes",
        "data_source",
        "source_url",
        "source_version",
        "raw_sha256",
    }
    missing = sorted(required - set(candidate_rows.columns))
    if missing:
        raise ValueError(f"MEDSL candidate rows missing required columns: {missing}.")

    rows = candidate_rows.copy()
    _validate_totalvotes_constant(rows)
    rows["candidatevotes"] = pd.to_numeric(rows["candidatevotes"], errors="coerce").fillna(0)
    rows["totalvotes"] = pd.to_numeric(rows["totalvotes"], errors="coerce")

    group_cols = ["county_fips", "year"]
    totals = (
        rows.groupby(group_cols, as_index=False)
        .agg(
            totalvotes=("totalvotes", "first"),
            source_version=("source_version", "first"),
            source_url=("source_url", "first"),
            raw_sha256=("raw_sha256", "first"),
        )
        .copy()
    )
    party_votes = (
        rows[rows["party_simplified"].isin(["DEMOCRAT", "REPUBLICAN"])]
        .pivot_table(
            index=group_cols,
            columns="party_simplified",
            values="candidatevotes",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={"DEMOCRAT": "democratic_votes", "REPUBLICAN": "republican_votes"})
    )
    for column in ("democratic_votes", "republican_votes"):
        if column not in party_votes.columns:
            party_votes[column] = 0

    measures = totals.merge(party_votes, on=group_cols, how="left")
    measures[["democratic_votes", "republican_votes"]] = measures[
        ["democratic_votes", "republican_votes"]
    ].fillna(0)
    measures["two_party_votes"] = measures["democratic_votes"] + measures["republican_votes"]
    measures["democratic_vote_share"] = _safe_divide(measures["democratic_votes"], measures["totalvotes"])
    measures["republican_vote_share"] = _safe_divide(measures["republican_votes"], measures["totalvotes"])
    measures["democratic_republican_vote_ratio"] = _safe_divide(
        measures["democratic_votes"],
        measures["republican_votes"],
    )
    measures["republican_democratic_vote_ratio"] = _safe_divide(
        measures["republican_votes"],
        measures["democratic_votes"],
    )
    measures["democratic_margin"] = measures["democratic_vote_share"] - measures["republican_vote_share"]
    measures["major_party_vote_share"] = _safe_divide(measures["two_party_votes"], measures["totalvotes"])
    measures["geo_type"] = "county"
    measures["geo_id"] = measures["county_fips"]
    measures["county_vintage"] = county_vintage
    measures["data_source"] = "medsl"

    return measures[MEDSL_PRESIDENT_COUNTY_MEASURE_COLUMNS].sort_values(
        ["county_fips", "year"]
    ).reset_index(drop=True)


def _validate_totalvotes_constant(rows: pd.DataFrame) -> None:
    counts = rows.groupby(["county_fips", "year"])["totalvotes"].nunique(dropna=False)
    inconsistent = counts[counts > 1]
    if not inconsistent.empty:
        examples = [
            {"county_fips": county_fips, "year": int(year)}
            for county_fips, year in inconsistent.head(5).index
        ]
        raise ValueError(f"MEDSL totalvotes vary within county/year groups: {examples}.")


def _safe_divide(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    numerator = pd.to_numeric(numerator, errors="coerce")
    denominator = pd.to_numeric(denominator, errors="coerce")
    result = numerator / denominator
    return result.mask(denominator.isna() | (denominator == 0))


def _default_output_path(
    output_dir: Path | str | None,
    county_vintage: int,
    expected_years: tuple[int, ...] | None,
) -> Path:
    start_year = min(expected_years or EXPECTED_PRESIDENTIAL_YEARS)
    end_year = max(expected_years or EXPECTED_PRESIDENTIAL_YEARS)
    if output_dir is None:
        return medsl_president_county_path(start_year, end_year, county_vintage)
    return Path(output_dir) / medsl_president_county_filename(start_year, end_year, county_vintage)
