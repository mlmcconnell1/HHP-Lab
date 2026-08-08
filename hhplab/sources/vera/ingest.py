"""Vera Institute county incarceration trends ingestion."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlretrieve

import pandas as pd

from hhplab.naming import vera_incarceration_county_filename, vera_incarceration_county_path
from hhplab.paths import raw_root
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS
from hhplab.source_registry import register_source
from hhplab.source_urls import (
    VERA_INCARCERATION_TRENDS_COUNTY_CSV,
    VERA_INCARCERATION_TRENDS_REPO,
)

VERA_COUNTY_FIRST_YEAR = 1970
VERA_COUNTY_JAIL_LAST_YEAR = 2026
VERA_COUNTY_PRISON_LAST_YEAR = 2019
VERA_SOURCE_VERSION = "3.1"

VERA_REQUIRED_COLUMNS: tuple[str, ...] = (
    "year",
    "county_fips",
    "county_name",
    "state_abbr",
    "state_fips",
)

VERA_INCARCERATION_NUMERIC_COLUMNS: tuple[str, ...] = (
    "land_area",
    "total_jail_pop",
    "male_jail_pop",
    "female_jail_pop",
    "black_jail_pop",
    "latinx_jail_pop",
    "white_jail_pop",
    "native_jail_pop",
    "aapi_jail_pop",
    "other_race_jail_pop",
    "total_pretrial_custody",
    "total_sentenced_custody",
    "total_jail_admits",
    "male_jail_admits",
    "female_jail_admits",
    "total_jail_discharges",
    "male_jail_discharges",
    "female_jail_discharges",
    "jail_rated_capacity",
    "total_prison_pop",
    "male_prison_pop",
    "female_prison_pop",
    "black_prison_pop",
    "latinx_prison_pop",
    "white_prison_pop",
    "native_prison_pop",
    "aapi_prison_pop",
    "other_race_prison_pop",
    "total_prison_admits",
    "male_prison_admits",
    "female_prison_admits",
    "total_pop_15to64",
    "male_pop_15to64",
    "female_pop_15to64",
    "black_pop_15to64",
    "latinx_pop_15to64",
    "white_pop_15to64",
    "native_pop_15to64",
    "aapi_pop_15to64",
    "total_incarceration",
)


def default_raw_path() -> Path:
    """Return the default retained Vera county CSV path."""
    return raw_root() / "vera" / "incarceration_trends_county.csv"


def default_output_path(
    output_dir: Path | str | None = None,
    *,
    county_vintage: int = 2020,
    start_year: int = VERA_COUNTY_FIRST_YEAR,
    end_year: int = VERA_COUNTY_JAIL_LAST_YEAR,
) -> Path:
    """Return the default curated Vera county incarceration artifact path."""
    if output_dir is None:
        return vera_incarceration_county_path(start_year, end_year, county_vintage)
    return (
        Path(output_dir)
        / vera_incarceration_county_filename(start_year, end_year, county_vintage)
    )


def download_county_incarceration_trends(
    destination: Path | str | None = None,
    *,
    force: bool = False,
) -> Path:
    """Download Vera's county incarceration trends CSV to the raw data cache."""
    path = default_raw_path() if destination is None else Path(destination)
    if path.exists() and not force:
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    urlretrieve(VERA_INCARCERATION_TRENDS_COUNTY_CSV, path)
    return path


def parse_county_incarceration_trends(
    raw_path: Path | str,
    *,
    county_vintage: int = 2020,
    expected_years: tuple[int, ...] | None = None,
) -> pd.DataFrame:
    """Parse Vera county incarceration trends into a panel-ready county-year artifact."""
    path = Path(raw_path)
    df = pd.read_csv(path, dtype={"county_fips": "string", "state_fips": "string"})
    _require_columns(df, VERA_REQUIRED_COLUMNS)

    rows = df.copy()
    rows["year"] = pd.to_numeric(rows["year"], errors="coerce")
    invalid_years = rows["year"].isna()
    if invalid_years.any():
        raise ValueError(f"Vera rows contain {int(invalid_years.sum())} missing/invalid years.")
    rows["year"] = rows["year"].astype(int)
    _validate_years(rows["year"], expected_years)

    rows["county_fips"] = rows["county_fips"].astype("string").str.strip().str.zfill(5)
    missing_fips = rows["county_fips"].isna() | (rows["county_fips"] == "")
    invalid_fips = rows["county_fips"].str.len() != 5
    if (missing_fips | invalid_fips).any():
        examples = rows.loc[missing_fips | invalid_fips, ["year", "county_name", "county_fips"]]
        raise ValueError(
            "Vera county_fips values must normalize to five-character county FIPS codes. "
            f"Examples: {examples.head(5).to_dict('records')}"
        )

    for column in VERA_INCARCERATION_NUMERIC_COLUMNS:
        if column not in rows.columns:
            rows[column] = pd.NA
        rows[column] = pd.to_numeric(rows[column], errors="coerce")

    for column in (
        "county_name",
        "state_abbr",
        "state_fips",
        "urbanicity",
        "region",
        "division",
        "metro_area",
        "commuting_zone",
    ):
        if column not in rows.columns:
            rows[column] = pd.NA
        rows[column] = rows[column].astype("string").str.strip()

    for column in ("is_regional_jail", "is_unified_state"):
        if column not in rows.columns:
            rows[column] = pd.NA
        rows[column] = rows[column].map(_parse_bool).astype("boolean")

    raw_sha256 = _sha256(path)
    ingested_at = datetime.now(UTC)
    result = pd.DataFrame(
        {
            "geo_type": "county",
            "geo_id": rows["county_fips"],
            "county_fips": rows["county_fips"],
            "county_vintage": county_vintage,
            "year": rows["year"],
            "data_source": "vera",
            "source_url": VERA_INCARCERATION_TRENDS_COUNTY_CSV,
            "source_version": VERA_SOURCE_VERSION,
            "raw_sha256": raw_sha256,
            "ingested_at": ingested_at,
        }
    )
    passthrough_columns = [
        column
        for column in VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS
        if column not in result.columns
    ]
    for column in passthrough_columns:
        result[column] = rows[column] if column in rows.columns else pd.NA

    result = result[VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS]
    return result.sort_values(["county_fips", "year"]).reset_index(drop=True)


def ingest_county_incarceration_trends(
    raw_path: Path | str | None = None,
    *,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    county_vintage: int = 2020,
    force: bool = False,
    download: bool = True,
    expected_years: tuple[int, ...] | None = None,
) -> Path:
    """Download or parse Vera county incarceration trends and write curated parquet."""
    source_path = default_raw_path() if raw_path is None else Path(raw_path)
    if not source_path.exists():
        if not download:
            raise FileNotFoundError(
                f"Vera incarceration trends raw file not found at {source_path}. "
                "Run `hhplab ingest vera-incarceration` with download enabled, or stage "
                "incarceration_trends_county.csv there / pass --raw-path."
            )
        source_path = download_county_incarceration_trends(source_path)

    start_year = min(expected_years or (VERA_COUNTY_FIRST_YEAR,))
    end_year = max(expected_years or (VERA_COUNTY_JAIL_LAST_YEAR,))
    destination = (
        Path(output_path)
        if output_path is not None
        else default_output_path(
            output_dir,
            county_vintage=county_vintage,
            start_year=start_year,
            end_year=end_year,
        )
    )
    if destination.exists() and not force:
        return destination

    measures = parse_county_incarceration_trends(
        source_path,
        county_vintage=county_vintage,
        expected_years=expected_years,
    )
    destination.parent.mkdir(parents=True, exist_ok=True)

    raw_sha256 = str(measures["raw_sha256"].iloc[0]) if not measures.empty else _sha256(source_path)
    register_source(
        source_type="vera",
        source_url=VERA_INCARCERATION_TRENDS_COUNTY_CSV,
        source_name="Vera Incarceration Trends County Dataset",
        raw_sha256=raw_sha256,
        file_size=source_path.stat().st_size,
        local_path=source_path,
        metadata={
            "curated_path": str(destination),
            "repository": VERA_INCARCERATION_TRENDS_REPO,
            "source_version": VERA_SOURCE_VERSION,
            "county_vintage": county_vintage,
            "county_jail_year_range": [VERA_COUNTY_FIRST_YEAR, VERA_COUNTY_JAIL_LAST_YEAR],
            "county_prison_year_range": [1983, VERA_COUNTY_PRISON_LAST_YEAR],
        },
    )
    provenance = ProvenanceBlock(
        extra={
            "dataset": "vera_incarceration_county",
            "source": "Vera Institute of Justice",
            "source_url": VERA_INCARCERATION_TRENDS_COUNTY_CSV,
            "source_repository": VERA_INCARCERATION_TRENDS_REPO,
            "source_version": VERA_SOURCE_VERSION,
            "source_file_path": str(source_path),
            "raw_sha256": raw_sha256,
            "row_count": len(measures),
            "county_count": int(measures["county_fips"].nunique()),
            "year_range": [int(measures["year"].min()), int(measures["year"].max())],
            "county_vintage": county_vintage,
            "jail_coverage_note": (
                "County jail columns are available from 1970-2026 where reported."
            ),
            "prison_coverage_note": "County prison columns are available from 1983-2019.",
            "output_schema": VERA_INCARCERATION_COUNTY_MEASURE_COLUMNS,
            "aggregation_policy": (
                "Counts and denominators are additive. Recompute rates after aggregating "
                "to target geography using recipe derived_measures."
            ),
        }
    )
    write_parquet_with_provenance(measures, destination, provenance)
    return destination


def _require_columns(df: pd.DataFrame, columns: tuple[str, ...]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(
            f"Vera incarceration trends file is missing required columns: {missing}. "
            f"Expected at least: {list(columns)}."
        )


def _validate_years(years: pd.Series, expected_years: tuple[int, ...] | None) -> None:
    if expected_years is None:
        return
    observed = set(int(year) for year in years.dropna().unique())
    missing = sorted(set(expected_years) - observed)
    if missing:
        raise ValueError(f"Vera incarceration trends missing expected years: {missing}.")


def _parse_bool(value: object) -> bool | None:
    if pd.isna(value):
        return None
    normalized = str(value).strip().lower()
    if normalized in {"true", "1", "yes", "y"}:
        return True
    if normalized in {"false", "0", "no", "n"}:
        return False
    return None


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
