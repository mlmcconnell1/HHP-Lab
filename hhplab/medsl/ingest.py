"""MEDSL county presidential election returns ingestion."""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from hhplab.paths import curated_dir, raw_root
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import MEDSL_COUNTY_PRESIDENTIAL_OUTPUT_COLUMNS
from hhplab.source_registry import register_source
from hhplab.sources import (
    MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API,
    MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI,
)

logger = logging.getLogger(__name__)

EXPECTED_PRESIDENTIAL_YEARS: tuple[int, ...] = (2000, 2004, 2008, 2012, 2016, 2020, 2024)
MEDSL_COUNTY_PRESIDENTIAL_COLUMNS: tuple[str, ...] = (
    "state",
    "county_name",
    "year",
    "state_po",
    "county_fips",
    "office",
    "candidate",
    "party",
    "candidatevotes",
    "totalvotes",
    "version",
    "mode",
)
PARTY_SIMPLIFICATION: dict[str, str] = {
    "DEMOCRAT": "DEMOCRAT",
    "DEMOCRATIC": "DEMOCRAT",
    "DEMOCRATIC-FARMER-LABOR": "DEMOCRAT",
    "REPUBLICAN": "REPUBLICAN",
    "GOP": "REPUBLICAN",
    "LIBERTARIAN": "OTHER",
    "GREEN": "OTHER",
    "OTHER": "OTHER",
}
KNOWN_NONCOUNTY_MISSING_FIPS: frozenset[tuple[str, str]] = frozenset(
    {
        ("CT", "STATEWIDE WRITEIN"),
        ("ME", "MAINE UOCAVA"),
        ("RI", "FEDERAL PRECINCT"),
    }
)
KNOWN_NONCOUNTY_INVALID_FIPS: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("MO", "KANSAS CITY", "2938000"),
    }
)
ALASKA_ELECTION_DISTRICT_FIPS_RANGE: range = range(2001, 2041)


def default_raw_path() -> Path:
    """Return the default staged MEDSL raw file path."""
    return raw_root() / "medsl" / "countypres_2000-2024.tab"


def default_output_path(output_dir: Path | str | None = None) -> Path:
    """Return the default curated output path for MEDSL county returns."""
    base = curated_dir("medsl") if output_dir is None else Path(output_dir)
    return base / "medsl_county_presidential_returns__Y2000-2024.parquet"


def parse_county_presidential_returns(
    raw_path: Path | str,
    *,
    expected_years: tuple[int, ...] | None = EXPECTED_PRESIDENTIAL_YEARS,
) -> pd.DataFrame:
    """Parse MEDSL county presidential returns into canonical candidate rows.

    The MEDSL county file includes voting-mode rows as well as county totals.
    This parser keeps only ``mode == "TOTAL"`` rows for US presidential office
    so downstream county-year measures do not double count votes.
    """
    path = Path(raw_path)
    df = pd.read_csv(path, sep=_separator_for(path), dtype={"county_fips": "string"})
    _require_columns(df, MEDSL_COUNTY_PRESIDENTIAL_COLUMNS)

    df = df.copy()
    df["office"] = _normalize_text(df["office"])
    df["mode"] = _normalize_text(df["mode"])
    missing_total_mode_count = _count_presidential_county_years_without_total(df)
    df = df[(df["office"] == "US PRESIDENT") & (df["mode"] == "TOTAL")].copy()
    if df.empty:
        raise ValueError(
            "No MEDSL county presidential TOTAL rows found. "
            "Check that the raw file is countypres_2000-2024.tab."
        )

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    missing_years = df["year"].isna()
    if missing_years.any():
        raise ValueError(f"MEDSL rows contain {int(missing_years.sum())} missing/invalid years.")
    df["year"] = df["year"].astype(int)
    _validate_presidential_years(df["year"], expected_years)

    for column in ("candidatevotes", "totalvotes"):
        df[column] = pd.to_numeric(df[column], errors="coerce")
    _validate_vote_columns(df)
    missing_candidatevotes_count = int(df["candidatevotes"].isna().sum())
    if missing_candidatevotes_count:
        logger.info(
            "Filling %s MEDSL candidatevotes values with zero after totalvotes validation.",
            missing_candidatevotes_count,
        )
        df["candidatevotes"] = df["candidatevotes"].fillna(0)

    df["county_fips"] = df["county_fips"].astype("string").str.strip()
    missing_fips = df["county_fips"].isna() | (df["county_fips"] == "")
    if missing_fips.any():
        missing_rows = df.loc[missing_fips].copy()
        known_noncounty = _known_noncounty_missing_fips_mask(missing_rows)
        unknown_missing = missing_rows.loc[~known_noncounty]
        if not unknown_missing.empty:
            examples = unknown_missing[["state_po", "county_name", "year"]].head(5)
            raise ValueError(
                "MEDSL rows missing county_fips cannot be assigned to county geography. "
                f"Examples: {examples.to_dict('records')}"
            )
        logger.info(
            "Dropping %s known non-county MEDSL rows with missing county_fips.",
            int(missing_fips.sum()),
        )
        df = df[~missing_fips].copy()
    dropped_missing_fips_count = int(missing_fips.sum())

    invalid_raw_fips = df["county_fips"].str.len() > 5
    if invalid_raw_fips.any():
        invalid_rows = df.loc[invalid_raw_fips].copy()
        known_noncounty = _known_noncounty_invalid_fips_mask(invalid_rows)
        unknown_invalid = invalid_rows.loc[~known_noncounty]
        if not unknown_invalid.empty:
            examples = unknown_invalid[["state_po", "county_name", "county_fips"]].head(5)
            raise ValueError(
                "MEDSL county_fips values must be county-level FIPS codes. "
                f"Examples: {examples.to_dict('records')}"
            )
        logger.info(
            "Dropping %s known non-county MEDSL rows with invalid county_fips.",
            int(invalid_raw_fips.sum()),
        )
        df = df[~invalid_raw_fips].copy()
    dropped_invalid_fips_count = int(invalid_raw_fips.sum())

    df["county_fips"] = _normalize_county_fips(df["county_fips"])
    invalid_fips = df["county_fips"].str.len() != 5
    if invalid_fips.any():
        examples = df.loc[invalid_fips, ["state_po", "county_name", "county_fips"]].head(5)
        raise ValueError(
            "MEDSL county_fips values must normalize to five characters. "
            f"Examples: {examples.to_dict('records')}"
        )

    alaska_district_rows = _alaska_election_district_mask(df)
    if alaska_district_rows.any():
        logger.info(
            "Dropping %s Alaska MEDSL election-district rows from county-native output.",
            int(alaska_district_rows.sum()),
        )
        df = df[~alaska_district_rows].copy()
    dropped_alaska_district_count = int(alaska_district_rows.sum())

    ingested_at = datetime.now(UTC)
    raw_sha256 = _sha256(path)
    result = pd.DataFrame(
        {
            "geo_type": "county",
            "geo_id": df["county_fips"],
            "county_fips": df["county_fips"],
            "state": _normalize_text(df["state"]),
            "state_po": df["state_po"].astype("string").str.upper().str.strip(),
            "county_name": _normalize_text(df["county_name"]),
            "year": df["year"],
            "office": df["office"],
            "candidate": _normalize_text(df["candidate"]),
            "party": _normalize_party(df["party"]),
            "party_simplified": _simplify_party(df["party"]),
            "candidatevotes": df["candidatevotes"].astype("int64"),
            "totalvotes": df["totalvotes"].astype("int64"),
            "source_version": df["version"].astype("string"),
            "data_source": "medsl",
            "source_url": MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API,
            "source_doi": MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI,
            "raw_sha256": raw_sha256,
            "ingested_at": ingested_at,
        }
    )
    result = result[MEDSL_COUNTY_PRESIDENTIAL_OUTPUT_COLUMNS]
    result.attrs["missing_candidatevotes_filled"] = missing_candidatevotes_count
    result.attrs["missing_county_fips_dropped"] = dropped_missing_fips_count
    result.attrs["invalid_county_fips_dropped"] = dropped_invalid_fips_count
    result.attrs["alaska_election_district_rows_dropped"] = dropped_alaska_district_count
    result.attrs["presidential_county_years_without_total"] = missing_total_mode_count
    return result.sort_values(["county_fips", "year", "party", "candidate"]).reset_index(drop=True)


def ingest_county_presidential_returns(
    raw_path: Path | str | None = None,
    *,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    force: bool = False,
    expected_years: tuple[int, ...] | None = EXPECTED_PRESIDENTIAL_YEARS,
) -> Path:
    """Parse staged MEDSL county presidential returns and write curated parquet."""
    source_path = default_raw_path() if raw_path is None else Path(raw_path)
    if not source_path.exists():
        raise FileNotFoundError(
            f"MEDSL raw file not found at {source_path}. "
            "Download County Presidential Election Returns 2000-2024 from "
            f"{MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI} and stage it there, or pass --raw-path."
        )

    destination = Path(output_path) if output_path is not None else default_output_path(output_dir)
    if destination.exists() and not force:
        return destination

    df = parse_county_presidential_returns(source_path, expected_years=expected_years)
    destination.parent.mkdir(parents=True, exist_ok=True)

    raw_sha256 = str(df["raw_sha256"].iloc[0])
    file_size = source_path.stat().st_size
    register_source(
        source_type="medsl",
        source_url=MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API,
        source_name="MEDSL County Presidential Election Returns 2000-2024",
        raw_sha256=raw_sha256,
        file_size=file_size,
        local_path=str(source_path),
        metadata={
            "doi": MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI,
            "dataset": "County Presidential Election Returns 2000-2024",
            "native_geometry": "county",
            "years": (
                list(expected_years)
                if expected_years is not None
                else sorted(df["year"].unique())
            ),
            "mode_filter": "TOTAL",
            "office_filter": "US PRESIDENT",
            "curated_path": str(destination),
        },
    )

    provenance = ProvenanceBlock(
        extra={
            "dataset": "medsl_county_presidential_returns",
            "source": "MIT Election Data and Science Lab",
            "source_url": MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DATAVERSE_API,
            "source_doi": MEDSL_COUNTY_PRESIDENTIAL_RETURNS_DOI,
            "raw_sha256": raw_sha256,
            "row_count": len(df),
            "county_count": int(df["county_fips"].nunique()),
            "year_range": [int(df["year"].min()), int(df["year"].max())],
            "years": sorted(int(year) for year in df["year"].unique()),
            "missing_candidatevotes_filled": int(df.attrs.get("missing_candidatevotes_filled", 0)),
            "missing_county_fips_dropped": int(df.attrs.get("missing_county_fips_dropped", 0)),
            "invalid_county_fips_dropped": int(df.attrs.get("invalid_county_fips_dropped", 0)),
            "alaska_election_district_rows_dropped": int(
                df.attrs.get("alaska_election_district_rows_dropped", 0)
            ),
            "presidential_county_years_without_total": int(
                df.attrs.get("presidential_county_years_without_total", 0)
            ),
            "office_filter": "US PRESIDENT",
            "mode_filter": "TOTAL",
            "license": "CC0 1.0",
        }
    )
    write_parquet_with_provenance(df, destination, provenance)
    return destination


def _separator_for(path: Path) -> str:
    return "\t" if path.suffix.lower() == ".tab" else ","


def _require_columns(df: pd.DataFrame, required: tuple[str, ...]) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise ValueError(
            "MEDSL county presidential file is missing required columns: "
            f"{missing}. Expected columns include: {list(required)}."
        )


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip().str.upper()


def _normalize_party(series: pd.Series) -> pd.Series:
    party = _normalize_text(series)
    return party.mask(party == "", "UNKNOWN")


def _count_presidential_county_years_without_total(df: pd.DataFrame) -> int:
    presidential = df[df["office"] == "US PRESIDENT"].copy()
    if presidential.empty:
        return 0
    group_cols = ["state_po", "county_name", "county_fips", "year"]
    has_total = presidential.groupby(group_cols, dropna=False)["mode"].agg(
        lambda modes: (modes == "TOTAL").any()
    )
    return int((~has_total).sum())


def _known_noncounty_missing_fips_mask(df: pd.DataFrame) -> pd.Series:
    labels = list(
        zip(
            df["state_po"].astype("string"),
            _normalize_text(df["county_name"]),
            strict=True,
        )
    )
    return pd.Series(
        [label in KNOWN_NONCOUNTY_MISSING_FIPS for label in labels],
        index=df.index,
    )


def _known_noncounty_invalid_fips_mask(df: pd.DataFrame) -> pd.Series:
    labels = list(
        zip(
            df["state_po"].astype("string"),
            _normalize_text(df["county_name"]),
            df["county_fips"].astype("string"),
            strict=True,
        )
    )
    return pd.Series(
        [label in KNOWN_NONCOUNTY_INVALID_FIPS for label in labels],
        index=df.index,
    )


def _alaska_election_district_mask(df: pd.DataFrame) -> pd.Series:
    county_numbers = pd.to_numeric(df["county_fips"], errors="coerce")
    return (
        df["state_po"].astype("string").str.upper().str.strip().eq("AK")
        & _normalize_text(df["county_name"]).str.match(r"^DISTRICT \d+$", na=False)
        & county_numbers.isin(ALASKA_ELECTION_DISTRICT_FIPS_RANGE)
    )


def _simplify_party(series: pd.Series) -> pd.Series:
    party = _normalize_party(series)
    return party.map(
        lambda value: PARTY_SIMPLIFICATION.get(
            str(value),
            "OTHER" if value != "UNKNOWN" else "UNKNOWN",
        )
    )


def _normalize_county_fips(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.isna().any():
        bad = series[numeric.isna()].head(5).tolist()
        raise ValueError(f"MEDSL county_fips contains non-numeric values: {bad}")
    return numeric.astype("Int64").astype("string").str.zfill(5)


def _validate_vote_columns(df: pd.DataFrame) -> None:
    missing_totalvotes = df["totalvotes"].isna()
    if missing_totalvotes.any():
        examples = df.loc[
            missing_totalvotes,
            ["state_po", "county_name", "year", "candidate"],
        ].head(5)
        raise ValueError(
            "MEDSL totalvotes contains missing/non-numeric values. "
            f"Examples: {examples.to_dict('records')}"
        )
    for column in ("candidatevotes", "totalvotes"):
        negative = df[column] < 0
        if negative.any():
            examples = df.loc[negative, ["state_po", "county_name", "year", "candidate"]].head(5)
            raise ValueError(
                f"MEDSL {column} contains negative values. Examples: {examples.to_dict('records')}"
            )


def _validate_presidential_years(
    years: pd.Series,
    expected_years: tuple[int, ...] | None,
) -> None:
    observed = set(int(year) for year in years.unique())
    non_presidential = sorted(year for year in observed if year % 4 != 0)
    if non_presidential:
        raise ValueError(
            "MEDSL presidential returns include non-presidential years: "
            f"{non_presidential}"
        )
    if expected_years is None:
        return
    expected = set(expected_years)
    missing = sorted(expected - observed)
    unexpected = sorted(observed - expected)
    if missing or unexpected:
        raise ValueError(
            "MEDSL presidential year coverage mismatch. "
            f"Missing expected years: {missing}; unexpected years: {unexpected}."
        )


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()
