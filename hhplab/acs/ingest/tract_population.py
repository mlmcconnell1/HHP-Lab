"""ACS tract-level data fetcher.

Fetches and caches tract-level ACS data from the Census Bureau API.
Retrieves population (B01003), income (B19013), rent (B25064), poverty
(C17002), tenure (B25003), and age/sex (B01001) variables in a single
pass per state, then computes derived columns (adult_population,
population_below_poverty).

Usage
-----
    from hhplab.acs.ingest.tract_population import ingest_tract_data

    path = ingest_tract_data(
        acs_vintage="2019-2023",
        tract_vintage="2023",
    )

Output Schema
-------------
- tract_geoid (str): Census tract GEOID (11 chars, e.g., "08031001000")
- acs_vintage (str): e.g., "2019-2023"
- tract_vintage (str): e.g., "2023"
- total_population (Int64)
- moe_total_population (Float64)
- adult_population (Int64): derived from B01001 age 18+ groups
- total_households (Int64)
- owner_households (Int64)
- renter_households (Int64)
- median_household_income (Float64)
- median_gross_rent (Float64)
- poverty_universe (Int64)
- below_50pct_poverty (Int64)
- 50_to_99pct_poverty (Int64)
- population_below_poverty (Int64): derived (below_50 + 50_to_99)
- data_source (str): always "acs_5yr"
- source_ref (str): dataset identifier
- ingested_at (datetime UTC)
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

import hhplab.naming as naming
from hhplab.acs.translate import (
    get_source_tract_vintage,
    needs_translation,
    translate_acs_to_target_vintage,
)
from hhplab.acs.variables import (
    ACS5_SAE_DENOMINATOR_COLUMNS,
    ACS5_SAE_SUPPORT_COLUMNS,
    ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE,
    ACS5_SAE_SUPPORT_OUTPUT_COLUMNS,
    ACS5_SAE_SUPPORT_TABLES,
    ACS_VARIABLES,
    ADULT_VARS,
    ALL_API_VARS,
    COUNT_COLUMNS,
    MEDIAN_COLUMNS,
    MOE_COLUMNS,
    TRACT_OUTPUT_COLUMNS,
    api_vars_for_year,
    tables_for_api_vars,
)
from hhplab.paths import curated_dir
from hhplab.provenance import (
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)
from hhplab.raw_snapshot import write_api_snapshot
from hhplab.source_registry import check_source_changed, register_source
from hhplab.sources import CENSUS_API_ACS5

logger = logging.getLogger(__name__)

# Census Bureau API endpoint for ACS 5-year estimates
CENSUS_API = CENSUS_API_ACS5

ACS_API_VARIABLE_CHUNK_SIZE = 45

# US State and territory FIPS codes
STATE_FIPS_CODES = [
    "01", "02", "04", "05", "06", "08", "09", "10", "11", "12",
    "13", "15", "16", "17", "18", "19", "20", "21", "22", "23",
    "24", "25", "26", "27", "28", "29", "30", "31", "32", "33",
    "34", "35", "36", "37", "38", "39", "40", "41", "42", "44",
    "45", "46", "47", "48", "49", "50", "51", "53", "54", "55",
    "56",
    "72",  # Puerto Rico
]


def _chunks(values: list[str], size: int) -> list[list[str]]:
    """Split Census API variables into request-sized chunks."""
    return [values[index : index + size] for index in range(0, len(values), size)]


def _translation_metadata(
    acs_vintage: str,
    tract_vintage: str,
) -> dict[str, object]:
    """Return source/target tract metadata for an ACS ingest request."""
    source_tract_vintage = get_source_tract_vintage(acs_vintage)
    translation_required = needs_translation(acs_vintage, tract_vintage)
    return {
        "source_tract_vintage": source_tract_vintage,
        "target_tract_vintage": int(tract_vintage),
        "translation_applied": translation_required,
    }


def _cached_translation_matches_request(
    output_path: Path,
    acs_vintage: str,
    tract_vintage: str,
) -> bool:
    """Return whether an existing cache satisfies this translation request."""
    translation = _translation_metadata(acs_vintage, tract_vintage)
    if not translation["translation_applied"]:
        return True

    provenance = read_provenance(output_path)
    if provenance is None:
        return False

    extra = provenance.extra or {}
    return (
        provenance.acs_vintage == acs_vintage
        and str(provenance.tract_vintage) == str(tract_vintage)
        and extra.get("translation_applied") is True
        and str(extra.get("source_tract_vintage")) == str(translation["source_tract_vintage"])
        and str(extra.get("target_tract_vintage")) == str(translation["target_tract_vintage"])
    )


def parse_acs_vintage(acs_vintage: str) -> int:
    """Parse ACS vintage string to extract the end year.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023" or "2023".

    Returns
    -------
    int
        The end year of the ACS vintage period.

    Raises
    ------
    ValueError
        If the vintage string cannot be parsed.

    Examples
    --------
    >>> parse_acs_vintage("2019-2023")
    2023
    >>> parse_acs_vintage("2023")
    2023
    """
    if "-" in acs_vintage:
        match = re.match(r"^(\d{4})-(\d{4})$", acs_vintage)
        if not match:
            raise ValueError(
                f"Invalid ACS vintage format: {acs_vintage!r}. "
                f"Expected format like '2019-2023' or '2023'"
            )
        start_year, end_year = int(match.group(1)), int(match.group(2))
        if end_year - start_year != 4:
            raise ValueError(
                f"Invalid ACS vintage range: {acs_vintage!r}. "
                f"5-year estimates should span exactly 4 years (e.g., 2019-2023)"
            )
    else:
        try:
            end_year = int(acs_vintage)
        except ValueError as e:
            raise ValueError(
                f"Invalid ACS vintage format: {acs_vintage!r}. "
                f"Expected format like '2019-2023' or '2023'"
            ) from e

    if end_year < 2009:
        raise ValueError(
            f"ACS 5-year estimates are not available before vintage 2009 "
            f"(covering 2005-2009). Got vintage end year {end_year} "
            f"from {acs_vintage!r}"
        )

    return end_year


def normalize_geoid(state: str, county: str, tract: str) -> str:
    """Normalize Census GEOID to 11-character format.

    Parameters
    ----------
    state : str
        2-digit state FIPS code.
    county : str
        3-digit county FIPS code.
    tract : str
        6-digit census tract code.

    Returns
    -------
    str
        11-character GEOID (e.g., "08031001000").
    """
    state_str = str(state).zfill(2)
    county_str = str(county).zfill(3)
    tract_str = str(tract).zfill(6)
    return f"{state_str}{county_str}{tract_str}"


def _resolve_single_value(
    df: pd.DataFrame,
    column: str,
    explicit_value: str | None,
    label: str,
) -> str:
    if explicit_value is not None:
        return str(explicit_value)
    if column in df.columns and df[column].notna().any():
        values = set(df[column].dropna().astype(str))
        if len(values) != 1:
            raise ValueError(
                f"ACS5 tract SAE support normalization requires one {label}. "
                f"Found values: {sorted(values)}."
            )
        return values.pop()
    raise ValueError(
        f"ACS5 tract SAE support normalization requires {label} either as an "
        f"argument or as an input column."
    )


def _normalize_support_tract_geoid(df: pd.DataFrame) -> pd.Series:
    if "tract_geoid" in df.columns:
        return df["tract_geoid"].astype("string").str.zfill(11)
    if {"state", "county", "tract"}.issubset(df.columns):
        return (
            df["state"].astype("string").str.zfill(2)
            + df["county"].astype("string").str.zfill(3)
            + df["tract"].astype("string").str.zfill(6)
        )
    raise ValueError(
        "ACS5 tract SAE support normalization requires tract_geoid or state, "
        "county, and tract columns."
    )


def _missing_support_tables(row: pd.Series) -> str:
    missing_tables = [
        table
        for table, columns in ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE.items()
        if row[columns].isna().all()
    ]
    return json.dumps(missing_tables, sort_keys=True)


def _zero_denominator_columns(row: pd.Series) -> str:
    zero_columns = [
        column
        for column in ACS5_SAE_DENOMINATOR_COLUMNS
        if column in row.index and pd.notna(row[column]) and row[column] == 0
    ]
    return json.dumps(zero_columns, sort_keys=True)


def normalize_acs5_tract_sae_support(
    df: pd.DataFrame,
    *,
    acs_vintage: str | None = None,
    tract_vintage: str | None = None,
) -> pd.DataFrame:
    """Normalize ACS5 tract distributions into the SAE support contract."""
    resolved_acs_vintage = _resolve_single_value(df, "acs_vintage", acs_vintage, "acs_vintage")
    resolved_tract_vintage = _resolve_single_value(
        df,
        "tract_vintage",
        tract_vintage,
        "tract_vintage",
    )
    if any(column.startswith("B") and column.endswith("E") for column in df.columns):
        result = df.rename(columns=ACS_VARIABLES).copy()
    else:
        result = df.copy()

    tract_geoid = _normalize_support_tract_geoid(result)
    normalized = pd.DataFrame(
        {
            "tract_geoid": tract_geoid,
            "county_fips": tract_geoid.str.slice(0, 5),
            "acs_vintage": resolved_acs_vintage,
            "tract_vintage": resolved_tract_vintage,
        }
    )

    for column in ACS5_SAE_SUPPORT_COLUMNS:
        if column in result.columns:
            normalized[column] = pd.to_numeric(result[column], errors="coerce")
            normalized.loc[normalized[column] < 0, column] = pd.NA
        else:
            normalized[column] = pd.NA
        normalized[column] = normalized[column].astype("Int64")

    column_table_map = {
        column: table
        for table, columns in ACS5_SAE_SUPPORT_COLUMNS_BY_TABLE.items()
        for column in columns
    }
    normalized["sae_support_tables"] = json.dumps(ACS5_SAE_SUPPORT_TABLES, sort_keys=True)
    normalized["sae_missing_support_tables"] = normalized.apply(_missing_support_tables, axis=1)
    normalized["sae_zero_denominator_columns"] = normalized.apply(
        _zero_denominator_columns,
        axis=1,
    )
    normalized["sae_support_column_tables"] = json.dumps(column_table_map, sort_keys=True)

    normalized = normalized[ACS5_SAE_SUPPORT_OUTPUT_COLUMNS].copy()
    normalized = normalized.sort_values("tract_geoid").reset_index(drop=True)
    normalized.attrs["acs_vintage"] = resolved_acs_vintage
    normalized.attrs["tract_vintage"] = resolved_tract_vintage
    normalized.attrs["sae_support_tables"] = ACS5_SAE_SUPPORT_TABLES
    normalized.attrs["sae_support_column_tables"] = column_table_map
    return normalized


def load_acs5_tract_sae_support(
    path: str | Path,
    *,
    acs_vintage: str | None = None,
    tract_vintage: str | None = None,
) -> pd.DataFrame:
    """Load a curated ACS5 tract artifact as normalized SAE support data."""
    df = pd.read_parquet(path)
    return normalize_acs5_tract_sae_support(
        df,
        acs_vintage=acs_vintage,
        tract_vintage=tract_vintage,
    )


def fetch_state_tract_data(
    year: int,
    state_fips: str,
    api_vars: list[str] | None = None,
) -> tuple[pd.DataFrame, bytes]:
    """Fetch all ACS tract-level data for a single state.

    Fetches population, income, rent, poverty, and age variables in a single
    API call, then computes derived columns (adult_population,
    population_below_poverty).

    Parameters
    ----------
    year : int
        ACS 5-year estimate end year (e.g., 2023 for 2019-2023 estimates).
    state_fips : str
        Two-digit state FIPS code (e.g., "06" for California).

    Returns
    -------
    tuple[pd.DataFrame, bytes]
        Tuple of (DataFrame with tract data, raw response content).

    Raises
    ------
    httpx.HTTPStatusError
        If the Census API request fails.
    """
    url = CENSUS_API.format(year=year)
    if api_vars is None:
        api_vars = api_vars_for_year(year)
    frames: list[pd.DataFrame] = []
    raw_parts: list[bytes] = []
    merge_columns = ["NAME", "state", "county", "tract"]

    with httpx.Client(timeout=60.0) as client:
        for chunk in _chunks(api_vars, ACS_API_VARIABLE_CHUNK_SIZE):
            variables = ",".join(chunk)
            params = {
                "get": f"NAME,{variables}",
                "for": "tract:*",
                "in": f"state:{state_fips}",
            }

            response = client.get(url, params=params)
            response.raise_for_status()
            raw_parts.append(response.content)
            data = response.json()

            headers = data[0]
            rows = data[1:]
            chunk_df = pd.DataFrame(rows, columns=headers)
            keep = [column for column in [*merge_columns, *chunk] if column in chunk_df.columns]
            frames.append(chunk_df[keep].copy())

    df = frames[0]
    for frame in frames[1:]:
        df = df.merge(frame, on=merge_columns, how="inner")

    # Build GEOID from state, county, tract
    df["tract_geoid"] = df.apply(
        lambda row: normalize_geoid(row["state"], row["county"], row["tract"]),
        axis=1,
    )

    # Convert all numeric columns; Census uses negative values for missing
    for var_code in api_vars:
        if var_code in df.columns:
            df[var_code] = pd.to_numeric(df[var_code], errors="coerce")
            df.loc[df[var_code] < 0, var_code] = pd.NA

    # Rename base ACS variables to friendly names
    df = df.rename(columns=ACS_VARIABLES)

    # Derive adult_population (18+) by summing B01001 age groups
    adult_cols_in_df = [c for c in ADULT_VARS if c in df.columns]
    if adult_cols_in_df:
        df["adult_population"] = df[adult_cols_in_df].fillna(0).sum(axis=1)
        all_na = df[adult_cols_in_df].isna().all(axis=1)
        df.loc[all_na, "adult_population"] = pd.NA
    else:
        df["adult_population"] = pd.NA

    # Derive population_below_poverty
    if "below_50pct_poverty" in df.columns and "50_to_99pct_poverty" in df.columns:
        df["population_below_poverty"] = (
            df["below_50pct_poverty"].fillna(0) + df["50_to_99pct_poverty"].fillna(0)
        )
        both_na = df["below_50pct_poverty"].isna() & df["50_to_99pct_poverty"].isna()
        df.loc[both_na, "population_below_poverty"] = pd.NA
    else:
        df["population_below_poverty"] = pd.NA

    # Select output columns (only those present)
    keep = ["tract_geoid", *COUNT_COLUMNS, *MEDIAN_COLUMNS, *MOE_COLUMNS]
    keep = [c for c in keep if c in df.columns]

    raw_bundle = json.dumps([json.loads(part) for part in raw_parts]).encode()
    return df[keep].copy(), raw_bundle


def fetch_tract_data(
    acs_vintage: str,
    tract_vintage: str,
    raw_root: Path | None = None,
) -> tuple[pd.DataFrame, str, int, Path | None]:
    """Fetch tract-level ACS data for all US states and territories.

    Raw API responses are persisted under ``data/raw/acs5_tract/<snapshot_id>/``.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023".
    tract_vintage : str
        Census tract geography vintage (e.g., "2023").
    raw_root : Path, optional
        Override the default raw data root (for testing).

    Returns
    -------
    tuple[pd.DataFrame, str, int, Path | None]
        Tuple of (DataFrame, SHA-256 hash, content size, raw snapshot dir).

    Raises
    ------
    ValueError
        If no tract data could be fetched from any state.
    """
    year = parse_acs_vintage(acs_vintage)
    ingested_at = datetime.now(UTC)
    translation = _translation_metadata(acs_vintage, tract_vintage)
    api_vars = api_vars_for_year(year)
    tables = tables_for_api_vars(api_vars)

    logger.info(f"Fetching ACS {acs_vintage} tract data (API year: {year})")

    dfs: list[pd.DataFrame] = []
    all_raw_content: list[bytes] = []
    for state_fips in STATE_FIPS_CODES:
        try:
            df, raw_content = fetch_state_tract_data(year, state_fips, api_vars)
            dfs.append(df)
            all_raw_content.append(raw_content)
            logger.debug(f"Fetched {len(df)} tracts for state {state_fips}")
        except httpx.HTTPStatusError as e:
            logger.warning(f"Failed to fetch data for state {state_fips}: {e}")
            continue
        except Exception as e:
            logger.warning(f"Unexpected error for state {state_fips}: {e}")
            continue

    if not dfs:
        raise ValueError("No ACS tract data could be fetched from any state")

    # Persist raw API snapshot
    source_url = CENSUS_API.format(year=year)
    snap_dir, content_sha256, content_size = write_api_snapshot(
        all_raw_content,
        "acs5_tract",
        year=year,
        variant="full",
        request_metadata={
            "url": source_url,
            "params": {
                "get": f"NAME,{','.join(api_vars)}",
                "for": "tract:*",
                "in": "state:{fips}",
            },
            "tables": tables,
            "variables": api_vars,
            "acs_vintage": acs_vintage,
        },
        record_count=sum(len(df) for df in dfs),
        raw_root=raw_root,
    )

    # Combine all states
    result = pd.concat(dfs, ignore_index=True)

    if translation["translation_applied"]:
        result, _ = translate_acs_to_target_vintage(
            result,
            acs_vintage,
            tract_vintage,
        )

    # Add metadata columns
    result["acs_vintage"] = acs_vintage
    result["tract_vintage"] = tract_vintage
    result["data_source"] = "acs_5yr"
    result["source_ref"] = f"census_api/acs/acs5/{year}/{'+'.join(tables)}"
    result["ingested_at"] = ingested_at

    for col in TRACT_OUTPUT_COLUMNS:
        if col not in result.columns:
            result[col] = pd.NA

    # Ensure proper column types
    result["tract_geoid"] = result["tract_geoid"].astype(str)
    for col in COUNT_COLUMNS:
        if col in result.columns:
            # Translation can yield fractional expected counts after area weighting.
            # Round back to the canonical nullable-integer schema used by ACS tract files.
            result[col] = pd.to_numeric(result[col], errors="coerce").round().astype("Int64")

    for col in [*MOE_COLUMNS, *MEDIAN_COLUMNS]:
        if col in result.columns:
            result[col] = result[col].astype("Float64")

    # Reorder columns to canonical order
    col_order = [c for c in TRACT_OUTPUT_COLUMNS if c in result.columns]
    result = result[col_order]

    logger.info(f"Fetched ACS data for {len(result)} tracts")
    return result, content_sha256, content_size, snap_dir


def get_output_path(
    acs_vintage: str,
    tract_vintage: str,
    base_dir: Path | str | None = None,
) -> Path:
    """Get the canonical output path for ACS tract data.

    Uses temporal shorthand naming: acs5_tracts__A{year}xT{tract}.parquet

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string (e.g., "2019-2023").
    tract_vintage : str
        Tract geography vintage (e.g., "2023").
    base_dir : Path or str, optional
        Base directory for output. Defaults to 'data/curated/acs'.

    Returns
    -------
    Path
        Output path like 'data/curated/acs/acs5_tracts__A2023xT2023.parquet'.
    """
    if base_dir is None:
        base_dir = curated_dir("acs")
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.acs5_tracts_filename(acs_vintage, tract_vintage)


def ingest_tract_data(
    acs_vintage: str,
    tract_vintage: str,
    force: bool = False,
    output_dir: Path | str | None = None,
    raw_root: Path | None = None,
) -> Path:
    """Fetch and cache full ACS tract-level data.

    Downloads tract data from the Census Bureau API (tables B01003, B01001,
    B19013, B25064, C17002) and saves as a Parquet file with provenance
    metadata.

    Parameters
    ----------
    acs_vintage : str
        ACS vintage string like "2019-2023".
    tract_vintage : str
        Census tract geography vintage (e.g., "2023").
    force : bool, optional
        If True, re-fetch even if cached file exists. Default is False.
    output_dir : Path or str, optional
        Output directory. Defaults to 'data/curated/acs'.
    raw_root : Path, optional
        Override the default raw data root (for testing).

    Returns
    -------
    Path
        Path to the output Parquet file.
    """
    output_path = get_output_path(acs_vintage, tract_vintage, output_dir)
    translation = _translation_metadata(acs_vintage, tract_vintage)

    if output_path.exists() and not force:
        if _cached_translation_matches_request(output_path, acs_vintage, tract_vintage):
            logger.info(f"Using cached file: {output_path}")
            return output_path
        logger.info(
            "Refreshing cached ACS tract file %s because it predates translated "
            "tract-vintage provenance for %s -> T%s.",
            output_path,
            acs_vintage,
            tract_vintage,
        )

    df, content_sha256, content_size, snap_dir = fetch_tract_data(
        acs_vintage, tract_vintage, raw_root=raw_root,
    )

    year = parse_acs_vintage(acs_vintage)
    api_vars = api_vars_for_year(year)
    tables = tables_for_api_vars(api_vars)
    source_url = f"{CENSUS_API.format(year=year)}?tables={'+'.join(tables)}"

    changed, details = check_source_changed(
        source_type="acs5_tract",
        source_url=source_url,
        current_sha256=content_sha256,
    )

    if changed:
        logger.warning(
            f"UPSTREAM DATA CHANGED: ACS tract data for {acs_vintage} has changed! "
            f"Previous hash: {details['previous_sha256'][:16]}... "
            f"Current hash: {content_sha256[:16]}... "
            f"Last ingested: {details['previous_ingested_at']}"
        )
    elif details.get("is_new"):
        logger.info(f"First time tracking ACS tract data {acs_vintage} in source registry")

    provenance = ProvenanceBlock(
        acs_vintage=acs_vintage,
        tract_vintage=tract_vintage,
        extra={
            "dataset": "acs5_tract_data",
            "tables": tables,
            "variables": api_vars,
            "unavailable_variables": sorted(set(ALL_API_VARS) - set(api_vars)),
            "api_year": year,
            "retrieved_at": datetime.now(UTC).isoformat(),
            "row_count": len(df),
            "raw_sha256": content_sha256,
            **translation,
        },
    )

    write_parquet_with_provenance(df, output_path, provenance)
    logger.info(f"Wrote ACS tract data to {output_path}")

    register_source(
        source_type="acs5_tract",
        source_url=source_url,
        source_name=f"ACS Tract Data {acs_vintage}",
        raw_sha256=content_sha256,
        file_size=content_size,
        local_path=str(snap_dir) if snap_dir else "",
        metadata={
            "acs_vintage": acs_vintage,
            "tract_vintage": tract_vintage,
            **translation,
            "tables": tables,
            "variables": api_vars,
            "unavailable_variables": sorted(set(ALL_API_VARS) - set(api_vars)),
            "row_count": len(df),
            "curated_path": str(output_path),
        },
    )

    return output_path
