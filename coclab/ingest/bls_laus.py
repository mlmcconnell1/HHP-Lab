"""BLS LAUS yearly metro ingest.

Fetches annual-average Local Area Unemployment Statistics (LAUS) data for
the 25 Glynn/Fox metropolitan areas from the BLS Public API v2, and writes
a curated Parquet file with provenance metadata.

The BLS LAUS API returns monthly values plus an annual average ("M13") when
``annualaverage: true`` is requested.  This module extracts only the annual
average rows.

Usage
-----
    from coclab.ingest.bls_laus import ingest_laus_metro

    path = ingest_laus_metro(year=2023)

Output schema
-------------
- metro_id (str): Glynn/Fox metro identifier (e.g., "GF01")
- metro_name (str): Metro area name
- definition_version (str): e.g., "glynn_fox_v1"
- year (int): Reference year for the annual-average data
- cbsa_code (str): 5-digit CBSA code for traceability
- labor_force (Int64): Civilian labor force count
- employed (Int64): Employed persons count
- unemployed (Int64): Unemployed persons count
- unemployment_rate (Float64): Unemployment rate (percent, e.g., 3.5 for 3.5%)
- data_source (str): always "bls_laus"
- series_ids (str): JSON-encoded dict of series IDs fetched
- source_ref (str): BLS LAUS home page URL
- ingested_at (datetime UTC)

Rate limits (BLS API v2)
------------------------
- 500 queries/day without a registration key
- 25 series IDs per request maximum (anonymous); 50 with a registration key
- 20-year window per request maximum

With 25 metros × 4 measures = 100 series IDs, this module issues four
anonymous requests of 25 series each, or two requests of 50 when a
registration key is provided.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

from coclab import naming
from coclab.metro.definitions import (
    METRO_CBSA_MAPPING,
    METRO_STATE_FIPS,
    metro_name_for_id,
)
from coclab.metro.laus import (
    BLS_ANNUAL_AVERAGE_PERIOD,
    LAUS_MEASURE_CODES,
    LAUS_METRO_OUTPUT_COLUMNS,
    build_laus_series_id,
)
from coclab.provenance import ProvenanceBlock, write_parquet_with_provenance
from coclab.sources import BLS_API_REGISTRATION_URL, BLS_API_V2, BLS_LAUS_SOURCE_REF

logger = logging.getLogger(__name__)

# Maximum series IDs per BLS API request.
# The BLS public API silently truncates anonymous requests to 25 series.
# A registration key raises the cap to 50.
_BLS_ANON_MAX_SERIES_PER_REQUEST: int = 25
_BLS_KEY_MAX_SERIES_PER_REQUEST: int = 50

# Substrings the BLS API uses in its message[] array when the daily query
# threshold is hit.  Detected case-insensitively against any message string
# regardless of the request status field — BLS sometimes returns
# REQUEST_NOT_PROCESSED, sometimes REQUEST_FAILED, with the same threshold
# wording in the message body.
_BLS_QUOTA_MESSAGE_TOKENS: tuple[str, ...] = (
    "daily threshold",
    "daily query limit",
    "threshold for total number of requests",
    "queries reached",
    "throttle",
)


class BlsQuotaExhausted(RuntimeError):
    """Raised when the BLS Public API rejects a request due to its daily query threshold.

    Carries an actionable message telling the caller how to recover: supply a
    BLS registration key (raising the daily threshold from the anonymous limit)
    or wait until the threshold resets at midnight US Eastern time.
    """


def _is_bls_quota_response(status: str | None, messages: list[str]) -> bool:
    """Return True if a BLS API response indicates the daily query threshold was hit."""
    haystack = " ".join(str(m) for m in messages).lower()
    if any(token in haystack for token in _BLS_QUOTA_MESSAGE_TOKENS):
        return True
    # Some quota responses come back with REQUEST_NOT_PROCESSED and an empty
    # message array; treat that status as quota-exhausted by default since the
    # only documented cause for it on this endpoint is throttling.
    return status == "REQUEST_NOT_PROCESSED"


def _bls_quota_message(*, has_api_key: bool) -> str:
    """Build the actionable message returned with BlsQuotaExhausted."""
    if has_api_key:
        key_hint = (
            "Your registered BLS API key has hit its daily threshold. "
            "Wait for the threshold to reset (midnight US Eastern time) and retry."
        )
    else:
        key_hint = (
            "The anonymous BLS API daily threshold has been reached. "
            "Either register for a free BLS API key at "
            f"{BLS_API_REGISTRATION_URL} and re-run with --api-key <KEY> "
            "(or set BLS_API_KEY in the environment), or wait for the "
            "threshold to reset (midnight US Eastern time) and retry."
        )
    return key_hint


def _build_metro_series_map(
    definition_version: str = "glynn_fox_v1",
) -> dict[str, dict[str, str]]:
    """Build a mapping from metro_id to {measure: series_id} for all metros.

    Series IDs follow the BLS LAUS metro format:
    LA + U + MT + state_fips(2) + cbsa(5) + 000000 + measure(2) = 20 chars.

    Returns
    -------
    dict[str, dict[str, str]]
        Outer key: metro_id; inner key: measure name; value: BLS series ID.
    """
    return {
        metro_id: {
            measure: build_laus_series_id(
                cbsa_code,
                measure,
                METRO_STATE_FIPS[metro_id],
            )
            for measure in LAUS_MEASURE_CODES
        }
        for metro_id, cbsa_code in METRO_CBSA_MAPPING.items()
    }


def _chunked(items: list, size: int):
    """Yield successive chunks of *size* from *items*."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_laus_annual_averages(
    series_ids: list[str],
    year: int,
    api_key: str | None = None,
) -> dict[str, float | int]:
    """Fetch BLS LAUS annual-average values for a list of series IDs.

    Sends one or more POST requests to the BLS API v2 (splitting into
    batches of at most 50 series each).

    Parameters
    ----------
    series_ids : list[str]
        BLS LAUS series IDs to fetch.
    year : int
        Reference year (used as both startyear and endyear).
    api_key : str, optional
        BLS registration key.  Falls back to BLS_API_KEY env var.

    Returns
    -------
    dict[str, float | int]
        Maps series ID to the annual-average value for *year*.
        Series with missing or non-numeric annual averages are omitted.

    Raises
    ------
    httpx.HTTPStatusError
        If the BLS API returns a non-2xx response.
    BlsQuotaExhausted
        If the BLS API rejects the request because the daily query threshold
        has been reached.  Carries an actionable message with recovery steps.
    ValueError
        If the API response cannot be parsed for any other reason.
    """
    if api_key is None:
        api_key = os.environ.get("BLS_API_KEY")
    has_api_key = bool(api_key)

    results: dict[str, float | int] = {}
    batch_size = _BLS_KEY_MAX_SERIES_PER_REQUEST if api_key else _BLS_ANON_MAX_SERIES_PER_REQUEST

    for batch in _chunked(series_ids, batch_size):
        payload: dict = {
            "seriesid": batch,
            "startyear": str(year),
            "endyear": str(year),
            "annualaverage": True,
        }
        if api_key:
            payload["registrationkey"] = api_key

        logger.info(
            "Fetching %d BLS LAUS series for year %d", len(batch), year
        )

        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                BLS_API_V2,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            data = response.json()

        if data.get("status") != "REQUEST_SUCCEEDED":
            status = data.get("status")
            msg = data.get("message", []) or []
            if _is_bls_quota_response(status, msg):
                raise BlsQuotaExhausted(_bls_quota_message(has_api_key=has_api_key))
            raise ValueError(
                f"BLS API request failed (status={status!r}): {msg}"
            )

        for series_data in data.get("Results", {}).get("series", []):
            sid = series_data["seriesID"]
            for obs in series_data.get("data", []):
                if obs.get("period") == BLS_ANNUAL_AVERAGE_PERIOD:
                    raw = obs.get("value")
                    if raw is not None and raw != "-":
                        try:
                            results[sid] = float(raw)
                        except (ValueError, TypeError):
                            logger.warning(
                                "Cannot parse BLS value %r for series %s", raw, sid
                            )

    logger.info(
        "Retrieved annual-average values for %d of %d series",
        len(results),
        len(series_ids),
    )
    return results


def ingest_laus_metro(
    year: int,
    definition_version: str = "glynn_fox_v1",
    project_root: Path | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch BLS LAUS annual-average data for Glynn/Fox metros and write curated Parquet.

    Parameters
    ----------
    year : int
        Reference year (e.g., 2023).  BLS LAUS annual averages are
        typically released a few months after the reference year ends.
    definition_version : str
        Metro definition version (default: "glynn_fox_v1").
    project_root : Path, optional
        Project root for output path resolution. Defaults to current directory.
    api_key : str, optional
        BLS registration key. Falls back to BLS_API_KEY environment variable.
        Without a key, the BLS API allows 500 requests/day.

    Returns
    -------
    Path
        Path to the written Parquet file.

    Raises
    ------
    httpx.HTTPStatusError
        If the BLS API request fails.
    ValueError
        If the API response cannot be parsed or no metros could be populated.
    """
    ingested_at = datetime.now(UTC)

    # Build series map: metro_id -> {measure: series_id}
    metro_series = _build_metro_series_map(definition_version)

    # Collect all series IDs in a stable order (metro ordering × measure ordering)
    measure_names = list(LAUS_MEASURE_CODES.keys())
    all_series_ids: list[str] = [
        metro_series[mid][measure]
        for mid in sorted(metro_series)
        for measure in measure_names
    ]

    # Fetch annual-average values from BLS API
    values = fetch_laus_annual_averages(all_series_ids, year, api_key=api_key)

    # Assemble one row per metro
    rows: list[dict] = []
    for metro_id in sorted(metro_series):
        cbsa_code = METRO_CBSA_MAPPING[metro_id]
        series_map = metro_series[metro_id]

        row: dict = {
            "metro_id": metro_id,
            "metro_name": metro_name_for_id(metro_id),
            "definition_version": definition_version,
            "year": year,
            "cbsa_code": cbsa_code,
        }

        for measure, sid in series_map.items():
            row[measure] = values.get(sid)

        row["data_source"] = "bls_laus"
        row["series_ids"] = json.dumps(series_map)
        row["source_ref"] = BLS_LAUS_SOURCE_REF
        row["ingested_at"] = ingested_at
        rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError(
            f"No LAUS data could be retrieved for year {year}. "
            f"Verify that BLS LAUS annual-average data is available for this year."
        )

    # Fail fast if any metro has completely missing data for every measure.
    # A metro with all-null measures means the BLS API returned no values for
    # any of its four series, which indicates API truncation or bad series IDs
    # rather than a legitimate data gap.  Writing a partial parquet silently
    # corrupts downstream panels.
    measure_cols_present = [c for c in ["unemployment_rate", "unemployed", "employed", "labor_force"] if c in df.columns]
    if measure_cols_present:
        all_null_mask = df[measure_cols_present].isna().all(axis=1)
        all_null_metros = df.loc[all_null_mask, "metro_id"].tolist()
        if all_null_metros:
            n_fetched = len(values)
            n_requested = len(all_series_ids)
            raise ValueError(
                f"{len(all_null_metros)} metro(s) have no data for any measure in year {year}: "
                f"{all_null_metros}. BLS API returned values for {n_fetched}/{n_requested} "
                f"series. This may indicate API truncation — verify series IDs and consider "
                f"registering for a BLS API key to raise the per-request series limit from "
                f"{_BLS_ANON_MAX_SERIES_PER_REQUEST} to {_BLS_KEY_MAX_SERIES_PER_REQUEST}. "
                f"Series IDs sampled: {all_series_ids[:4]}..."
            )

        # Also fail if any populated metro is missing one or more required
        # measures.  After the all-null-metro guard above every remaining row has
        # at least one measure, so any row that still has a null is partial.
        # This catches both "one measure missing for all metros" and "one measure
        # missing for a single metro" — both silently corrupt downstream panels.
        partial_null_mask = df[measure_cols_present].isna().any(axis=1)
        partial_metros = df.loc[partial_null_mask, "metro_id"].tolist()
        if partial_metros:
            affected_measures = [
                c for c in measure_cols_present
                if df.loc[partial_null_mask, c].isna().any()
            ]
            raise ValueError(
                f"{len(partial_metros)} metro(s) have partial measure data in year {year}: "
                f"{partial_metros}. Missing measure(s): {affected_measures}. "
                f"One or more BLS LAUS series IDs returned no data for these metros — "
                f"verify series IDs."
            )

    # Enforce types
    int_cols = ["labor_force", "employed", "unemployed"]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "unemployment_rate" in df.columns:
        df["unemployment_rate"] = pd.to_numeric(
            df["unemployment_rate"], errors="coerce"
        ).astype("Float64")

    df["year"] = df["year"].astype(int)
    df["metro_id"] = df["metro_id"].astype(str)
    df["cbsa_code"] = df["cbsa_code"].astype(str)

    # Canonical column order
    col_order = [c for c in LAUS_METRO_OUTPUT_COLUMNS if c in df.columns]
    result = df[col_order].copy()
    result = result.sort_values("metro_id").reset_index(drop=True)

    # Write output
    base_dir = Path("data") if project_root is None else project_root / "data"
    output_path = naming.laus_metro_path(year, definition_version, base_dir=base_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    n_complete = result["unemployment_rate"].notna().sum()

    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=definition_version,
        extra={
            "dataset_type": "laus_metro_annual",
            "provider": "bls",
            "product": "laus",
            "reference_year": year,
            "retrieved_at": ingested_at.isoformat(),
            "row_count": len(result),
            "metros_with_complete_data": int(n_complete),
            "series_fetched": len(all_series_ids),
            "series_populated": len(values),
            "cbsa_mapping_version": definition_version,
        },
    )

    write_parquet_with_provenance(result, output_path, provenance)
    logger.info(
        "Wrote BLS LAUS metro annual data to %s (%d metros, year=%d)",
        output_path,
        len(result),
        year,
    )

    return output_path
