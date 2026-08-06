"""BLS CPI-U annual index ingest.

Fetches the Consumer Price Index for All Urban Consumers (CPI-U), U.S.
city average, all items, not seasonally adjusted, and writes one curated
annual index artifact for inflation adjustment in recipe outputs.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

import hhplab.naming as naming
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.source_urls import BLS_API_V2, BLS_CPI_SOURCE_REF
from hhplab.sources.bls.laus.ingest import (
    BlsQuotaExhausted,
    _bls_quota_message,
    _is_bls_quota_response,
)

logger = logging.getLogger(__name__)

CPI_U_ALL_ITEMS_SERIES_ID = "CUUR0000SA0"
CPI_U_OUTPUT_COLUMNS: tuple[str, ...] = (
    "year",
    "cpi_u",
    "series_id",
    "period",
    "period_name",
    "data_source",
    "source_ref",
    "ingested_at",
)


def fetch_cpi_u_annual_index(
    start_year: int,
    end_year: int,
    api_key: str | None = None,
) -> pd.DataFrame:
    """Fetch annual-average CPI-U index values from the BLS Public API."""
    if start_year > end_year:
        raise ValueError("start_year must be <= end_year.")
    if api_key is None:
        api_key = os.environ.get("BLS_API_KEY")

    payload: dict[str, object] = {
        "seriesid": [CPI_U_ALL_ITEMS_SERIES_ID],
        "startyear": str(start_year),
        "endyear": str(end_year),
        "annualaverage": True,
    }
    if api_key:
        payload["registrationkey"] = api_key

    logger.info("Fetching BLS CPI-U annual index for %d-%d", start_year, end_year)
    with httpx.Client(timeout=30) as client:
        response = client.post(BLS_API_V2, json=payload)
        response.raise_for_status()
        body = response.json()

    status = body.get("status")
    messages = [str(m) for m in body.get("message", [])]
    if _is_bls_quota_response(status, messages):
        raise BlsQuotaExhausted(_bls_quota_message(has_api_key=bool(api_key)))
    if status != "REQUEST_SUCCEEDED":
        msg = "; ".join(messages) if messages else "unknown error"
        raise ValueError(f"BLS API request failed (status={status!r}): {msg}")

    series = body.get("Results", {}).get("series", [])
    if not series:
        raise ValueError("BLS CPI-U response contained no series data.")

    rows: list[dict[str, object]] = []
    for obs in series[0].get("data", []):
        if obs.get("period") != "M13":
            continue
        try:
            value = float(str(obs.get("value", "")).replace(",", ""))
            year = int(obs["year"])
        except (TypeError, ValueError, KeyError):
            continue
        rows.append(
            {
                "year": year,
                "cpi_u": value,
                "series_id": CPI_U_ALL_ITEMS_SERIES_ID,
                "period": "M13",
                "period_name": obs.get("periodName", "Annual"),
            }
        )

    if not rows:
        raise ValueError(
            "BLS CPI-U response contained no annual-average rows. "
            "Verify that annual-average data is available for the requested years."
        )

    result = pd.DataFrame(rows).sort_values("year").reset_index(drop=True)
    missing = sorted(set(range(start_year, end_year + 1)) - set(result["year"].astype(int)))
    if missing:
        raise ValueError(f"BLS CPI-U annual index missing requested year(s): {missing}.")
    return result


def ingest_cpi_u(
    *,
    start_year: int,
    end_year: int,
    api_key: str | None = None,
    project_root: Path | str | None = None,
) -> Path:
    """Fetch annual CPI-U values and write the canonical curated Parquet artifact."""
    if project_root is None:
        base_dir = Path("data")
    else:
        base_dir = Path(project_root) / "data"

    df = fetch_cpi_u_annual_index(start_year, end_year, api_key=api_key).copy()
    now = datetime.now(UTC).isoformat()
    df["data_source"] = "bls_cpi_u"
    df["source_ref"] = BLS_CPI_SOURCE_REF
    df["ingested_at"] = now
    df = df.loc[:, CPI_U_OUTPUT_COLUMNS]

    output_path = naming.cpi_u_path(base_dir=base_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    provenance = ProvenanceBlock(
        extra={
            "dataset_type": "cpi_u_annual",
            "provider": "bls",
            "product": "cpi_u",
            "series_id": CPI_U_ALL_ITEMS_SERIES_ID,
            "start_year": int(start_year),
            "end_year": int(end_year),
            "row_count": int(len(df)),
            "source_ref": BLS_CPI_SOURCE_REF,
        }
    )
    write_parquet_with_provenance(df, output_path, provenance)
    return output_path
