"""PL 94-171 block-level population ingest."""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

import hhplab.naming as naming
from hhplab.census.ingest.decennial_tract_population import STATE_FIPS_CODES
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import PL_BLOCK_POPULATION_COLUMNS

PL_BLOCK_API_SPECS: dict[str, tuple[str, str]] = {
    "2010": ("https://api.census.gov/data/2010/dec/pl", "P001001"),
    "2020": ("https://api.census.gov/data/2020/dec/pl", "P1_001N"),
}


def get_pl_block_population_output_path(
    decennial_vintage: str,
    block_vintage: str | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Return the canonical PL block population output path."""
    if base_dir is None:
        base_dir = curated_dir("census")
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.pl_block_population_filename(decennial_vintage, block_vintage)


def _api_spec(decennial_vintage: str) -> tuple[str, str]:
    try:
        return PL_BLOCK_API_SPECS[decennial_vintage]
    except KeyError as exc:
        supported = ", ".join(sorted(PL_BLOCK_API_SPECS))
        raise ValueError(
            f"Unsupported PL block population vintage {decennial_vintage!r}. "
            f"Supported vintages: {supported}."
        ) from exc


def _normalize_block_response(
    data: list[list[str]],
    *,
    population_var: str,
    decennial_vintage: str,
    base_url: str,
    ingested_at: str,
) -> pd.DataFrame:
    """Normalize one Census API JSON response into canonical PL block rows."""
    if not data or len(data) == 1:
        return pd.DataFrame(columns=PL_BLOCK_POPULATION_COLUMNS)

    headers = data[0]
    frame = pd.DataFrame(data[1:], columns=headers)
    required = {"state", "county", "tract", "block", population_var}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"Census PL block response is missing required fields: {missing}.")

    state = frame["state"].astype(str).str.zfill(2)
    county = frame["county"].astype(str).str.zfill(3)
    tract = frame["tract"].astype(str).str.zfill(6)
    block = frame["block"].astype(str).str.zfill(4)
    normalized = pd.DataFrame(
        {
            "block_geoid": state + county + tract + block,
            "state_fips": state,
            "county_fips": state + county,
            "tract_geoid": state + county + tract,
            "block_vintage": decennial_vintage,
            "decennial_vintage": decennial_vintage,
            "total_population": pd.to_numeric(frame[population_var], errors="coerce").astype(
                "Int64"
            ),
            "data_source": "census_pl_94_171",
            "source_ref": f"{base_url}:{population_var}",
            "ingested_at": ingested_at,
        }
    )
    return normalized[list(PL_BLOCK_POPULATION_COLUMNS)]


def fetch_pl_block_population(
    decennial_vintage: str,
    *,
    state_fips_codes: tuple[str, ...] = STATE_FIPS_CODES,
    api_key: str | None = None,
) -> tuple[pd.DataFrame, str, int]:
    """Fetch PL 94-171 block-level total population for requested states."""
    base_url, population_var = _api_spec(decennial_vintage)
    if api_key is None:
        api_key = os.environ.get("CENSUS_API_KEY")
    frames: list[pd.DataFrame] = []
    raw_parts: list[bytes] = []
    ingested_at = datetime.now(UTC).isoformat()

    with httpx.Client(timeout=300.0) as client:
        for state_fips in state_fips_codes:
            params = {
                "get": f"NAME,{population_var}",
                "for": "block:*",
                "in": f"state:{state_fips} county:* tract:*",
            }
            if api_key:
                params["key"] = api_key
            response = client.get(base_url, params=params)
            _raise_for_block_api_status(response)
            response.raise_for_status()
            if not response.content:
                continue
            raw_parts.append(response.content)
            frame = _normalize_block_response(
                response.json(),
                population_var=population_var,
                decennial_vintage=decennial_vintage,
                base_url=base_url,
                ingested_at=ingested_at,
            )
            if not frame.empty:
                frames.append(frame)

    if not frames:
        raise ValueError(
            f"No PL block population rows fetched for {decennial_vintage}. "
            "Verify Census API availability and requested state FIPS codes."
        )

    result = pd.concat(frames, ignore_index=True)
    digest = hashlib.sha256(b"\n".join(raw_parts)).hexdigest()
    content_size = sum(len(part) for part in raw_parts)
    return result[list(PL_BLOCK_POPULATION_COLUMNS)], digest, content_size


def _raise_for_block_api_status(response: httpx.Response) -> None:
    """Raise actionable errors for known Census block API failures."""
    if response.status_code in {301, 302, 303, 307, 308}:
        location = response.headers.get("location", "")
        if "missing_key" in location:
            raise ValueError(
                "Census PL block API requires a Census API key for this request. "
                "Set CENSUS_API_KEY or pass --api-key, then retry."
            )


def ingest_pl_block_population(
    decennial_vintage: str,
    *,
    block_vintage: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
    api_key: str | None = None,
) -> Path:
    """Fetch and cache PL 94-171 block population denominators."""
    resolved_block_vintage = decennial_vintage if block_vintage is None else block_vintage
    if resolved_block_vintage != decennial_vintage:
        raise ValueError(
            "PL block population denominators are native to their decennial block "
            f"era; got decennial {decennial_vintage} with block vintage "
            f"{resolved_block_vintage}."
        )

    output_path = get_pl_block_population_output_path(
        decennial_vintage,
        resolved_block_vintage,
        output_dir,
    )
    if output_path.exists() and not force:
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df, content_sha256, content_size = fetch_pl_block_population(
        decennial_vintage,
        api_key=api_key,
    )
    provenance = ProvenanceBlock(
        weighting="denominator",
        extra={
            "dataset_type": "pl_block_population",
            "decennial_vintage": decennial_vintage,
            "block_vintage": resolved_block_vintage,
            "denominator_source": "pl_94_171_block_population",
            "content_sha256": content_sha256,
            "content_size": content_size,
        },
    )
    write_parquet_with_provenance(df, output_path, provenance)
    return output_path
