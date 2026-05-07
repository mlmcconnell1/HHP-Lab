"""Decennial tract population denominator ingest.

Fetches tract-level total population from decennial Census APIs and writes a
small denominator artifact for tract-mediated county weights. Supported native
eras are 2010 SF1 and 2020 PL 94-171.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd

import hhplab.naming as naming
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance

STATE_FIPS_CODES: tuple[str, ...] = (
    "01",
    "02",
    "04",
    "05",
    "06",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
    "24",
    "25",
    "26",
    "27",
    "28",
    "29",
    "30",
    "31",
    "32",
    "33",
    "34",
    "35",
    "36",
    "37",
    "38",
    "39",
    "40",
    "41",
    "42",
    "44",
    "45",
    "46",
    "47",
    "48",
    "49",
    "50",
    "51",
    "53",
    "54",
    "55",
    "56",
    "60",
    "66",
    "69",
    "72",
    "78",
)

DECENNIAL_API_SPECS: dict[str, tuple[str, str]] = {
    "2010": ("https://api.census.gov/data/2010/dec/sf1", "P001001"),
    "2020": ("https://api.census.gov/data/2020/dec/pl", "P1_001N"),
}


def get_output_path(
    decennial_vintage: str,
    tract_vintage: str | None = None,
    base_dir: Path | str | None = None,
) -> Path:
    """Return the canonical decennial tract denominator output path."""
    if base_dir is None:
        base_dir = curated_dir("census")
    else:
        base_dir = Path(base_dir)
    return base_dir / naming.decennial_tracts_filename(decennial_vintage, tract_vintage)


def fetch_decennial_tract_population(
    decennial_vintage: str,
    *,
    state_fips_codes: tuple[str, ...] = STATE_FIPS_CODES,
) -> tuple[pd.DataFrame, str, int]:
    """Fetch decennial tract total population for all requested states."""
    if decennial_vintage not in DECENNIAL_API_SPECS:
        raise ValueError(
            f"Unsupported decennial vintage {decennial_vintage!r}. Supported vintages: 2010, 2020."
        )

    base_url, population_var = DECENNIAL_API_SPECS[decennial_vintage]
    frames: list[pd.DataFrame] = []
    raw_parts: list[bytes] = []
    with httpx.Client(timeout=60.0) as client:
        for state_fips in state_fips_codes:
            response = client.get(
                base_url,
                params={
                    "get": f"NAME,{population_var}",
                    "for": "tract:*",
                    "in": f"state:{state_fips}",
                },
            )
            response.raise_for_status()
            if not response.content:
                continue
            raw_parts.append(response.content)
            data = response.json()
            headers = data[0]
            rows = data[1:]
            frame = pd.DataFrame(rows, columns=headers)
            frame["tract_geoid"] = (
                frame["state"].astype(str).str.zfill(2)
                + frame["county"].astype(str).str.zfill(3)
                + frame["tract"].astype(str).str.zfill(6)
            )
            frame["total_population"] = pd.to_numeric(
                frame[population_var],
                errors="coerce",
            ).astype("Int64")
            frames.append(frame[["tract_geoid", "total_population"]])

    result = pd.concat(frames, ignore_index=True)
    result["decennial_vintage"] = decennial_vintage
    result["tract_vintage"] = decennial_vintage
    result["data_source"] = "decennial_census"
    result["source_ref"] = f"{base_url}:{population_var}"
    result["ingested_at"] = datetime.now(UTC).isoformat()
    digest = hashlib.sha256(b"\n".join(raw_parts)).hexdigest()
    content_size = sum(len(part) for part in raw_parts)
    return result, digest, content_size


def ingest_decennial_tract_population(
    decennial_vintage: str,
    *,
    tract_vintage: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
) -> Path:
    """Fetch and cache decennial tract total population denominators."""
    resolved_tract_vintage = decennial_vintage if tract_vintage is None else tract_vintage
    if resolved_tract_vintage != decennial_vintage:
        raise ValueError(
            "Decennial tract population denominators are native to their census "
            f"era; got decennial {decennial_vintage} with tract vintage "
            f"{resolved_tract_vintage}."
        )

    output_path = get_output_path(decennial_vintage, resolved_tract_vintage, output_dir)
    if output_path.exists() and not force:
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df, content_sha256, content_size = fetch_decennial_tract_population(decennial_vintage)
    provenance = ProvenanceBlock(
        tract_vintage=resolved_tract_vintage,
        weighting="denominator",
        extra={
            "dataset_type": "decennial_tract_population",
            "decennial_vintage": decennial_vintage,
            "denominator_source": "decennial",
            "content_sha256": content_sha256,
            "content_size": content_size,
        },
    )
    write_parquet_with_provenance(df, output_path, provenance)
    return output_path
