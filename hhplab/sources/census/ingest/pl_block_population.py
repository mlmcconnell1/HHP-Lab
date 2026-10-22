"""PL 94-171 block-level population ingest."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pandas as pd
import pyarrow.parquet as pq

import hhplab.artifacts.naming.naming as naming
from hhplab.schema.columns import PL_BLOCK_POPULATION_COLUMNS
from hhplab.sources.census.api import get_census_api_key, raise_for_census_api_status
from hhplab.sources.census.ingest.decennial_tract_population import STATE_FIPS_CODES
from hhplab.storage.paths import curated_dir
from hhplab.storage.provenance import (
    PROVENANCE_KEY,
    ProvenanceBlock,
    read_provenance,
    write_parquet_with_provenance,
)
from hhplab.storage.raw_snapshot import persist_file_snapshot

PL_BLOCK_API_SPECS: dict[str, tuple[str, str]] = {
    "2010": ("https://api.census.gov/data/2010/dec/pl", "P001001"),
    "2020": ("https://api.census.gov/data/2020/dec/pl", "P1_001N"),
}

PL_2010_BULK_BASE_URL = (
    "https://www2.census.gov/programs-surveys/decennial/2010/data/01-Redistricting_File--PL_94-171"
)
PL_2010_ABBREVIATIONS_BY_FIPS: dict[str, str] = {
    "01": "AL",
    "02": "AK",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "15": "HI",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
    "72": "PR",
}
PL_2010_STATE_DIRECTORY_BY_ABBREVIATION: dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District_of_Columbia",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New_Hampshire",
    "NJ": "New_Jersey",
    "NM": "New_Mexico",
    "NY": "New_York",
    "NC": "North_Carolina",
    "ND": "North_Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "PR": "Puerto_Rico",
    "RI": "Rhode_Island",
    "SC": "South_Carolina",
    "SD": "South_Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West_Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}
PL_2010_STATE_FIPS_CODES = tuple(PL_2010_ABBREVIATIONS_BY_FIPS)


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
    api_key = get_census_api_key(api_key)
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
            raise_for_census_api_status(response)
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


def _parse_pl_2010_geography_file(
    file_obj: io.BufferedIOBase,
    state_fips: str,
    source_url: str,
) -> tuple[pd.DataFrame, int, dict[str, int]]:
    """Read block population records from one official 2010 PL geography file.

    The official ``pl_geohd_2010.sas`` source defines SUMLEV at bytes 9-11,
    GEOCOMP at 12-13, state/county/tract/block at 28-65, and POP100 at 319-327.
    Census SAS positions are one-based; slices here are their zero-based equivalents.
    Source: https://www2.census.gov/programs-surveys/decennial/2010/data/
    01-Redistricting_File--PL_94-171/pl_geohd_2010.sas
    """
    rows: list[tuple[str, str, str, str, int]] = []
    county_totals: dict[str, int] = {}
    state_total: int | None = None

    for line_number, line in enumerate(file_obj, start=1):
        line = line.rstrip(b"\r\n")
        if len(line) < 327:
            raise ValueError(
                f"2010 PL geography record {line_number} is shorter than the "
                "documented 327-byte population field."
            )
        summary_level = line[8:11]
        geographic_component = line[11:13]
        row_state = line[27:29].decode("ascii")
        if row_state != state_fips:
            continue
        if summary_level not in {b"040", b"050", b"750"} or geographic_component != b"00":
            continue
        raw_population = line[318:327].strip()
        if not raw_population:
            raise ValueError(
                f"2010 PL geography record {line_number} has missing POP100; "
                "missing population cannot be interpreted as zero."
            )
        try:
            population = int(raw_population)
        except ValueError as exc:
            raise ValueError(
                f"2010 PL geography record {line_number} has invalid POP100: {line[318:327]!r}."
            ) from exc
        if population < 0:
            raise ValueError(
                f"2010 PL geography record {line_number} has negative POP100: {population}."
            )

        if summary_level == b"040" and geographic_component == b"00":
            if state_total is not None:
                raise ValueError(f"2010 PL file for state {state_fips} has duplicate state totals.")
            state_total = population
        elif summary_level == b"050" and geographic_component == b"00":
            county_fips = row_state + line[29:32].decode("ascii")
            if county_fips in county_totals:
                raise ValueError(f"2010 PL file has duplicate county total for {county_fips}.")
            county_totals[county_fips] = population
        elif summary_level == b"750" and geographic_component == b"00":
            county = line[29:32].decode("ascii")
            tract = line[54:60].decode("ascii")
            block = line[61:65].decode("ascii")
            block_geoid = row_state + county + tract + block
            if len(block_geoid) != 15 or not block_geoid.isdigit():
                raise ValueError(
                    f"2010 PL block record {line_number} has invalid GEOID {block_geoid!r}."
                )
            rows.append(
                (
                    block_geoid,
                    row_state,
                    row_state + county,
                    row_state + county + tract,
                    population,
                )
            )

    if state_total is None:
        raise ValueError(f"2010 PL file for state {state_fips} has no total-population row.")
    if not rows:
        raise ValueError(f"2010 PL file for state {state_fips} has no block rows.")

    frame = pd.DataFrame(
        {
            "block_geoid": [row[0] for row in rows],
            "state_fips": [row[1] for row in rows],
            "county_fips": [row[2] for row in rows],
            "tract_geoid": [row[3] for row in rows],
            "block_vintage": "2010",
            "decennial_vintage": "2010",
            "total_population": pd.array([row[4] for row in rows], dtype="Int64"),
            "data_source": "census_pl_94_171",
            "source_ref": source_url,
            "ingested_at": datetime.now(UTC).isoformat(),
        },
        columns=PL_BLOCK_POPULATION_COLUMNS,
    )
    if frame["block_geoid"].duplicated().any():
        raise ValueError(f"2010 PL file for state {state_fips} has duplicate block GEOIDs.")
    if frame["total_population"].isna().any():
        raise ValueError(f"2010 PL file for state {state_fips} has missing block population.")
    if int(frame["total_population"].sum()) != state_total:
        raise ValueError(
            f"2010 PL block population sum for state {state_fips} "
            f"({int(frame['total_population'].sum())}) does not match the official "
            f"state total ({state_total})."
        )
    block_county_totals = frame.groupby("county_fips", sort=True)["total_population"].sum()
    if set(block_county_totals.index) != set(county_totals):
        raise ValueError(
            f"2010 PL block county coverage for state {state_fips} does not match "
            "the official county summary-level rows."
        )
    mismatches = {
        county_fips: (int(block_county_totals[county_fips]), county_population)
        for county_fips, county_population in county_totals.items()
        if int(block_county_totals[county_fips]) != county_population
    }
    if mismatches:
        raise ValueError(
            f"2010 PL block sums do not match county totals for state {state_fips}: {mismatches}"
        )
    return frame, state_total, county_totals


def _pl_2010_state_url(state_fips: str) -> tuple[str, str]:
    try:
        abbreviation = PL_2010_ABBREVIATIONS_BY_FIPS[state_fips]
        state_directory = PL_2010_STATE_DIRECTORY_BY_ABBREVIATION[abbreviation]
    except KeyError as exc:
        raise ValueError(f"No 2010 PL bulk file is defined for state FIPS {state_fips!r}.") from exc
    filename = f"{abbreviation.lower()}2010.pl.zip"
    url = f"{PL_2010_BULK_BASE_URL}/{state_directory}/{filename}"
    return url, filename


def _write_json_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _ingest_pl_2010_bulk(
    output_path: Path,
    *,
    state_fips_codes: tuple[str, ...],
    raw_root: Path | None,
    force: bool,
) -> Path:
    """Ingest public 2010 PL fixed-width state files with resumable state shards."""
    state_codes = tuple(state_fips_codes)
    if set(state_codes) != set(PL_2010_STATE_FIPS_CODES) or len(state_codes) != len(
        PL_2010_STATE_FIPS_CODES
    ):
        raise ValueError(
            "2010 PL block population output requires all 50 states, DC, and Puerto Rico "
            f"({len(PL_2010_STATE_FIPS_CODES)} state FIPS codes)."
        )
    unsupported = sorted(set(state_codes) - set(PL_2010_ABBREVIATIONS_BY_FIPS))
    if unsupported:
        raise ValueError(f"2010 PL bulk files do not cover state FIPS codes: {unsupported}.")
    if len(set(state_codes)) != len(state_codes):
        raise ValueError("2010 PL state FIPS codes must be unique.")

    raw_base = raw_root or output_path.parent / "raw"
    pl_raw_dir = raw_base / "census_pl" / "2010"
    parts_dir = output_path.with_name(f"{output_path.stem}_parts")
    parts_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = pl_raw_dir / "manifest.json"
    manifest: dict[str, object] = {
        "dataset": "2010 Census PL 94-171 block POP100",
        "source_root": PL_2010_BULK_BASE_URL,
        "block_vintage": "2010",
        "states": {},
    }
    state_entries: dict[str, dict[str, object]] = {}
    with httpx.Client(timeout=300.0, follow_redirects=True) as client:
        for state_fips in state_codes:
            url, filename = _pl_2010_state_url(state_fips)
            cached_zip = pl_raw_dir / filename
            if force or not cached_zip.exists():
                response = client.get(url)
                response.raise_for_status()
                if not response.content:
                    raise ValueError(f"2010 PL bulk download was empty: {url}")
                cached_zip.parent.mkdir(parents=True, exist_ok=True)
                persist_file_snapshot(
                    response.content,
                    "census_pl",
                    filename,
                    subdirs=("2010",),
                    raw_root=raw_base,
                )

            content = cached_zip.read_bytes()
            digest = hashlib.sha256(content).hexdigest()
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as archive:
                    abbreviation = PL_2010_ABBREVIATIONS_BY_FIPS[state_fips].lower()
                    geography_name = f"{abbreviation}geo2010.pl"
                    with archive.open(geography_name) as geography_file:
                        frame, state_population, county_totals = _parse_pl_2010_geography_file(
                            geography_file, state_fips, url
                        )
            except KeyError as exc:
                raise ValueError(
                    f"2010 PL archive {filename} does not contain its expected geography "
                    f"file {geography_name}."
                ) from exc

            part_path = parts_dir / f"state_fips={state_fips}.parquet"
            tmp_part_path = part_path.with_suffix(".parquet.tmp")
            part_provenance = ProvenanceBlock(
                weighting="denominator",
                extra={
                    "dataset_type": "pl_block_population_state_part",
                    "decennial_vintage": "2010",
                    "block_vintage": "2010",
                    "state_fips": state_fips,
                    "source_url": url,
                    "content_sha256": digest,
                    "content_size": len(content),
                    "block_count": len(frame),
                    "state_population_total": state_population,
                    "county_population_total_count": len(county_totals),
                },
            )
            write_parquet_with_provenance(frame, tmp_part_path, part_provenance)
            tmp_part_path.replace(part_path)
            state_entries[state_fips] = {
                "url": url,
                "raw_path": str(cached_zip),
                "sha256": digest,
                "content_size": len(content),
                "block_count": len(frame),
                "population": state_population,
                "county_population_totals": len(county_totals),
                "curated_part": str(part_path),
            }
            manifest["states"] = state_entries
            _write_json_manifest(manifest_path, manifest)
            del frame

    if set(state_entries) != set(state_codes):
        raise ValueError("2010 PL state coverage is incomplete; final output was not written.")
    national_population = sum(int(entry["population"]) for entry in state_entries.values())
    expected_population = 308_745_538 + (3_725_789 if "72" in state_codes else 0)
    if (
        "01" in state_codes
        and len(state_codes) >= 51
        and national_population != expected_population
    ):
        raise ValueError(
            f"2010 PL population total {national_population:,} does not match expected "
            f"50-state/DC/PR total {expected_population:,}."
        )

    provenance = ProvenanceBlock(
        weighting="denominator",
        extra={
            "dataset_type": "pl_block_population",
            "decennial_vintage": "2010",
            "block_vintage": "2010",
            "denominator_source": "pl_94_171_block_population",
            "source_url_template": f"{PL_2010_BULK_BASE_URL}/<State>/<abbr>2010.pl.zip",
            "source_sha256_by_state": {
                state_fips: entry["sha256"] for state_fips, entry in state_entries.items()
            },
            "source_content_size": sum(
                int(entry["content_size"]) for entry in state_entries.values()
            ),
            "state_population_totals": {
                state_fips: entry["population"] for state_fips, entry in state_entries.items()
            },
            "county_population_total_count": sum(
                int(entry["county_population_totals"]) for entry in state_entries.values()
            ),
            "block_count": sum(int(entry["block_count"]) for entry in state_entries.values()),
            "raw_manifest_path": str(manifest_path),
            "state_fips_codes": list(state_codes),
        },
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_output_path = output_path.with_suffix(output_path.suffix + ".tmp")
    writer: pq.ParquetWriter | None = None
    try:
        for state_fips in state_codes:
            table = pq.read_table(parts_dir / f"state_fips={state_fips}.parquet")
            if writer is None:
                schema = table.schema.with_metadata(
                    {
                        **(table.schema.metadata or {}),
                        PROVENANCE_KEY: provenance.to_json().encode("utf-8"),
                    }
                )
                writer = pq.ParquetWriter(tmp_output_path, schema)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("No 2010 PL population state parts were available to assemble.")
    tmp_output_path.replace(output_path)
    manifest["curated_output"] = str(output_path)
    manifest["national_population"] = national_population
    manifest["block_count"] = sum(int(entry["block_count"]) for entry in state_entries.values())
    _write_json_manifest(manifest_path, manifest)
    return output_path


def ingest_pl_block_population(
    decennial_vintage: str,
    *,
    block_vintage: str | None = None,
    force: bool = False,
    output_dir: Path | str | None = None,
    api_key: str | None = None,
    raw_root: Path | None = None,
    state_fips_codes: tuple[str, ...] | None = None,
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
    if decennial_vintage == "2010":
        if output_path.exists() and not force:
            cached_provenance = read_provenance(output_path)
            cached_states = (
                cached_provenance.extra.get("state_fips_codes")
                if cached_provenance is not None
                else None
            )
            if set(cached_states or ()) != set(PL_2010_STATE_FIPS_CODES):
                raise ValueError(
                    f"Cached 2010 PL output at {output_path} is missing verified coverage "
                    "for all 50 states, DC, and Puerto Rico. Re-run with --force."
                )
            return output_path
        return _ingest_pl_2010_bulk(
            output_path,
            state_fips_codes=(state_fips_codes or PL_2010_STATE_FIPS_CODES),
            raw_root=raw_root,
            force=force,
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
