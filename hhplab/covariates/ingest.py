"""Generic tabular ingest for expanded covariate sources."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.covariates.catalog import CovariateSourceSpec, covariate_source_spec
from hhplab.covariates.mpi_contract import (
    MPI_COUNTY_SHEET,
    MPI_CURATED_COUNTY_COLUMNS,
    MPI_ESTIMATE_PERIOD,
    MPI_ESTIMATE_YEAR,
    MPI_PRODUCT,
    MPI_PROVIDER,
    MPI_PUBLICATION_YEAR,
    MPI_SOURCE_CITATION,
    MPI_SOURCE_ID,
    validate_mpi_workbook_contract,
)
from hhplab.metro.metro_definitions import STATE_ABBREV_TO_FIPS
from hhplab.naming import covariate_curated_filename
from hhplab.paths import curated_dir
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.source_registry import register_source

COMMON_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "county_fips": ("county_fips", "fips", "countyfips", "geoid", "geo_id"),
    "coc_number": ("coc_number", "coc", "cocnum", "geo_id"),
    "state": ("state", "state_abbr", "state_po", "stusps", "geo_id"),
    "year": ("year", "data_year", "fy", "fiscal_year"),
}

STATE_NAME_TO_ABBREV: dict[str, str] = {
    "ALABAMA": "AL",
    "ALASKA": "AK",
    "ARIZONA": "AZ",
    "ARKANSAS": "AR",
    "CALIFORNIA": "CA",
    "COLORADO": "CO",
    "CONNECTICUT": "CT",
    "DELAWARE": "DE",
    "DISTRICT OF COLUMBIA": "DC",
    "WASHINGTON DC": "DC",
    "WASHINGTON D.C.": "DC",
    "FLORIDA": "FL",
    "GEORGIA": "GA",
    "HAWAII": "HI",
    "IDAHO": "ID",
    "ILLINOIS": "IL",
    "INDIANA": "IN",
    "IOWA": "IA",
    "KANSAS": "KS",
    "KENTUCKY": "KY",
    "LOUISIANA": "LA",
    "MAINE": "ME",
    "MARYLAND": "MD",
    "MASSACHUSETTS": "MA",
    "MICHIGAN": "MI",
    "MINNESOTA": "MN",
    "MISSISSIPPI": "MS",
    "MISSOURI": "MO",
    "MONTANA": "MT",
    "NEBRASKA": "NE",
    "NEVADA": "NV",
    "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM",
    "NEW YORK": "NY",
    "NORTH CAROLINA": "NC",
    "NORTH DAKOTA": "ND",
    "OHIO": "OH",
    "OKLAHOMA": "OK",
    "OREGON": "OR",
    "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI",
    "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN",
    "TEXAS": "TX",
    "UTAH": "UT",
    "VERMONT": "VT",
    "VIRGINIA": "VA",
    "WASHINGTON": "WA",
    "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI",
    "WYOMING": "WY",
}


def default_covariate_output_path(
    source_id: str,
    *,
    output_dir: Path | str | None = None,
) -> Path:
    """Return the deterministic curated output path for a covariate source."""
    spec = covariate_source_spec(source_id)
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    return base / covariate_curated_filename(
        source_id,
        spec.first_year,
        spec.last_year,
    )


def ingest_covariate_source(
    source_id: str,
    raw_path: Path | str,
    *,
    output_dir: Path | str | None = None,
    output_path: Path | str | None = None,
    county_reference_path: Path | str | None = None,
    force: bool = False,
) -> Path:
    """Normalize a staged CSV/Parquet covariate source into curated parquet."""
    spec = covariate_source_spec(source_id)
    source_path = Path(raw_path)
    if not source_path.exists():
        raise FileNotFoundError(
            f"Covariate raw file not found: {source_path}. Stage the provider file "
            f"from {spec.source_page} and pass --raw-path."
        )

    destination = (
        Path(output_path)
        if output_path is not None
        else default_covariate_output_path(source_id, output_dir=output_dir)
    )
    if destination.exists() and not force:
        return destination

    if source_id == MPI_SOURCE_ID:
        return _ingest_mpi_unauthorized_immigrants(
            source_path,
            spec=spec,
            destination=destination,
            county_reference_path=county_reference_path,
        )

    raw = _read_tabular(source_path)
    result = normalize_covariate_frame(raw, spec=spec)
    raw_sha256 = _sha256(source_path)
    ingested_at = datetime.now(UTC)
    result["source_id"] = source_id
    result["provider"] = spec.provider
    result["product"] = spec.product
    result["topic"] = spec.topic
    result["data_source"] = spec.provider
    result["source_url"] = spec.source_url
    result["raw_sha256"] = raw_sha256
    result["ingested_at"] = ingested_at

    ordered_columns = [
        "geo_type",
        "geo_id",
        *spec.required_columns,
        *spec.measure_columns,
        "source_id",
        "provider",
        "product",
        "topic",
        "data_source",
        "source_url",
        "state_fips",
        "raw_sha256",
        "ingested_at",
    ]
    result = result[[column for column in ordered_columns if column in result.columns]]

    provenance = ProvenanceBlock(
        geo_type=spec.native_geo,
        extra={
            "dataset_type": "expanded_covariate",
            "source_id": source_id,
            "provider": spec.provider,
            "product": spec.product,
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
            "raw_path": str(source_path),
            "raw_sha256": raw_sha256,
        },
    )
    write_parquet_with_provenance(result, destination, provenance)
    register_source(
        source_type="other",
        source_url=spec.source_url,
        raw_sha256=raw_sha256,
        source_name=f"{spec.provider}:{spec.product}",
        file_size=source_path.stat().st_size,
        local_path=source_path,
        metadata={
            "source_id": source_id,
            "curated_path": str(destination),
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
        },
    )
    return destination


def normalize_covariate_frame(
    df: pd.DataFrame,
    *,
    spec: CovariateSourceSpec,
) -> pd.DataFrame:
    """Normalize provider tabular data to canonical geography/year columns."""
    rows = df.copy()
    rows.columns = [_clean_column(column) for column in rows.columns]
    rename = _canonical_renames(rows.columns, spec=spec)
    rows = rows.rename(columns=rename)
    missing = [column for column in spec.required_columns if column not in rows.columns]
    if missing:
        raise ValueError(
            f"{spec.source_id} raw data is missing required columns {missing}. "
            f"Expected canonical columns or aliases: {COMMON_COLUMN_ALIASES}"
        )
    missing_measures = [column for column in spec.measure_columns if column not in rows.columns]
    if spec.source_id == "prism_tmin_january" and "tmin_c" in rows.columns:
        from hhplab.covariates.aggregate import derive_prism_temperature_basis

        rows = derive_prism_temperature_basis(rows)
        missing_measures = [column for column in spec.measure_columns if column not in rows.columns]
    if missing_measures:
        raise ValueError(
            f"{spec.source_id} raw data is missing measure columns {missing_measures}. "
            "Add the source-specific measures before ingesting."
        )

    result = pd.DataFrame(index=rows.index)
    result["geo_type"] = spec.native_geo
    for column in spec.required_columns:
        result[column] = rows[column]
    result["year"] = pd.to_numeric(result["year"], errors="coerce").astype("Int64")
    if result["year"].isna().any():
        raise ValueError(f"{spec.source_id} raw data contains missing or invalid years.")
    result["year"] = result["year"].astype(int)

    if spec.native_geo == "county":
        result["county_fips"] = result["county_fips"].astype("string").str.strip().str.zfill(5)
        result["geo_id"] = result["county_fips"]
    elif spec.native_geo == "coc":
        result["coc_number"] = result["coc_number"].astype("string").str.strip()
        result["geo_id"] = result["coc_number"]
    elif spec.native_geo == "state":
        result["state"] = result["state"].map(
            lambda value: _normalize_state_abbrev(value, source_id=spec.source_id)
        )
        result["state_fips"] = result["state"].map(STATE_ABBREV_TO_FIPS)
        result["geo_id"] = result["state"]
    else:
        raise ValueError(f"Unsupported covariate native geography: {spec.native_geo}")

    for column in spec.measure_columns:
        result[column] = pd.to_numeric(rows[column], errors="coerce")
    return result.dropna(subset=["geo_id"]).sort_values(["geo_id", "year"]).reset_index(drop=True)


def _ingest_mpi_unauthorized_immigrants(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path,
    county_reference_path: Path | str | None,
) -> Path:
    validate_mpi_workbook_contract(source_path)
    reference_paths = (
        (Path(county_reference_path),)
        if county_reference_path is not None
        else _default_county_reference_paths()
    )
    county_reference = _load_county_references(reference_paths)
    raw = pd.read_excel(source_path, sheet_name=MPI_COUNTY_SHEET, header=2)
    rows, skipped_rows = _normalize_mpi_county_rows(raw, county_reference=county_reference)
    if rows.empty:
        raise ValueError(
            "MPI workbook produced no county rows with resolved county_fips. "
            "Provide a county reference CSV/Parquet via --county-reference-path; "
            f"tried {[str(path) for path in reference_paths]}."
        )

    raw_sha256 = _sha256(source_path)
    ingested_at = datetime.now(UTC)
    rows["source_id"] = spec.source_id
    rows["provider"] = spec.provider
    rows["product"] = spec.product
    rows["topic"] = spec.topic
    rows["data_source"] = spec.provider
    rows["source_url"] = spec.source_url
    rows["raw_sha256"] = raw_sha256
    rows["ingested_at"] = ingested_at
    rows = rows[list(MPI_CURATED_COUNTY_COLUMNS)]

    skipped_preview = skipped_rows.head(20).to_dict(orient="records")
    provenance = ProvenanceBlock(
        geo_type=spec.native_geo,
        extra={
            "dataset_type": "expanded_covariate",
            "source_id": spec.source_id,
            "provider": spec.provider,
            "product": spec.product,
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
            "raw_path": str(source_path),
            "raw_sha256": raw_sha256,
            "source_sheet": MPI_COUNTY_SHEET,
            "publication_year": MPI_PUBLICATION_YEAR,
            "estimate_year": MPI_ESTIMATE_YEAR,
            "estimate_period": MPI_ESTIMATE_PERIOD,
            "source_citation": MPI_SOURCE_CITATION,
            "county_reference_path": [str(path) for path in reference_paths],
            "rows_written": int(len(rows)),
            "skipped_rows": int(len(skipped_rows)),
            "skipped_reasons": _value_counts(skipped_rows, "exclusion_reason"),
            "skipped_preview": skipped_preview,
        },
    )
    write_parquet_with_provenance(rows, destination, provenance)
    register_source(
        source_type="other",
        source_url=spec.source_url,
        raw_sha256=raw_sha256,
        source_name=f"{MPI_PROVIDER}:{MPI_PRODUCT}",
        file_size=source_path.stat().st_size,
        local_path=source_path,
        metadata={
            "source_id": spec.source_id,
            "curated_path": str(destination),
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
            "source_sheet": MPI_COUNTY_SHEET,
            "rows_written": int(len(rows)),
            "skipped_rows": int(len(skipped_rows)),
            "county_reference_path": [str(path) for path in reference_paths],
        },
    )
    return destination


def _normalize_mpi_county_rows(
    raw: pd.DataFrame,
    *,
    county_reference: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    lookup = {
        (_normalize_county_key(row.state_name), _normalize_county_key(row.county_name)): (
            row.county_fips,
            row.state_fips,
        )
        for row in county_reference.itertuples(index=False)
    }
    written: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for source_row, row in raw.reset_index(drop=True).iterrows():
        excel_row = int(source_row) + 4
        state_name = _clean_mpi_value(row.get("State"))
        county_label = _clean_mpi_value(row.get("County"))
        if not county_label:
            continue
        population = row.get("Number of Unauthorized Immigrants")
        share = row.get("County Share of the Total Unauthorized Immigrant Population")
        county_name, exclusion_reason = _mpi_county_name_from_label(county_label, state_name)
        if exclusion_reason is None:
            match = lookup.get(
                (_normalize_county_key(state_name), _normalize_county_key(county_name))
            )
            if match is None:
                exclusion_reason = "unmatched_county_reference"
        if exclusion_reason is not None:
            skipped.append(
                {
                    "source_sheet": MPI_COUNTY_SHEET,
                    "source_row": excel_row,
                    "state_name": state_name,
                    "county_label": county_label,
                    "exclusion_reason": exclusion_reason,
                }
            )
            continue
        county_fips, state_fips = match
        written.append(
            {
                "geo_type": "county",
                "geo_id": county_fips,
                "county_fips": county_fips,
                "state_fips": state_fips,
                "state_name": state_name,
                "county_label": county_label,
                "year": MPI_ESTIMATE_YEAR,
                "estimate_period": MPI_ESTIMATE_PERIOD,
                "unauthorized_immigrant_population": population,
                "unauthorized_immigrant_share_of_us_total": share,
                "source_sheet": MPI_COUNTY_SHEET,
                "source_row": excel_row,
            }
        )
    result = pd.DataFrame(written)
    if not result.empty:
        result["unauthorized_immigrant_population"] = pd.to_numeric(
            result["unauthorized_immigrant_population"],
            errors="raise",
        ).astype("int64")
        result["unauthorized_immigrant_share_of_us_total"] = pd.to_numeric(
            result["unauthorized_immigrant_share_of_us_total"],
            errors="raise",
        )
        result = result.sort_values(["county_fips", "year"]).reset_index(drop=True)
    return result, pd.DataFrame(skipped)


def _mpi_county_name_from_label(
    county_label: str,
    state_name: str,
) -> tuple[str | None, str | None]:
    if not county_label:
        return None, "missing_county_label"
    if county_label == "United States":
        return None, "us_total"
    if "MSA" in county_label:
        return None, "msa_row"
    if "Counties" in county_label or "Parishes" in county_label:
        return None, "multi_county_row"
    if not state_name:
        return None, "missing_state"
    suffix = f", {state_name}"
    if not county_label.endswith(suffix):
        return None, "state_suffix_mismatch"
    county_name = county_label[: -len(suffix)].strip()
    if "-" in county_name:
        return None, "multi_county_row"
    return county_name, None


def _default_county_reference_paths() -> tuple[Path, ...]:
    candidates = sorted(Path("data/raw/pep").glob("pep_county__v*__*.csv"))
    if candidates:
        return tuple(candidates)
    raise FileNotFoundError(
        "No default county reference found for MPI ingest. Expected a Census PEP "
        "county raw CSV under data/raw/pep, or pass --county-reference-path with "
        "columns STNAME/CTYNAME/STATE/COUNTY or state_name/county_name/county_fips."
    )


def _load_county_references(paths: tuple[Path, ...]) -> pd.DataFrame:
    references = [_load_county_reference(path) for path in paths]
    return (
        pd.concat(references, ignore_index=True)
        .drop_duplicates(["state_name", "county_name", "county_fips"])
        .reset_index(drop=True)
    )


def _load_county_reference(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"County reference file not found: {path}. Provide a CSV or Parquet "
            "with state/county names and county FIPS for MPI ingest."
        )
    if path.suffix.lower() == ".parquet":
        reference = pd.read_parquet(path)
    else:
        try:
            reference = pd.read_csv(path)
        except UnicodeDecodeError:
            reference = pd.read_csv(path, encoding="latin1")
    columns = {_clean_column(column): column for column in reference.columns}
    if {"stname", "ctyname", "state", "county"} <= set(columns):
        result = pd.DataFrame(
            {
                "state_name": reference[columns["stname"]],
                "county_name": reference[columns["ctyname"]],
                "state_fips": reference[columns["state"]].astype(str).str.zfill(2),
                "county_code": reference[columns["county"]].astype(str).str.zfill(3),
            }
        )
        result["county_fips"] = result["state_fips"] + result["county_code"]
    elif {"state_name", "county_name", "county_fips"} <= set(columns):
        result = pd.DataFrame(
            {
                "state_name": reference[columns["state_name"]],
                "county_name": reference[columns["county_name"]],
                "county_fips": reference[columns["county_fips"]].astype(str).str.zfill(5),
            }
        )
        result["state_fips"] = result["county_fips"].str[:2]
    else:
        raise ValueError(
            "County reference for MPI ingest is missing required columns. Provide "
            "either STNAME/CTYNAME/STATE/COUNTY or state_name/county_name/county_fips."
        )
    result = result[result["county_fips"].str.len() == 5].drop_duplicates(
        ["state_name", "county_name", "county_fips"]
    )
    return result[["state_name", "county_name", "state_fips", "county_fips"]]


def _clean_mpi_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_county_key(value: Any) -> str:
    normalized = str(value).replace(".", "").replace("'", "").replace("’", "")
    return re.sub(r"\s+", " ", normalized).strip().casefold()


def _value_counts(df: pd.DataFrame, column: str) -> dict[str, int]:
    if df.empty or column not in df.columns:
        return {}
    return {str(key): int(value) for key, value in df[column].value_counts().items()}


def _read_tabular(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(path)
    raise ValueError(
        f"Unsupported covariate raw file type '{suffix}'. Use CSV, TXT, or Parquet."
    )


def _canonical_renames(columns: pd.Index, *, spec: CovariateSourceSpec) -> dict[str, str]:
    renames: dict[str, str] = {}
    for canonical in spec.required_columns:
        aliases = COMMON_COLUMN_ALIASES.get(canonical, (canonical,))
        match = next((column for column in columns if column in aliases), None)
        if match is not None:
            renames[match] = canonical
    return renames


def _clean_column(value: Any) -> str:
    return str(value).strip().lower().replace(" ", "_").replace("-", "_")


def _normalize_state_abbrev(value: Any, *, source_id: str) -> str:
    raw = str(value).strip()
    normalized = " ".join(raw.upper().replace(".", "").split())
    if normalized in STATE_ABBREV_TO_FIPS:
        return normalized
    if normalized in STATE_NAME_TO_ABBREV:
        return STATE_NAME_TO_ABBREV[normalized]
    raise ValueError(
        f"{source_id} raw data contains unrecognized state value {raw!r}. "
        "Use a USPS state abbreviation or full state name."
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
