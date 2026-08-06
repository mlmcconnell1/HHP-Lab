"""Generic tabular ingest for expanded covariate sources."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.covariates.catalog import CovariateSourceSpec, covariate_source_spec
from hhplab.covariates.frame_adapters import covariate_frame_adapter
from hhplab.covariates.irs_soi_contract import (
    IRS_SOI_COUNTY_CURATED_COLUMNS,
    IRS_SOI_OTHER_FLOW_STATE_FIPS,
    IRS_SOI_PAIR_CURATED_COLUMNS,
    IRS_SOI_PRODUCT,
    IRS_SOI_PROVIDER,
    IRS_SOI_REQUIRED_RAW_COLUMNS,
    IRS_SOI_SOURCE_ID,
    IRS_SOI_STATE_TOTAL_COUNTY_FIPS,
    IRS_SOI_SUMMARY_STATE_FIPS,
)
from hhplab.covariates.saiz_contract import (
    SAIZ_ESTIMATE_YEAR,
    SAIZ_MATCH_DIAGNOSTIC_COLUMNS,
    SAIZ_SOURCE_ID,
    validate_saiz_source_contract,
    validate_saiz_source_path,
)
from hhplab.metro.metro_definitions import STATE_ABBREV_TO_FIPS
from hhplab.msa import DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION
from hhplab.msa.msa_io import read_msa_definitions
from hhplab.naming import covariate_curated_filename, covariate_pair_filename
from hhplab.paths import curated_dir, raw_root
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.source_registry import register_source
from hhplab.sources.bls.qcew.contract import (
    QCEW_ALL_INDUSTRY_CODE,
    QCEW_COUNTY_AREA_TYPE,
    QCEW_COUNTY_TOTAL_COVERED_AGGLVL,
    QCEW_DERIVED_MEASURE_COLUMNS,
    QCEW_MEASURE_COLUMNS,
    QCEW_REQUIRED_RAW_COLUMNS,
    QCEW_SOURCE_ID,
    QCEW_TOTAL_COVERED_OWN_CODE,
)
from hhplab.sources.census.bps.census_bps_contract import (
    CENSUS_BPS_ANNUAL_FILE_GLOB,
    CENSUS_BPS_BUILDING_COLUMNS,
    CENSUS_BPS_CLASS_UNIT_COLUMNS,
    CENSUS_BPS_CLASS_VALUE_COLUMNS,
    CENSUS_BPS_COUNTY_FIPS3_COLUMN,
    CENSUS_BPS_HEADER_ROWS,
    CENSUS_BPS_RAW_URL_TEMPLATE,
    CENSUS_BPS_SOURCE_ID,
    CENSUS_BPS_STATE_FIPS_COLUMN,
    CENSUS_BPS_SURVEY_YEAR_COLUMN,
    CENSUS_BPS_UNIT_COLUMNS,
    CENSUS_BPS_VALUE_COLUMNS,
)
from hhplab.sources.mpi.mpi_contract import (
    MPI_ALLOWED_COUNTY_EQUIVALENT_LABELS,
    MPI_COUNTY_SHEET,
    MPI_CURATED_COUNTY_COLUMNS,
    MPI_ESTIMATE_PERIOD,
    MPI_ESTIMATE_YEAR,
    MPI_FIRST_DATA_ROW,
    MPI_HEADER_ROW,
    MPI_PRODUCT,
    MPI_PROVIDER,
    MPI_PUBLICATION_YEAR,
    MPI_SOURCE_CITATION,
    MPI_SOURCE_ID,
    validate_mpi_workbook_contract,
)

COMMON_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "county_fips": ("county_fips", "fips", "countyfips", "fips_county", "geoid", "geo_id"),
    "coc_number": ("coc_number", "coc", "cocnum", "geo_id"),
    "state": ("state", "state_abbr", "state_po", "stusps", "geo_id"),
    "msa_id": ("msa_id", "cbsa_code", "geoid", "geo_id"),
    "year": ("year", "data_year", "fy", "fiscal_year"),
}

HUD_PSH_WORKBOOK_GLOB = "COUNTY_*.xlsx"
HUD_PSH_FILENAME_RE = re.compile(
    r"COUNTY_(?P<year>\d{4})(?:_(?P<census>2020census))?\.xlsx$",
    re.IGNORECASE,
)
HUD_PSH_REQUIRED_RAW_COLUMNS = ("gsl", "program_label", "code", "total_units")
HUD_PSH_COUNTY_LEVEL = "county"
HUD_PSH_PROGRAM_MEASURES: dict[str, str] = {
    "Summary of All HUD Programs": "subsidized_households",
    "Housing Choice Vouchers": "housing_choice_vouchers",
}
QCEW_RAW_SUFFIXES = {".csv", ".txt", ".xlsx", ".xls"}
QCEW_HIGH_LEVEL_ALIASES: dict[str, tuple[str, ...]] = {
    "area_fips": ("area_fips", "area_code"),
    "state_fips": ("state_fips", "st"),
    "county_fips3": ("county_fips3", "cnty"),
    "own_code": ("own_code", "own"),
    "industry_code": ("industry_code", "naics"),
    "agglvl_code": ("agglvl_code",),
    "year": ("year",),
    "qtr": ("qtr",),
    "area_type": ("area_type",),
    "annual_avg_estabs": (
        "annual_avg_estabs",
        "annual_average_establishment_count",
    ),
    "annual_avg_emplvl": (
        "annual_avg_emplvl",
        "annual_average_employment",
    ),
    "total_annual_wages": (
        "total_annual_wages",
        "annual_total_wages",
    ),
    "annual_avg_wkly_wage": (
        "annual_avg_wkly_wage",
        "annual_average_weekly_wage",
    ),
    "avg_annual_pay": ("avg_annual_pay", "annual_average_pay"),
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

    if source_id == IRS_SOI_SOURCE_ID:
        return _ingest_irs_soi_migration(
            source_path,
            spec=spec,
            destination=Path(output_path) if output_path is not None else None,
            output_dir=output_dir,
            force=force,
        )

    if source_id == CENSUS_BPS_SOURCE_ID and _is_census_bps_raw_path(source_path):
        return _ingest_census_bps(
            source_path,
            spec=spec,
            destination=Path(output_path) if output_path is not None else None,
            output_dir=output_dir,
            force=force,
        )

    if source_id == "hud_psh" and _is_hud_psh_raw_path(source_path):
        return _ingest_hud_psh(
            source_path,
            spec=spec,
            destination=Path(output_path) if output_path is not None else None,
            output_dir=output_dir,
            force=force,
        )

    if source_id == QCEW_SOURCE_ID and _is_qcew_raw_path(source_path):
        return _ingest_qcew(
            source_path,
            spec=spec,
            destination=Path(output_path) if output_path is not None else None,
            output_dir=output_dir,
            force=force,
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

    if source_id == SAIZ_SOURCE_ID:
        return _ingest_saiz_supply_elasticity(
            source_path,
            spec=spec,
            destination=destination,
        )

    raw = _read_tabular(source_path)
    result = normalize_covariate_frame(raw, spec=spec)
    return _finalize_covariate_curated(
        result,
        spec=spec,
        source_path=source_path,
        destination=destination,
        raw_sha256=_sha256(source_path),
        file_size=source_path.stat().st_size,
    )


def _finalize_covariate_curated(
    result: pd.DataFrame,
    *,
    spec: CovariateSourceSpec,
    source_path: Path,
    destination: Path,
    raw_sha256: str,
    file_size: int,
    provenance_extra: dict[str, Any] | None = None,
) -> Path:
    """Annotate, order, and persist a normalized covariate frame with provenance."""
    ingested_at = datetime.now(UTC)
    result["source_id"] = spec.source_id
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
        *[column for column in QCEW_DERIVED_MEASURE_COLUMNS if column in result.columns],
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
    extra_columns = [
        column for column in result.columns if column not in ordered_columns
    ]
    ordered_columns = [*ordered_columns, *extra_columns]
    result = result[[column for column in ordered_columns if column in result.columns]]

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
            **(provenance_extra or {}),
        },
    )
    write_parquet_with_provenance(result, destination, provenance)
    register_source(
        source_type="other",
        source_url=spec.source_url,
        raw_sha256=raw_sha256,
        source_name=f"{spec.provider}:{spec.product}",
        file_size=file_size,
        local_path=source_path,
        metadata={
            "source_id": spec.source_id,
            "curated_path": str(destination),
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
        },
    )
    return destination


def _is_census_bps_raw_path(source_path: Path) -> bool:
    """True when the path is raw BPS annual county data rather than a staged file.

    Raw input is either a directory of ``co{YYYY}a.txt`` files or a single such
    file; anything else falls through to the generic staged-tabular ingest.
    """
    if source_path.is_dir():
        return True
    return re.fullmatch(r"co\d{4}a\.txt", source_path.name) is not None


def _ingest_census_bps(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path | None,
    output_dir: Path | str | None,
    force: bool,
) -> Path:
    paths = (
        sorted(source_path.glob(CENSUS_BPS_ANNUAL_FILE_GLOB))
        if source_path.is_dir()
        else [source_path]
    )
    if not paths:
        raise ValueError(
            "No Census BPS county annual files found. Expected files named "
            f"co{{YYYY}}a.txt under {source_path}. Download them from "
            f"{CENSUS_BPS_RAW_URL_TEMPLATE} (one file per year)."
        )
    raw = pd.concat(
        [_read_census_bps_annual_file(path) for path in paths],
        ignore_index=True,
    )
    result = normalize_covariate_frame(raw, spec=spec)
    years_present = sorted(result["year"].unique().tolist())
    if destination is None:
        base = curated_dir("covariates") if output_dir is None else Path(output_dir)
        destination = base / covariate_curated_filename(
            spec.source_id,
            min(years_present),
            max(years_present),
        )
    if destination.exists() and not force:
        return destination
    raw_sha256 = _sha256_tree(source_path) if source_path.is_dir() else _sha256(source_path)
    file_size = sum(path.stat().st_size for path in paths)
    return _finalize_covariate_curated(
        result,
        spec=spec,
        source_path=source_path,
        destination=destination,
        raw_sha256=raw_sha256,
        file_size=file_size,
        provenance_extra={
            "years_present": years_present,
            "year_range_token_policy": "derived_from_staged_file_years",
            "raw_files": [path.name for path in paths],
            "structure_class_policy": (
                "permitted_units, permitted_buildings, and permit_value_thousands sum "
                "the estimates-with-imputation series across 1-unit, 2-unit, 3-4-unit, "
                "and 5+-unit structure classes; class-level unit and value columns are "
                "retained for fixed-mix value-per-unit derivation; reporting-places-only "
                "columns are excluded"
            ),
        },
    )


def _read_census_bps_annual_file(path: Path) -> pd.DataFrame:
    """Read one BPS county annual co{YYYY}a.txt into canonical covariate columns."""
    raw = pd.read_csv(
        path,
        skiprows=CENSUS_BPS_HEADER_ROWS,
        header=None,
        dtype=str,
        skip_blank_lines=True,
    )
    year = pd.to_numeric(raw[CENSUS_BPS_SURVEY_YEAR_COLUMN], errors="coerce")
    state = raw[CENSUS_BPS_STATE_FIPS_COLUMN].astype("string").str.strip()
    county3 = raw[CENSUS_BPS_COUNTY_FIPS3_COLUMN].astype("string").str.strip()
    valid = (
        year.notna()
        & state.str.fullmatch(r"\d{1,2}").fillna(False)
        & county3.str.fullmatch(r"\d{1,3}").fillna(False)
    )
    if not valid.any():
        raise ValueError(
            f"Census BPS file {path} produced no county rows. Expected the positional "
            "annual county layout with survey year, state FIPS, and county FIPS columns."
        )
    rows = pd.DataFrame(
        {
            "year": year[valid].astype(int),
            "county_fips": state[valid].str.zfill(2) + county3[valid].str.zfill(3),
        }
    )
    unit_values = raw.loc[valid, list(CENSUS_BPS_UNIT_COLUMNS)].apply(
        pd.to_numeric, errors="coerce"
    )
    building_values = raw.loc[valid, list(CENSUS_BPS_BUILDING_COLUMNS)].apply(
        pd.to_numeric, errors="coerce"
    )
    value_values = raw.loc[valid, list(CENSUS_BPS_VALUE_COLUMNS)].apply(
        pd.to_numeric, errors="coerce"
    )
    rows["permitted_units"] = unit_values.sum(axis=1, min_count=1)
    rows["permitted_buildings"] = building_values.sum(axis=1, min_count=1)
    rows["permit_value_thousands"] = value_values.sum(axis=1, min_count=1)
    for source_column, output_column in zip(
        unit_values.columns,
        CENSUS_BPS_CLASS_UNIT_COLUMNS,
        strict=True,
    ):
        rows[output_column] = unit_values[source_column]
    for source_column, output_column in zip(
        value_values.columns,
        CENSUS_BPS_CLASS_VALUE_COLUMNS,
        strict=True,
    ):
        rows[output_column] = value_values[source_column]
    measure_columns = [
        "permitted_units",
        "permitted_buildings",
        "permit_value_thousands",
        *CENSUS_BPS_CLASS_UNIT_COLUMNS,
        *CENSUS_BPS_CLASS_VALUE_COLUMNS,
    ]
    return (
        rows.groupby(["county_fips", "year"], as_index=False)[measure_columns]
        .sum(min_count=1)
        .reset_index(drop=True)
    )


def _is_hud_psh_raw_path(source_path: Path) -> bool:
    """True when the path is an official HUD PSH county workbook or directory."""
    if source_path.is_dir():
        return True
    return HUD_PSH_FILENAME_RE.fullmatch(source_path.name) is not None


def _ingest_hud_psh(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path | None,
    output_dir: Path | str | None,
    force: bool,
) -> Path:
    paths = _hud_psh_workbook_paths(source_path)
    years_present = [_hud_psh_workbook_year(path) for path in paths]
    duplicate_years = sorted({year for year in years_present if years_present.count(year) > 1})
    if duplicate_years:
        raise ValueError(
            "HUD PSH workbook input contains duplicate county files for year(s) "
            f"{duplicate_years}. Keep one COUNTY_YYYY workbook per year."
        )
    if destination is None:
        destination = default_covariate_output_path(spec.source_id, output_dir=output_dir)
    if destination.exists() and not force:
        return destination

    result = pd.concat(
        [_read_hud_psh_workbook(path, spec=spec) for path in paths],
        ignore_index=True,
    ).sort_values(["county_fips", "year"])
    raw_sha256 = _sha256_tree(source_path) if source_path.is_dir() else _sha256(source_path)
    file_size = sum(path.stat().st_size for path in paths)
    return _finalize_covariate_curated(
        result,
        spec=spec,
        source_path=source_path,
        destination=destination,
        raw_sha256=raw_sha256,
        file_size=file_size,
        provenance_extra={
            "years_present": sorted(years_present),
            "raw_files": [path.name for path in paths],
            "source_layout_policy": "official_hud_psh_county_workbook",
            "program_measure_map": dict(HUD_PSH_PROGRAM_MEASURES),
        },
    )


def _hud_psh_workbook_paths(source_path: Path) -> list[Path]:
    """Resolve one or more official HUD PSH county workbooks."""
    if source_path.is_dir():
        paths = sorted(
            path
            for path in source_path.glob(HUD_PSH_WORKBOOK_GLOB)
            if HUD_PSH_FILENAME_RE.fullmatch(path.name) is not None
        )
        if not paths:
            raise ValueError(
                "No HUD PSH county workbooks found. Expected files named "
                "COUNTY_YYYY.xlsx or COUNTY_YYYY_2020census.xlsx."
            )
        return paths
    if HUD_PSH_FILENAME_RE.fullmatch(source_path.name) is None:
        raise ValueError(
            f"Unsupported HUD PSH raw file name {source_path.name!r}. Expected "
            "COUNTY_YYYY.xlsx or COUNTY_YYYY_2020census.xlsx."
        )
    return [source_path]


def _is_qcew_raw_path(source_path: Path) -> bool:
    """True when the path points to official BLS QCEW county annual files."""
    if source_path.is_dir():
        return True
    return source_path.suffix.lower() in QCEW_RAW_SUFFIXES


def _ingest_qcew(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path | None,
    output_dir: Path | str | None,
    force: bool,
) -> Path:
    paths = _qcew_raw_paths(source_path)
    frames: list[pd.DataFrame] = []
    year_to_paths: dict[int, list[str]] = {}
    for path in paths:
        frame = _read_qcew_raw_file(path)
        if frame.empty:
            continue
        years_in_file = sorted({int(year) for year in frame["year"].unique().tolist()})
        for year in years_in_file:
            year_to_paths.setdefault(year, []).append(path.name)
        frames.append(frame)
    duplicate_years = {
        year: names for year, names in year_to_paths.items() if len(names) > 1
    }
    if duplicate_years:
        duplicates = ", ".join(
            f"{year}: {sorted(names)}" for year, names in sorted(duplicate_years.items())
        )
        raise ValueError(
            "QCEW staged input contains duplicate annual county files for the same year(s): "
            f"{duplicates}. Keep one official county annual file per year."
        )
    if not frames:
        raise ValueError(
            "QCEW input produced no county rows after filtering to county Total Covered "
            "(Own=0), total all-industries (NAICS=10), annual rows."
        )
    result = pd.concat(frames, ignore_index=True).sort_values(["county_fips", "year"])
    result = _derive_qcew_wage_columns(result)
    years_present = sorted(int(year) for year in result["year"].unique().tolist())
    if destination is None:
        base = curated_dir("covariates") if output_dir is None else Path(output_dir)
        destination = base / covariate_curated_filename(
            spec.source_id,
            min(years_present),
            max(years_present),
        )
    if destination.exists() and not force:
        return destination
    raw_sha256 = _sha256_tree(source_path) if source_path.is_dir() else _sha256(source_path)
    file_size = sum(path.stat().st_size for path in paths)
    return _finalize_covariate_curated(
        result,
        spec=spec,
        source_path=source_path,
        destination=destination,
        raw_sha256=raw_sha256,
        file_size=file_size,
        provenance_extra={
            "years_present": years_present,
            "year_range_token_policy": "derived_from_staged_file_years",
            "raw_files": [path.name for path in paths],
            "source_layout_policy": "bls_qcew_county_high_level_annual",
            "filters": {
                "own_code": QCEW_TOTAL_COVERED_OWN_CODE,
                "industry_code": QCEW_ALL_INDUSTRY_CODE,
                "qtr": "A",
                "county_selector": {
                    "agglvl_code": QCEW_COUNTY_TOTAL_COVERED_AGGLVL,
                    "or_area_type": QCEW_COUNTY_AREA_TYPE,
                },
            },
            "derived_measure_columns": list(QCEW_DERIVED_MEASURE_COLUMNS),
            "derived_measure_policy": (
                "annual_avg_weekly_wage and avg_annual_pay are derived from "
                "total_annual_wages and annual_avg_emplvl"
            ),
        },
    )


def _qcew_raw_paths(source_path: Path) -> list[Path]:
    """Resolve one or more staged QCEW annual county files."""
    if source_path.is_dir():
        paths = sorted(
            path
            for path in source_path.iterdir()
            if path.is_file() and path.suffix.lower() in QCEW_RAW_SUFFIXES
        )
        if not paths:
            raise ValueError(
                "No QCEW county annual files found. Stage the official BLS county "
                "high-level annual CSV/TXT/XLSX files under the directory and pass "
                "--raw-path to that directory."
            )
        return paths
    if source_path.suffix.lower() not in QCEW_RAW_SUFFIXES:
        raise ValueError(
            f"Unsupported QCEW raw file type '{source_path.suffix}'. Use CSV, TXT, or Excel."
        )
    return [source_path]


def _read_qcew_raw_file(path: Path) -> pd.DataFrame:
    """Read one staged QCEW annual county file into canonical county rows."""
    if path.suffix.lower() in {".xlsx", ".xls"}:
        workbook = pd.ExcelFile(path)
        frames = [
            _normalize_qcew_frame(workbook.parse(sheet_name))
            for sheet_name in workbook.sheet_names
        ]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            raise ValueError(
                f"QCEW workbook {path.name} produced no county rows with the expected "
                "annual county layout."
            )
        return pd.concat(frames, ignore_index=True)
    return _normalize_qcew_frame(_read_tabular(path))


def _normalize_qcew_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Normalize QCEW annual county rows from high-level or full annual layouts."""
    rows = raw.copy()
    rows.columns = [_clean_column(column) for column in rows.columns]
    rename = _rename_columns(rows.columns, aliases=QCEW_HIGH_LEVEL_ALIASES)
    rows = rows.rename(columns=rename)
    if "area_fips" not in rows.columns and {"state_fips", "county_fips3"} <= set(rows.columns):
        rows["area_fips"] = (
            rows["state_fips"].map(_qcew_code)
            + rows["county_fips3"].map(lambda value: _qcew_code(value, width=3))
        )
    missing = [column for column in QCEW_REQUIRED_RAW_COLUMNS if column not in rows.columns]
    if missing:
        raise ValueError(
            f"QCEW raw data is missing required columns {missing}. Expected the official "
            "county high-level annual layout or the annual CSV field layout."
        )
    if "area_type" not in rows.columns and "agglvl_code" not in rows.columns:
        raise ValueError(
            "QCEW raw data must include either area_type (county high-level layout) "
            "or agglvl_code (annual CSV layout) to identify county rows."
        )
    own_code = rows["own_code"].map(lambda value: _qcew_code(value, width=1))
    industry_code = rows["industry_code"].map(_qcew_industry_code)
    quarter = rows["qtr"].astype("string").str.strip().str.upper()
    if "agglvl_code" in rows.columns:
        county_selector = rows["agglvl_code"].map(_qcew_code).eq(
            QCEW_COUNTY_TOTAL_COVERED_AGGLVL
        )
    else:
        county_selector = rows["area_type"].astype("string").str.strip().str.casefold().eq(
            QCEW_COUNTY_AREA_TYPE
        )
    filtered = rows[
        county_selector
        & own_code.eq(QCEW_TOTAL_COVERED_OWN_CODE)
        & industry_code.eq(QCEW_ALL_INDUSTRY_CODE)
        & quarter.eq("A")
    ].copy()
    if filtered.empty:
        return pd.DataFrame(
            columns=[
                "geo_type",
                "geo_id",
                "county_fips",
                "state_fips",
                "year",
                *QCEW_MEASURE_COLUMNS,
            ]
        )
    filtered["year"] = pd.to_numeric(filtered["year"], errors="coerce").astype("Int64")
    filtered["state_fips"] = (
        filtered.get(
            "state_fips",
            filtered["area_fips"].map(lambda value: _qcew_code(value, width=5)[:2]),
        ).map(_qcew_code)
    )
    filtered["county_fips"] = filtered["area_fips"].map(lambda value: _qcew_code(value, width=5))
    result = pd.DataFrame(
        {
            "geo_type": "county",
            "geo_id": filtered["county_fips"],
            "county_fips": filtered["county_fips"],
            "state_fips": filtered["state_fips"],
            "year": filtered["year"].astype(int),
            "annual_avg_emplvl": pd.to_numeric(
                filtered["annual_avg_emplvl"], errors="coerce"
            ),
            "total_annual_wages": pd.to_numeric(
                filtered["total_annual_wages"], errors="coerce"
            ),
            "annual_avg_estabs": pd.to_numeric(
                filtered["annual_avg_estabs"], errors="coerce"
            ),
        }
    )
    return (
        result.groupby(
            ["geo_type", "geo_id", "county_fips", "state_fips", "year"],
            as_index=False,
        )[
            list(QCEW_MEASURE_COLUMNS)
        ]
        .sum(min_count=1)
        .reset_index(drop=True)
    )


def _derive_qcew_wage_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Derive county/MSA wage-per-worker columns from primitive QCEW totals."""
    result = df.copy()
    employment = pd.to_numeric(result["annual_avg_emplvl"], errors="coerce")
    wages = pd.to_numeric(result["total_annual_wages"], errors="coerce")
    employment_positive = employment.where(employment > 0)
    result["annual_avg_weekly_wage"] = wages / (employment_positive * 52.0)
    result["avg_annual_pay"] = wages / employment_positive
    return result


def _rename_columns(columns: pd.Index, *, aliases: dict[str, tuple[str, ...]]) -> dict[str, str]:
    renames: dict[str, str] = {}
    for canonical, alias_options in aliases.items():
        match = next((column for column in columns if column in alias_options), None)
        if match is not None:
            renames[match] = canonical
    return renames


def _qcew_code(value: Any, *, width: int = 2) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    return digits.zfill(width)[-width:]


def _qcew_industry_code(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.replace("_", "-")


def _read_hud_psh_workbook(
    path: Path,
    *,
    spec: CovariateSourceSpec,
) -> pd.DataFrame:
    """Read one official HUD PSH county workbook into canonical covariate rows."""
    year = _hud_psh_workbook_year(path)
    raw = pd.read_excel(path, sheet_name=0)
    raw.columns = [_clean_column(column) for column in raw.columns]
    missing = sorted(set(HUD_PSH_REQUIRED_RAW_COLUMNS) - set(raw.columns))
    if missing:
        raise ValueError(
            f"HUD PSH workbook {path.name} is missing required columns {missing}. "
            "Expected the official county workbook layout with gsl/program_label/"
            "code/total_units columns."
        )
    quarter_years = _hud_psh_quarter_years(raw)
    if quarter_years and quarter_years != {year}:
        raise ValueError(
            f"HUD PSH workbook {path.name} filename implies year {year}, but Quarter "
            f"column contains {sorted(quarter_years)}."
        )

    county_rows = raw[
        raw["gsl"].astype("string").str.strip().str.casefold().eq(HUD_PSH_COUNTY_LEVEL)
    ].copy()
    county_rows["program_label"] = county_rows["program_label"].astype("string").str.strip()
    county_rows = county_rows[county_rows["program_label"].isin(HUD_PSH_PROGRAM_MEASURES)].copy()
    if county_rows.empty:
        raise ValueError(
            f"HUD PSH workbook {path.name} produced no county rows for "
            f"{sorted(HUD_PSH_PROGRAM_MEASURES)}."
        )

    county_rows["county_fips"] = county_rows["code"].map(_hud_psh_county_fips)
    county_rows["year"] = year
    county_rows["measure"] = county_rows["program_label"].map(HUD_PSH_PROGRAM_MEASURES)
    county_rows["value"] = pd.to_numeric(county_rows["total_units"], errors="coerce")
    grouped = (
        county_rows.dropna(subset=["county_fips"])
        .groupby(["county_fips", "year", "measure"], as_index=False)["value"]
        .sum(min_count=1)
    )
    if grouped.empty:
        raise ValueError(
            f"HUD PSH workbook {path.name} produced no rows with county_fips and "
            "program totals."
        )
    result = grouped.pivot(index=["county_fips", "year"], columns="measure", values="value")
    result = result.reset_index()
    result.columns.name = None
    for column in spec.measure_columns:
        if column not in result.columns:
            result[column] = pd.NA
    result["geo_type"] = spec.native_geo
    result["geo_id"] = result["county_fips"].astype("string").str.zfill(5)
    result["state_fips"] = result["county_fips"].astype("string").str[:2]
    return result[
        ["geo_type", "geo_id", "county_fips", "state_fips", "year", *spec.measure_columns]
    ]


def _hud_psh_workbook_year(path: Path) -> int:
    match = HUD_PSH_FILENAME_RE.fullmatch(path.name)
    if match is None:
        raise ValueError(
            f"Unsupported HUD PSH workbook name {path.name!r}. Expected "
            "COUNTY_YYYY.xlsx or COUNTY_YYYY_2020census.xlsx."
        )
    return int(match.group("year"))


def _hud_psh_county_fips(value: Any) -> str | None:
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    if not digits:
        return None
    return digits.zfill(5)[-5:]


def _hud_psh_quarter_years(raw: pd.DataFrame) -> set[int]:
    if "quarter" not in raw.columns:
        return set()
    parsed = pd.to_datetime(raw["quarter"], errors="coerce")
    return {int(year) for year in parsed.dt.year.dropna().unique()}


def default_covariate_pair_output_path(
    source_id: str,
    *,
    output_dir: Path | str | None = None,
) -> Path:
    """Return the deterministic curated output path for a pair-level covariate."""
    spec = covariate_source_spec(source_id)
    base = curated_dir("covariates") if output_dir is None else Path(output_dir)
    return base / covariate_pair_filename(source_id, spec.first_year, spec.last_year)


def normalize_covariate_frame(
    df: pd.DataFrame,
    *,
    spec: CovariateSourceSpec,
) -> pd.DataFrame:
    """Normalize provider tabular data to canonical geography/year columns."""
    rows = df.copy()
    rows.columns = [_clean_column(column) for column in rows.columns]
    adapter = covariate_frame_adapter(spec.source_id)
    rows = adapter.prepare_raw(rows)
    rename = _canonical_renames(rows.columns, spec=spec)
    rows = rows.rename(columns=rename)
    missing = [column for column in spec.required_columns if column not in rows.columns]
    if missing:
        raise ValueError(
            f"{spec.source_id} raw data is missing required columns {missing}. "
            f"Expected canonical columns or aliases: {COMMON_COLUMN_ALIASES}"
        )
    rows = adapter.derive_measures(rows)
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
    elif spec.native_geo == "msa":
        result["msa_id"] = result["msa_id"].astype("string").str.strip().str.zfill(5)
        result["geo_id"] = result["msa_id"]
    else:
        raise ValueError(f"Unsupported covariate native geography: {spec.native_geo}")

    for column in spec.measure_columns:
        result[column] = pd.to_numeric(rows[column], errors="coerce")
    return result.dropna(subset=["geo_id"]).sort_values(["geo_id", "year"]).reset_index(drop=True)


SAIZ_NAME_OVERRIDES: dict[str, str] = {
    "Virginia Beach-Chesapeake-Norfolk, VA-NC": "Norfolk-Virginia Beach-Newport News, VA-NC",
    "Urban Honolulu, HI": "Honolulu, HI",
    "Louisville/Jefferson County, KY-IN": "Louisville, KY-IN",
}


def _ingest_saiz_supply_elasticity(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path,
) -> Path:
    validate_saiz_source_path(source_path)
    raw = pd.read_stata(source_path)
    validate_saiz_source_contract(source_path, raw_columns=raw.columns)

    saiz = raw.copy()
    saiz["saiz_name"] = saiz["msaname"].astype(str).str.strip()
    definitions = read_msa_definitions(DEFAULT_MSA_DEFINITION_VERSION)
    definitions = definitions[["msa_id", "msa_name"]].drop_duplicates().copy()
    definitions["msa_id"] = definitions["msa_id"].astype("string").str.zfill(5)

    rows: list[dict[str, object]] = []
    for definition in definitions.itertuples(index=False):
        msa_name = str(definition.msa_name)
        title = SAIZ_NAME_OVERRIDES.get(msa_name, msa_name)
        city_part, _, state_part = title.partition(",")
        primary_city = city_part.split("-")[0].strip()
        primary_state = state_part.strip().split("-")[0][:2]
        candidates = saiz[
            saiz["saiz_name"].str.startswith(primary_city)
            & saiz["saiz_name"].str.contains(primary_state)
        ]
        match_rule = "city_state"
        if candidates.empty:
            candidates = saiz[saiz["saiz_name"].str.startswith(primary_city)]
            match_rule = "city"
        best = (
            candidates.sort_values("population", ascending=False).iloc[0]
            if not candidates.empty
            else None
        )
        rows.append(
            {
                "msa_id": str(definition.msa_id),
                "msa_name": msa_name,
                "year": SAIZ_ESTIMATE_YEAR,
                "saiz_msanecma": (
                    float(best["msanecma"]) if best is not None else pd.NA
                ),
                "saiz_name": best["saiz_name"] if best is not None else pd.NA,
                "saiz_match_rule": (
                    "override"
                    if msa_name in SAIZ_NAME_OVERRIDES
                    else match_rule
                    if best is not None
                    else "unmatched"
                ),
                "saiz_elasticity": (
                    float(best["elasticity"]) if best is not None else pd.NA
                ),
                "saiz_inverse_elasticity": (
                    float(1.0 / best["elasticity"])
                    if best is not None and float(best["elasticity"]) != 0
                    else pd.NA
                ),
                "saiz_undevelopable_share": (
                    float(best["unaval"]) if best is not None else pd.NA
                ),
                "saiz_wrluri": float(best["WRLURI"]) if best is not None else pd.NA,
            }
        )

    matched = pd.DataFrame(rows)
    matched_rows = matched[matched["saiz_name"].notna()].copy()
    normalized = normalize_covariate_frame(matched_rows, spec=spec)
    diagnostic_columns = list(SAIZ_MATCH_DIAGNOSTIC_COLUMNS)
    diagnostics = matched[diagnostic_columns].to_dict(orient="records")
    raw_sha256 = _sha256(source_path)
    return _finalize_covariate_curated(
        normalized,
        spec=spec,
        source_path=source_path,
        destination=destination,
        raw_sha256=raw_sha256,
        file_size=source_path.stat().st_size,
        provenance_extra={
            "msa_definition_version": DEFAULT_MSA_DEFINITION_VERSION,
            "match_diagnostics": diagnostics,
            "matched_msa_count": int(matched["saiz_name"].notna().sum()),
            "unmatched_msa_count": int(matched["saiz_name"].isna().sum()),
            "static_year_policy": "source_cross_section",
        },
    )


def _ingest_irs_soi_migration(
    source_path: Path,
    *,
    spec: CovariateSourceSpec,
    destination: Path | None,
    output_dir: Path | str | None,
    force: bool,
) -> Path:
    if not source_path.is_dir():
        raise ValueError(
            "IRS SOI migration ingest expects --raw-path to be a staging directory "
            "containing countyinflowYYZZ.csv and countyoutflowYYZZ.csv files. "
            f"Stage IRS files under {raw_root() / 'irs_soi'} or pass that directory."
        )

    file_pairs = _irs_soi_file_pairs(source_path)
    if not file_pairs:
        raise ValueError(
            "No matched IRS SOI county migration CSV pairs found. Expected files named "
            "countyinflowYYZZ.csv and countyoutflowYYZZ.csv, for example "
            "countyinflow2122.csv and countyoutflow2122.csv."
        )

    years_present = sorted(_irs_soi_later_year(year_token) for year_token in file_pairs)
    if destination is None:
        base = curated_dir("covariates") if output_dir is None else Path(output_dir)
        destination = base / covariate_curated_filename(
            spec.source_id,
            min(years_present),
            max(years_present),
        )
    if destination.exists() and not force:
        return destination

    county_frames: list[pd.DataFrame] = []
    pair_frames: list[pd.DataFrame] = []
    skipped_frames: list[pd.DataFrame] = []
    pair_reconciliation_diagnostics: list[dict[str, object]] = []
    for year_token, paths in sorted(file_pairs.items()):
        year = _irs_soi_later_year(year_token)
        inflow_raw = _read_irs_soi_csv(paths["inflow"], source_label="inflow")
        outflow_raw = _read_irs_soi_csv(paths["outflow"], source_label="outflow")
        inflow_flows, inflow_skipped = _normalize_irs_soi_flow_rows(
            inflow_raw,
            year=year,
            perspective="inflow",
            raw_file=paths["inflow"],
        )
        outflow_flows, outflow_skipped = _normalize_irs_soi_flow_rows(
            outflow_raw,
            year=year,
            perspective="outflow",
            raw_file=paths["outflow"],
        )
        county_frames.append(
            _irs_soi_county_marginals(
                inflow_flows=inflow_flows,
                outflow_flows=outflow_flows,
                inflow_skipped=inflow_skipped,
                outflow_skipped=outflow_skipped,
                year=year,
            )
        )
        pair_table, reconciliation = _irs_soi_pair_table(inflow_flows, outflow_flows)
        pair_frames.append(pair_table)
        pair_reconciliation_diagnostics.extend(reconciliation)
        skipped_frames.extend([inflow_skipped, outflow_skipped])

    county_rows = pd.concat(county_frames, ignore_index=True)
    pair_rows = pd.concat(pair_frames, ignore_index=True)
    skipped_rows = pd.concat(skipped_frames, ignore_index=True)
    if county_rows.empty:
        raise ValueError(
            "IRS SOI migration files produced no county-year rows. Check that the "
            "CSV files use IRS county-to-county layouts and include non-summary rows."
        )
    if pair_rows.empty:
        raise ValueError(
            "IRS SOI migration files produced no pair-level county flows. Check that "
            "the files are not only state totals, pseudo-state summaries, or same-county "
            "non-migrant rows."
        )

    raw_sha256 = _sha256_tree(source_path)
    ingested_at = datetime.now(UTC)
    for rows in (county_rows, pair_rows):
        rows["source_id"] = spec.source_id
        rows["provider"] = spec.provider
        rows["product"] = spec.product
        rows["topic"] = spec.topic
        rows["data_source"] = spec.provider
        rows["source_url"] = spec.source_url
        rows["raw_sha256"] = raw_sha256
        rows["ingested_at"] = ingested_at

    county_rows = county_rows[list(IRS_SOI_COUNTY_CURATED_COLUMNS)]
    pair_rows = pair_rows[list(IRS_SOI_PAIR_CURATED_COLUMNS)]
    pair_destination = destination.with_name(
        covariate_pair_filename(spec.source_id, min(years_present), max(years_present))
    )

    skipped_preview = skipped_rows.head(50).to_dict(orient="records")
    summary_rows = (
        skipped_rows[
            skipped_rows["exclusion_reason"].isin(
                ["state_total_row", "summary_pseudo_state_row", "same_county_non_migrant"]
            )
        ]
        if "exclusion_reason" in skipped_rows.columns
        else pd.DataFrame()
    )
    provenance_extra = {
        "dataset_type": "expanded_covariate",
        "source_id": spec.source_id,
        "provider": spec.provider,
        "product": spec.product,
        "native_geo": spec.native_geo,
        "measure_columns": list(spec.measure_columns),
        "raw_path": str(source_path),
        "raw_sha256": raw_sha256,
        "year_convention": "later filing year; countyinflow2122.csv maps to year 2022",
        "rows_written": int(len(county_rows)),
        "pair_rows_written": int(len(pair_rows)),
        "years_present": years_present,
        "year_range_token_policy": "derived_from_staged_file_years",
        "pair_output_path": str(pair_destination),
        "skipped_rows": int(len(skipped_rows)),
        "skipped_reasons": _value_counts(skipped_rows, "exclusion_reason"),
        "skipped_preview": skipped_preview,
        "summary_row_counts": _irs_soi_summary_row_counts(summary_rows),
        "summary_rows_preview": summary_rows.head(50).to_dict(orient="records"),
        "pair_reconciliation_mismatch_count": len(pair_reconciliation_diagnostics),
        "pair_reconciliation_mismatches_preview": pair_reconciliation_diagnostics[:50],
    }
    write_parquet_with_provenance(
        county_rows,
        destination,
        ProvenanceBlock(geo_type=spec.native_geo, extra=provenance_extra),
    )
    write_parquet_with_provenance(
        pair_rows,
        pair_destination,
        ProvenanceBlock(
            geo_type="county_pair",
            extra={
                **provenance_extra,
                "dataset_type": "expanded_covariate_pair",
                "row_grain": "origin_county_fips x destination_county_fips x year",
                "county_output_path": str(destination),
                "measure_columns": [
                    "migration_returns",
                    "migration_exemptions",
                    "migration_agi_thousands",
                ],
            },
        ),
    )
    register_source(
        source_type="other",
        source_url=spec.source_url,
        raw_sha256=raw_sha256,
        source_name=f"{IRS_SOI_PROVIDER}:{IRS_SOI_PRODUCT}",
        file_size=sum(
            path.stat().st_size
            for pair in file_pairs.values()
            for path in pair.values()
        ),
        local_path=source_path,
        metadata={
            "source_id": spec.source_id,
            "curated_path": str(destination),
            "pair_curated_path": str(pair_destination),
            "native_geo": spec.native_geo,
            "measure_columns": list(spec.measure_columns),
            "rows_written": int(len(county_rows)),
            "pair_rows_written": int(len(pair_rows)),
            "skipped_rows": int(len(skipped_rows)),
        },
    )
    return destination


def _irs_soi_file_pairs(source_dir: Path) -> dict[str, dict[str, Path]]:
    pairs: dict[str, dict[str, Path]] = {}
    for path in sorted(source_dir.glob("county*flow*.csv")):
        match = re.fullmatch(r"county(inflow|outflow)(\d{4})\.csv", path.name.lower())
        if match is None:
            continue
        perspective, year_token = match.groups()
        pairs.setdefault(year_token, {})[perspective] = path
    return {
        year_token: paths
        for year_token, paths in pairs.items()
        if {"inflow", "outflow"} <= set(paths)
    }


def _irs_soi_later_year(year_token: str) -> int:
    later_two_digit = int(year_token[2:])
    return 2000 + later_two_digit


def _read_irs_soi_csv(path: Path, *, source_label: str) -> pd.DataFrame:
    try:
        rows = pd.read_csv(path, dtype=str)
    except UnicodeDecodeError:
        rows = pd.read_csv(path, dtype=str, encoding="latin1")
    rows.columns = [_clean_column(column) for column in rows.columns]
    missing = [column for column in IRS_SOI_REQUIRED_RAW_COLUMNS if column not in rows.columns]
    if missing:
        raise ValueError(
            f"IRS SOI {source_label} file {path} is missing required columns {missing}. "
            "Expected IRS county migration CSV columns including y2_statefips, "
            "y2_countyfips, y1_statefips, y1_countyfips, n1, n2, and agi. "
            "Check the file-year users guide and record layout."
        )
    return rows


def _normalize_irs_soi_flow_rows(
    raw: pd.DataFrame,
    *,
    year: int,
    perspective: str,
    raw_file: Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    flows: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for source_row, row in raw.reset_index(drop=True).iterrows():
        origin_state = _irs_soi_fips(row["y1_statefips"], width=2)
        origin_county = _irs_soi_fips(row["y1_countyfips"], width=3)
        destination_state = _irs_soi_fips(row["y2_statefips"], width=2)
        destination_county = _irs_soi_fips(row["y2_countyfips"], width=3)
        origin_fips = f"{origin_state}{origin_county}"
        destination_fips = f"{destination_state}{destination_county}"
        values = {
            "returns": _irs_soi_number(row["n1"]),
            "exemptions": _irs_soi_number(row["n2"]),
            "agi_thousands": _irs_soi_number(row["agi"]),
        }
        exclusion_reason = _irs_soi_exclusion_reason(
            origin_state=origin_state,
            origin_county=origin_county,
            destination_state=destination_state,
            destination_county=destination_county,
        )
        record = {
            "year": year,
            "perspective": perspective,
            "raw_file": str(raw_file),
            "source_row": int(source_row) + 2,
            "origin_county_fips": origin_fips,
            "destination_county_fips": destination_fips,
            **values,
        }
        if exclusion_reason is not None:
            skipped.append({**record, "exclusion_reason": exclusion_reason})
            continue
        flows.append(record)
    flow_columns = [
        "year",
        "perspective",
        "raw_file",
        "source_row",
        "origin_county_fips",
        "destination_county_fips",
        "returns",
        "exemptions",
        "agi_thousands",
    ]
    skipped_columns = [*flow_columns, "exclusion_reason"]
    return pd.DataFrame(flows, columns=flow_columns), pd.DataFrame(
        skipped,
        columns=skipped_columns,
    )


def _irs_soi_exclusion_reason(
    *,
    origin_state: str,
    origin_county: str,
    destination_state: str,
    destination_county: str,
) -> str | None:
    if (
        origin_state in IRS_SOI_SUMMARY_STATE_FIPS
        or destination_state in IRS_SOI_SUMMARY_STATE_FIPS
    ):
        return "summary_pseudo_state_row"
    if (
        origin_county == IRS_SOI_STATE_TOTAL_COUNTY_FIPS
        or destination_county == IRS_SOI_STATE_TOTAL_COUNTY_FIPS
    ):
        return "state_total_row"
    if origin_state == destination_state and origin_county == destination_county:
        return "same_county_non_migrant"
    return None


def _irs_soi_county_marginals(
    *,
    inflow_flows: pd.DataFrame,
    outflow_flows: pd.DataFrame,
    inflow_skipped: pd.DataFrame,
    outflow_skipped: pd.DataFrame,
    year: int,
) -> pd.DataFrame:
    inflows = _irs_soi_group_flows(
        inflow_flows,
        county_column="destination_county_fips",
        prefix="inflow",
    )
    outflows = _irs_soi_group_flows(
        outflow_flows,
        county_column="origin_county_fips",
        prefix="outflow",
    )
    other_inflows = _irs_soi_group_other_flows(
        inflow_skipped,
        county_column="destination_county_fips",
        prefix="other_flows_inflow",
    )
    other_outflows = _irs_soi_group_other_flows(
        outflow_skipped,
        county_column="origin_county_fips",
        prefix="other_flows_outflow",
    )
    county_fips = sorted(
        set(inflows.index)
        | set(outflows.index)
        | set(other_inflows.index)
        | set(other_outflows.index)
    )
    result = pd.DataFrame({"county_fips": county_fips})
    result["geo_type"] = "county"
    result["geo_id"] = result["county_fips"]
    result["year"] = year
    for grouped in (inflows, outflows, other_inflows, other_outflows):
        result = result.merge(grouped, left_on="county_fips", right_index=True, how="left")
    for column in (
        "inflow_returns",
        "inflow_exemptions",
        "inflow_agi_thousands",
        "outflow_returns",
        "outflow_exemptions",
        "outflow_agi_thousands",
        "other_flows_inflow_returns",
        "other_flows_inflow_exemptions",
        "other_flows_inflow_agi_thousands",
        "other_flows_outflow_returns",
        "other_flows_outflow_exemptions",
        "other_flows_outflow_agi_thousands",
    ):
        result[column] = (
            pd.to_numeric(result[column], errors="coerce").fillna(0).astype("int64")
        )
    return result.sort_values(["county_fips", "year"]).reset_index(drop=True)


def _irs_soi_group_flows(
    flows: pd.DataFrame,
    *,
    county_column: str,
    prefix: str,
) -> pd.DataFrame:
    if flows.empty:
        return pd.DataFrame(
            columns=[f"{prefix}_returns", f"{prefix}_exemptions", f"{prefix}_agi_thousands"]
        )
    grouped = flows.groupby(county_column, as_index=True)[
        ["returns", "exemptions", "agi_thousands"]
    ].sum()
    return grouped.rename(
        columns={
            "returns": f"{prefix}_returns",
            "exemptions": f"{prefix}_exemptions",
            "agi_thousands": f"{prefix}_agi_thousands",
        }
    )


def _irs_soi_group_other_flows(
    skipped: pd.DataFrame,
    *,
    county_column: str,
    prefix: str,
) -> pd.DataFrame:
    if skipped.empty:
        return pd.DataFrame(
            columns=[f"{prefix}_returns", f"{prefix}_exemptions", f"{prefix}_agi_thousands"]
        )
    rows = skipped[skipped["exclusion_reason"] == "summary_pseudo_state_row"]
    other_state_column = (
        "origin_county_fips"
        if county_column == "destination_county_fips"
        else "destination_county_fips"
    )
    rows = rows[rows[other_state_column].str[:2].isin(IRS_SOI_OTHER_FLOW_STATE_FIPS)]
    rows = rows[
        (rows[county_column].str.len() == 5)
        & (~rows[county_column].str[:2].isin(IRS_SOI_SUMMARY_STATE_FIPS))
        & (~rows[county_column].str.endswith(IRS_SOI_STATE_TOTAL_COUNTY_FIPS))
    ]
    return _irs_soi_group_flows(rows, county_column=county_column, prefix=prefix)


def _irs_soi_summary_row_counts(summary_rows: pd.DataFrame) -> list[dict[str, object]]:
    if summary_rows.empty:
        return []
    grouped = (
        summary_rows.groupby(["year", "perspective", "exclusion_reason"], dropna=False)
        .size()
        .reset_index(name="row_count")
        .sort_values(["year", "perspective", "exclusion_reason"])
    )
    return grouped.to_dict(orient="records")


def _irs_soi_pair_table(
    inflow_flows: pd.DataFrame,
    outflow_flows: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    combined = pd.concat([inflow_flows, outflow_flows], ignore_index=True)
    if combined.empty:
        return (
            pd.DataFrame(
                columns=[
                    "year",
                    "origin_county_fips",
                    "destination_county_fips",
                    "migration_returns",
                    "migration_exemptions",
                    "migration_agi_thousands",
                ]
            ),
            [],
        )
    keys = ["year", "origin_county_fips", "destination_county_fips", "perspective"]
    combined = (
        combined.groupby(keys, as_index=False, dropna=False)[
            ["returns", "exemptions", "agi_thousands"]
        ]
        .sum()
        .reset_index(drop=True)
    )
    reconciliation = _irs_soi_pair_reconciliation_mismatches(combined)
    combined["perspective_rank"] = combined["perspective"].map({"inflow": 0, "outflow": 1})
    combined = combined.sort_values("perspective_rank").drop_duplicates(
        ["year", "origin_county_fips", "destination_county_fips"],
        keep="first",
    )
    result = (
        combined[
            [
                "year",
                "origin_county_fips",
                "destination_county_fips",
                "returns",
                "exemptions",
                "agi_thousands",
            ]
        ]
        .rename(
            columns={
                "returns": "migration_returns",
                "exemptions": "migration_exemptions",
                "agi_thousands": "migration_agi_thousands",
            }
        )
        .sort_values(["year", "origin_county_fips", "destination_county_fips"])
        .reset_index(drop=True)
    )
    return result, reconciliation


def _irs_soi_pair_reconciliation_mismatches(
    combined: pd.DataFrame,
) -> list[dict[str, object]]:
    keys = ["year", "origin_county_fips", "destination_county_fips"]
    value_columns = ["returns", "exemptions", "agi_thousands"]
    rows: list[dict[str, object]] = []
    for key, group in combined.groupby(keys, sort=True, dropna=False):
        perspectives = set(group["perspective"].dropna().astype(str))
        if not {"inflow", "outflow"} <= perspectives:
            continue
        by_perspective = group.set_index("perspective")
        inflow = by_perspective.loc["inflow"]
        outflow = by_perspective.loc["outflow"]
        if isinstance(inflow, pd.DataFrame):
            inflow = inflow.iloc[0]
        if isinstance(outflow, pd.DataFrame):
            outflow = outflow.iloc[0]
        mismatch_fields = [
            column
            for column in value_columns
            if int(inflow[column]) != int(outflow[column])
        ]
        if not mismatch_fields:
            continue
        year, origin, destination = key
        row: dict[str, object] = {
            "year": int(year),
            "origin_county_fips": str(origin),
            "destination_county_fips": str(destination),
            "mismatch_fields": mismatch_fields,
            "preferred_perspective": "inflow",
        }
        for column in value_columns:
            row[f"inflow_{column}"] = int(inflow[column])
            row[f"outflow_{column}"] = int(outflow[column])
        rows.append(row)
    return rows


def _irs_soi_fips(value: Any, *, width: int) -> str:
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    return text.zfill(width)


def _irs_soi_number(value: Any) -> int:
    if pd.isna(value):
        return 0
    text = str(value).strip().replace(",", "")
    if text in {"", ".", "-"}:
        return 0
    return int(float(text))


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
    raw = pd.read_excel(source_path, sheet_name=MPI_COUNTY_SHEET, header=MPI_HEADER_ROW - 1)
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
    rows["data_source"] = spec.provider
    rows["source_url"] = spec.source_url
    rows["raw_sha256"] = raw_sha256
    rows["ingested_at"] = ingested_at
    rows = rows[list(MPI_CURATED_COUNTY_COLUMNS)]

    skipped_preview = skipped_rows.head(20).to_dict(orient="records")
    multi_county_rows = (
        skipped_rows[
            skipped_rows.get("member_county_fips", pd.Series(dtype=object)).notna()
        ].to_dict(orient="records")
        if not skipped_rows.empty
        else []
    )
    msa_rows = (
        skipped_rows[skipped_rows.get("exclusion_reason") == "msa_row"].to_dict(
            orient="records"
        )
        if not skipped_rows.empty
        else []
    )
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
            "multi_county_rows": multi_county_rows,
            "msa_rows": msa_rows,
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
        excel_row = int(source_row) + MPI_FIRST_DATA_ROW
        state_name = _clean_mpi_value(row.get("State"))
        county_label = _clean_mpi_value(row.get("County"))
        if not county_label:
            continue
        population = row.get("Number of Unauthorized Immigrants")
        share = row.get("County Share of the Total Unauthorized Immigrant Population")
        county_name, exclusion_reason = _mpi_county_name_from_label(county_label, state_name)
        member_county_fips: list[str] | None = None
        if exclusion_reason is None:
            match = lookup.get(
                (_normalize_county_key(state_name), _normalize_county_key(county_name))
            )
            if match is None:
                exclusion_reason = "unmatched_county_reference"
                if county_name is not None and "-" in county_name:
                    exclusion_reason = "hyphenated_unresolved"
        elif exclusion_reason == "multi_county_row":
            member_county_fips = _mpi_multi_county_fips_from_label(
                county_label,
                state_name,
                lookup,
            )
        if exclusion_reason is not None:
            skipped.append(
                {
                    "source_sheet": MPI_COUNTY_SHEET,
                    "source_row": excel_row,
                    "state_name": state_name,
                    "county_label": county_label,
                    "year": MPI_ESTIMATE_YEAR,
                    "unauthorized_immigrant_population": population,
                    "unauthorized_immigrant_share_of_us_total": share,
                    "member_county_fips": member_county_fips,
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
    if not county_name.endswith(MPI_ALLOWED_COUNTY_EQUIVALENT_LABELS):
        return None, "unsupported_county_equivalent"
    return county_name, None


def _mpi_multi_county_fips_from_label(
    county_label: str,
    state_name: str,
    lookup: dict[tuple[str, str], tuple[str, str]],
) -> list[str] | None:
    if not state_name:
        return None
    suffix = f", {state_name}"
    if not county_label.endswith(suffix):
        return None
    county_part = county_label[: -len(suffix)].strip()
    if county_part.endswith(" Counties"):
        county_part = county_part[: -len(" Counties")]
        county_suffix = " County"
    elif county_part.endswith(" Parishes"):
        county_part = county_part[: -len(" Parishes")]
        county_suffix = " Parish"
    else:
        return None
    county_fips: list[str] = []
    for name_part in county_part.split("-"):
        county_name = f"{name_part.strip()}{county_suffix}"
        match = lookup.get(
            (_normalize_county_key(state_name), _normalize_county_key(county_name))
        )
        if match is None:
            return None
        county_fips.append(match[0])
    return county_fips


def _default_county_reference_paths() -> tuple[Path, ...]:
    candidates = sorted((raw_root() / "pep").glob("pep_county__v*__*.csv"))
    if candidates:
        return tuple(candidates)
    raise FileNotFoundError(
        "No default county reference found for MPI ingest. Expected a Census PEP "
        f"county raw CSV under {raw_root() / 'pep'}, or pass --county-reference-path with "
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
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(
        f"Unsupported covariate raw file type '{suffix}'. Use CSV, TXT, Parquet, or Excel."
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


def _sha256_tree(path: Path) -> str:
    if path.is_file():
        return _sha256(path)
    digest = hashlib.sha256()
    for child in sorted(child for child in path.rglob("*") if child.is_file()):
        digest.update(str(child.relative_to(path)).encode("utf-8"))
        with child.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    return digest.hexdigest()
