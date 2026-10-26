"""Curated, jurisdiction-year right-to-shelter coding and geography mapping.

The source table contains cited legal status, not pre-collapsed flags. Every
annual row represents the status in force on January 31 of its year (the usual
late-January PIT reference date); effective and ended dates retain the legal
interval used to make that coding decision. Partial-year changes therefore
take effect in the first annual row whose January 31 reference date falls in
the new interval.

Strict coding counts only enforceable year-round strength. Broad coding counts
enforceable and conditional year-round strength, and can optionally include
the separate weather-only indication. Geography flags default to a population
weighted majority; accompanying shares preserve the degree of coverage.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

from hhplab.geographies.msa.msa_definitions import (
    DEFINITION_VERSION as DEFAULT_MSA_DEFINITION_VERSION,
)
from hhplab.schema.contracts import DataAnnotationSpec, serialize_annotation_contract
from hhplab.storage.provenance import ProvenanceBlock, write_parquet_with_provenance

STRENGTH_LEVELS = ("enforceable", "conditional", "weather_only", "none")
POPULATIONS = ("single_adults", "families", "pregnant_people")
CODING_RULES = ("strict", "broad")
GEOGRAPHY_RULES = ("majority", "any")

JURISDICTION_SOURCE_COLUMNS = (
    "jurisdiction_id",
    "jurisdiction_type",
    "year",
    "single_adults_strength",
    "single_adults_weather_only",
    "families_strength",
    "families_weather_only",
    "pregnant_people_strength",
    "pregnant_people_weather_only",
    "citation",
    "citation_source_type",
    "citation_redistribution",
    "effective_date",
    "ended_date",
)
JURISDICTION_FLAG_COLUMNS = tuple(f"rts_{population}" for population in POPULATIONS)
POPULATION_SHARE_COLUMNS = tuple(
    f"rts_{population}_population_share" for population in POPULATIONS
)
MAPPING_COLUMNS = ("geo_id", "year", *JURISDICTION_FLAG_COLUMNS, *POPULATION_SHARE_COLUMNS)
COUNTY_MEMBERSHIP_COLUMNS = ("geo_id", "county_fips", "allocation_share")
COUNTY_POPULATION_COLUMNS = (
    "county_fips",
    "year",
    "population_single_adults",
    "population_families",
    "population_pregnant_people",
)

# Census county equivalents for New York City. Statewide rows use two-digit
# state FIPS identifiers; these five counties are assigned to the explicit NYC
# sub-state jurisdiction when present in the curated table.
NYC_COUNTY_FIPS = ("36005", "36047", "36061", "36081", "36085")
DEFAULT_SUBSTATE_COUNTY_MEMBERSHIP = pd.DataFrame(
    {"jurisdiction_id": ["NYC"] * len(NYC_COUNTY_FIPS),
     "jurisdiction_type": ["sub_state"] * len(NYC_COUNTY_FIPS),
     "county_fips": list(NYC_COUNTY_FIPS)}
)

RTS_ANNOTATION_ID = "right-to-shelter-geographic-approximation"
RTS_SOURCE_TABLE_FILENAME = "right_to_shelter__jurisdiction_year.csv"


class RightToShelterError(ValueError):
    """Raised when a right-to-shelter source or mapping is incomplete or invalid."""


def curated_source_table_path(data_root: Path | str | None = None) -> Path:
    """Return the canonical source-table path under ``data/curated``."""
    root = Path("data") if data_root is None else Path(data_root)
    return root / "curated" / "right_to_shelter" / RTS_SOURCE_TABLE_FILENAME


def mapping_output_path(
    geography: Literal["msa", "coc"],
    first_year: int,
    last_year: int,
    vintage: str | int,
    data_root: Path | str | None = None,
) -> Path:
    """Return a deterministic curated output path for one geography family."""
    if geography not in {"msa", "coc"}:
        raise RightToShelterError("geography must be 'msa' or 'coc'.")
    if first_year > last_year:
        raise RightToShelterError("first_year must be less than or equal to last_year.")
    token = f"@M{vintage}" if geography == "msa" else f"@B{vintage}"
    filename = f"right_to_shelter__{geography}__Y{first_year}-{last_year}{token}.parquet"
    root = Path("data") if data_root is None else Path(data_root)
    return root / "curated" / "right_to_shelter" / filename


def load_jurisdiction_source_table(path: Path | str | None = None) -> pd.DataFrame:
    """Load and validate the curated CSV; report a direct action on failure."""
    source_path = curated_source_table_path() if path is None else Path(path)
    if not source_path.is_file():
        raise FileNotFoundError(
            f"Right-to-shelter table not found at {source_path}. Add cited rows to "
            f"{RTS_SOURCE_TABLE_FILENAME} using JURISDICTION_SOURCE_COLUMNS, then rerun."
        )
    try:
        frame = pd.read_csv(source_path, dtype={"jurisdiction_id": "string"})
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise RightToShelterError(
            f"Could not read right-to-shelter CSV at {source_path}: {exc}. "
            "Check that it is a UTF-8 CSV with one header row."
        ) from exc
    return validate_jurisdiction_source_table(frame)


def validate_jurisdiction_source_table(frame: pd.DataFrame) -> pd.DataFrame:
    """Validate source columns and row evidence without requiring any real rows."""
    missing = sorted(set(JURISDICTION_SOURCE_COLUMNS) - set(frame.columns))
    if missing:
        raise RightToShelterError(
            f"Right-to-shelter table is missing columns {missing}. "
            "Use JURISDICTION_SOURCE_COLUMNS as the header."
        )
    result = frame.loc[:, JURISDICTION_SOURCE_COLUMNS].copy()
    if result.empty:
        return result

    for column in ("jurisdiction_id", "jurisdiction_type", "citation"):
        result[column] = result[column].astype("string").str.strip()
        empty = result[column].isna() | result[column].eq("")
        if empty.any():
            raise RightToShelterError(f"Rows {list(result.index[empty])} need a nonempty {column}.")

    source_types = {"statute", "regulation", "court_order", "secondary"}
    result["citation_source_type"] = (
        result["citation_source_type"].astype("string").str.strip()
    )
    result["citation_redistribution"] = (
        result["citation_redistribution"].astype("string").str.strip()
    )
    invalid_source_types = sorted(
        set(result["citation_source_type"].dropna()) - source_types
    )
    if result["citation_source_type"].isna().any() or invalid_source_types:
        raise RightToShelterError(
            "citation_source_type must be statute, regulation, court_order, or secondary."
        )
    redistribution_statuses = {
        "public_domain", "public", "permission_required", "unknown", "not_applicable"
    }
    invalid_redistribution = sorted(
        set(result["citation_redistribution"].dropna()) - redistribution_statuses
    )
    if result["citation_redistribution"].isna().any() or invalid_redistribution:
        raise RightToShelterError(
            "citation_redistribution must record public_domain, public, permission_required, "
            "unknown, or not_applicable for every cited row."
        )
    secondary_missing_status = result["citation_source_type"].eq("secondary") & (
        result["citation_redistribution"].eq("not_applicable")
    )
    if secondary_missing_status.any():
        raise RightToShelterError(
            "Secondary citations need an explicit redistribution status; "
            "not_applicable is reserved for primary legal sources."
        )

    try:
        years = pd.to_numeric(result["year"], errors="raise")
    except (TypeError, ValueError) as exc:
        raise RightToShelterError("Each jurisdiction-year needs an integer year.") from exc
    if years.isna().any() or (years % 1 != 0).any():
        raise RightToShelterError("Each jurisdiction-year needs an integer year.")
    result["year"] = years.astype("int64")

    duplicate_key = result.duplicated(["jurisdiction_id", "year"], keep=False)
    if duplicate_key.any():
        keys = result.loc[duplicate_key, ["jurisdiction_id", "year"]].to_dict("records")
        raise RightToShelterError(f"Duplicate jurisdiction-year rows found: {keys}.")

    for population in POPULATIONS:
        strength_column = f"{population}_strength"
        result[strength_column] = result[strength_column].astype("string").str.strip()
        bad_levels = sorted(set(result[strength_column].dropna()) - set(STRENGTH_LEVELS))
        if result[strength_column].isna().any() or bad_levels:
            raise RightToShelterError(
                f"{strength_column} must use {STRENGTH_LEVELS}; invalid values: {bad_levels}."
            )
        weather_column = f"{population}_weather_only"
        result[weather_column] = _normalize_boolean(result[weather_column], weather_column)

    for column in ("effective_date", "ended_date"):
        values = result[column].replace({"": pd.NA})
        try:
            parsed = pd.to_datetime(values, format="%Y-%m-%d", errors="raise")
        except (TypeError, ValueError) as exc:
            raise RightToShelterError(
                f"{column} values must be ISO dates (YYYY-MM-DD) or blank."
            ) from exc
        result[column] = parsed.dt.strftime("%Y-%m-%d").astype("string")

    effective = pd.to_datetime(result["effective_date"], errors="coerce")
    ended = pd.to_datetime(result["ended_date"], errors="coerce")
    reversed_dates = effective.notna() & ended.notna() & (effective > ended)
    if reversed_dates.any():
        raise RightToShelterError(
            f"Rows {list(result.index[reversed_dates])} end before their effective date."
        )
    return result.reset_index(drop=True)


def _normalize_boolean(values: pd.Series, column: str) -> pd.Series:
    truth_values = {"1", "true", "yes"}
    false_values = {"0", "false", "no"}
    normalized: list[bool] = []
    for index, value in values.items():
        if pd.isna(value):
            raise RightToShelterError(f"{column} cannot be blank (row {index}).")
        normalized_value = str(value).strip().lower()
        if normalized_value in truth_values:
            normalized.append(True)
        elif normalized_value in false_values:
            normalized.append(False)
        else:
            raise RightToShelterError(
                f"{column} must contain true/false or 1/0 values (row {index}: {value!r})."
            )
    return pd.Series(normalized, index=values.index, dtype="bool")


def derive_rts_flags(
    frame: pd.DataFrame,
    *,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
) -> pd.DataFrame:
    """Derive per-population 0/1 flags from validated strength fields.

    ``strict`` means enforceable year-round protection only. ``broad`` means
    enforceable or conditional year-round protection; when
    ``include_weather_only`` is true, the independent weather-only field or
    ``weather_only`` strength also qualifies. This option requires broad
    coding; combining it with strict coding raises ``RightToShelterError``.
    """
    if coding_rule not in CODING_RULES:
        raise RightToShelterError(f"coding_rule must be one of {CODING_RULES}.")
    if coding_rule == "strict" and include_weather_only:
        raise RightToShelterError(
            "include_weather_only requires coding_rule='broad'; use broad coding or disable "
            "the weather-only option."
        )
    result = validate_jurisdiction_source_table(frame)
    for population in POPULATIONS:
        strength = result[f"{population}_strength"]
        weather = result[f"{population}_weather_only"]
        if coding_rule == "strict":
            qualifies = strength.eq("enforceable")
        else:
            qualifies = strength.isin(("enforceable", "conditional"))
            if include_weather_only:
                qualifies |= strength.eq("weather_only") | weather
        result[f"rts_{population}"] = qualifies.astype("int8")
    return result


def build_coc_county_membership(
    coc_geometries: pd.DataFrame,
    county_geometries: pd.DataFrame,
    *,
    coc_id_column: str = "coc_id",
    county_id_column: str = "GEOID",
) -> pd.DataFrame:
    """Overlay CoC/county polygons to get county area shares for each CoC.

    Shares use the existing CoC and TIGER county boundary geometries. Population
    mapping applies each county's population weight by this area share, which is
    an explicit approximation for county fragments.
    """
    import geopandas as gpd

    if coc_id_column not in coc_geometries or county_id_column not in county_geometries:
        raise RightToShelterError(
            f"Geometry inputs need '{coc_id_column}' and '{county_id_column}' IDs."
        )
    if coc_geometries.crs is None or county_geometries.crs is None:
        raise RightToShelterError("CoC and county geometries need declared coordinate systems.")
    if not coc_geometries.crs.is_projected or not county_geometries.crs.is_projected:
        raise RightToShelterError(
            "CoC and county geometries must use projected coordinates so area shares are valid."
        )
    county = county_geometries[[county_id_column, "geometry"]].copy()
    if county.crs != coc_geometries.crs:
        county = county.to_crs(coc_geometries.crs)
    county["_county_area"] = county.geometry.area
    pieces = gpd.overlay(
        coc_geometries[[coc_id_column, "geometry"]], county, how="intersection",
        keep_geom_type=False,
    )
    pieces["_intersection_area"] = pieces.geometry.area
    pieces["allocation_share"] = pieces["_intersection_area"] / pieces["_county_area"]
    result = pieces.rename(
        columns={coc_id_column: "geo_id", county_id_column: "county_fips"}
    )[["geo_id", "county_fips", "allocation_share"]]
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    return result.loc[result["allocation_share"] > 0].reset_index(drop=True)


def map_jurisdiction_years(
    source_table: pd.DataFrame,
    geography_county_membership: pd.DataFrame,
    county_population: pd.DataFrame,
    *,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
    geography_rule: Literal["majority", "any"] = "majority",
    majority_threshold: float = 0.5,
    substate_county_membership: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Map cited jurisdiction-year flags to geography-years and population shares.

    State jurisdiction IDs are two-digit state FIPS prefixes. Explicit sub-state
    membership overrides the state assignment; NYC's five borough counties are
    supplied as the default override. County weights are multiplied by geometry
    allocation shares where membership is fractional.
    """
    if geography_rule not in GEOGRAPHY_RULES:
        raise RightToShelterError(f"geography_rule must be one of {GEOGRAPHY_RULES}.")
    if not 0 <= majority_threshold <= 1:
        raise RightToShelterError("majority_threshold must be between 0 and 1.")
    source = derive_rts_flags(
        source_table, coding_rule=coding_rule, include_weather_only=include_weather_only
    )
    membership = _validate_geography_membership(geography_county_membership)
    population = _validate_county_population(county_population)

    overrides = (DEFAULT_SUBSTATE_COUNTY_MEMBERSHIP if substate_county_membership is None
                 else substate_county_membership.copy())
    required_override = {"jurisdiction_id", "jurisdiction_type", "county_fips"}
    if required_override - set(overrides.columns):
        raise RightToShelterError(
            "substate_county_membership needs jurisdiction_id, jurisdiction_type, and county_fips."
        )
    overrides["county_fips"] = overrides["county_fips"].astype(str).str.zfill(5)
    if overrides["county_fips"].duplicated().any():
        raise RightToShelterError("A county may map to only one sub-state jurisdiction.")

    counties = membership.merge(
        population, on="county_fips", how="left", validate="many_to_many",
        suffixes=("", "_population"),
    )
    if counties["year"].isna().any():
        missing = counties.loc[counties["year"].isna(), "county_fips"].drop_duplicates().tolist()
        raise RightToShelterError(
            f"County population weights are missing for {missing}. Supply year-specific "
            "county population weights for all three populations."
        )
    counties["year"] = counties["year"].astype("int64")
    counties["jurisdiction_id"] = counties["county_fips"].str[:2]
    counties = counties.merge(
        overrides[["county_fips", "jurisdiction_id"]].rename(
            columns={"jurisdiction_id": "substate_jurisdiction_id"}
        ), on="county_fips", how="left", validate="many_to_one",
    )
    counties["jurisdiction_id"] = counties["substate_jurisdiction_id"].fillna(
        counties["jurisdiction_id"]
    )

    source_flags = source[["jurisdiction_id", "year", *JURISDICTION_FLAG_COLUMNS]]
    joined = counties.merge(
        source_flags, on=["jurisdiction_id", "year"], how="left", validate="many_to_one",
        indicator=True,
    )
    missing_rows = joined["_merge"].eq("left_only")
    if missing_rows.any():
        missing_keys = joined.loc[missing_rows, ["jurisdiction_id", "year"]].drop_duplicates()
        preview = missing_keys.to_dict("records")
        raise RightToShelterError(
            f"No cited status row exists for mapped jurisdiction-years {preview}. Add those "
            "rows before building geography outputs; unknown status is not coded as zero."
        )
    joined = joined.drop(columns="_merge")

    output_rows: list[dict[str, object]] = []
    for (geo_id, year), group in joined.groupby(["geo_id", "year"], sort=True):
        row: dict[str, object] = {"geo_id": str(geo_id), "year": int(year)}
        for population_name, flag_column, weight_column, share_column in zip(
            POPULATIONS, JURISDICTION_FLAG_COLUMNS,
            COUNTY_POPULATION_COLUMNS[2:], POPULATION_SHARE_COLUMNS, strict=True,
        ):
            weighted = group[weight_column] * group["allocation_share"]
            denominator = float(weighted.sum())
            if denominator <= 0:
                raise RightToShelterError(
                    f"Geography {geo_id} in {year} has no positive {population_name} weight."
                )
            covered = float((weighted * group[flag_column]).sum())
            share = covered / denominator
            row[share_column] = share
            row[flag_column] = int(share > 0) if geography_rule == "any" else int(
                share >= majority_threshold
            )
        output_rows.append(row)

    result = pd.DataFrame(output_rows, columns=MAPPING_COLUMNS)
    result["year"] = result["year"].astype("int64")
    for column in JURISDICTION_FLAG_COLUMNS:
        result[column] = result[column].astype("int8")
    result.attrs["annotations"] = _geographic_approximation_contract(result)
    return result


def map_msa_years(
    source_table: pd.DataFrame,
    msa_county_membership: pd.DataFrame,
    county_population: pd.DataFrame,
    **options,
) -> pd.DataFrame:
    """Map using Census MSA county membership rows from ``msa`` geography code."""
    if not {"msa_id", "county_fips"} <= set(msa_county_membership.columns):
        raise RightToShelterError("MSA membership needs msa_id and county_fips columns.")
    membership = msa_county_membership[["msa_id", "county_fips"]].rename(
        columns={"msa_id": "geo_id"}
    )
    membership["allocation_share"] = 1.0
    return map_jurisdiction_years(
        source_table, membership, county_population, **options
    )


def map_coc_years(
    source_table: pd.DataFrame,
    coc_geometries: pd.DataFrame,
    county_geometries: pd.DataFrame,
    county_population: pd.DataFrame,
    **options,
) -> pd.DataFrame:
    """Map using CoC/county geometry intersections from ``coc`` geography code."""
    membership = build_coc_county_membership(coc_geometries, county_geometries)
    return map_jurisdiction_years(
        source_table, membership, county_population, **options
    )


def _validate_geography_membership(frame: pd.DataFrame) -> pd.DataFrame:
    required = {"geo_id", "county_fips"}
    if required - set(frame.columns):
        raise RightToShelterError(
            "Geography membership needs geo_id and county_fips columns."
        )
    result = frame.copy()
    result["geo_id"] = result["geo_id"].astype("string").str.strip()
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    if result["geo_id"].isna().any() or result["geo_id"].eq("").any():
        raise RightToShelterError("Geography membership contains a blank geo_id.")
    if "allocation_share" not in result:
        result["allocation_share"] = 1.0
    result["allocation_share"] = pd.to_numeric(result["allocation_share"], errors="raise")
    if result["allocation_share"].isna().any() or (
        (result["allocation_share"] <= 0) | (result["allocation_share"] > 1)
    ).any():
            raise RightToShelterError(
                "Membership allocation_share must be greater than 0 and at most 1."
            )
    if result.duplicated(["geo_id", "county_fips"]).any():
        raise RightToShelterError("Geography membership repeats a geo_id/county_fips pair.")
    return result[["geo_id", "county_fips", "allocation_share"]]


def _validate_county_population(frame: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(COUNTY_POPULATION_COLUMNS) - set(frame.columns))
    if missing:
        raise RightToShelterError(
            f"County population weights are missing columns {missing}. Required columns are "
            f"{COUNTY_POPULATION_COLUMNS}."
        )
    result = frame.loc[:, COUNTY_POPULATION_COLUMNS].copy()
    result["county_fips"] = result["county_fips"].astype(str).str.zfill(5)
    result["year"] = pd.to_numeric(result["year"], errors="raise").astype("int64")
    for column in COUNTY_POPULATION_COLUMNS[2:]:
        result[column] = pd.to_numeric(result[column], errors="raise")
        if result[column].isna().any() or (result[column] < 0).any():
            raise RightToShelterError(f"{column} must contain nonnegative numeric weights.")
    if result.duplicated(["county_fips", "year"]).any():
        raise RightToShelterError("County population weights repeat a county-year.")
    return result


def _geographic_approximation_contract(frame: pd.DataFrame) -> dict:
    partial_cells: dict[str, list[list[object]]] = {}
    for flag_column, share_column in zip(
        JURISDICTION_FLAG_COLUMNS, POPULATION_SHARE_COLUMNS, strict=True
    ):
        partial = frame[share_column].between(0, 1, inclusive="neither")
        partial_cells[flag_column] = [
            [str(geo_id), int(year)]
            for geo_id, year in frame.loc[partial, ["geo_id", "year"]].itertuples(
                index=False, name=None
            )
        ]
    if not any(partial_cells.values()):
        return serialize_annotation_contract([])

    affected_geographies = sorted({cell[0] for cells in partial_cells.values() for cell in cells})
    affected_years = sorted({cell[1] for cells in partial_cells.values() for cell in cells})
    annotation = DataAnnotationSpec(
        id=RTS_ANNOTATION_ID,
        category="geographic_approximation",
        summary="Right-to-shelter status is assigned to mixed jurisdictions approximately",
        text=(
            "Right-to-shelter protections are defined by state or sub-state law. "
            "MSA and CoC values are assigned using county population weights; the "
            "listed geography-years are only partially covered by the coded status."
        ),
        public_text=(
            "Right-to-shelter protections are defined by state or sub-state law and "
            "assigned to MSAs and CoCs using county population weights. This "
            "geography-year has partial population coverage."
        ),
        treatment="Use the population-specific covered share alongside each rts flag.",
        uncertainty="County-level population weighting approximates sub-county legal boundaries.",
        evidence=[{
            "name": "Results recipe right-to-shelter coding rule",
            "ref": "docs/results-recipe-spec_2.md",
        }],
        scope={
            "columns": list(JURISDICTION_FLAG_COLUMNS),
            "years": affected_years,
            "geographies": {"include": affected_geographies},
        },
    ).model_dump(mode="json", exclude_none=True)
    identity = f"{RTS_ANNOTATION_ID}#r1"
    bindings = {identity: {"columns": {
        column: cells for column, cells in partial_cells.items() if cells
    }}}
    return serialize_annotation_contract([annotation], bindings=bindings)


def write_mapping_with_provenance(
    frame: pd.DataFrame,
    path: Path | str,
    *,
    geography: Literal["msa", "coc"],
    coding_rule: Literal["strict", "broad"],
    include_weather_only: bool,
    geography_rule: Literal["majority", "any"],
    boundary_vintage: str | int,
    membership_vintage: str,
) -> Path:
    """Write one mapping table with coding lineage and exact annotation cells."""
    annotations = frame.attrs.get("annotations", serialize_annotation_contract([]))
    provenance = ProvenanceBlock(
        geo_type=geography,
        boundary_vintage=str(boundary_vintage) if geography == "coc" else None,
        definition_version=membership_vintage if geography == "msa" else None,
        extra={
            "dataset_type": "right_to_shelter_geography_year",
            "coding_rule": coding_rule,
            "include_weather_only": bool(include_weather_only),
            "geography_rule": geography_rule,
            "membership_vintage": str(membership_vintage),
            "annotations": annotations,
        },
    )
    return write_parquet_with_provenance(frame, path, provenance)


def build_curated_outputs(
    *,
    county_population_path: Path | str,
    msa_definition_version: str = DEFAULT_MSA_DEFINITION_VERSION,
    coc_boundary_vintage: str,
    county_vintage: str,
    source_table_path: Path | str | None = None,
    data_root: Path | str | None = None,
    coding_rule: Literal["strict", "broad"] = "strict",
    include_weather_only: bool = False,
    geography_rule: Literal["majority", "any"] = "majority",
    majority_threshold: float = 0.5,
) -> dict[str, Path]:
    """Build both geography outputs from curated membership and geometry files."""
    from hhplab.artifacts.naming.naming import county_path
    from hhplab.geographies.coc.coc_io import read_geoparquet, resolve_curated_boundary_path
    from hhplab.geographies.msa.msa_io import read_msa_county_membership

    source = load_jurisdiction_source_table(source_table_path)
    if source.empty:
        raise RightToShelterError(
            f"{RTS_SOURCE_TABLE_FILENAME} contains only its header. Add cited "
            "jurisdiction-year status rows before building MSA or CoC outputs."
        )
    try:
        county_population = pd.read_csv(county_population_path, dtype={"county_fips": "string"})
    except (OSError, pd.errors.ParserError, UnicodeDecodeError) as exc:
        raise RightToShelterError(
            f"Could not read county population weights at {county_population_path}: {exc}. "
            f"Provide a CSV with columns {COUNTY_POPULATION_COLUMNS}."
        ) from exc

    msa_membership = read_msa_county_membership(msa_definition_version, data_root)
    msa = map_msa_years(
        source, msa_membership, county_population,
        coding_rule=coding_rule, include_weather_only=include_weather_only,
        geography_rule=geography_rule, majority_threshold=majority_threshold,
    )
    coc_path = resolve_curated_boundary_path(coc_boundary_vintage, data_root)
    county_geometry_path = county_path(county_vintage, data_root)
    coc_geometry = read_geoparquet(coc_path)
    county_geometry = read_geoparquet(county_geometry_path)
    coc = map_coc_years(
        source, coc_geometry, county_geometry, county_population,
        coding_rule=coding_rule, include_weather_only=include_weather_only,
        geography_rule=geography_rule, majority_threshold=majority_threshold,
    )

    first_year = int(source["year"].min())
    last_year = int(source["year"].max())
    msa_output = mapping_output_path(
        "msa", first_year, last_year, msa_definition_version, data_root
    )
    coc_output = mapping_output_path(
        "coc", first_year, last_year, coc_boundary_vintage, data_root
    )
    write_mapping_with_provenance(
        msa, msa_output, geography="msa", coding_rule=coding_rule,
        include_weather_only=include_weather_only, geography_rule=geography_rule,
        boundary_vintage=msa_definition_version, membership_vintage=msa_definition_version,
    )
    write_mapping_with_provenance(
        coc, coc_output, geography="coc", coding_rule=coding_rule,
        include_weather_only=include_weather_only, geography_rule=geography_rule,
        boundary_vintage=coc_boundary_vintage, membership_vintage=county_vintage,
    )
    return {"msa": msa_output, "coc": coc_output}
