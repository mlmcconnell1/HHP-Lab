"""Build MSA-CoC overlap coverage artifacts."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import geopandas as gpd
import pandas as pd

from hhplab.geographies.msa.selectors import (
    PopulationRankingSource,
    select_top_msa_ids_by_population,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.schema.columns import MSA_COC_COVERAGE_COLUMNS, PRIMARY_MSA_ANNOTATION_COLUMNS
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS

OverlapBasis = Literal["area", "population"]

REQUIRED_MSA_MEMBERSHIP_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "county_fips",
)


def build_msa_coc_coverage(
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
    ranking_population_df: pd.DataFrame,
    *,
    year: int,
    top_n: int,
    ranking_population_source: PopulationRankingSource,
    ranking_reference_year: int,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    overlap_bases: tuple[OverlapBasis, ...] = ("area",),
    acs5_population_df: pd.DataFrame | None = None,
    tract_gdf: gpd.GeoDataFrame | None = None,
    acs5_population_vintage: int | str | None = None,
    population_column: str = "total_population",
    ranking_population_column: str = "population",
    min_msa_area_coverage_share: float | None = None,
    min_msa_population_coverage_share: float | None = None,
    min_coc_area_containment_share: float | None = None,
    allow_incomplete_population_denominators: bool = False,
) -> pd.DataFrame:
    """Build area and population overlap coverage for top-N MSAs.

    The returned artifact is long-form: each MSA/CoC pair can have one row for
    ``overlap_basis == "area"`` and one row for ``"population"``. Percent
    columns are reciprocal denominator views of the same numerator:
    ``msa_covered_by_coc_percent`` uses the MSA denominator and
    ``coc_contained_in_msa_percent`` uses the CoC denominator.
    """
    bases = _normalize_overlap_bases(overlap_bases)
    membership = _normalize_membership(msa_county_membership)
    selection = select_top_msa_ids_by_population(
        ranking_population_df,
        membership,
        ranking_source=ranking_population_source,
        reference_year=ranking_reference_year,
        top_n=top_n,
        population_column=ranking_population_column,
    )
    selected_msa_ids = tuple(selection.selector_ids)
    if not selected_msa_ids:
        return _empty_coverage()

    coc_proj = _prepare_coc_geometries(coc_gdf)
    msa_proj = _build_msa_geometries(
        county_gdf,
        membership,
        selected_msa_ids=selected_msa_ids,
    )
    if coc_proj.empty or msa_proj.empty:
        return _empty_coverage()

    frames: list[pd.DataFrame] = []
    pair_geometries: gpd.GeoDataFrame | None = None
    if "area" in bases or "population" in bases:
        pair_geometries = _intersect_msa_coc(msa_proj, coc_proj)
    if "area" in bases:
        frames.append(
            _area_coverage_rows(
                pair_geometries,
                msa_proj,
                coc_proj,
                year=year,
                top_n=top_n,
                ranking_population_source=ranking_population_source,
                ranking_reference_year=ranking_reference_year,
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                definition_version=definition_version,
                min_msa_coverage_share=min_msa_area_coverage_share,
                min_coc_containment_share=min_coc_area_containment_share,
            )
        )
    if "population" in bases:
        population_msa_proj = msa_proj
        population_coc_proj = coc_proj
        if pair_geometries is not None and not pair_geometries.empty:
            population_msa_ids = set(pair_geometries["msa_id"].astype(str))
            population_coc_ids = set(pair_geometries["coc_id"].astype(str))
            population_msa_proj = msa_proj[
                msa_proj["msa_id"].astype(str).isin(population_msa_ids)
            ].copy()
            population_coc_proj = coc_proj[
                coc_proj["coc_id"].astype(str).isin(population_coc_ids)
            ].copy()
        frames.append(
            _population_coverage_rows(
                pair_geometries,
                population_msa_proj,
                population_coc_proj,
                acs5_population_df=acs5_population_df,
                tract_gdf=tract_gdf,
                year=year,
                top_n=top_n,
                ranking_population_source=ranking_population_source,
                ranking_reference_year=ranking_reference_year,
                boundary_vintage=boundary_vintage,
                county_vintage=county_vintage,
                definition_version=definition_version,
                acs5_population_vintage=acs5_population_vintage,
                population_column=population_column,
                min_msa_coverage_share=min_msa_population_coverage_share,
                min_coc_containment_share=None,
                allow_incomplete_population_denominators=allow_incomplete_population_denominators,
            )
        )

    result = pd.concat(frames, ignore_index=True) if frames else _empty_coverage()
    if result.empty:
        return _empty_coverage()
    result = (
        result.loc[:, MSA_COC_COVERAGE_COLUMNS]
        .sort_values(["msa_id", "coc_id", "overlap_basis"])
        .reset_index(drop=True)
    )
    result.attrs["selection_diagnostics"] = selection.diagnostics.to_dict()
    result.attrs["selected_msa_ids"] = list(selected_msa_ids)
    result.attrs["selected_msa_count"] = len(selected_msa_ids)
    return result


def save_msa_coc_coverage(
    coverage: pd.DataFrame,
    output_path: Path | str,
    *,
    year: int,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    overlap_bases: tuple[OverlapBasis, ...],
    ranking_population_source: PopulationRankingSource,
    ranking_reference_year: int,
    top_n: int,
    acs5_population_vintage: int | str | None = None,
    input_artifacts: dict[str, str] | None = None,
) -> Path:
    """Persist MSA-CoC coverage with embedded provenance."""
    basis_values = _normalize_overlap_bases(overlap_bases)
    provenance = ProvenanceBlock(
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        acs_vintage=str(acs5_population_vintage) if acs5_population_vintage is not None else None,
        geo_type="msa",
        definition_version=definition_version,
        weighting="+".join(basis_values),
        extra={
            "dataset_type": "msa_coc_coverage",
            "year": year,
            "top_n": top_n,
            "overlap_bases": list(basis_values),
            "ranking_population_source": ranking_population_source,
            "ranking_reference_year": ranking_reference_year,
            "selection_diagnostics": coverage.attrs.get("selection_diagnostics", {}),
            "selected_msa_ids": coverage.attrs.get("selected_msa_ids", []),
            "denominator_definitions": {
                "area": "projected geometry area in ESRI:102003",
                "population": "ACS5 tract total_population allocated by tract area share",
            },
            "input_artifacts": input_artifacts or {},
            "row_count": int(len(coverage)),
            "selected_msa_count": int(coverage["msa_id"].nunique()) if "msa_id" in coverage else 0,
        },
    )
    return write_parquet_with_provenance(coverage, output_path, provenance)


def read_msa_coc_coverage(path: Path | str) -> pd.DataFrame:
    """Read an MSA-CoC overlap coverage artifact from an explicit path."""
    try:
        return pd.read_parquet(path)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"MSA-CoC overlap coverage artifact not found at {path}. "
            "Build it through an MSA-CoC coverage recipe or recipe-backed helper."
        ) from None


def select_primary_msa_for_cocs(
    overlap: pd.DataFrame,
    *,
    coc_ids: list[str] | tuple[str, ...] | None = None,
    overlap_basis: OverlapBasis = "area",
    min_coc_contained_share: float = 0.0,
) -> pd.DataFrame:
    """Select one primary MSA per CoC from MSA-CoC overlap rows.

    ``overlap`` may be a modern MSA-CoC coverage artifact, which carries
    percent columns, or the older area crosswalk shape, which carries
    ``allocation_share`` as a fraction of CoC area.  Ties are deterministic:
    highest CoC-contained share, then lowest ``msa_id``.
    """
    if overlap_basis not in {"area", "population"}:
        raise ValueError("overlap_basis must be 'area' or 'population'.")
    if not 0.0 <= min_coc_contained_share <= 1.0:
        raise ValueError("min_coc_contained_share must be between 0.0 and 1.0.")

    universe = _primary_msa_coc_universe(overlap, coc_ids)
    if overlap.empty:
        return _empty_primary_msa_annotations(universe)

    rows = overlap.copy()
    missing = sorted({"coc_id", "msa_id"} - set(rows.columns))
    if missing:
        raise ValueError(
            "MSA-CoC overlap rows are missing required columns "
            f"{missing}. Available: {list(rows.columns)}"
        )
    if "overlap_basis" in rows.columns:
        rows = rows[rows["overlap_basis"].astype(str) == overlap_basis].copy()
    if rows.empty:
        return _empty_primary_msa_annotations(universe)

    rows["coc_id"] = rows["coc_id"].astype(str)
    rows["msa_id"] = rows["msa_id"].astype(str).str.zfill(5)
    rows["primary_msa_coc_contained_percent"] = _coc_contained_percent(rows)
    rows["primary_msa_covered_by_coc_percent"] = _msa_covered_percent(rows)
    rows = rows[
        rows["primary_msa_coc_contained_percent"].fillna(-1.0)
        >= min_coc_contained_share * 100.0
    ].copy()
    if rows.empty:
        return _empty_primary_msa_annotations(universe)

    if "msa_name" not in rows.columns:
        rows["msa_name"] = pd.NA
    if overlap_basis == "population" and "msa_denominator" in rows.columns:
        rows["primary_msa_population"] = pd.to_numeric(
            rows["msa_denominator"],
            errors="coerce",
        )
    else:
        rows["primary_msa_population"] = pd.Series(
            pd.NA,
            index=rows.index,
            dtype="Float64",
        )
    rows["primary_msa_overlap_basis"] = overlap_basis
    selected = (
        rows.sort_values(
            ["coc_id", "primary_msa_coc_contained_percent", "msa_id"],
            ascending=[True, False, True],
            kind="mergesort",
        )
        .drop_duplicates("coc_id", keep="first")
        .rename(
            columns={
                "msa_id": "primary_msa_id",
                "msa_name": "primary_msa_name",
            }
        )
        .loc[:, PRIMARY_MSA_ANNOTATION_COLUMNS]
    )
    if universe:
        base = _empty_primary_msa_annotations(universe).drop(columns=selected.columns[1:])
        selected = base.merge(selected, on="coc_id", how="left")
    return (
        selected.loc[:, PRIMARY_MSA_ANNOTATION_COLUMNS]
        .sort_values("coc_id")
        .reset_index(drop=True)
    )


def _primary_msa_coc_universe(
    overlap: pd.DataFrame,
    coc_ids: list[str] | tuple[str, ...] | None,
) -> list[str]:
    values: list[str] = []
    if coc_ids is not None:
        values.extend(str(coc_id) for coc_id in coc_ids)
    if "coc_id" in overlap.columns:
        values.extend(overlap["coc_id"].dropna().astype(str).tolist())
    return sorted(dict.fromkeys(values))


def _empty_primary_msa_annotations(coc_ids: list[str]) -> pd.DataFrame:
    result = pd.DataFrame({"coc_id": coc_ids})
    result["primary_msa_id"] = pd.NA
    result["primary_msa_name"] = pd.NA
    result["primary_msa_population"] = pd.Series(
        pd.NA,
        index=result.index,
        dtype="Float64",
    )
    result["primary_msa_overlap_basis"] = pd.NA
    result["primary_msa_coc_contained_percent"] = pd.Series(
        pd.NA,
        index=result.index,
        dtype="Float64",
    )
    result["primary_msa_covered_by_coc_percent"] = pd.Series(
        pd.NA,
        index=result.index,
        dtype="Float64",
    )
    return result.loc[:, PRIMARY_MSA_ANNOTATION_COLUMNS]


def _coc_contained_percent(rows: pd.DataFrame) -> pd.Series:
    if "coc_contained_in_msa_percent" in rows.columns:
        return pd.to_numeric(rows["coc_contained_in_msa_percent"], errors="coerce")
    if "allocation_share" in rows.columns:
        return pd.to_numeric(rows["allocation_share"], errors="coerce") * 100.0
    if {"intersection_area", "coc_area"} <= set(rows.columns):
        numerator = pd.to_numeric(rows["intersection_area"], errors="coerce")
        denominator = pd.to_numeric(rows["coc_area"], errors="coerce")
        return (numerator / denominator.where(denominator > 0)) * 100.0
    raise ValueError(
        "MSA-CoC overlap rows must include coc_contained_in_msa_percent, "
        "allocation_share, or intersection_area + coc_area."
    )


def _msa_covered_percent(rows: pd.DataFrame) -> pd.Series:
    if "msa_covered_by_coc_percent" in rows.columns:
        return pd.to_numeric(rows["msa_covered_by_coc_percent"], errors="coerce")
    if {"intersection_value", "msa_denominator"} <= set(rows.columns):
        numerator = pd.to_numeric(rows["intersection_value"], errors="coerce")
        denominator = pd.to_numeric(rows["msa_denominator"], errors="coerce")
        return (numerator / denominator.where(denominator > 0)) * 100.0
    if {"intersection_area", "msa_denominator"} <= set(rows.columns):
        numerator = pd.to_numeric(rows["intersection_area"], errors="coerce")
        denominator = pd.to_numeric(rows["msa_denominator"], errors="coerce")
        return (numerator / denominator.where(denominator > 0)) * 100.0
    return pd.Series(pd.NA, index=rows.index, dtype="Float64")


def _empty_coverage() -> pd.DataFrame:
    return pd.DataFrame(columns=list(MSA_COC_COVERAGE_COLUMNS))


def _normalize_overlap_bases(overlap_bases: tuple[OverlapBasis, ...]) -> tuple[OverlapBasis, ...]:
    if not overlap_bases:
        raise ValueError("overlap_bases must include at least one basis: area or population.")
    invalid = sorted(set(overlap_bases) - {"area", "population"})
    if invalid:
        raise ValueError(f"Unsupported overlap_bases {invalid}; expected area and/or population.")
    return tuple(dict.fromkeys(overlap_bases))


def _normalize_membership(msa_county_membership: pd.DataFrame) -> pd.DataFrame:
    missing = sorted(set(REQUIRED_MSA_MEMBERSHIP_COLUMNS) - set(msa_county_membership.columns))
    if missing:
        raise ValueError(
            "msa_county_membership is missing required columns "
            f"{missing}. Available: {list(msa_county_membership.columns)}"
        )
    membership = msa_county_membership.copy()
    membership["msa_id"] = membership["msa_id"].astype(str).str.zfill(5)
    membership["county_fips"] = membership["county_fips"].astype(str).str.zfill(5)
    if "cbsa_code" not in membership.columns:
        membership["cbsa_code"] = membership["msa_id"]
    if "msa_name" not in membership.columns:
        if "cbsa_title" in membership.columns:
            membership["msa_name"] = membership["cbsa_title"]
        else:
            membership["msa_name"] = membership["msa_id"]
    return membership


def _prepare_coc_geometries(coc_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    if "coc_id" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'coc_id' column")
    if "geometry" not in coc_gdf.columns:
        raise ValueError("coc_gdf must have 'geometry' column")
    coc = coc_gdf.copy()
    if "coc_name" not in coc.columns:
        coc["coc_name"] = coc["coc_id"].astype(str)
    coc = coc[["coc_id", "coc_name", "geometry"]].to_crs(ALBERS_EQUAL_AREA_CRS)
    coc["coc_id"] = coc["coc_id"].astype(str)
    coc["coc_area"] = coc.geometry.area
    return coc


def _build_msa_geometries(
    county_gdf: gpd.GeoDataFrame,
    membership: pd.DataFrame,
    *,
    selected_msa_ids: tuple[str, ...],
) -> gpd.GeoDataFrame:
    county_id_col = "GEOID" if "GEOID" in county_gdf.columns else "geoid"
    if county_id_col not in county_gdf.columns:
        raise ValueError("county_gdf must have 'GEOID' or 'geoid' column")
    if "geometry" not in county_gdf.columns:
        raise ValueError("county_gdf must have 'geometry' column")

    counties = county_gdf[[county_id_col, "geometry"]].rename(
        columns={county_id_col: "county_fips"}
    )
    counties = counties.to_crs(ALBERS_EQUAL_AREA_CRS)
    counties["county_fips"] = counties["county_fips"].astype(str).str.zfill(5)
    selected = membership[membership["msa_id"].isin(selected_msa_ids)].copy()
    joined = selected.merge(counties, on="county_fips", how="left")
    missing = sorted(joined.loc[joined["geometry"].isna(), "county_fips"].unique())
    if missing:
        preview = ", ".join(missing[:5])
        suffix = ", ..." if len(missing) > 5 else ""
        raise ValueError(
            "County geometry is missing counties required for selected MSAs: "
            f"{preview}{suffix}. Run: hhplab ingest tiger --year <county_vintage> --type counties"
        )

    gdf = gpd.GeoDataFrame(joined, geometry="geometry", crs=ALBERS_EQUAL_AREA_CRS)
    dissolved = (
        gdf.dissolve(by="msa_id", as_index=False)
        .loc[:, ["msa_id", "cbsa_code", "msa_name", "geometry"]]
        .sort_values("msa_id")
        .reset_index(drop=True)
    )
    dissolved["msa_area"] = dissolved.geometry.area
    return dissolved


def _intersect_msa_coc(
    msa_proj: gpd.GeoDataFrame,
    coc_proj: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    intersections = gpd.overlay(
        msa_proj[["msa_id", "msa_name", "msa_area", "geometry"]],
        coc_proj[["coc_id", "coc_name", "coc_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if intersections.empty:
        return intersections
    intersections = intersections[~intersections.geometry.is_empty].copy()
    intersections["intersection_area"] = intersections.geometry.area
    intersections = intersections[intersections["intersection_area"] > 0].copy()
    return intersections


def _area_coverage_rows(
    pair_geometries: gpd.GeoDataFrame,
    msa_proj: gpd.GeoDataFrame,
    coc_proj: gpd.GeoDataFrame,
    *,
    year: int,
    top_n: int,
    ranking_population_source: PopulationRankingSource,
    ranking_reference_year: int,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    min_msa_coverage_share: float | None,
    min_coc_containment_share: float | None,
) -> pd.DataFrame:
    if pair_geometries.empty:
        return _empty_coverage()
    rows = pd.DataFrame(pair_geometries.drop(columns="geometry"))
    rows["overlap_basis"] = "area"
    rows["denominator_source"] = "geometry"
    rows["denominator_vintage"] = pd.NA
    rows["denominator_column"] = "area"
    rows["intersection_value"] = rows["intersection_area"]
    rows["msa_denominator"] = rows["msa_area"]
    rows["coc_denominator"] = rows["coc_area"]
    return _finalize_rows(
        rows,
        year=year,
        top_n=top_n,
        ranking_population_source=ranking_population_source,
        ranking_reference_year=ranking_reference_year,
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        definition_version=definition_version,
        min_msa_coverage_share=min_msa_coverage_share,
        min_coc_containment_share=min_coc_containment_share,
    )


def _population_coverage_rows(
    pair_geometries: gpd.GeoDataFrame,
    msa_proj: gpd.GeoDataFrame,
    coc_proj: gpd.GeoDataFrame,
    *,
    acs5_population_df: pd.DataFrame | None,
    tract_gdf: gpd.GeoDataFrame | None,
    year: int,
    top_n: int,
    ranking_population_source: PopulationRankingSource,
    ranking_reference_year: int,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    acs5_population_vintage: int | str | None,
    population_column: str,
    min_msa_coverage_share: float | None,
    min_coc_containment_share: float | None,
    allow_incomplete_population_denominators: bool,
) -> pd.DataFrame:
    if pair_geometries.empty:
        return _empty_coverage()
    if acs5_population_df is None or tract_gdf is None:
        raise ValueError(
            "Population overlap requires ACS5 tract population rows and tract geometry. "
            "Provide acs5_population_df and tract_gdf, or request overlap_bases=('area',)."
        )

    tracts = _prepare_population_tracts(
        tract_gdf,
        acs5_population_df,
        acs5_population_vintage=acs5_population_vintage,
        population_column=population_column,
        allow_incomplete_population_denominators=allow_incomplete_population_denominators,
    )
    pair_values = _population_totals_for_geometries(
        pair_geometries[["msa_id", "coc_id", "geometry"]],
        tracts,
        group_cols=["msa_id", "coc_id"],
        value_name="intersection_value",
    )
    msa_values = _population_totals_for_geometries(
        msa_proj[["msa_id", "geometry"]],
        tracts,
        group_cols=["msa_id"],
        value_name="msa_denominator",
    )
    coc_values = _population_totals_for_geometries(
        coc_proj[["coc_id", "geometry"]],
        tracts,
        group_cols=["coc_id"],
        value_name="coc_denominator",
    )

    rows = pd.DataFrame(pair_geometries.drop(columns="geometry"))
    rows = rows.merge(pair_values, on=["msa_id", "coc_id"], how="left")
    rows = rows.merge(msa_values, on="msa_id", how="left")
    rows = rows.merge(coc_values, on="coc_id", how="left")
    rows["overlap_basis"] = "population"
    rows["denominator_source"] = "acs5"
    rows["denominator_vintage"] = (
        str(acs5_population_vintage) if acs5_population_vintage is not None else pd.NA
    )
    rows["denominator_column"] = population_column
    return _finalize_rows(
        rows,
        year=year,
        top_n=top_n,
        ranking_population_source=ranking_population_source,
        ranking_reference_year=ranking_reference_year,
        boundary_vintage=boundary_vintage,
        county_vintage=county_vintage,
        definition_version=definition_version,
        min_msa_coverage_share=min_msa_coverage_share,
        min_coc_containment_share=min_coc_containment_share,
    )


def _prepare_population_tracts(
    tract_gdf: gpd.GeoDataFrame,
    acs5_population_df: pd.DataFrame,
    *,
    acs5_population_vintage: int | str | None,
    population_column: str,
    allow_incomplete_population_denominators: bool,
) -> gpd.GeoDataFrame:
    tract_id_col = next(
        (column for column in ("tract_geoid", "GEOID", "geoid") if column in tract_gdf.columns),
        "GEOID",
    )
    if tract_id_col not in tract_gdf.columns:
        raise ValueError("tract_gdf must have 'tract_geoid', 'GEOID', or 'geoid' column")
    pop_id_col = next(
        (
            column
            for column in ("tract_geoid", "GEOID", "geoid")
            if column in acs5_population_df.columns
        ),
        "GEOID",
    )
    if pop_id_col not in acs5_population_df.columns:
        raise ValueError(
            "ACS5 population rows must have 'tract_geoid', 'GEOID', or 'geoid' column"
        )
    if population_column not in acs5_population_df.columns:
        raise ValueError(
            f"ACS5 population rows are missing '{population_column}'. "
            f"Available columns: {list(acs5_population_df.columns)}"
        )

    pop = acs5_population_df.copy()
    if acs5_population_vintage is not None and "year" in pop.columns:
        pop = pop[pop["year"].astype(str) == str(acs5_population_vintage)].copy()
    if pop.empty:
        raise ValueError(
            "No ACS5 tract population rows are available for population overlap. "
            "Run: hhplab ingest acs-population for the requested ACS5 vintage."
        )
    pop["tract_geoid"] = pop[pop_id_col].astype(str).str.zfill(11)
    pop["total_population"] = pd.to_numeric(pop[population_column], errors="coerce")
    pop = pop[["tract_geoid", "total_population"]].drop_duplicates("tract_geoid")

    tracts = tract_gdf[[tract_id_col, "geometry"]].rename(columns={tract_id_col: "tract_geoid"})
    tracts = tracts.to_crs(ALBERS_EQUAL_AREA_CRS)
    tracts["tract_geoid"] = tracts["tract_geoid"].astype(str).str.zfill(11)
    tracts = tracts.merge(pop, on="tract_geoid", how="left")
    missing = sorted(tracts.loc[tracts["total_population"].isna(), "tract_geoid"].unique())
    if missing and not allow_incomplete_population_denominators:
        preview = ", ".join(missing[:5])
        suffix = ", ..." if len(missing) > 5 else ""
        raise ValueError(
            "ACS5 tract population denominator coverage is incomplete for population "
            f"overlap. Missing tract_geoid: {preview}{suffix}. "
            "Run: hhplab ingest acs-population for the requested ACS5 vintage."
        )
    tracts["total_population"] = tracts["total_population"].fillna(0.0)
    tracts["tract_area"] = tracts.geometry.area
    zero_area = tracts["tract_area"] <= 0
    if zero_area.any():
        raise ValueError("Tract geometry contains zero-area rows; cannot allocate population.")
    return tracts


def _population_totals_for_geometries(
    geometries: gpd.GeoDataFrame,
    tracts: gpd.GeoDataFrame,
    *,
    group_cols: list[str],
    value_name: str,
) -> pd.DataFrame:
    if geometries.empty:
        return pd.DataFrame(columns=[*group_cols, value_name])
    minx, miny, maxx, maxy = geometries.total_bounds
    candidate_tracts = tracts.cx[minx:maxx, miny:maxy].copy()
    if candidate_tracts.empty:
        return pd.DataFrame(columns=[*group_cols, value_name])
    overlay = gpd.overlay(
        geometries,
        candidate_tracts[["tract_geoid", "tract_area", "total_population", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )
    if overlay.empty:
        return pd.DataFrame(columns=[*group_cols, value_name])
    overlay = overlay[~overlay.geometry.is_empty].copy()
    overlay["intersection_area"] = overlay.geometry.area
    overlay[value_name] = (
        overlay["total_population"] * overlay["intersection_area"] / overlay["tract_area"]
    )
    return overlay.groupby(group_cols, as_index=False)[value_name].sum()


def _finalize_rows(
    rows: pd.DataFrame,
    *,
    year: int,
    top_n: int,
    ranking_population_source: PopulationRankingSource,
    ranking_reference_year: int,
    boundary_vintage: str,
    county_vintage: str,
    definition_version: str,
    min_msa_coverage_share: float | None,
    min_coc_containment_share: float | None,
) -> pd.DataFrame:
    if rows.empty:
        return _empty_coverage()
    rows = rows.copy()
    rows["msa_covered_by_coc_percent"] = _percent(
        rows["intersection_value"],
        rows["msa_denominator"],
    )
    rows["coc_contained_in_msa_percent"] = _percent(
        rows["intersection_value"],
        rows["coc_denominator"],
    )
    if min_msa_coverage_share is not None:
        rows = rows[rows["msa_covered_by_coc_percent"] >= min_msa_coverage_share * 100].copy()
    if min_coc_containment_share is not None:
        rows = rows[
            rows["coc_contained_in_msa_percent"] >= min_coc_containment_share * 100
        ].copy()
    rows["year"] = int(year)
    rows["boundary_vintage"] = str(boundary_vintage)
    rows["county_vintage"] = str(county_vintage)
    rows["definition_version"] = definition_version
    rows["top_n"] = int(top_n)
    rows["ranking_population_source"] = ranking_population_source
    rows["ranking_reference_year"] = int(ranking_reference_year)
    return rows


def _percent(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    valid = denominator.notna() & (denominator != 0)
    result = pd.Series(pd.NA, index=numerator.index, dtype="Float64")
    result.loc[valid] = (numerator.loc[valid] / denominator.loc[valid]) * 100
    return result
