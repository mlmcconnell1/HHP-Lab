"""Containment-list builders for recipe outputs."""

from __future__ import annotations

import geopandas as gpd
import pandas as pd

from hhplab.msa.crosswalk import build_coc_msa_crosswalk
from hhplab.recipe.recipe_schema import ContainmentSpec
from hhplab.xwalks.county import ALBERS_EQUAL_AREA_CRS, build_coc_county_crosswalk

CONTAINMENT_COLUMNS: tuple[str, ...] = (
    "container_type",
    "container_id",
    "candidate_type",
    "candidate_id",
    "contained_share",
    "intersection_area",
    "candidate_area",
    "container_area",
    "method",
    "container_vintage",
    "candidate_vintage",
    "definition_version",
)


def build_containment_list(
    spec: ContainmentSpec,
    *,
    coc_gdf: gpd.GeoDataFrame | None = None,
    county_gdf: gpd.GeoDataFrame | None = None,
    msa_county_membership: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a canonical containment list from supported recipe geometry pairs."""
    pair = (spec.container.type, spec.candidate.type)
    if pair == ("msa", "coc"):
        raw = _build_msa_coc_containment(
            spec,
            coc_gdf=coc_gdf,
            county_gdf=county_gdf,
            msa_county_membership=msa_county_membership,
        )
    elif pair == ("coc", "county"):
        raw = _build_coc_county_containment(
            spec,
            coc_gdf=coc_gdf,
            county_gdf=county_gdf,
        )
    else:
        raise ValueError(
            "Unsupported containment geometry pair "
            f"'{spec.container.type} -> {spec.candidate.type}'. "
            "Supported pairs: msa -> coc, coc -> county."
        )

    filtered = _apply_selector_filters(raw, spec)
    filtered = filtered[filtered["contained_share"] >= spec.min_share].copy()
    return _sort_containment(filtered)


def _build_msa_coc_containment(
    spec: ContainmentSpec,
    *,
    coc_gdf: gpd.GeoDataFrame | None,
    county_gdf: gpd.GeoDataFrame | None,
    msa_county_membership: pd.DataFrame | None,
) -> pd.DataFrame:
    _require_input(coc_gdf, "CoC boundary geometry", "hhplab ingest hud-boundaries")
    _require_input(county_gdf, "county geometry", "hhplab ingest tiger-counties")
    _require_input(
        msa_county_membership,
        "MSA county membership",
        "hhplab generate msa-definitions",
    )

    boundary_vintage = _required_vintage(spec.candidate, "candidate CoC")
    county_vintage = _required_vintage(spec.container, "container MSA")
    definition_version = _definition_version(spec)

    crosswalk = build_coc_msa_crosswalk(
        coc_gdf,
        county_gdf,
        msa_county_membership,
        boundary_vintage=str(boundary_vintage),
        county_vintage=str(county_vintage),
        definition_version=definition_version,
    )
    msa_areas = _msa_container_areas(
        county_gdf,
        msa_county_membership,
    )

    output = pd.DataFrame(
        {
            "container_type": "msa",
            "container_id": crosswalk["msa_id"].astype(str),
            "candidate_type": "coc",
            "candidate_id": crosswalk["coc_id"].astype(str),
            "intersection_area": crosswalk["intersection_area"],
            "candidate_area": crosswalk["coc_area"],
            "container_area": crosswalk["msa_id"].astype(str).map(msa_areas),
            "method": spec.method,
            "container_vintage": spec.container.vintage,
            "candidate_vintage": spec.candidate.vintage,
            "definition_version": definition_version,
        }
    )
    output["contained_share"] = _contained_share(output, spec)
    return output.loc[:, CONTAINMENT_COLUMNS]


def _build_coc_county_containment(
    spec: ContainmentSpec,
    *,
    coc_gdf: gpd.GeoDataFrame | None,
    county_gdf: gpd.GeoDataFrame | None,
) -> pd.DataFrame:
    _require_input(coc_gdf, "CoC boundary geometry", "hhplab ingest hud-boundaries")
    _require_input(county_gdf, "county geometry", "hhplab ingest tiger-counties")

    boundary_vintage = _required_vintage(spec.container, "container CoC")
    crosswalk = build_coc_county_crosswalk(
        coc_gdf,
        county_gdf,
        str(boundary_vintage),
    )
    output = pd.DataFrame(
        {
            "container_type": "coc",
            "container_id": crosswalk["coc_id"].astype(str),
            "candidate_type": "county",
            "candidate_id": crosswalk["county_fips"].astype(str),
            "intersection_area": crosswalk["intersection_area"],
            "candidate_area": crosswalk["county_area"],
            "container_area": crosswalk["coc_area"],
            "method": spec.method,
            "container_vintage": spec.container.vintage,
            "candidate_vintage": spec.candidate.vintage,
            "definition_version": spec.definition_version,
        }
    )
    output["contained_share"] = _contained_share(output, spec)
    return output.loc[:, CONTAINMENT_COLUMNS]


def _contained_share(containment: pd.DataFrame, spec: ContainmentSpec) -> pd.Series:
    denominator_col = spec.denominator
    denominator = containment[denominator_col]
    if (denominator <= 0).any():
        raise ValueError(
            f"Cannot compute containment share with non-positive {denominator_col}."
        )
    return containment["intersection_area"] / denominator


def _msa_container_areas(
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
) -> dict[str, float]:
    county_id_col = "GEOID" if "GEOID" in county_gdf.columns else "geoid"
    if county_id_col not in county_gdf.columns:
        raise ValueError(
            "County geometry is missing a GEOID/geoid column. "
            "Run: hhplab ingest tiger-counties"
        )
    membership = msa_county_membership[["msa_id", "county_fips"]].copy()
    membership["county_fips"] = membership["county_fips"].astype(str)

    counties = county_gdf[[county_id_col, "geometry"]].copy()
    counties[county_id_col] = counties[county_id_col].astype(str)
    counties = counties.merge(
        membership,
        left_on=county_id_col,
        right_on="county_fips",
        how="inner",
    )
    if counties.empty:
        raise ValueError(
            "No county geometries matched the MSA membership artifact. "
            "Run: hhplab ingest tiger-counties for the MSA county vintage."
        )

    projected = counties.to_crs(ALBERS_EQUAL_AREA_CRS)
    dissolved = projected.dissolve(by="msa_id")
    return {
        str(msa_id): float(area)
        for msa_id, area in dissolved.geometry.area.items()
    }


def _apply_selector_filters(
    containment: pd.DataFrame,
    spec: ContainmentSpec,
) -> pd.DataFrame:
    filtered = containment
    if spec.selector_ids is not None:
        _require_selector_matches(
            containment["container_id"],
            spec.selector_ids,
            "container selector_ids",
        )
        filtered = filtered[filtered["container_id"].isin(spec.selector_ids)]
    if spec.candidate_selector_ids is not None:
        _require_selector_matches(
            containment["candidate_id"],
            spec.candidate_selector_ids,
            "candidate_selector_ids",
        )
        filtered = filtered[filtered["candidate_id"].isin(spec.candidate_selector_ids)]
    return filtered.copy()


def _require_selector_matches(
    available: pd.Series,
    requested: list[str],
    label: str,
) -> None:
    available_ids = set(available.astype(str))
    missing = sorted(set(requested) - available_ids)
    if missing:
        preview = ", ".join(missing[:5])
        suffix = ", ..." if len(missing) > 5 else ""
        raise ValueError(
            f"Containment {label} did not match available geography IDs: "
            f"{preview}{suffix}. Check selector IDs or build the required geometry artifacts."
        )


def _sort_containment(containment: pd.DataFrame) -> pd.DataFrame:
    if containment.empty:
        return pd.DataFrame(columns=list(CONTAINMENT_COLUMNS))
    return (
        containment.loc[:, CONTAINMENT_COLUMNS]
        .sort_values(["container_id", "candidate_id"])
        .reset_index(drop=True)
    )


def _require_input(value: object | None, label: str, command: str) -> None:
    if value is None:
        raise ValueError(f"Missing {label} for containment output. Run: {command}")


def _required_vintage(ref: object, label: str) -> int:
    vintage = getattr(ref, "vintage")
    if vintage is None:
        raise ValueError(f"Missing {label} vintage for containment output.")
    return vintage


def _definition_version(spec: ContainmentSpec) -> str:
    if spec.definition_version:
        return spec.definition_version
    if spec.container.source:
        return spec.container.source
    if spec.container.vintage is not None:
        return str(spec.container.vintage)
    raise ValueError(
        "Missing MSA definition version for containment output. "
        "Set containment_spec.definition_version or container.source."
    )
