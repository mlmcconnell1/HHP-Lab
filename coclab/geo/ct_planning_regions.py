"""Connecticut planning region helper utilities.

Connecticut transitioned from county-based FIPS to planning region county
equivalents starting with the 2022 ACS release. These helpers provide
concordances to align legacy county GEOIDs with planning region GEOIDs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd

from coclab.naming import county_path, tract_path

CT_STATE_FIPS = "09"
CT_LEGACY_COUNTY_CODES = {"001", "003", "005", "007", "009", "011", "013", "015"}
CT_PLANNING_REGION_CODES = {"110", "120", "130", "140", "150", "160", "170", "180", "190"}

CT_LEGACY_COUNTY_VINTAGE = 2020
CT_PLANNING_REGION_VINTAGE = 2023

# ESRI:102003 - USA Contiguous Albers Equal Area Conic
ALBERS_EQUAL_AREA_CRS = "ESRI:102003"


@dataclass(frozen=True)
class CtPlanningRegionCrosswalk:
    """Crosswalk between legacy CT counties and planning regions."""

    mapping: pd.DataFrame
    legacy_vintage: int
    planning_vintage: int


def _load_geometries(path: Path, geoid_column: str) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Required geometry file not found: {path}. "
            f"Run the corresponding ingest command first."
        )
    gdf = gpd.read_parquet(path)
    if geoid_column not in gdf.columns:
        raise ValueError(f"Expected '{geoid_column}' column in {path}")
    gdf = gdf.rename(columns={geoid_column: "GEOID"})
    return gdf


def _filter_ct(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return gdf[gdf["GEOID"].astype(str).str.startswith(CT_STATE_FIPS)].copy()


def _ct_county_code(geoid: str) -> str:
    return str(geoid)[2:5]


def is_ct_legacy_county_fips(geoid: str) -> bool:
    return str(geoid).startswith(CT_STATE_FIPS) and _ct_county_code(geoid) in CT_LEGACY_COUNTY_CODES


def is_ct_planning_region_fips(geoid: str) -> bool:
    return (
        str(geoid).startswith(CT_STATE_FIPS)
        and _ct_county_code(geoid) in CT_PLANNING_REGION_CODES
    )


def build_ct_tract_planning_region_map(
    tract_vintage: int | str,
    planning_region_vintage: int | str = CT_PLANNING_REGION_VINTAGE,
) -> pd.DataFrame:
    """Build a mapping from planning-region tract GEOIDs to legacy tract GEOIDs."""
    tracts_path = tract_path(tract_vintage)
    planning_path = county_path(planning_region_vintage)

    tracts = _filter_ct(_load_geometries(tracts_path, "geoid"))
    planning_regions = _filter_ct(_load_geometries(planning_path, "geoid"))

    if tracts.empty or planning_regions.empty:
        raise ValueError("CT tract or planning region geometries are empty")

    tracts = tracts.to_crs(ALBERS_EQUAL_AREA_CRS)
    planning_regions = planning_regions.to_crs(ALBERS_EQUAL_AREA_CRS)

    tracts = tracts.copy()
    tracts["geometry"] = tracts.geometry.centroid

    joined = gpd.sjoin(
        tracts[["GEOID", "geometry"]],
        planning_regions[["GEOID", "geometry"]],
        how="left",
        predicate="within",
    ).rename(columns={"GEOID_left": "legacy_geoid", "GEOID_right": "planning_region_geoid"})

    unmatched = joined["planning_region_geoid"].isna().sum()
    if unmatched:
        raise ValueError(
            "Unable to map some CT tracts to planning regions. "
            f"Unmatched tracts: {unmatched}"
        )

    tract_code = joined["legacy_geoid"].astype(str).str[-6:]
    planning_geoid = joined["planning_region_geoid"].astype(str).str[:5] + tract_code

    mapping = pd.DataFrame(
        {
            "legacy_geoid": joined["legacy_geoid"].astype(str),
            "planning_geoid": planning_geoid.astype(str),
            "legacy_county_fips": joined["legacy_geoid"].astype(str).str[:5],
            "planning_region_fips": joined["planning_region_geoid"].astype(str).str[:5],
        }
    )

    return mapping.drop_duplicates().reset_index(drop=True)


def remap_ct_planning_region_geoids(
    acs_data: pd.DataFrame,
    mapping: pd.DataFrame,
    *,
    geoid_column: str = "GEOID",
) -> pd.DataFrame:
    """Replace CT planning-region GEOIDs with legacy county GEOIDs using a mapping."""
    if geoid_column not in acs_data.columns:
        raise ValueError(f"Missing required column: {geoid_column}")
    if "planning_geoid" not in mapping.columns or "legacy_geoid" not in mapping.columns:
        raise ValueError("Mapping must include planning_geoid and legacy_geoid columns")

    result = acs_data.copy()
    ct_mask = result[geoid_column].astype(str).str.startswith(CT_STATE_FIPS)
    if not ct_mask.any():
        return result

    mapping_index = mapping.set_index("planning_geoid")["legacy_geoid"]
    remapped = result.loc[ct_mask, geoid_column].astype(str).map(mapping_index)
    result.loc[ct_mask, geoid_column] = remapped.fillna(result.loc[ct_mask, geoid_column])
    return result


def build_ct_county_planning_region_crosswalk(
    legacy_county_vintage: int | str = CT_LEGACY_COUNTY_VINTAGE,
    planning_region_vintage: int | str = CT_PLANNING_REGION_VINTAGE,
) -> CtPlanningRegionCrosswalk:
    """Build an area-share crosswalk between legacy counties and planning regions."""
    legacy_path = county_path(legacy_county_vintage)
    planning_path = county_path(planning_region_vintage)

    legacy = _filter_ct(_load_geometries(legacy_path, "geoid"))
    planning = _filter_ct(_load_geometries(planning_path, "geoid"))

    if legacy.empty or planning.empty:
        raise ValueError("CT legacy or planning region county geometries are empty")

    legacy = legacy.to_crs(ALBERS_EQUAL_AREA_CRS)
    planning = planning.to_crs(ALBERS_EQUAL_AREA_CRS)

    legacy["legacy_area"] = legacy.geometry.area
    planning["planning_area"] = planning.geometry.area

    overlay = gpd.overlay(
        legacy[["GEOID", "legacy_area", "geometry"]],
        planning[["GEOID", "planning_area", "geometry"]],
        how="intersection",
        keep_geom_type=False,
    )

    overlay["intersection_area"] = overlay.geometry.area
    overlay = overlay.rename(
        columns={
            "GEOID_1": "legacy_county_fips",
            "GEOID_2": "planning_region_fips",
        }
    )

    overlay["legacy_share"] = overlay["intersection_area"] / overlay["legacy_area"]
    overlay["planning_share"] = overlay["intersection_area"] / overlay["planning_area"]

    mapping = overlay[
        [
            "legacy_county_fips",
            "planning_region_fips",
            "legacy_share",
            "planning_share",
        ]
    ].copy()

    return CtPlanningRegionCrosswalk(
        mapping=mapping.reset_index(drop=True),
        legacy_vintage=int(legacy_county_vintage),
        planning_vintage=int(planning_region_vintage),
    )


def translate_zori_legacy_to_planning(
    zori_df: pd.DataFrame,
    crosswalk: CtPlanningRegionCrosswalk,
) -> pd.DataFrame:
    """Translate legacy CT county ZORI values to planning regions using area shares."""
    required_cols = {"geo_id", "date", "zori"}
    if not required_cols.issubset(zori_df.columns):
        raise ValueError(f"ZORI data must include columns: {sorted(required_cols)}")

    mapping = crosswalk.mapping.copy()
    mapping = mapping.rename(columns={"planning_share": "weight"})

    zori_ct = zori_df[zori_df["geo_id"].apply(is_ct_legacy_county_fips)].copy()
    if zori_ct.empty:
        return zori_df

    merged = zori_ct.merge(
        mapping,
        left_on="geo_id",
        right_on="legacy_county_fips",
        how="left",
    )

    merged["weighted_zori"] = merged["zori"] * merged["weight"].fillna(0)
    grouped = (
        merged.groupby(["planning_region_fips", "date"], as_index=False)["weighted_zori"]
        .sum()
        .rename(columns={"planning_region_fips": "geo_id", "weighted_zori": "zori"})
    )

    non_ct = zori_df[~zori_df["geo_id"].apply(is_ct_legacy_county_fips)].copy()
    return pd.concat([non_ct, grouped], ignore_index=True)


def translate_weights_planning_to_legacy(
    weights_df: pd.DataFrame,
    crosswalk: CtPlanningRegionCrosswalk,
) -> pd.DataFrame:
    """Translate CT planning-region weights to legacy county weights via area shares."""
    required_cols = {"county_fips", "weight_value"}
    if not required_cols.issubset(weights_df.columns):
        raise ValueError(f"Weights data must include columns: {sorted(required_cols)}")

    mapping = crosswalk.mapping.copy()
    mapping = mapping.rename(columns={"legacy_share": "weight"})

    ct_planning = weights_df[weights_df["county_fips"].apply(is_ct_planning_region_fips)].copy()
    if ct_planning.empty:
        return weights_df

    merged = ct_planning.merge(
        mapping,
        left_on="county_fips",
        right_on="planning_region_fips",
        how="left",
    )

    merged["weighted_value"] = merged["weight_value"] * merged["weight"].fillna(0)
    grouped = (
        merged.groupby("legacy_county_fips", as_index=False)["weighted_value"]
        .sum()
        .rename(columns={"legacy_county_fips": "county_fips", "weighted_value": "weight_value"})
    )

    non_ct = weights_df[~weights_df["county_fips"].apply(is_ct_planning_region_fips)].copy()
    return pd.concat([non_ct, grouped], ignore_index=True)
