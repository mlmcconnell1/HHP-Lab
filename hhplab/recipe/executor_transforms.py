"""Transform artifact resolution helpers for recipe execution.

Resolves the on-disk path for a recipe transform, identifies synthetic
analysis geographies (metro/MSA) versus CoC/base geometry roles, and
materializes generated crosswalks on demand. These helpers are imported
back into ``hhplab.recipe.executor`` so legacy callers (and the lazy
probe import in ``hhplab.recipe.probes``) keep working unchanged.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from hhplab.naming import (
    county_xwalk_path,
    msa_coc_xwalk_path,
    tract_mediated_county_xwalk_path,
    tract_path,
    tract_xwalk_path,
)
from hhplab.provenance import ProvenanceBlock, write_parquet_with_provenance
from hhplab.recipe.executor_core import ExecutorError, _get_transform
from hhplab.recipe.recipe_schema import RecipeV1
from hhplab.recipe.schema_common import GeometryRef

_RECIPE_TRANSFORM_DIR = Path(".recipe_cache") / "transforms"


def _resolve_transform_path(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> Path:
    """Map a transform spec to its expected crosswalk file path.

    Uses the transform's ``from_`` / ``to`` geometry refs to determine
    the canonical crosswalk filename via the naming module.

    Raises ExecutorError if the geometry pair is not recognised.
    """
    transform = _get_transform(recipe, transform_id)

    from_ = transform.from_
    to = transform.to

    metro_ref, base_ref = _identify_metro_and_base(from_, to)
    if metro_ref is not None:
        return _generated_metro_transform_path(
            transform_id,
            metro_ref=metro_ref,
            base_ref=base_ref,
            project_root=project_root,
        )
    msa_ref, base_ref = _identify_msa_and_base(from_, to)
    if msa_ref is not None:
        return _generated_msa_transform_path(
            transform_id,
            msa_ref=msa_ref,
            base_ref=base_ref,
            project_root=project_root,
        )

    # Determine which geometry is the CoC boundary and which is the
    # base geography so we can build the right crosswalk filename.
    coc_ref, base_ref = _identify_coc_and_base(from_, to)
    if coc_ref is None:
        raise ExecutorError(
            f"Transform '{transform_id}' connects "
            f"{from_.type}@{from_.vintage} → {to.type}@{to.vintage}: "
            f"cannot resolve crosswalk path (no 'coc' geometry in pair)."
        )

    if coc_ref.vintage is None:
        raise ExecutorError(
            f"Transform '{transform_id}': CoC geometry has no vintage. "
            f"Cannot resolve crosswalk path without a concrete boundary vintage. "
            f"Set vintage on the 'coc' geometry ref (e.g., vintage: 2025)."
        )
    if base_ref.vintage is None:
        raise ExecutorError(
            f"Transform '{transform_id}': {base_ref.type} geometry has no vintage. "
            f"Cannot resolve crosswalk path without a concrete {base_ref.type} vintage. "
            f"Set vintage on the '{base_ref.type}' geometry ref."
        )
    boundary_vintage = str(coc_ref.vintage)
    base_vintage: str | int = base_ref.vintage

    if base_ref.type == "tract":
        return project_root / tract_xwalk_path(boundary_vintage, base_vintage)
    elif base_ref.type == "county":
        weighting = getattr(transform.spec, "weighting", None)
        if weighting is not None and weighting.scheme == "tract_mediated":
            if weighting.tract_vintage is None:
                raise ExecutorError(
                    f"Transform '{transform_id}': tract-mediated county "
                    "weighting requires weighting.tract_vintage."
                )
            return project_root / tract_mediated_county_xwalk_path(
                boundary_vintage,
                base_vintage,
                weighting.tract_vintage,
                weighting.acs_vintage,
                denominator_source=weighting.denominator_source,
                denominator_vintage=weighting.resolved_denominator_vintage,
            )
        return project_root / county_xwalk_path(boundary_vintage, base_vintage)
    else:
        raise ExecutorError(
            f"Transform '{transform_id}': unsupported geometry pair "
            f"{from_.type} → {to.type}. Only tract↔coc and county↔coc "
            f"crosswalks plus generated metro/MSA transforms are currently supported."
        )


def _identify_coc_and_base(
    from_: GeometryRef,
    to: GeometryRef,
) -> tuple[GeometryRef | None, GeometryRef]:
    """Identify which end of a transform is the CoC boundary."""
    if to.type == "coc":
        return to, from_
    if from_.type == "coc":
        return from_, to
    return None, from_


def _identify_metro_and_base(
    from_: GeometryRef,
    to: GeometryRef,
) -> tuple[GeometryRef | None, GeometryRef]:
    """Identify which end of a transform is the metro geometry."""
    if to.type == "metro":
        return to, from_
    if from_.type == "metro":
        return from_, to
    return None, from_


def _identify_msa_and_base(
    from_: GeometryRef,
    to: GeometryRef,
) -> tuple[GeometryRef | None, GeometryRef]:
    """Identify which end of a transform is the MSA geometry."""
    if to.type == "msa":
        return to, from_
    if from_.type == "msa":
        return from_, to
    return None, from_


def _generated_metro_transform_path(
    transform_id: str,
    *,
    metro_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> Path:
    """Return the recipe-cache path for a generated metro transform."""
    definition = metro_ref.source or "unknown_definition"
    subset_definition = metro_ref.subset_profile_definition_version
    base_suffix = base_ref.type
    if base_ref.vintage is not None:
        base_suffix = f"{base_suffix}_{base_ref.vintage}"
    if subset_definition:
        definition = f"{definition}__subset_{subset_definition}"
    filename = f"{transform_id}__{base_suffix}__{definition}.parquet"
    return project_root / _RECIPE_TRANSFORM_DIR / filename


def _metro_uses_legacy_membership(metro_ref: GeometryRef) -> bool:
    """Return True when the metro ref should use legacy Glynn/Fox artifacts."""
    return (
        metro_ref.source == metro_ref.resolved_metro_subset_definition_version()
        and metro_ref.subset_profile is None
        and metro_ref.subset_profile_definition_version is None
    )


def _metro_subset_membership(
    metro_ref: GeometryRef,
    *,
    data_root: Path,
) -> pd.DataFrame | None:
    """Load the subset selector table for a metro target, if one is active."""
    profile_definition_version = metro_ref.resolved_metro_subset_definition_version()
    if profile_definition_version is None:
        return None

    from hhplab.metro.metro_io import read_metro_subset_membership

    metro_definition_version = metro_ref.resolved_metro_definition_version()
    if metro_definition_version is None:
        raise ExecutorError(
            "Metro subset selectors require geometry.source to identify the "
            "canonical metro-universe definition version."
        )

    subset_df = read_metro_subset_membership(
        profile_definition_version=profile_definition_version,
        metro_definition_version=metro_definition_version,
        base_dir=data_root,
    ).copy()
    profile_name = metro_ref.resolved_metro_subset_profile()
    if profile_name is not None and "profile" in subset_df.columns:
        subset_df = subset_df[subset_df["profile"].astype(str) == profile_name].copy()
    return subset_df


def _generated_msa_transform_path(
    transform_id: str,
    *,
    msa_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> Path:
    """Return the recipe-cache path for a generated MSA transform."""
    definition = msa_ref.source or "unknown_definition"
    base_suffix = base_ref.type
    if base_ref.vintage is not None:
        base_suffix = f"{base_suffix}_{base_ref.vintage}"
    filename = f"{transform_id}__{base_suffix}__{definition}.parquet"
    return project_root / _RECIPE_TRANSFORM_DIR / filename


def _resolve_metro_transform_df(
    *,
    metro_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> pd.DataFrame:
    """Build a metro crosswalk DataFrame from curated membership artifacts."""
    if not metro_ref.source:
        raise ExecutorError(
            "Metro transforms require geometry.source to identify the "
            "definition version (for example 'glynn_fox_v1')."
        )

    data_root = project_root / "data"
    definition_version = metro_ref.source

    if _metro_uses_legacy_membership(metro_ref):
        from hhplab.metro.metro_io import (
            read_metro_coc_membership,
            read_metro_county_membership,
        )

        if base_ref.type == "coc":
            xwalk = read_metro_coc_membership(
                definition_version=definition_version,
                base_dir=data_root,
            )
            xwalk["area_share"] = 1.0
            return xwalk[["metro_id", "coc_id", "area_share", "definition_version"]]

        if base_ref.type == "county":
            xwalk = read_metro_county_membership(
                definition_version=definition_version,
                base_dir=data_root,
            )
            xwalk["area_share"] = 1.0
            return xwalk[["metro_id", "county_fips", "area_share", "definition_version"]]

        if base_ref.type == "tract":
            if base_ref.vintage is None:
                raise ExecutorError(
                    "Metro tract transforms require a tract vintage so the "
                    "executor can load the tract geometry artifact."
                )
            county_membership = read_metro_county_membership(
                definition_version=definition_version,
                base_dir=data_root,
            )
            tracts = pd.read_parquet(tract_path(base_ref.vintage, data_root))
            tract_col: str | None = None
            for candidate in ("tract_geoid", "GEOID", "geoid"):
                if candidate in tracts.columns:
                    tract_col = candidate
                    break
            if tract_col is None:
                raise ExecutorError(
                    "Tract geometry artifact is missing a tract identifier column. "
                    f"Expected one of tract_geoid/GEOID/geoid. "
                    f"Available columns: {sorted(tracts.columns)}"
                )
            tract_index = tracts[[tract_col]].copy()
            tract_index["tract_geoid"] = tract_index[tract_col].astype(str)
            tract_index["county_fips"] = tract_index["tract_geoid"].str[:5]
            xwalk = county_membership.merge(tract_index, on="county_fips", how="inner")
            xwalk["area_share"] = 1.0
            return xwalk[["metro_id", "tract_geoid", "area_share", "definition_version"]]

        raise ExecutorError(
            f"Metro transforms currently support tract, county, or coc bases; "
            f"got '{base_ref.type}'."
        )

    msa_ref = GeometryRef(
        type="msa",
        vintage=metro_ref.vintage,
        source=metro_ref.resolved_metro_definition_version(),
    )
    xwalk = _resolve_msa_transform_df(
        msa_ref=msa_ref,
        base_ref=base_ref,
        project_root=project_root,
    ).rename(columns={"msa_id": "metro_id"})

    subset_df = _metro_subset_membership(metro_ref, data_root=data_root)
    if subset_df is not None:
        keep_cols = [
            "metro_id",
            "profile",
            "profile_definition_version",
            "profile_metro_id",
            "profile_metro_name",
            "profile_rank",
        ]
        available = [col for col in keep_cols if col in subset_df.columns]
        xwalk = xwalk.merge(
            subset_df[available].drop_duplicates(subset=["metro_id"]),
            on="metro_id",
            how="inner",
        )
    xwalk["definition_version"] = definition_version
    return xwalk


def _resolve_msa_transform_df(
    *,
    msa_ref: GeometryRef,
    base_ref: GeometryRef,
    project_root: Path,
) -> pd.DataFrame:
    """Build an MSA crosswalk DataFrame from curated artifacts."""
    if not msa_ref.source:
        raise ExecutorError(
            "MSA transforms require geometry.source to identify the "
            "definition version (for example 'census_msa_2023')."
        )

    from hhplab.msa.crosswalk import build_coc_msa_crosswalk
    from hhplab.msa.msa_io import read_msa_county_membership
    from hhplab.naming import coc_base_path, county_path

    data_root = project_root / "data"
    definition_version = msa_ref.source

    if base_ref.type == "county":
        xwalk = read_msa_county_membership(
            definition_version=definition_version,
            base_dir=data_root,
        ).copy()
        xwalk["area_share"] = 1.0
        return xwalk[["msa_id", "county_fips", "area_share", "definition_version"]]

    if base_ref.type == "tract":
        if base_ref.vintage is None:
            raise ExecutorError(
                "MSA tract transforms require a tract vintage so the executor "
                "can load the tract geometry artifact."
            )
        county_membership = read_msa_county_membership(
            definition_version=definition_version,
            base_dir=data_root,
        )
        tracts = pd.read_parquet(tract_path(base_ref.vintage, data_root))
        tract_col: str | None = None
        for candidate in ("tract_geoid", "GEOID", "geoid"):
            if candidate in tracts.columns:
                tract_col = candidate
                break
        if tract_col is None:
            raise ExecutorError(
                "Tract geometry artifact is missing a tract identifier column. "
                f"Expected one of tract_geoid/GEOID/geoid. "
                f"Available columns: {sorted(tracts.columns)}"
            )
        tract_index = tracts[[tract_col]].copy()
        tract_index["tract_geoid"] = tract_index[tract_col].astype(str)
        tract_index["county_fips"] = tract_index["tract_geoid"].str[:5]
        xwalk = county_membership.merge(tract_index, on="county_fips", how="inner")
        xwalk["area_share"] = 1.0
        return xwalk[["msa_id", "tract_geoid", "area_share", "definition_version"]]

    if base_ref.type == "coc":
        if base_ref.vintage is None:
            raise ExecutorError(
                "MSA CoC transforms require a CoC boundary vintage so the executor "
                "can build the CoC-to-MSA allocation crosswalk."
            )
        import geopandas as gpd

        boundary_vintage = str(base_ref.vintage)
        county_vintage = boundary_vintage
        cached_path = msa_coc_xwalk_path(
            boundary_vintage=boundary_vintage,
            definition_version=definition_version,
            county_vintage=county_vintage,
            base_dir=data_root,
        )
        if cached_path.exists():
            return pd.read_parquet(cached_path)

        coc_boundaries = gpd.read_parquet(coc_base_path(boundary_vintage, data_root))
        counties = gpd.read_parquet(county_path(county_vintage, data_root))
        membership = read_msa_county_membership(
            definition_version=definition_version,
            base_dir=data_root,
        )
        return build_coc_msa_crosswalk(
            coc_boundaries,
            counties,
            membership,
            boundary_vintage=boundary_vintage,
            county_vintage=county_vintage,
            definition_version=definition_version,
        )

    raise ExecutorError(
        f"MSA transforms currently support tract, county, or coc bases; got '{base_ref.type}'."
    )


def _materialize_generated_metro_transform(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> Path:
    """Generate and persist a metro transform artifact for recipe execution."""
    transform = _get_transform(recipe, transform_id)

    metro_ref, base_ref = _identify_metro_and_base(transform.from_, transform.to)
    if metro_ref is None:
        raise ExecutorError(f"Transform '{transform_id}' does not target metro geometry.")

    output_path = _generated_metro_transform_path(
        transform_id,
        metro_ref=metro_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    if output_path.exists():
        return output_path

    xwalk = _resolve_metro_transform_df(
        metro_ref=metro_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    provenance = ProvenanceBlock(
        geo_type="metro",
        definition_version=metro_ref.source,
        tract_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "tract" and base_ref.vintage is not None
            else None
        ),
        county_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "county" and base_ref.vintage is not None
            else None
        ),
        extra={
            "dataset_type": "recipe_transform",
            "transform_id": transform_id,
            "from_type": transform.from_.type,
            "to_type": transform.to.type,
            "metro_definition_version": metro_ref.resolved_metro_definition_version(),
            "subset_profile": metro_ref.resolved_metro_subset_profile(),
            "subset_profile_definition_version": (
                metro_ref.resolved_metro_subset_definition_version()
            ),
        },
    )
    write_parquet_with_provenance(xwalk, output_path, provenance)
    return output_path


def _materialize_generated_msa_transform(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> Path:
    """Generate and persist an MSA transform artifact for recipe execution."""
    transform = _get_transform(recipe, transform_id)

    msa_ref, base_ref = _identify_msa_and_base(transform.from_, transform.to)
    if msa_ref is None:
        raise ExecutorError(f"Transform '{transform_id}' does not target msa geometry.")

    output_path = _generated_msa_transform_path(
        transform_id,
        msa_ref=msa_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    if output_path.exists():
        return output_path

    xwalk = _resolve_msa_transform_df(
        msa_ref=msa_ref,
        base_ref=base_ref,
        project_root=project_root,
    )
    provenance = ProvenanceBlock(
        geo_type="msa",
        definition_version=msa_ref.source,
        tract_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "tract" and base_ref.vintage is not None
            else None
        ),
        county_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "county" and base_ref.vintage is not None
            else None
        ),
        boundary_vintage=(
            str(base_ref.vintage)
            if base_ref.type == "coc" and base_ref.vintage is not None
            else None
        ),
        extra={
            "dataset_type": "recipe_transform",
            "transform_id": transform_id,
            "from_type": transform.from_.type,
            "to_type": transform.to.type,
        },
    )
    write_parquet_with_provenance(xwalk, output_path, provenance)
    return output_path
