"""Manifest, provenance, and output-path helpers for recipe execution.

Builds the provenance dict embedded in panel parquet files and the
sidecar :class:`RecipeManifest` written next to each output, plus the
canonical path resolution helpers used by both the executor and the
CLI's ``hhplab build recipe`` command.  These helpers are imported back
into ``hhplab.recipe.executor`` so legacy callers (CLI, tests, manual
docs) keep working unchanged.
"""

from __future__ import annotations

import re
from pathlib import Path

from hhplab.config import StorageConfig, load_config
from hhplab.naming import geo_map_filename, geo_panel_filename
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    _classify_path,
)
from hhplab.recipe.manifest import (
    AssetRecord,
    RecipeManifest,
)
from hhplab.recipe.recipe_schema import (
    RecipeV1,
)
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec


def _deduplicate_assets(
    assets: list[AssetRecord],
) -> list[AssetRecord]:
    """Deduplicate asset records by (role, path)."""
    seen: set[tuple[str, str]] = set()
    result: list[AssetRecord] = []
    for a in assets:
        key = (a.role, a.path)
        if key not in seen:
            seen.add(key)
            result.append(a)
    return result


def _build_provenance(
    recipe: RecipeV1,
    pipeline_id: str,
    ctx: ExecutionContext,
) -> dict[str, object]:
    """Build provenance metadata for the output artifact.

    Derives from :func:`_build_manifest` so that both the Parquet-embedded
    provenance and the sidecar JSON share a single code path for asset
    deduplication and dataset/transform extraction.
    """
    manifest = _build_manifest(recipe, pipeline_id, ctx)
    d = manifest.to_dict()
    # Rename for backward-compatible Parquet metadata key
    d["consumed_assets"] = d.pop("assets")
    # Remove manifest-only fields not needed in Parquet provenance
    d.pop("executed_at", None)
    d.pop("output_path", None)
    return d


def _build_manifest(
    recipe: RecipeV1,
    pipeline_id: str,
    ctx: ExecutionContext,
    output_path: str | None = None,
) -> RecipeManifest:
    """Build a full provenance manifest for the execution."""
    return RecipeManifest(
        recipe_name=recipe.name,
        recipe_version=recipe.version,
        pipeline_id=pipeline_id,
        assets=_deduplicate_assets(ctx.consumed_assets),
        datasets={
            ds_id: {
                "provider": ds.provider,
                "product": ds.product,
                "version": ds.version,
                "path": ds.path,
            }
            for ds_id, ds in recipe.datasets.items()
        },
        transforms={
            tid: _classify_path(path, ctx)[1]
            for tid, path in ctx.transform_paths.items()
        },
        output_path=output_path,
    )


def _target_geometry_metadata(
    target_geometry: GeometryRef,
) -> tuple[str, str | None, str | None, str | None]:
    """Return target geometry metadata for naming and panel provenance."""
    geo_type = target_geometry.type
    boundary_vintage = (
        str(target_geometry.vintage)
        if target_geometry.vintage is not None
        else None
    )
    definition_version = (
        target_geometry.source if geo_type in {"metro", "msa"} else None
    )
    profile_definition_version = (
        target_geometry.subset_profile_definition_version
        if geo_type == "metro"
        else None
    )
    return (
        geo_type,
        boundary_vintage,
        definition_version,
        profile_definition_version,
    )


def _resolve_pipeline_target(
    recipe: RecipeV1,
    pipeline_id: str,
):
    """Return the pipeline and target referenced by *pipeline_id*."""
    pipeline = next((p for p in recipe.pipelines if p.id == pipeline_id), None)
    if pipeline is None:
        raise ExecutorError(f"Pipeline '{pipeline_id}' not found in recipe.")

    target = next((t for t in recipe.targets if t.id == pipeline.target), None)
    if target is None:
        raise ExecutorError(f"Target '{pipeline.target}' not found in recipe.")

    return pipeline, target


def _recipe_output_dirname(recipe_name: str) -> str:
    """Return a deterministic directory name for a recipe output namespace."""
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", recipe_name.strip().lower())
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-.")
    return normalized or "recipe"


def _resolve_panel_output_file(
    recipe: RecipeV1,
    pipeline_id: str,
    project_root: Path,
    storage_config: StorageConfig | None = None,
) -> Path:
    """Return the canonical panel parquet path for a pipeline."""
    _, target = _resolve_pipeline_target(recipe, pipeline_id)
    (
        target_geo_type,
        boundary_vintage,
        definition_version,
        profile_definition_version,
    ) = _target_geometry_metadata(target.geometry)

    if target_geo_type in {"metro", "msa"} and definition_version is None:
        raise ExecutorError(
            f"{target_geo_type.upper()} recipe targets must set geometry.source "
            "to the geography definition version so panel outputs can be named."
        )

    universe_years = expand_year_spec(recipe.universe)
    start_year = min(universe_years)
    end_year = max(universe_years)

    cfg = storage_config or load_config(project_root=project_root)
    recipe_dir = _recipe_output_dirname(recipe.name)
    return (
        cfg.output_root / recipe_dir / geo_panel_filename(
            start_year,
            end_year,
            geo_type=target_geo_type,
            boundary_vintage=boundary_vintage if target_geo_type == "coc" else None,
            definition_version=definition_version,
            profile_definition_version=profile_definition_version,
        )
    )


def _resolve_map_output_file(
    recipe: RecipeV1,
    pipeline_id: str,
    project_root: Path,
    storage_config: StorageConfig | None = None,
) -> Path:
    """Return the canonical HTML map path for a pipeline."""
    _, target = _resolve_pipeline_target(recipe, pipeline_id)
    (
        target_geo_type,
        boundary_vintage,
        definition_version,
        profile_definition_version,
    ) = _target_geometry_metadata(target.geometry)

    if target_geo_type in {"metro", "msa"} and definition_version is None:
        raise ExecutorError(
            f"{target_geo_type.upper()} recipe targets must set geometry.source "
            "to the geography definition version so map outputs can be named."
        )

    universe_years = expand_year_spec(recipe.universe)
    start_year = min(universe_years)
    end_year = max(universe_years)

    cfg = storage_config or load_config(project_root=project_root)
    recipe_dir = _recipe_output_dirname(recipe.name)
    return (
        cfg.output_root / recipe_dir / geo_map_filename(
            start_year,
            end_year,
            geo_type=target_geo_type,
            boundary_vintage=boundary_vintage if target_geo_type == "coc" else None,
            definition_version=definition_version,
            profile_definition_version=profile_definition_version,
        )
    )


def resolve_pipeline_artifacts(
    recipe: RecipeV1,
    pipeline_id: str,
    *,
    project_root: Path | None = None,
    storage_config: StorageConfig | None = None,
) -> dict[str, str]:
    """Return canonical output paths for a pipeline's declared outputs.

    Paths are relative to *project_root* when the output falls within the
    project tree, otherwise absolute.
    """
    if project_root is None:
        project_root = Path.cwd()

    _, target = _resolve_pipeline_target(recipe, pipeline_id)
    panel_file = _resolve_panel_output_file(
        recipe, pipeline_id, project_root, storage_config=storage_config,
    )
    artifacts: dict[str, str] = {}

    def _display_path(p: Path) -> str:
        try:
            return str(p.relative_to(project_root))
        except ValueError:
            return str(p)

    if "panel" in target.outputs:
        artifacts["panel_path"] = _display_path(panel_file)
        artifacts["manifest_path"] = _display_path(
            panel_file.with_suffix(".manifest.json"),
        )

    if "diagnostics" in target.outputs:
        diagnostics_file = panel_file.with_name(
            f"{panel_file.stem}__diagnostics.json",
        )
        artifacts["diagnostics_path"] = _display_path(diagnostics_file)

    if "map" in target.outputs:
        map_file = _resolve_map_output_file(
            recipe, pipeline_id, project_root, storage_config=storage_config,
        )
        artifacts["map_path"] = _display_path(map_file)

    return artifacts
