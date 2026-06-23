"""Shared primitives for the recipe executor and its submodules.

Contains the core exception, result, and context types plus the small
helper functions (``_classify_path``, ``_echo``, ``_record_step_note``,
``_get_transform``) that every extracted executor submodule needs to
reach.  This module exists to break the import cycle between
``hhplab.recipe.executor`` and its decomposed siblings
(``transforms``, ``manifest``, ``inputs``, ``resample``): each submodule and
``executor`` itself import the primitives from here instead of from
``executor``, so direct imports like
``import hhplab.recipe.executor.transforms`` no longer hit a
partially-initialized module (coclab-l6be).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import typer

from hhplab.config import StorageConfig, load_config
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.manifest import (
    ROOT_ASSET_STORE,
    ROOT_OUTPUT,
    AssetRecord,
)
from hhplab.recipe.recipe_schema import RecipeV1

if TYPE_CHECKING:
    import pandas as pd

    from hhplab.geo.ct_planning_regions import CtPlanningRegionCrosswalk


class ExecutorError(Exception):
    """Raised when recipe execution fails at runtime.

    Attributes
    ----------
    partial_results : list[PipelineResult]
        Results collected before (and including) the failure.  When
        ``execute_recipe`` encounters pipeline errors it continues
        through all remaining pipelines so callers can inspect what
        succeeded and what failed.
    """

    partial_results: list[PipelineResult]

    def __init__(self, message: str, *, partial_results: list[PipelineResult] | None = None):
        super().__init__(message)
        self.partial_results = partial_results or []


@dataclass
class StepResult:
    """Outcome of a single execution step."""

    step_kind: str
    detail: str
    success: bool
    error: str | None = None
    notes: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Aggregate outcome for one pipeline execution."""

    pipeline_id: str
    steps: list[StepResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(s.success for s in self.steps)

    @property
    def error_count(self) -> int:
        return sum(1 for s in self.steps if not s.success)


@dataclass
class ExecutionContext:
    """Shared mutable state across step executions within a pipeline."""

    project_root: Path
    recipe: RecipeV1
    # transform_id → resolved file path on disk
    transform_paths: dict[str, Path] = field(default_factory=dict)
    # (dataset_id, year) → resampled DataFrame
    intermediates: dict[tuple[str, int], pd.DataFrame] = field(
        default_factory=dict,
    )
    # (dataset_id, year) → resolved per-dataset metadata such as ACS vintages
    dataset_year_metadata: dict[tuple[str, int], dict[str, str]] = field(
        default_factory=dict,
    )
    # Asset cache for avoiding redundant reads
    cache: RecipeCache = field(default_factory=RecipeCache)
    # Cached CT county bridge overlays keyed by (legacy_vintage, planning_vintage)
    ct_county_alignment_cache: dict[tuple[int, int], CtPlanningRegionCrosswalk] = field(
        default_factory=dict,
    )
    # Assets consumed during execution (for provenance manifest)
    consumed_assets: list[AssetRecord] = field(default_factory=list)
    # Storage roots (asset store, output) — resolved from config precedence
    storage_config: StorageConfig | None = None
    # Suppress progress output (for --json mode)
    quiet: bool = False
    # Cache: dataset_id → number of distinct resolved paths (for broadcast check)
    _distinct_paths_cache: dict[str, int | None] = field(
        default_factory=dict,
    )
    # Output paths written by prior pipelines in this execution, used to catch collisions.
    _written_outputs: set[Path] = field(default_factory=set)


def _classify_path(
    file_path: Path,
    ctx: ExecutionContext,
) -> tuple[str | None, str]:
    """Classify a file path to its logical root and compute the relative path.

    Returns ``(root, relative_path)`` where *root* is ``"asset_store"``,
    ``"output"``, or ``None`` (fallback to project-relative).
    """
    cfg = ctx.storage_config or load_config(project_root=ctx.project_root)
    resolved = file_path.resolve()

    # Check output root first (it may be nested inside asset store)
    try:
        rel = resolved.relative_to(cfg.output_root.resolve())
        return ROOT_OUTPUT, str(rel)
    except ValueError:
        pass

    try:
        rel = resolved.relative_to(cfg.asset_store_root.resolve())
        return ROOT_ASSET_STORE, str(rel)
    except ValueError:
        pass

    # Fallback: project-relative
    try:
        return None, str(resolved.relative_to(ctx.project_root.resolve()))
    except ValueError:
        return None, str(file_path)


def _echo(ctx: ExecutionContext, message: str) -> None:
    """Print progress message unless quiet mode is active."""
    if not ctx.quiet:
        typer.echo(message)


def _record_step_note(
    ctx: ExecutionContext,
    step_notes: list[str] | None,
    message: str,
) -> None:
    """Attach a human- and machine-visible note to the current step."""
    if step_notes is None or message in step_notes:
        return
    step_notes.append(message)
    _echo(ctx, f"    note: {message}")


def _get_transform(recipe: RecipeV1, transform_id: str):
    """Return a transform by id or raise an ExecutorError."""
    for transform in recipe.transforms:
        if transform.id == transform_id:
            return transform
    raise ExecutorError(
        f"Transform '{transform_id}' referenced in materialize step "
        "but not found in recipe transforms."
    )
