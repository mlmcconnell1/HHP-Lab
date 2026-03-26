"""Non-executing dataset and transform probes for recipe preflight.

Shared inspection helpers that validate dataset schemas, column
presence, temporal filters, static broadcast safety, and transform
prerequisites without running a build.  Both the executor and the
preflight analyzer consume these primitives so validation logic stays
in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

from coclab.recipe.recipe_schema import (
    DatasetSpec,
    GeometryRef,
    RecipeV1,
    TemporalFilter,
    expand_year_spec,
)


# Auto-detect candidates for geo-ID and year columns.
GEO_CANDIDATES: list[str] = [
    "geo_id", "GEOID", "geoid", "coc_id", "metro_id",
    "tract_geoid", "county_fips",
]
YEAR_CANDIDATES: list[str] = ["year", "pit_year", "acs1_vintage"]


@dataclass
class ProbeResult:
    """Outcome of a single probe check."""

    ok: bool
    message: str | None = None
    detail: dict | None = None


# ---------------------------------------------------------------------------
# Column detection (schema-only, no DataFrame required)
# ---------------------------------------------------------------------------

def probe_year_column(
    columns: list[str],
    declared: str | None,
) -> ProbeResult:
    """Check whether a year column can be resolved from available columns.

    Parameters
    ----------
    columns : list[str]
        Column names available in the dataset.
    declared : str | None
        Explicitly declared year_column from the dataset spec, or None.

    Returns
    -------
    ProbeResult
        ok=True with detail["year_column"] set, or ok=False with message.
    """
    if declared is not None:
        if declared in columns:
            return ProbeResult(ok=True, detail={"year_column": declared})
        return ProbeResult(
            ok=False,
            message=(
                f"Declared year_column '{declared}' not found. "
                f"Available: {sorted(columns)}"
            ),
        )
    matches = [c for c in YEAR_CANDIDATES if c in columns]
    if len(matches) > 1:
        return ProbeResult(
            ok=False,
            message=(
                f"Ambiguous year column: found {matches}. "
                f"Declare year_column in the dataset spec to resolve."
            ),
        )
    year_col = matches[0] if matches else None
    return ProbeResult(ok=True, detail={"year_column": year_col})


def probe_geo_column(
    columns: list[str],
    declared: str | None,
) -> ProbeResult:
    """Check whether a geo-ID column can be resolved from available columns.

    Parameters
    ----------
    columns : list[str]
        Column names available in the dataset.
    declared : str | None
        Explicitly declared geo_column from the dataset spec, or None.

    Returns
    -------
    ProbeResult
        ok=True with detail["geo_column"] set, or ok=False with message.
    """
    if declared is not None:
        if declared in columns:
            return ProbeResult(ok=True, detail={"geo_column": declared})
        return ProbeResult(
            ok=False,
            message=(
                f"Declared geo_column '{declared}' not found. "
                f"Available: {sorted(columns)}"
            ),
        )
    matches = [c for c in GEO_CANDIDATES if c in columns]
    if len(matches) == 0:
        return ProbeResult(
            ok=False,
            message=(
                f"Cannot find geo-ID column. "
                f"Expected one of {GEO_CANDIDATES}, "
                f"got columns: {sorted(columns)}"
            ),
        )
    if len(matches) > 1:
        return ProbeResult(
            ok=False,
            message=(
                f"Ambiguous geo-ID column: found {matches}. "
                f"Declare geo_column in the dataset spec to resolve."
            ),
        )
    return ProbeResult(ok=True, detail={"geo_column": matches[0]})


def probe_measures(
    columns: list[str],
    measures: list[str],
    dataset_id: str,
) -> ProbeResult:
    """Check that all required measure columns exist.

    Returns
    -------
    ProbeResult
        ok=True if all measures present, ok=False with missing list.
    """
    missing = [m for m in measures if m not in columns]
    if missing:
        return ProbeResult(
            ok=False,
            message=(
                f"Dataset '{dataset_id}': missing measure columns "
                f"{missing}. Available: {sorted(columns)}"
            ),
            detail={"missing_measures": missing},
        )
    return ProbeResult(ok=True)


def probe_temporal_filter(
    columns: list[str],
    filt: TemporalFilter,
    dataset_id: str,
) -> ProbeResult:
    """Check that a temporal filter's column exists in the dataset.

    Returns
    -------
    ProbeResult
        ok=True if the filter column is present, ok=False otherwise.
    """
    if filt.column not in columns:
        return ProbeResult(
            ok=False,
            message=(
                f"Temporal filter for '{dataset_id}': column "
                f"'{filt.column}' not found. "
                f"Available: {sorted(columns)}"
            ),
        )
    return ProbeResult(ok=True)


def probe_static_broadcast(
    dataset_spec: DatasetSpec,
    dataset_id: str,
    year_column_found: bool,
    universe_year_count: int,
    *,
    distinct_paths: int | None = None,
) -> ProbeResult:
    """Check whether a multi-year build would silently broadcast static data.

    A dataset without a year column will be reused for every requested year.
    This is unsafe unless the user opts in with broadcast_static=true.

    Parameters
    ----------
    dataset_spec : DatasetSpec
        The dataset specification.
    dataset_id : str
        Dataset identifier for messaging.
    year_column_found : bool
        Whether a year column was detected.
    universe_year_count : int
        Number of years in the recipe universe.
    distinct_paths : int | None
        Number of distinct file paths across universe years (file_set).
        When >1, each year maps to a different file, so broadcast is safe.

    Returns
    -------
    ProbeResult
        ok=True if safe, ok=False if implicit broadcast detected.
    """
    if year_column_found:
        return ProbeResult(ok=True)
    if universe_year_count <= 1:
        return ProbeResult(ok=True)
    if bool(dataset_spec.params.get("broadcast_static", False)):
        return ProbeResult(ok=True)
    if distinct_paths is not None and distinct_paths > 1:
        return ProbeResult(ok=True)

    return ProbeResult(
        ok=False,
        message=(
            f"Dataset '{dataset_id}': no year column found, but recipe "
            f"universe spans {universe_year_count} years. Reusing the same "
            "dataset for every year would broadcast a static snapshot across "
            "time. Add a year_column, switch to file_set for year-specific "
            "files, or set params.broadcast_static=true if this broadcast "
            "is intentional."
        ),
    )


# ---------------------------------------------------------------------------
# Transform prerequisite probes
# ---------------------------------------------------------------------------

def probe_transform_path(
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
) -> ProbeResult:
    """Check whether a transform artifact exists on disk.

    Resolves the expected crosswalk path using the same logic as the
    executor, then checks existence.

    Returns
    -------
    ProbeResult
        ok=True if the file exists, ok=False with the expected path.
    """
    from coclab.recipe.executor import (
        _identify_metro_and_base,
        _resolve_transform_path,
        ExecutorError,
    )

    transform = None
    for t in recipe.transforms:
        if t.id == transform_id:
            transform = t
            break
    if transform is None:
        return ProbeResult(
            ok=False,
            message=f"Transform '{transform_id}' not found in recipe.",
        )

    try:
        path = _resolve_transform_path(transform_id, recipe, project_root)
    except ExecutorError as exc:
        return ProbeResult(ok=False, message=str(exc))

    # For metro transforms, the artifact can be generated on demand
    metro_ref, _ = _identify_metro_and_base(transform.from_, transform.to)
    can_generate = metro_ref is not None

    if path.exists():
        return ProbeResult(
            ok=True,
            detail={
                "path": str(path.relative_to(project_root)),
                "can_generate": can_generate,
            },
        )

    return ProbeResult(
        ok=False,
        message=(
            f"Transform '{transform_id}' artifact not found at "
            f"{path.relative_to(project_root)}"
        ),
        detail={
            "path": str(path.relative_to(project_root)),
            "can_generate": can_generate,
        },
    )


# ---------------------------------------------------------------------------
# Dataset file probe (schema-level)
# ---------------------------------------------------------------------------

def probe_dataset_schema(
    path: Path,
) -> ProbeResult:
    """Read parquet schema without loading data.

    Returns
    -------
    ProbeResult
        ok=True with detail["columns"] listing column names,
        or ok=False if the file cannot be read.
    """
    if not path.exists():
        return ProbeResult(
            ok=False,
            message=f"File not found: {path}",
        )
    try:
        schema = pq.read_schema(path)
        columns = schema.names
        return ProbeResult(ok=True, detail={"columns": columns})
    except Exception as exc:
        return ProbeResult(
            ok=False,
            message=f"Cannot read parquet schema: {exc}",
        )
