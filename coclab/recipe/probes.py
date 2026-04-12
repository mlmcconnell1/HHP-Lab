"""Non-executing dataset and transform probes for recipe preflight.

Shared inspection helpers that validate dataset schemas, column
presence, temporal filters, static broadcast safety, transform
prerequisites, and support-dataset requirements without running a
build.  Both the executor and the preflight analyzer consume these
primitives so validation logic stays in one place.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from coclab.recipe.recipe_schema import (
    CrosswalkTransform,
    DatasetSpec,
    RecipeV1,
    TemporalFilter,
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
    *,
    year_column: str | None = None,
    column_types: dict[str, str] | None = None,
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
    if filt.method == "interpolate_to_month":
        if year_column is None:
            return ProbeResult(
                ok=False,
                message=(
                    f"Temporal filter for '{dataset_id}': "
                    "interpolate_to_month requires a year column. "
                    "Set year_column on the dataset spec."
                ),
            )
        if column_types is not None:
            type_name = column_types.get(filt.column, "")
            if type_name and "timestamp" not in type_name and "date" not in type_name:
                return ProbeResult(
                    ok=False,
                    message=(
                        f"Temporal filter for '{dataset_id}': "
                        f"interpolate_to_month requires a datetime column "
                        f"'{filt.column}', found parquet type '{type_name}'."
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
    from coclab.naming import (
        metro_coc_membership_path,
        metro_county_membership_path,
        tract_path,
    )
    from coclab.recipe.executor import (
        ExecutorError,
        _identify_metro_and_base,
        _resolve_transform_path,
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
    metro_ref, base_ref = _identify_metro_and_base(transform.from_, transform.to)
    can_generate = metro_ref is not None

    if path.exists():
        return ProbeResult(
            ok=True,
            detail={
                "path": str(path.relative_to(project_root)),
                "can_generate": can_generate,
            },
        )

    generation_ready = False
    missing_inputs: list[str] = []
    if can_generate and metro_ref is not None and metro_ref.source:
        data_root = project_root / "data"
        prereq_paths: list[Path] = []
        if base_ref.type == "coc":
            prereq_paths.append(
                metro_coc_membership_path(metro_ref.source, data_root)
            )
        elif base_ref.type == "county":
            prereq_paths.append(
                metro_county_membership_path(metro_ref.source, data_root)
            )
        elif base_ref.type == "tract":
            prereq_paths.append(
                metro_county_membership_path(metro_ref.source, data_root)
            )
            if base_ref.vintage is not None:
                prereq_paths.append(tract_path(base_ref.vintage, data_root))

        missing_inputs = [
            str(path.relative_to(project_root))
            for path in prereq_paths
            if not path.exists()
        ]
        generation_ready = len(missing_inputs) == 0

    return ProbeResult(
        ok=False,
        message=(
            f"Transform '{transform_id}' artifact not found at "
            f"{path.relative_to(project_root)}"
        ),
        detail={
            "path": str(path.relative_to(project_root)),
            "can_generate": can_generate,
            "generation_ready": generation_ready,
            "missing_inputs": missing_inputs,
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
        column_types = {
            field.name: str(field.type)
            for field in schema
        }
        return ProbeResult(
            ok=True,
            detail={
                "columns": columns,
                "column_types": column_types,
            },
        )
    except Exception as exc:
        return ProbeResult(
            ok=False,
            message=f"Cannot read parquet schema: {exc}",
        )


def probe_interpolate_to_month_data(
    path: Path,
    filt: TemporalFilter,
    dataset_id: str,
) -> ProbeResult:
    """Validate interpolate_to_month against source data values."""
    if filt.method != "interpolate_to_month":
        return ProbeResult(ok=True)
    try:
        df = pd.read_parquet(path, columns=[filt.column])
    except Exception as exc:
        return ProbeResult(
            ok=False,
            message=(
                f"Temporal filter for '{dataset_id}': cannot read column "
                f"'{filt.column}' from {path}: {exc}"
            ),
        )
    series = df[filt.column]
    parsed = pd.to_datetime(series, errors="coerce")
    non_null = int(series.notna().sum())
    if non_null == 0 or int(parsed.notna().sum()) != non_null:
        return ProbeResult(
            ok=False,
            message=(
                f"Temporal filter for '{dataset_id}': "
                f"interpolate_to_month requires a datetime column '{filt.column}'."
            ),
        )
    source_months = sorted(int(month) for month in parsed.dt.month.dropna().unique())
    if len(source_months) != 1:
        return ProbeResult(
            ok=False,
            message=(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                f"expects a single source month but found {source_months}."
            ),
        )
    return ProbeResult(ok=True, detail={"source_month": source_months[0]})


# ---------------------------------------------------------------------------
# Weighted-transform support-dataset probes
# ---------------------------------------------------------------------------

def get_weighted_transform_requirements(
    transform,
) -> tuple[str, str] | None:
    """Extract (population_source, population_field) if a transform needs them.

    Returns None if the transform does not use population weighting or
    if the required fields are not configured.

    Both the executor and preflight call this to decide whether
    support-dataset validation is needed.
    """
    if not isinstance(transform, CrosswalkTransform):
        return None
    weighting = transform.spec.weighting
    if weighting.scheme != "population":
        return None
    if not weighting.population_source or not weighting.population_field:
        return None
    return (weighting.population_source, weighting.population_field)


def probe_support_dataset(
    *,
    population_source: str,
    population_field: str,
    transform_id: str,
    recipe: RecipeV1,
    project_root: Path,
    years: list[int],
) -> list[ProbeResult]:
    """Check that a weighted transform's support dataset exists and has the required field.

    Validates:
    1. The population_source dataset is declared in the recipe.
    2. For each year, the resolved support-dataset file exists on disk.
    3. The population_field column exists in the support-dataset schema.

    Returns a list of ProbeResults (one per issue found, empty if all ok).
    """
    results: list[ProbeResult] = []

    ds = recipe.datasets.get(population_source)
    if ds is None:
        results.append(ProbeResult(
            ok=False,
            message=(
                f"Transform '{transform_id}' requires population_source "
                f"'{population_source}' but it is not declared in the recipe."
            ),
        ))
        return results

    # Resolve paths for each year and check existence + schema
    from coclab.recipe.planner import PlannerError, _resolve_dataset_year

    checked_paths: set[str] = set()
    missing_years: list[int] = []

    for year in years:
        try:
            resolved = _resolve_dataset_year(population_source, year, recipe)
        except PlannerError:
            missing_years.append(year)
            continue
        path = resolved.path
        if path is None:
            path = ds.path
        if path is None:
            continue
        if path in checked_paths:
            continue
        checked_paths.add(path)

        full_path = project_root / path
        if not full_path.exists():
            missing_years.append(year)
            continue

        # Check schema for population_field
        schema_result = probe_dataset_schema(full_path)
        if schema_result.ok:
            columns = schema_result.detail["columns"]
            if population_field not in columns:
                results.append(ProbeResult(
                    ok=False,
                    message=(
                        f"Transform '{transform_id}' requires field "
                        f"'{population_field}' in dataset "
                        f"'{population_source}' ({path}), but it is "
                        f"not present. Available: {sorted(columns)}"
                    ),
                    detail={
                        "transform_id": transform_id,
                        "population_source": population_source,
                        "population_field": population_field,
                        "path": path,
                    },
                ))

    if missing_years:
        results.append(ProbeResult(
            ok=False,
            message=(
                f"Transform '{transform_id}' requires dataset "
                f"'{population_source}' for years {missing_years}, "
                f"but file(s) are missing."
            ),
            detail={
                "transform_id": transform_id,
                "population_source": population_source,
                "missing_years": missing_years,
            },
        ))

    return results
