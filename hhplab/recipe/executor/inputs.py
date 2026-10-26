"""Dataset input preparation: column resolution, year filtering, temporal filters.

Houses the helpers that turn a raw dataset DataFrame into the year-
sliced, column-validated form the resample step expects.  Also owns
``_load_support_dataset_for_year``, which loads weighted-transform
support datasets and registers them as consumed assets.  All helpers
are re-exported from ``hhplab.recipe.executor`` so existing call sites
in the resample step (and the test file's direct imports) keep working.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hhplab.recipe.executor.annotations import (
    frame_column_map,
    has_annotations,
    propagate_annotations,
    resolve_annotation_geo_column,
)
from hhplab.recipe.executor.core import (
    ExecutionContext,
    ExecutorError,
    _classify_path,
)
from hhplab.recipe.manifest import AssetRecord
from hhplab.recipe.planner import (
    ResampleTask,
    _resolve_dataset_year,
)
from hhplab.recipe.probes import (
    probe_acs5_tract_translation_provenance,
    probe_geo_column,
    probe_measures,
    probe_static_broadcast,
    probe_year_column,
)
from hhplab.recipe.recipe_schema import (
    TemporalFilter,
)
from hhplab.recipe.schema_common import expand_year_spec

_ACS_PATH_VINTAGE_RE = re.compile(r"__A(\d{4})")


@dataclass(frozen=True)
class SiteUniverseResolution:
    """Validated identity and source files for a frozen overlap-site universe."""

    artifact_id: str
    threshold: float
    schema_version: str
    msa_definition_version: str
    boundary_vintage: str
    membership_identity: str
    selected_msa_ids: tuple[str, ...]
    memberships: tuple[tuple[str, str], ...]
    eligible_site_count: int
    excluded_site_count: int
    paths: tuple[Path, Path, Path]
    fingerprints: tuple[str, str, str]
    manifest_fingerprint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_id": self.artifact_id,
            "threshold": self.threshold,
            "schema_version": self.schema_version,
            "msa_definition_version": self.msa_definition_version,
            "boundary_vintage": self.boundary_vintage,
            "membership_identity": self.membership_identity,
            "selected_msa_ids": list(self.selected_msa_ids),
            "memberships": [list(pair) for pair in self.memberships],
            "eligible_site_count": self.eligible_site_count,
            "excluded_site_count": self.excluded_site_count,
            "paths": [str(path) for path in self.paths],
            "fingerprints": list(self.fingerprints),
            "manifest_fingerprint": self.manifest_fingerprint,
        }


def resolve_site_universe_files(
    *, artifact_id: str, threshold: float, sites_path: Path, memberships_path: Path,
    summary_path: Path | None = None,
    expected_fingerprints: tuple[str, str, str] | None = None,
) -> SiteUniverseResolution:
    """Read and validate the producer's canonical paired site artifacts."""
    from hhplab.artifacts.naming.naming import msa_coc_sites_filename
    from hhplab.recipe.executor.core import ExecutorError
    from hhplab.storage.provenance import PROVENANCE_KEY

    expected_names = (
        msa_coc_sites_filename(artifact_id, "sites"),
        msa_coc_sites_filename(artifact_id, "memberships"),
        msa_coc_sites_filename(artifact_id, "summary"),
    )
    summary_path = summary_path or sites_path.parent / expected_names[2]
    paths = (sites_path, memberships_path, summary_path)
    if (
        (sites_path.name, memberships_path.name) != expected_names[:2]
        or summary_path.name != expected_names[2]
    ):
        raise ExecutorError(
            f"site_universe artifact {artifact_id!r} paths must use canonical producer "
            f"filenames {expected_names[0]!r} and {expected_names[1]!r}."
        )
    if any(not path.is_file() for path in paths):
        missing = [str(path) for path in paths if not path.is_file()]
        raise ExecutorError(
            "Missing site-universe source files: " + ", ".join(missing)
            + ". Rebuild the MSA-CoC sites producer or correct the recipe paths."
        )
    try:
        file_bytes = tuple(path.read_bytes() for path in paths)
        fingerprints = tuple(hashlib.sha256(contents).hexdigest() for contents in file_bytes)
        if expected_fingerprints is not None and fingerprints != expected_fingerprints:
            raise ValueError("site-universe input fingerprints changed since the plan was recorded")
        summary = json.loads(file_bytes[2])
        if summary.get("schema_version") != "msa_coc_sites/1":
            raise ValueError(f"unsupported schema_version {summary.get('schema_version')!r}")
        parameters = summary["parameters"]
        thresholds = [float(value) for value in parameters["thresholds"]]
        if threshold not in thresholds:
            raise ValueError(f"requested threshold {threshold:g} is absent from producer summary")
        boundary_vintage = str(parameters["coc_boundary_vintage"])
        if not boundary_vintage:
            raise ValueError("producer summary has an empty CoC boundary vintage")
        msa_definition_version = parameters.get("msa_definition_version")
        if not isinstance(msa_definition_version, str) or not msa_definition_version.strip():
            raise ValueError("producer summary has no canonical MSA definition version")
        msa_definition_version = msa_definition_version.strip()
        frames: list[pd.DataFrame] = []
        provenance_blocks: list[dict[str, Any]] = []
        for path, contents in zip(paths[:2], file_bytes[:2], strict=True):
            metadata = pq.read_schema(pa.BufferReader(contents)).metadata or {}
            raw = metadata.get(PROVENANCE_KEY)
            if raw is None:
                raise ValueError(f"{path.name} lacks HHP-Lab provenance metadata")
            provenance = json.loads(raw.decode("utf-8"))
            extra = provenance.get("extra", {})
            if extra.get("dataset_type") != "msa_coc_sites":
                raise ValueError(f"{path.name} is not an msa_coc_sites producer artifact")
            if extra.get("parameters") != parameters:
                raise ValueError(f"{path.name} parameters disagree with the summary")
            if str(provenance.get("boundary_vintage")) != boundary_vintage:
                raise ValueError(f"{path.name} boundary vintage disagrees with the summary")
            if provenance.get("definition_version") != parameters.get("msa_definition_version"):
                raise ValueError(f"{path.name} MSA definition version disagrees with the summary")
            provenance_blocks.append(provenance)
            frames.append(pq.read_table(pa.BufferReader(contents)).to_pandas())
        def provenance_identity(provenance: dict[str, Any]) -> dict[str, Any]:
            return {
                key: provenance.get(key)
                for key in ("boundary_vintage", "definition_version", "geo_type", "extra")
            }

        if provenance_identity(provenance_blocks[0]) != provenance_identity(
            provenance_blocks[1]
        ):
            raise ValueError("sites and memberships Parquet provenance blocks disagree")
        sites, memberships = frames
        required_sites = {"threshold", "msa_id", "eligible"}
        required_memberships = {"threshold", "msa_id", "coc_id", "site_eligible"}
        if not required_sites <= set(sites.columns) or not required_memberships <= set(
            memberships.columns
        ):
            raise ValueError("site tables are missing required identity and eligibility columns")
        sites = sites.loc[sites.threshold.astype(float) == threshold].copy()
        memberships = memberships.loc[memberships.threshold.astype(float) == threshold].copy()
        if sites.empty:
            raise ValueError(f"producer tables have no site rows for threshold {threshold:g}")
        for label, frame, columns in (
            ("sites", sites, ("msa_id",)),
            ("memberships", memberships, ("msa_id", "coc_id")),
        ):
            for column in columns:
                values = frame[column]
                if values.isna().any() or not values.map(
                    lambda item: isinstance(item, str) and bool(item.strip())
                ).all():
                    raise ValueError(f"{label} {column} identifiers must be nonempty strings")
        for label, frame, column in (
            ("sites", sites, "eligible"),
            ("memberships", memberships, "site_eligible"),
        ):
            if frame[column].isna().any() or not pd.api.types.is_bool_dtype(frame[column].dtype):
                raise ValueError(f"{label} {column} flags must be nonmissing booleans")
        sites["msa_id"] = sites.msa_id.astype(str)
        memberships["msa_id"] = memberships.msa_id.astype(str)
        memberships["coc_id"] = memberships.coc_id.astype(str)
        if sites.duplicated(["threshold", "msa_id"]).any():
            raise ValueError("duplicate site key (threshold, msa_id)")
        if memberships.duplicated(["threshold", "msa_id", "coc_id"]).any():
            raise ValueError("duplicate membership key (threshold, msa_id, coc_id)")
        eligible_mask = sites["eligible"].astype(bool)
        eligible_ids = tuple(sorted(sites.loc[eligible_mask, "msa_id"].astype(str)))
        eligibility_by_msa = dict(zip(sites.msa_id, sites.eligible, strict=True))
        if not memberships.msa_id.isin(eligibility_by_msa).all():
            raise ValueError("membership rows reference MSA IDs absent from the site table")
        expected_flags = memberships.msa_id.map(eligibility_by_msa)
        if not memberships.site_eligible.eq(expected_flags).all():
            raise ValueError("membership eligibility flags disagree with their site rows")
        selected = memberships.loc[memberships.site_eligible.astype(bool)].copy()
        pairs = tuple(
            sorted(set(zip(selected.msa_id.astype(str), selected.coc_id.astype(str), strict=True)))
        )
        if not pairs and eligible_ids:
            raise ValueError("eligible site rows have no eligible membership rows")
        if set(eligible_ids) != {msa_id for msa_id, _ in pairs}:
            raise ValueError("eligible site and membership MSA IDs disagree")
        coc_ids = [coc_id for _, coc_id in pairs]
        if len(coc_ids) != len(set(coc_ids)):
            raise ValueError("a CoC belongs to multiple selected MSAs; counts would be duplicated")
        membership_payload = {
            "schema_version": "msa_coc_sites/1",
            "artifact_id": artifact_id,
            "threshold": threshold,
            "site_key": "msa_id",
            "memberships": [list(pair) for pair in pairs],
        }
        from hhplab.schema.results import canonical_bytes

        membership_identity = hashlib.sha256(canonical_bytes(membership_payload)).hexdigest()
        return SiteUniverseResolution(
            artifact_id=artifact_id,
            threshold=threshold,
            schema_version="msa_coc_sites/1",
            msa_definition_version=msa_definition_version,
            boundary_vintage=boundary_vintage,
            membership_identity=membership_identity,
            selected_msa_ids=eligible_ids,
            memberships=pairs,
            eligible_site_count=len(eligible_ids),
            excluded_site_count=int((~eligible_mask).sum()),
            paths=paths,
            fingerprints=fingerprints,
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExecutorError(
            f"Invalid site-universe artifact {artifact_id!r}: {exc}. "
            "Use matching canonical sites, memberships, and summary outputs from one producer run."
        ) from exc


def resolve_site_universe_source(
    *, source: Any, threshold: float, source_base_dir: Path | None
) -> SiteUniverseResolution:
    """Resolve local or portable-bundle artifact paths and validate their contents."""
    from hhplab.artifacts.naming.naming import msa_coc_sites_filename
    from hhplab.recipe.executor.core import ExecutorError

    def resolve_locator(raw: str) -> Path:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            if source_base_dir is None:
                raise ExecutorError(
                    "Relative site_universe paths in an in-memory recipe require an explicit "
                    "source_base_dir."
                )
            path = source_base_dir / path
        return path.resolve()

    if source.bundle_path is None:
        sites_path = resolve_locator(source.sites_path)
        memberships_path = resolve_locator(source.memberships_path)
        return resolve_site_universe_files(
            artifact_id=source.artifact_id,
            threshold=threshold,
            sites_path=sites_path,
            memberships_path=memberships_path,
        )

    bundle_root = resolve_locator(source.bundle_path)
    manifest_path = bundle_root / "manifest.json"
    sidecar_path = bundle_root / "manifest.sha256"
    try:
        manifest_bytes = manifest_path.read_bytes()
        manifest_digest = hashlib.sha256(manifest_bytes).hexdigest()
        sidecar = sidecar_path.read_text(encoding="utf-8").strip().split()
        if len(sidecar) != 2 or sidecar != [manifest_digest, "manifest.json"]:
            raise ValueError("bundle manifest checksum does not match manifest.sha256")
        manifest = json.loads(manifest_bytes)
        artifact = manifest["site_universes"][source.artifact_id]
        if artifact.get("schema_version") != "msa_coc_sites/1":
            raise ValueError("bundle site-universe index has an unsupported schema version")
        file_map = {
            "sites": artifact["sites"],
            "memberships": artifact["memberships"],
            "summary": artifact["summary"],
        }
        inventory = {item["path"]: item for item in manifest["files"]}
        paths: dict[str, Path] = {}
        for key, relative in file_map.items():
            rel = Path(relative)
            if rel.is_absolute() or ".." in rel.parts:
                raise ValueError(f"bundle {key} path must stay inside the bundle")
            path = (bundle_root / rel).resolve()
            if bundle_root.resolve() not in path.parents:
                raise ValueError(f"bundle {key} path escapes the bundle root")
            record = inventory.get(rel.as_posix())
            if record is None:
                raise ValueError(f"bundle {key} path is absent from the verified inventory")
            contents = path.read_bytes()
            if (
                len(contents) != record["bytes"]
                or hashlib.sha256(contents).hexdigest() != record["sha256"]
            ):
                raise ValueError(f"bundle {key} file differs from the manifest inventory")
            paths[key] = path
        expected_names = (
            msa_coc_sites_filename(source.artifact_id, "sites"),
            msa_coc_sites_filename(source.artifact_id, "memberships"),
            msa_coc_sites_filename(source.artifact_id, "summary"),
        )
        if (
            paths["sites"].name != expected_names[0]
            or paths["memberships"].name != expected_names[1]
        ):
            raise ValueError("bundle site-universe files do not match canonical artifact names")
        result = resolve_site_universe_files(
            artifact_id=source.artifact_id,
            threshold=threshold,
            sites_path=paths["sites"],
            memberships_path=paths["memberships"],
            summary_path=paths["summary"],
        )
        return SiteUniverseResolution(
            **{
                **result.__dict__,
                "paths": (paths["sites"], paths["memberships"], paths["summary"]),
                "manifest_fingerprint": manifest_digest,
            }
        )
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ExecutorError(
            f"Invalid site-universe bundle {bundle_root}: {exc}. Use a verified results bundle "
            "that indexes this artifact under manifest.json.site_universes."
        ) from exc


def _resolve_year_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str | None:
    """Resolve year column: use declared value, or auto-detect, or None."""
    result = probe_year_column(list(df.columns), declared)
    if not result.ok:
        raise ExecutorError(result.message)
    return result.detail["year_column"] if result.detail else None


def _normalize_acs5_vintage(value: object) -> str:
    """Normalize an ACS5 vintage value to its end-year string."""
    text = str(value).strip()
    if not text:
        raise ExecutorError("Empty ACS5 vintage value cannot be normalized.")
    if "-" in text:
        end = text.split("-")[-1]
        if end.isdigit() and len(end) == 4:
            return end
    if text.isdigit() and len(text) == 4:
        return text
    match = re.search(r"(\d{4})$", text)
    if match is not None:
        return match.group(1)
    raise ExecutorError(
        f"Could not normalize ACS5 vintage value {value!r} to a 4-digit end year."
    )


def _single_string_value(values: pd.Series, label: str) -> str | None:
    """Return a single distinct non-null string value, or None when absent."""
    distinct = values.dropna().astype(str).unique().tolist()
    if not distinct:
        return None
    if len(distinct) > 1:
        raise ExecutorError(
            f"{label} has multiple distinct values in one dataset-year slice: "
            f"{sorted(distinct)}."
        )
    return str(distinct[0])


def _fallback_dataset_year_value(
    *,
    df: pd.DataFrame,
    task: ResampleTask,
) -> str | None:
    """Return a single year-like value from the active dataset slice."""
    year_column = _resolve_year_column(df, task.year_column)
    if year_column is None or year_column not in df.columns:
        if "year" not in df.columns:
            return str(task.year)
        year_column = "year"
    return _single_string_value(
        df[year_column],
        f"Dataset '{task.dataset_id}' year {task.year}: {year_column}",
    )


def record_dataset_year_metadata(
    *,
    task: ResampleTask,
    ctx: ExecutionContext,
    df: pd.DataFrame,
) -> None:
    """Capture dataset/year provenance needed during panel assembly."""
    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None:
        return

    metadata: dict[str, str] = {}

    if ds.provider == "census" and ds.product in {"acs", "acs5"}:
        acs5_vintage = None
        if "acs_vintage" in df.columns:
            acs5_vintage = _single_string_value(
                df["acs_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs_vintage",
            )
            if acs5_vintage is not None:
                acs5_vintage = _normalize_acs5_vintage(acs5_vintage)
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs5_vintage = match.group(1)
        if acs5_vintage is None:
            fallback_year = _fallback_dataset_year_value(df=df, task=task)
            if fallback_year is not None:
                acs5_vintage = _normalize_acs5_vintage(fallback_year)
        if acs5_vintage is not None:
            metadata["acs5_vintage_used"] = acs5_vintage

    if ds.provider == "census" and ds.product == "acs1":
        acs1_vintage = None
        if "acs1_vintage" in df.columns:
            acs1_vintage = _single_string_value(
                df["acs1_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs1_vintage",
            )
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs1_vintage = match.group(1)
        if acs1_vintage is None:
            acs1_vintage = _fallback_dataset_year_value(df=df, task=task)
        if acs1_vintage is not None:
            metadata["acs1_vintage_used"] = acs1_vintage

    if metadata:
        ctx.dataset_year_metadata[(task.dataset_id, task.year)] = metadata


def _resolve_geo_column(
    df: pd.DataFrame,
    declared: str | None,
) -> str:
    """Resolve geo-ID column: use declared value or auto-detect."""
    result = probe_geo_column(list(df.columns), declared)
    if not result.ok:
        raise ExecutorError(result.message)
    return result.detail["geo_column"]


def _filter_to_year(
    df: pd.DataFrame,
    year_col: str,
    year: int,
) -> pd.DataFrame:
    """Filter a DataFrame to a requested year, tolerating string year values."""
    series = df[year_col]
    if pd.api.types.is_numeric_dtype(series):
        mask = series == year
    else:
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            mask = coerced == year
        else:
            mask = series.astype(str) == str(year)
    return df[mask].copy()


def _validate_columns(
    df: pd.DataFrame,
    measures: list[str],
    dataset_id: str,
    year: int,
) -> None:
    """Validate that all required measure columns exist."""
    result = probe_measures(list(df.columns), measures, dataset_id)
    if not result.ok:
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: {result.message}"
        )


def _validate_input_dataset_provenance(
    *,
    ctx: ExecutionContext,
    dataset_id: str,
    effective_geometry,
    input_path: str,
) -> None:
    """Reject known-stale translated ACS tract caches before reading them."""
    ds = ctx.recipe.datasets.get(dataset_id)
    if ds is None:
        return

    input_file = ctx.project_root / input_path
    result = probe_acs5_tract_translation_provenance(
        dataset_id=dataset_id,
        dataset_spec=ds,
        effective_geometry=effective_geometry,
        path=input_file,
        path_label=input_path,
    )
    if result.ok:
        return

    remediation_command = (
        result.detail.get("remediation_command")
        if result.detail is not None
        else None
    )
    remediation = (
        f" Rebuild it with '{remediation_command}'."
        if remediation_command is not None
        else ""
    )
    raise ExecutorError(
        f"{result.message}{remediation}"
    )


def _reject_implicit_static_broadcast(
    *,
    ctx: ExecutionContext,
    task: ResampleTask,
    year_column: str | None,
) -> str | None:
    """Return an error when a multi-year build would silently broadcast data."""
    if year_column is not None:
        return None

    universe_years = expand_year_spec(ctx.recipe.universe)
    if len(universe_years) <= 1:
        return None

    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None:
        return None

    if task.dataset_id in ctx._distinct_paths_cache:
        distinct_paths = ctx._distinct_paths_cache[task.dataset_id]
    else:
        distinct_paths: int | None = None
        if ds.file_set is not None:
            resolved_paths = {
                _resolve_dataset_year(task.dataset_id, year, ctx.recipe).path
                for year in universe_years
            }
            distinct_paths = len(resolved_paths)
        ctx._distinct_paths_cache[task.dataset_id] = distinct_paths

    result = probe_static_broadcast(
        ds,
        task.dataset_id,
        year_column_found=year_column is not None,
        universe_year_count=len(universe_years),
        distinct_paths=distinct_paths,
    )
    if not result.ok:
        return (
            f"Dataset '{task.dataset_id}' year {task.year}: {result.message}"
        )
    return None


def _apply_temporal_filter(
    df: pd.DataFrame,
    filt: TemporalFilter,
    year: int,
    dataset_id: str,
    year_column: str | None = None,
    geo_column: str | None = None,
) -> pd.DataFrame:
    """Apply a temporal filter to a DataFrame before resampling."""
    if has_annotations(df) and filt.method in {
        "calendar_mean", "calendar_median", "interpolate_to_month",
    }:
        raise ExecutorError(
            f"Temporal filter for '{dataset_id}': annotated inputs cannot use "
            f"{filt.method!r} until exact source contributors are mapped. "
            "Use point_in_time filtering or prepare the annotated panel upstream."
        )

    def rebind(filtered: pd.DataFrame) -> pd.DataFrame:
        if not has_annotations(df):
            return filtered
        source_geo = resolve_annotation_geo_column(df, geo_column)
        if source_geo is None:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': annotated input needs an explicit "
                "geography key to rebind exact cells."
            )
        source_year = year_column if year_column in df.columns else "year"
        output_year = source_year if source_year in filtered.columns else "year"
        if source_year not in df.columns or output_year not in filtered.columns:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': annotated input needs a retained "
                "geography/year key to rebind exact cells."
            )
        return propagate_annotations(
            filtered,
            [(df, frame_column_map(df, filtered), source_geo, source_year)],
            output_geo_column=source_geo,
            year_column=output_year,
        )
    col = filt.column
    if col not in df.columns:
        raise ExecutorError(
            f"Temporal filter for '{dataset_id}': column '{col}' not found. "
            f"Available: {sorted(df.columns)}"
        )
    if filt.method == "point_in_time":
        series = df[col]
        if hasattr(series, "dt") and hasattr(series.dt, "month"):
            # Datetime column: filter by month and derive year column
            filtered = df[series.dt.month == filt.month].copy()
            if filtered.empty:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}' year {year}: "
                    f"no rows where {col}.month=={filt.month}."
                )
            if "year" not in filtered.columns:
                filtered["year"] = filtered[col].dt.year
            return rebind(filtered.drop(columns=[col]))
        else:
            filtered = df[series == filt.month]
            if filtered.empty:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}' year {year}: "
                    f"no rows where {col}=={filt.month}."
                )
            return rebind(filtered.drop(columns=[col]))
    elif filt.method in ("calendar_mean", "calendar_median"):
        # Group by identifier columns and preserve year keys when present.
        numeric = set(df.select_dtypes("number").columns)
        group_cols = [c for c in df.columns if c != col and c not in numeric]
        if year_column is not None:
            if year_column not in df.columns:
                raise ExecutorError(
                    f"Temporal filter for '{dataset_id}': declared year column "
                    f"'{year_column}' not found. Available: {sorted(df.columns)}"
                )
            if year_column != col and year_column not in group_cols:
                group_cols.append(year_column)
        elif "year" in df.columns and "year" != col and "year" not in group_cols:
            group_cols.append("year")
        measure_cols = [
            c for c in df.select_dtypes("number").columns
            if c != col and c not in group_cols
        ]
        if not group_cols:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': no grouping columns found "
                f"after excluding temporal column '{col}'."
            )
        agg_func = "mean" if filt.method == "calendar_mean" else "median"
        return df.groupby(group_cols, as_index=False)[measure_cols].agg(agg_func)
    elif filt.method == "interpolate_to_month":
        # Linear interpolation between adjacent annual observations to estimate
        # a value at a target month.  E.g. PEP July→January: for year Y,
        # jan(Y) = 0.5 * jul(Y-1) + 0.5 * jul(Y).
        if filt.month is None:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': "
                "interpolate_to_month requires 'month'."
            )
        target_month = filt.month
        series = df[col]
        if not (hasattr(series, "dt") and hasattr(series.dt, "month")):
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                f"requires a datetime column '{col}'."
            )
        source_months = series.dt.month.unique()
        if len(source_months) != 1:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                f"expects a single source month but found {sorted(source_months)}."
            )
        source_month = int(source_months[0])
        if source_month == target_month:
            # No interpolation needed — fall through to point_in_time semantics.
            if "year" not in df.columns:
                df = df.copy()
                df["year"] = df[col].dt.year
            return rebind(df.drop(columns=[col]))

        # Determine the interpolation fraction and direction.
        # months_forward = distance (in months) from the prior source observation
        # to the target month.
        months_forward = (target_month - source_month) % 12
        fraction = months_forward / 12.0

        # Identify grouping and measure columns.
        yr_col = year_column if year_column else (
            "year" if "year" in df.columns else None
        )
        if yr_col is None or yr_col not in df.columns:
            raise ExecutorError(
                f"Temporal filter for '{dataset_id}': interpolate_to_month "
                "requires a year column. Set year_column on the dataset spec."
            )
        numeric_cols = set(df.select_dtypes("number").columns)
        geo_cols = [
            c for c in df.columns
            if c != col and c != yr_col and c not in numeric_cols
        ]
        measure_cols = [
            c for c in df.select_dtypes("number").columns
            if c != yr_col
        ]

        # Build a current-year and previous-year copy for the merge.
        keep = geo_cols + [yr_col] + measure_cols
        df_curr = df[keep].copy()

        # Coerce year column to int for arithmetic (handles string-typed years).
        original_yr_dtype = df_curr[yr_col].dtype
        df_curr[yr_col] = pd.to_numeric(df_curr[yr_col], errors="coerce").astype("Int64")

        if target_month < source_month:
            # Target is before source in the calendar year.
            # January(Y) sits between Jul(Y-1) and Jul(Y).
            df_prev = df_curr.copy()
            df_prev[yr_col] = df_prev[yr_col] + 1  # align Y-1 row to year Y
        else:
            # Target is after source in the calendar year.
            # e.g. Jul(Y) sits between Jan(Y) and Jan(Y+1).
            # prev = source data at year Y, curr = source data from year Y+1
            # shifted down so it merges on year Y.
            df_prev = df_curr.copy()
            df_curr = df_curr.copy()
            df_curr[yr_col] = df_curr[yr_col] - 1

        merged = df_curr.merge(
            df_prev,
            on=geo_cols + [yr_col],
            suffixes=("", "_prev"),
            how="left",
        )

        for mcol in measure_cols:
            curr_vals = merged[mcol]
            prev_col = f"{mcol}_prev"
            if prev_col in merged.columns:
                prev_vals = merged[prev_col]
                merged[mcol] = np.where(
                    prev_vals.notna(),
                    (1 - fraction) * prev_vals + fraction * curr_vals,
                    curr_vals,
                )
        # Drop the _prev helper columns.
        prev_drop = [c for c in merged.columns if c.endswith("_prev")]
        result = merged.drop(columns=prev_drop)

        # Restore original year column dtype (e.g. string).
        if pd.api.types.is_object_dtype(original_yr_dtype):
            result[yr_col] = result[yr_col].astype(str)
        elif original_yr_dtype != result[yr_col].dtype:
            result[yr_col] = result[yr_col].astype(original_yr_dtype)

        return result
    else:
        raise ExecutorError(f"Unknown temporal filter method '{filt.method}'.")


def _load_support_dataset_for_year(
    *,
    ctx: ExecutionContext,
    dataset_id: str,
    year: int,
) -> pd.DataFrame:
    """Load an auxiliary dataset and filter it to the requested year."""
    resolved = _resolve_dataset_year(dataset_id, year, ctx.recipe)
    if resolved.path is None:
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: no input path resolved."
        )

    input_file = ctx.project_root / resolved.path
    if not input_file.exists():
        raise ExecutorError(
            f"Dataset '{dataset_id}' year {year}: input file not found at "
            f"{resolved.path}"
        )

    _validate_input_dataset_provenance(
        ctx=ctx,
        dataset_id=dataset_id,
        effective_geometry=resolved.effective_geometry,
        input_path=resolved.path,
    )
    df = ctx.cache.read_parquet(input_file, reject_annotations=True)
    identity = ctx.cache.file_identity(input_file)
    root, rel_path = _classify_path(input_file, ctx)
    ctx.consumed_assets.append(AssetRecord(
        role="dataset",
        path=rel_path,
        sha256=identity.sha256,
        size=identity.size,
        root=root,
        dataset_id=dataset_id,
    ))

    ds = ctx.recipe.datasets[dataset_id]
    filt = ctx.recipe.filters.get(dataset_id)
    if filt is not None and isinstance(filt, TemporalFilter):
        df = _apply_temporal_filter(
            df,
            filt,
            year,
            dataset_id,
            year_column=ds.year_column,
            geo_column=ds.geo_column,
        )

    year_col = _resolve_year_column(df, ds.year_column)
    if year_col is not None:
        df = _filter_to_year(df, year_col, year)
        if df.empty:
            raise ExecutorError(
                f"Dataset '{dataset_id}' year {year}: no rows after filtering "
                f"{year_col}=={year}."
            )

    return df
