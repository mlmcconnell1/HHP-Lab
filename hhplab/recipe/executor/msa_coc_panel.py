"""MSA-CoC containment panel assembly for recipe execution."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from numbers import Integral, Real
from typing import Any

import geopandas as gpd
import pandas as pd

from hhplab.geographies.coc.coc_validate import BoundaryCompatibilityValidation
from hhplab.geographies.msa.selectors import select_top_msa_ids_by_population
from hhplab.geographies.msa.unemployment import resolve_msa_unemployment
from hhplab.recipe.executor.containment import build_msa_coc_membership
from hhplab.recipe.executor.core import ExecutionContext, ExecutorError
from hhplab.recipe.planner import ExecutionPlan
from hhplab.recipe.recipe_schema import ContainmentSpec
from hhplab.recipe.schema_common import GeometryRef, expand_year_spec
from hhplab.schema.columns import HIC_PANEL_MEASURE_COLUMNS, MSA_COC_PANEL_COLUMNS

SITE_YEAR_DIAGNOSTIC_SUFFIXES = (
    "expected_coc_count",
    "observed_coc_count",
    "missing_coc_ids",
    "unknown_boundary_coc_ids",
    "incompatible_boundary_coc_ids",
    "status",
)
SITE_YEAR_STATUSES = frozenset(
    {"complete", "incomplete", "source_limited", "incomplete_source_limited"}
)
PIT_SITE_YEAR_LIMITED_MEASURES = frozenset({"pit_total", "pit_unsheltered"})
SITE_YEAR_ALLOWED_MEASURES = {
    "pit": frozenset({"pit_total", "pit_sheltered", "pit_unsheltered"}),
    "hic": frozenset(HIC_PANEL_MEASURE_COLUMNS),
}


@dataclass(frozen=True)
class MsaCocPanelAssembly:
    """Assembled MSA-CoC panel plus provenance-ready diagnostics."""

    panel: pd.DataFrame
    provenance: dict[str, object]


@dataclass(frozen=True)
class SiteYearSourceBinding:
    """One named CoC/year panel and the measures selected from it."""

    panel: str
    measures: tuple[str, ...]


@dataclass(frozen=True)
class SiteYearPanelAssembly:
    """Site-year result and the lineage needed by result persistence."""

    panel: pd.DataFrame
    provenance: dict[str, object]


def site_year_panel_columns(measures: Sequence[str]) -> tuple[str, ...]:
    """Return canonical output columns for declared measures in input order."""
    columns = ["msa_id", "year"]
    for measure in measures:
        columns.append(measure)
        columns.extend(f"{measure}__{suffix}" for suffix in SITE_YEAR_DIAGNOSTIC_SUFFIXES)
    return tuple(columns)


def assemble_site_year_panel(
    *,
    frozen_memberships: Sequence[tuple[str, str]],
    requested_years: Sequence[int],
    source_panels: Mapping[str, pd.DataFrame],
    outcome_bindings: Mapping[str, SiteYearSourceBinding],
    boundary_compatibility: BoundaryCompatibilityValidation,
    artifact_id: str,
    threshold: float,
    membership_identity: str,
    frozen_boundary_vintage: str | int,
    source_panel_fingerprints: Mapping[str, str],
) -> SiteYearPanelAssembly:
    """Build one complete-or-null row per frozen MSA site and requested year.

    This builder is pure: callers resolve paths, load frames, and provide the
    fingerprints that are preserved in the returned provenance block.
    """
    sites_by_coc = _validate_site_year_memberships(frozen_memberships)
    years = _validate_site_year_years(requested_years)
    bindings = _normalize_site_year_bindings(outcome_bindings)
    if not bindings:
        raise ExecutorError("site-year aggregation needs at least one outcome binding.")
    measures = tuple(
        measure
        for binding in bindings.values()
        for measure in binding.measures
    )
    if len(set(measures)) != len(measures):
        raise ExecutorError("site-year outcome measure names must be unique across bindings.")

    frozen_vintage = str(frozen_boundary_vintage).strip()
    if not frozen_vintage:
        raise ExecutorError("Frozen CoC boundary vintage must be non-empty.")
    if (
        boundary_compatibility.artifact_id != artifact_id
        or boundary_compatibility.threshold != float(threshold)
        or boundary_compatibility.membership_identity != membership_identity
        or boundary_compatibility.frozen_boundary_vintage != frozen_vintage
    ):
        raise ExecutorError(
            "Boundary compatibility evidence does not match the selected site-universe identity."
        )

    panel_ids = tuple(dict.fromkeys(binding.panel for binding in bindings.values()))
    missing_panels = sorted(set(panel_ids) - set(source_panels))
    if missing_panels:
        raise ExecutorError(
            f"Missing source panel frames for site-year aggregation: {missing_panels}."
        )
    missing_fingerprints = sorted(set(panel_ids) - set(source_panel_fingerprints))
    if missing_fingerprints:
        raise ExecutorError(
            f"Missing source panel fingerprints for site-year aggregation: {missing_fingerprints}."
        )
    invalid_fingerprints = sorted(
        panel_id
        for panel_id in panel_ids
        if not isinstance(source_panel_fingerprints[panel_id], str)
        or not source_panel_fingerprints[panel_id].strip()
    )
    if invalid_fingerprints:
        raise ExecutorError(
            f"Source panel fingerprints must be non-empty strings: {invalid_fingerprints}."
        )

    source_lookups = {
        panel_id: _site_year_source_lookup(
            panel_id,
            source_panels[panel_id],
            required_measures=tuple(
                measure
                for binding in bindings.values()
                if binding.panel == panel_id
                for measure in binding.measures
            ),
        )
        for panel_id in panel_ids
    }
    evidence = _index_site_year_boundary_evidence(
        boundary_compatibility,
        panel_ids=panel_ids,
        coc_ids=tuple(sites_by_coc),
        years=years,
        sites_by_coc=sites_by_coc,
    )

    site_ids = tuple(sorted(set(sites_by_coc.values())))
    rows: list[dict[str, object]] = []
    for msa_id in site_ids:
        coc_ids = tuple(
            sorted(
                coc_id
                for coc_id, site_id in sites_by_coc.items()
                if site_id == msa_id
            )
        )
        for year in years:
            row: dict[str, object] = {"msa_id": msa_id, "year": year}
            for outcome_name, binding in bindings.items():
                panel_id = binding.panel
                source_lookup = source_lookups[panel_id]
                for measure in binding.measures:
                    observed_cocs: set[str] = set()
                    missing_cocs: set[str] = set()
                    unknown_cocs: set[str] = set()
                    incompatible_cocs: set[str] = set()
                    values: list[int] = []

                    for coc_id in coc_ids:
                        compatibility = evidence[(panel_id, coc_id, year)]
                        if compatibility.status == "unknown":
                            unknown_cocs.add(coc_id)
                        elif compatibility.status == "incompatible":
                            incompatible_cocs.add(coc_id)

                        # V1's unresolved rows retain their old frozen-ID
                        # presence diagnostic. V2 never infers a source join:
                        # only IDs explicitly declared by the evidence are read.
                        source_coc_ids = compatibility.source_coc_ids
                        if not source_coc_ids and compatibility.count_operation is None:
                            source_coc_ids = (
                                (coc_id,)
                                if boundary_compatibility.schema_version.endswith("/1")
                                else ()
                            )
                        constituent_values = [
                            _valid_site_year_count(
                                source_lookup.get((source_coc_id, year), {}).get(measure)
                            )
                            for source_coc_id in source_coc_ids
                        ]
                        if not constituent_values or any(
                            value is None for value in constituent_values
                        ):
                            missing_cocs.add(coc_id)
                        else:
                            observed_cocs.add(coc_id)
                            if compatibility.status in {"compatible", "mapped", "reconciled"}:
                                if compatibility.count_operation == "sum":
                                    values.append(sum(constituent_values))
                                elif compatibility.count_operation in {"single", "preaggregated"}:
                                    values.append(constituent_values[0])

                    limited = (
                        outcome_name == "pit"
                        and year == 2021
                        and measure in PIT_SITE_YEAR_LIMITED_MEASURES
                    )
                    complete = (
                        len(observed_cocs) == len(coc_ids)
                        and not unknown_cocs
                        and not incompatible_cocs
                    )
                    if limited:
                        status = "source_limited" if complete else "incomplete_source_limited"
                        aggregate: int | None = None
                    else:
                        status = "complete" if complete else "incomplete"
                        aggregate = sum(values) if complete else None

                    row[measure] = aggregate
                    row[f"{measure}__expected_coc_count"] = len(coc_ids)
                    row[f"{measure}__observed_coc_count"] = len(observed_cocs)
                    row[f"{measure}__missing_coc_ids"] = sorted(missing_cocs)
                    row[f"{measure}__unknown_boundary_coc_ids"] = sorted(unknown_cocs)
                    row[f"{measure}__incompatible_boundary_coc_ids"] = sorted(
                        incompatible_cocs
                    )
                    row[f"{measure}__status"] = status
            rows.append(row)

    columns = site_year_panel_columns(measures)
    panel = pd.DataFrame(rows, columns=columns, dtype=object)
    panel["year"] = panel["year"].astype("int64")
    for measure in measures:
        try:
            panel[measure] = pd.array(panel[measure], dtype="Int64")
            for suffix in ("expected_coc_count", "observed_coc_count"):
                column = f"{measure}__{suffix}"
                panel[column] = pd.array(panel[column], dtype="Int64")
        except (TypeError, ValueError, OverflowError) as exc:
            raise ExecutorError(
                f"Aggregated measure '{measure}' is outside the supported nullable integer range."
            ) from exc
    if panel.duplicated(["msa_id", "year"]).any():
        raise ExecutorError("Site-year aggregation produced duplicate (msa_id, year) keys.")
    provenance = {
        "site_year_panel": {
            "schema_version": "site_year_panel/1",
            "artifact_id": artifact_id,
            "threshold": float(threshold),
            "membership_identity": membership_identity,
            "frozen_boundary_vintage": frozen_vintage,
            "requested_years": list(years),
            "selected_sites": list(site_ids),
            "selected_membership_count": len(sites_by_coc),
            "source_panels": {
                panel_id: {
                    "fingerprint": source_panel_fingerprints[panel_id],
                    "measures": [
                        measure
                        for binding in bindings.values()
                        if binding.panel == panel_id
                        for measure in binding.measures
                    ],
                }
                for panel_id in panel_ids
            },
            "boundary_evidence": {
                "evidence_id": boundary_compatibility.evidence_id,
                "fingerprint": boundary_compatibility.evidence_fingerprint,
                "schema_version": boundary_compatibility.schema_version,
            },
            "measures": list(measures),
            "row_count": len(panel),
        }
    }
    return SiteYearPanelAssembly(panel=panel, provenance=provenance)


@dataclass(frozen=True)
class _FrameRecord:
    dataset_id: str
    source_token: str
    geo_type: str
    year: int
    frame: pd.DataFrame


def _validate_site_year_memberships(
    memberships: Sequence[tuple[str, str]],
) -> dict[str, str]:
    if not memberships:
        raise ExecutorError("Site-year aggregation requires selected CoC memberships.")
    sites_by_coc: dict[str, str] = {}
    for pair in memberships:
        if not isinstance(pair, tuple) or len(pair) != 2:
            raise ExecutorError("Frozen memberships must be (msa_id, coc_id) pairs.")
        msa_value, coc_value = pair
        if not isinstance(msa_value, str) or not isinstance(coc_value, str):
            raise ExecutorError("Frozen membership msa_id and coc_id values must be strings.")
        msa_id, coc_id = msa_value.strip(), coc_value.strip()
        if not msa_id or not coc_id:
            raise ExecutorError("Frozen membership msa_id and coc_id values must be non-empty.")
        if coc_id in sites_by_coc:
            raise ExecutorError(
                f"Frozen CoC '{coc_id}' appears more than once in selected site memberships."
            )
        sites_by_coc[coc_id] = msa_id
    return sites_by_coc


def _validate_site_year_years(years: Sequence[int]) -> tuple[int, ...]:
    normalized: list[int] = []
    for year in years:
        if isinstance(year, bool) or not isinstance(year, Integral):
            raise ExecutorError("Requested site-year values must be integer years.")
        normalized.append(int(year))
    if not normalized:
        raise ExecutorError("Site-year aggregation requires at least one requested year.")
    if len(set(normalized)) != len(normalized):
        raise ExecutorError("Requested site-year values must be unique.")
    return tuple(sorted(normalized))


def _normalize_site_year_bindings(
    bindings: Mapping[str, SiteYearSourceBinding],
) -> dict[str, SiteYearSourceBinding]:
    if not isinstance(bindings, Mapping) or set(bindings) != {"pit", "hic"}:
        raise ExecutorError("Site-year outcome bindings must declare exactly 'pit' and 'hic'.")
    normalized: dict[str, SiteYearSourceBinding] = {}
    for outcome_name in ("pit", "hic"):
        binding = bindings[outcome_name]
        if isinstance(binding, Mapping):
            panel = binding.get("panel")
            measures = binding.get("measures")
        else:
            panel = getattr(binding, "panel", None)
            measures = getattr(binding, "measures", None)
        if not isinstance(panel, str) or not panel.strip():
            raise ExecutorError(f"site-year {outcome_name} binding needs a panel name.")
        if not isinstance(measures, Sequence) or isinstance(measures, str) or not measures:
            raise ExecutorError(f"site-year {outcome_name} binding needs explicit measures.")
        normalized_measures = tuple(str(measure).strip() for measure in measures)
        if any(not measure for measure in normalized_measures):
            raise ExecutorError(f"site-year {outcome_name} measure names must be non-empty.")
        if len(set(normalized_measures)) != len(normalized_measures):
            raise ExecutorError(f"site-year {outcome_name} measure names must be unique.")
        unsupported_measures = sorted(
            set(normalized_measures) - SITE_YEAR_ALLOWED_MEASURES[outcome_name]
        )
        if unsupported_measures:
            raise ExecutorError(
                f"site-year {outcome_name} measures are not registered additive counts: "
                f"{unsupported_measures}."
            )
        normalized[outcome_name] = SiteYearSourceBinding(
            panel=panel.strip(), measures=normalized_measures
        )
    return normalized


def _site_year_source_lookup(
    panel_id: str,
    frame: pd.DataFrame,
    *,
    required_measures: tuple[str, ...],
) -> dict[tuple[str, int], dict[str, object]]:
    required_columns = {"coc_id", "year", *required_measures}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ExecutorError(
            f"Site-year source panel '{panel_id}' is missing required columns {missing_columns}."
        )
    lookup: dict[tuple[str, int], dict[str, object]] = {}
    for row in frame.loc[:, ["coc_id", "year", *required_measures]].to_dict(orient="records"):
        coc_value = row["coc_id"]
        if pd.isna(coc_value) or not str(coc_value).strip():
            raise ExecutorError(f"Site-year source panel '{panel_id}' has a null or empty coc_id.")
        coc_id = str(coc_value).strip()
        try:
            year_numeric = pd.to_numeric(row["year"], errors="raise")
        except (TypeError, ValueError) as exc:
            raise ExecutorError(
                f"Site-year source panel '{panel_id}' has a non-numeric year key {row['year']!r}."
            ) from exc
        if isinstance(year_numeric, bool) or pd.isna(year_numeric):
            raise ExecutorError(
                f"Site-year source panel '{panel_id}' has a non-integer year key {row['year']!r}."
            )
        if isinstance(year_numeric, Integral):
            normalized_year = int(year_numeric)
        elif isinstance(year_numeric, Real) and math.isfinite(float(year_numeric)):
            if not float(year_numeric).is_integer():
                raise ExecutorError(
                    "Site-year source panel "
                    f"'{panel_id}' has a non-integer year key {row['year']!r}."
                )
            normalized_year = int(year_numeric)
        else:
            raise ExecutorError(
                f"Site-year source panel '{panel_id}' has a non-integer year key {row['year']!r}."
            )
        key = (coc_id, normalized_year)
        if key in lookup:
            raise ExecutorError(
                f"Site-year source panel '{panel_id}' has duplicate normalized key {key!r}."
            )
        lookup[key] = {measure: row[measure] for measure in required_measures}
    return lookup


def _index_site_year_boundary_evidence(
    validation: BoundaryCompatibilityValidation,
    *,
    panel_ids: Sequence[str],
    coc_ids: Sequence[str],
    years: Sequence[int],
    sites_by_coc: Mapping[str, str],
) -> dict[tuple[str, str, int], Any]:
    expected_keys = {
        (panel_id, coc_id, year)
        for panel_id in panel_ids
        for coc_id in coc_ids
        for year in years
    }
    indexed: dict[tuple[str, str, int], Any] = {}
    resolved_sources: dict[tuple[str, int], dict[str, str]] = {}
    for record in validation.records:
        key = (record.source_panel, record.frozen_coc_id, record.year)
        if key not in expected_keys:
            raise ExecutorError(
                "Boundary compatibility evidence contains a row outside the selected "
                f"universe: {key!r}."
            )
        if key in indexed:
            raise ExecutorError(f"Boundary compatibility evidence repeats key {key!r}.")
        if record.msa_id != sites_by_coc[record.frozen_coc_id]:
            raise ExecutorError(
                f"Boundary compatibility evidence assigns CoC '{record.frozen_coc_id}' "
                "to a different MSA than the selected membership."
            )
        if record.status not in {"compatible", "mapped", "reconciled", "incompatible", "unknown"}:
            raise ExecutorError(
                f"Boundary compatibility evidence has unsupported status {record.status!r}."
            )
        if record.status == "compatible" and record.source_coc_ids != (record.frozen_coc_id,):
            raise ExecutorError(
                f"Compatible evidence for CoC '{record.frozen_coc_id}' must retain its source ID."
            )
        if record.status == "mapped" and len(record.source_coc_ids) != 1:
            raise ExecutorError(
                f"Mapped evidence for CoC '{record.frozen_coc_id}' needs a source CoC ID."
            )
        if record.status == "reconciled" and (
            not record.source_coc_ids
            or record.count_operation not in {"single", "preaggregated", "sum"}
            or not record.count_evidence_ref
            or not record.approval_ref
            or not record.reconciliation_basis
        ):
            raise ExecutorError(
                f"Reconciled evidence for CoC '{record.frozen_coc_id}' is missing its "
                "reviewed count resolution."
            )
        if record.status in {"unknown", "incompatible"} and record.source_coc_ids:
            raise ExecutorError(
                f"{record.status.title()} evidence for CoC '{record.frozen_coc_id}' "
                "cannot resolve a source ID."
            )
        if record.status == "reconciled":
            if record.count_operation == "single" and len(record.source_coc_ids) != 1:
                raise ExecutorError("single count operation requires exactly one source CoC ID.")
            if record.count_operation == "sum" and len(record.source_coc_ids) < 2:
                raise ExecutorError("sum count operation requires at least two source CoC IDs.")
            if record.count_operation == "preaggregated" and (
                len(record.source_coc_ids) != 1
                or len(record.preaggregated_source_coc_ids) < 2
                or not set(record.source_coc_ids).issubset(
                    record.preaggregated_source_coc_ids
                )
            ):
                raise ExecutorError(
                    "preaggregated count operation requires one consumed ID and its "
                    "predecessor scope."
                )
        if record.source_coc_ids:
            panel_year = (record.source_panel, record.year)
            source_map = resolved_sources.setdefault(panel_year, {})
            claimed_source_ids = set(record.source_coc_ids)
            if record.count_operation == "preaggregated":
                claimed_source_ids.update(record.preaggregated_source_coc_ids)
            for source_coc_id in sorted(claimed_source_ids):
                previous_frozen = source_map.get(source_coc_id)
                if previous_frozen is not None:
                    raise ExecutorError(
                        f"Boundary evidence maps both '{previous_frozen}' and "
                        f"'{record.frozen_coc_id}' to '{source_coc_id}'."
                    )
                source_map[source_coc_id] = record.frozen_coc_id
        indexed[key] = record

    missing_keys = sorted(expected_keys - set(indexed))
    if missing_keys:
        raise ExecutorError(
            "Boundary compatibility validation is missing selected "
            f"panel/member/year rows: {missing_keys}."
        )
    return indexed


def _valid_site_year_count(value: object) -> int | None:
    if value is None or pd.isna(value) or isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        count = int(value)
    elif isinstance(value, Real):
        if not math.isfinite(float(value)) or not float(value).is_integer():
            return None
        count = int(value)
    else:
        return None
    return count if count >= 0 else None


def build_msa_coc_containment_spec(
    panel_spec: Any,
    *,
    selector_ids: list[str] | None = None,
) -> ContainmentSpec:
    """Translate ``MsaCocPanelSpec`` into the containment builder contract."""
    county_vintage = _county_vintage_from_msa_definition_version(panel_spec.msa_definition_version)
    return ContainmentSpec(
        container=GeometryRef(
            type="msa",
            vintage=county_vintage,
            source=panel_spec.msa_definition_version,
        ),
        candidate=GeometryRef(
            type="coc",
            vintage=panel_spec.coc_boundary_vintage,
        ),
        min_share=panel_spec.containment_min_share,
        denominator=panel_spec.containment_denominator,
        definition_version=panel_spec.msa_definition_version,
        selector_ids=selector_ids,
    )


def assemble_msa_coc_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
    *,
    target: Any,
    coc_gdf: gpd.GeoDataFrame,
    county_gdf: gpd.GeoDataFrame,
    msa_county_membership: pd.DataFrame,
) -> MsaCocPanelAssembly:
    """Assemble one row per ``msa_id x coc_id x year`` for an MSA-CoC target."""
    panel_spec = target.msa_coc_panel
    if panel_spec is None:
        raise ExecutorError(f"Target '{target.id}' does not declare msa_coc_panel.")

    containment_spec = build_msa_coc_containment_spec(panel_spec)
    records = _collect_frame_records(plan, ctx)
    universe_years = expand_year_spec(ctx.recipe.universe)
    ranking_frame = _source_year_frame(
        records,
        source_token=panel_spec.ranking_population_source,
        geo_type="msa",
        year=panel_spec.ranking_reference_year,
        purpose="MSA ranking population",
    )
    ranking_column = _population_column(ranking_frame, panel_spec.ranking_population_source)
    selection = select_top_msa_ids_by_population(
        ranking_frame,
        msa_county_membership,
        ranking_source=panel_spec.ranking_population_source,
        reference_year=panel_spec.ranking_reference_year,
        top_n=panel_spec.top_n,
        population_column=ranking_column,
    )
    containment_spec = build_msa_coc_containment_spec(
        panel_spec,
        selector_ids=list(selection.selector_ids),
    )
    membership = build_msa_coc_membership(
        containment_spec,
        coc_gdf=coc_gdf,
        county_gdf=county_gdf,
        msa_county_membership=msa_county_membership,
    )
    if membership.empty:
        raise ExecutorError(
            "MSA-CoC panel membership is empty after applying top-N selector "
            f"and containment_min_share={panel_spec.containment_min_share}."
        )

    year_frames: list[pd.DataFrame] = []
    for year in universe_years:
        base = membership[["msa_id", "coc_id"]].copy()
        base["year"] = year

        msa_covariates = _msa_covariates_for_year(records, year=year, panel_spec=panel_spec)
        unemployment = _unemployment_for_year(records, year=year, panel_spec=panel_spec)
        coc_measures = _coc_measures_for_year(records, year=year)

        frame = base.merge(msa_covariates, on=["msa_id", "year"], how="left")
        frame = frame.merge(unemployment, on=["msa_id", "year"], how="left")
        frame = frame.merge(coc_measures, on=["coc_id", "year"], how="left")
        year_frames.append(frame)

    panel = pd.concat(year_frames, ignore_index=True)
    panel = _add_msa_coc_metadata(panel, panel_spec)
    panel = _apply_output_aliases(panel, panel_spec.output_aliases)
    panel = _order_columns(panel)

    provenance = {
        "msa_coc_panel": {
            "row_count": len(panel),
            "membership_row_count": len(membership),
            "top_n": panel_spec.top_n,
            "selected_msa_ids": list(selection.selector_ids),
            "ranking_diagnostics": selection.diagnostics.to_dict(),
            "msa_population_source": panel_spec.msa_population_source,
            "unemployment_source": panel_spec.unemployment_source,
            "output_aliases": dict(panel_spec.output_aliases),
        },
        "containment_spec": containment_spec.model_dump(mode="json"),
    }
    return MsaCocPanelAssembly(panel=panel, provenance=provenance)


def _collect_frame_records(plan: ExecutionPlan, ctx: ExecutionContext) -> list[_FrameRecord]:
    records: list[_FrameRecord] = []
    for task in plan.resample_tasks:
        key = (task.dataset_id, task.year)
        if key not in ctx.intermediates:
            continue
        ds = ctx.recipe.datasets[task.dataset_id]
        geo_type = task.to_geometry.type
        source_token = _source_token(ds.provider, ds.product)
        records.append(
            _FrameRecord(
                dataset_id=task.dataset_id,
                source_token=source_token,
                geo_type=geo_type,
                year=task.year,
                frame=_normalize_geo_frame(ctx.intermediates[key], geo_type=geo_type),
            )
        )

    for year in expand_year_spec(ctx.recipe.universe):
        joined = ctx.intermediates.get(("__joined__", year))
        if joined is not None:
            records.append(
                _FrameRecord(
                    dataset_id="__joined__",
                    source_token="joined",
                    geo_type="coc",
                    year=year,
                    frame=_normalize_geo_frame(joined, geo_type="coc"),
                )
            )
    return records


def _normalize_geo_frame(frame: pd.DataFrame, *, geo_type: str) -> pd.DataFrame:
    normalized = frame.copy()
    target_col = _geo_column_for_type(geo_type)
    if target_col not in normalized.columns and "geo_id" in normalized.columns:
        normalized = normalized.rename(columns={"geo_id": target_col})
    if target_col in normalized.columns:
        raw_id = normalized[target_col].astype(str)
        normalized[target_col] = raw_id.mask(raw_id.str.fullmatch(r"\d+"), raw_id.str.zfill(5))
    if "year" in normalized.columns:
        normalized["year"] = pd.to_numeric(normalized["year"], errors="raise").astype(int)
    return normalized


def _source_year_frame(
    records: list[_FrameRecord],
    *,
    source_token: str,
    geo_type: str,
    year: int,
    purpose: str,
) -> pd.DataFrame:
    for record in records:
        if (
            record.source_token == source_token
            and record.geo_type == geo_type
            and record.year == year
        ):
            return record.frame
    raise ExecutorError(
        f"No {purpose} frame found for source '{source_token}', "
        f"geometry '{geo_type}', year {year}. Add a recipe resample step "
        "that materializes this source at MSA geometry."
    )


def _msa_covariates_for_year(
    records: list[_FrameRecord],
    *,
    year: int,
    panel_spec: Any,
) -> pd.DataFrame:
    acs5 = _source_year_frame(
        records,
        source_token="acs5",
        geo_type="msa",
        year=year,
        purpose="ACS5 MSA covariates",
    )
    population = _source_year_frame(
        records,
        source_token=panel_spec.msa_population_source,
        geo_type="msa",
        year=year,
        purpose="MSA population",
    )

    result = acs5[["msa_id", "year"]].copy()
    result["msa_median_rent"] = _first_available(acs5, ["msa_median_rent", "median_gross_rent"])
    result["msa_vacancy_rate"] = _first_available(
        acs5,
        ["msa_vacancy_rate", "vacancy_rate"],
        fallback=_rate_from_columns(
            acs5,
            numerator="vacant_housing_units",
            denominator="total_housing_units",
        ),
    )
    result["msa_poverty_rate"] = _first_available(
        acs5,
        ["msa_poverty_rate", "poverty_rate"],
        fallback=_rate_from_columns(
            acs5,
            numerator="population_below_poverty",
            denominator="poverty_universe",
        ),
    )
    result["msa_income"] = _first_available(acs5, ["msa_income", "median_household_income"])
    result["msa_rent_burden"] = _first_available(
        acs5,
        ["msa_rent_burden", "rent_burden_30_plus"],
        fallback=_rate_from_rent_burden_columns(acs5),
    )
    population_column = _population_column(population, panel_spec.msa_population_source)
    population_values = population[["msa_id", "year", population_column]].rename(
        columns={population_column: "msa_population"}
    )
    result = result.merge(population_values, on=["msa_id", "year"], how="left")
    result[f"msa_population_{panel_spec.msa_population_source}"] = result["msa_population"]
    return result


def _unemployment_for_year(
    records: list[_FrameRecord],
    *,
    year: int,
    panel_spec: Any,
) -> pd.DataFrame:
    if panel_spec.unemployment_source == "acs5":
        source = _source_year_frame(
            records,
            source_token="acs5",
            geo_type="msa",
            year=year,
            purpose="ACS5 MSA unemployment",
        )
        return resolve_msa_unemployment("acs5", acs5_msa=source, years=[year])

    source = _source_year_frame(
        records,
        source_token="laus",
        geo_type="msa",
        year=year,
        purpose="BLS LAUS MSA unemployment",
    )
    return resolve_msa_unemployment("laus", laus_msa=source, years=[year])


def _coc_measures_for_year(records: list[_FrameRecord], *, year: int) -> pd.DataFrame:
    coc_frames = [
        record.frame
        for record in records
        if record.geo_type == "coc" and record.year == year and "coc_id" in record.frame.columns
    ]
    if not coc_frames:
        raise ExecutorError(
            f"No CoC-year measures found for MSA-CoC panel year {year}. "
            "Add recipe steps that join PIT counts and CoC population at CoC geometry."
        )

    merged = coc_frames[0]
    for frame in coc_frames[1:]:
        extra_cols = [
            column
            for column in frame.columns
            if column not in merged.columns or column in {"coc_id", "year"}
        ]
        merged = merged.merge(frame[extra_cols], on=["coc_id", "year"], how="outer")

    result = merged[["coc_id", "year"]].copy()
    for column in ("pit_total", "pit_sheltered", "pit_unsheltered"):
        result[column] = _first_available(merged, [column])
    result["coc_population"] = _first_available(
        merged,
        [
            "coc_population",
            "total_population",
            "total_population_acs5",
            "total_population_pep",
            "population",
        ],
    )
    return result


def _add_msa_coc_metadata(panel: pd.DataFrame, panel_spec: Any) -> pd.DataFrame:
    result = panel.copy()
    result["ranking_population_source"] = panel_spec.ranking_population_source
    result["ranking_reference_year"] = panel_spec.ranking_reference_year
    result["containment_min_share"] = panel_spec.containment_min_share
    result["containment_denominator"] = panel_spec.containment_denominator
    result["coc_boundary_vintage_used"] = panel_spec.coc_boundary_vintage
    result["msa_definition_version_used"] = panel_spec.msa_definition_version
    result["msa_population_source"] = panel_spec.msa_population_source
    result["unemployment_source"] = panel_spec.unemployment_source
    result["source"] = "msa_coc_panel"
    return result


def _apply_output_aliases(panel: pd.DataFrame, aliases: dict[str, str]) -> pd.DataFrame:
    result = panel.copy()
    for source, alias in aliases.items():
        if source not in result.columns:
            raise ExecutorError(
                f"MSA-CoC panel output_aliases references missing column '{source}'. "
                f"Available columns: {list(result.columns)}"
            )
        if alias != source:
            result[alias] = result[source]
    return result


def _order_columns(panel: pd.DataFrame) -> pd.DataFrame:
    required = list(MSA_COC_PANEL_COLUMNS)
    missing = [column for column in required if column not in panel.columns]
    if missing:
        raise ExecutorError(f"MSA-CoC panel is missing required columns: {missing}")
    extra = [column for column in panel.columns if column not in required]
    return panel[required + extra].sort_values(["msa_id", "coc_id", "year"]).reset_index(drop=True)


def _population_column(frame: pd.DataFrame, source_token: str) -> str:
    candidates = (
        ["population", "total_population", f"total_population_{source_token}"]
        if source_token == "pep"
        else ["total_population", "population", f"total_population_{source_token}"]
    )
    for column in candidates:
        if column in frame.columns:
            return column
    raise ExecutorError(
        f"MSA population source '{source_token}' is missing a population column. "
        f"Expected one of {candidates}; available columns: {list(frame.columns)}"
    )


def _first_available(
    frame: pd.DataFrame,
    candidates: list[str],
    *,
    fallback: pd.Series | None = None,
) -> pd.Series:
    for column in candidates:
        if column in frame.columns:
            values = pd.to_numeric(frame[column], errors="coerce").astype("Float64")
            if fallback is not None:
                values = values.fillna(fallback)
            return values
    if fallback is not None:
        return fallback
    return pd.Series(pd.NA, index=frame.index, dtype="Float64")


def _rate_from_columns(
    frame: pd.DataFrame,
    *,
    numerator: str,
    denominator: str,
) -> pd.Series:
    if numerator not in frame.columns or denominator not in frame.columns:
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    num = pd.to_numeric(frame[numerator], errors="coerce")
    den = pd.to_numeric(frame[denominator], errors="coerce")
    return (num / den.where(den > 0)).astype("Float64")


def _rate_from_rent_burden_columns(frame: pd.DataFrame) -> pd.Series:
    numerator_columns = [
        "gross_rent_pct_income_30_to_34_9",
        "gross_rent_pct_income_35_to_39_9",
        "gross_rent_pct_income_40_to_49_9",
        "gross_rent_pct_income_50_plus",
    ]
    denominator = "gross_rent_pct_income_total"
    if denominator not in frame.columns or not all(
        column in frame.columns for column in numerator_columns
    ):
        return pd.Series(pd.NA, index=frame.index, dtype="Float64")
    numerator = sum(
        pd.to_numeric(frame[column], errors="coerce").fillna(0) for column in numerator_columns
    )
    den = pd.to_numeric(frame[denominator], errors="coerce")
    return (numerator / den.where(den > 0)).astype("Float64")


def _source_token(provider: str, product: str) -> str:
    if provider == "census" and product in {"acs", "acs5"}:
        return "acs5"
    if provider == "census" and product == "pep":
        return "pep"
    if provider == "bls" and product == "laus":
        return "laus"
    if provider == "hud" and product == "pit":
        return "pit"
    return product


def _geo_column_for_type(geo_type: str) -> str:
    return {
        "coc": "coc_id",
        "msa": "msa_id",
        "metro": "msa_id",
    }.get(geo_type, "geo_id")


def _county_vintage_from_msa_definition_version(definition_version: str) -> int:
    matches = re.findall(r"(?:19|20)\d{2}", definition_version)
    if not matches:
        raise ExecutorError(
            "MSA-CoC panel requires an MSA definition version containing a county "
            "geometry year, for example 'census_msa_2023'."
        )
    return int(matches[-1])
