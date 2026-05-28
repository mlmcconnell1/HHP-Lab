"""Parquet, manifest, and diagnostics persistence for recipe execution.

Consumes a pre-assembled ``AssembledPanel`` from ``executor_panel`` and
writes the canonical outputs: the panel parquet with embedded
provenance metadata, the sidecar ``*.manifest.json`` file, and the
``*__diagnostics.json`` report.  Reads ``target.panel_policy`` only
through the conformance-flag helper in ``executor_panel_policies`` so
assembly and persistence share a single policy-read path.

This module is one leg of the executor panel/persistence split tracked
in coclab-anb0; the step-by-step extraction plan lives in
``background/executor_panel_split_design.md``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from hhplab.config import load_config
from hhplab.msa.coverage import build_msa_coc_coverage, save_msa_coc_coverage
from hhplab.naming import (
    acs5_tracts_filename,
    coc_base_path,
    county_path,
    msa_county_membership_path,
    tract_path,
)
from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.panel.panel_diagnostics import generate_diagnostics_report
from hhplab.panel.zori_eligibility import summarize_zori_eligibility
from hhplab.recipe.executor_containment import build_containment_list
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    StepResult,
    _classify_path,
    _echo,
)
from hhplab.recipe.executor_manifest import (
    _build_manifest,
    _build_provenance,
    _resolve_containment_output_file,
    _resolve_msa_coc_coverage_output_file,
    _resolve_panel_output_file,
    _resolve_pipeline_target,
)
from hhplab.recipe.executor_msa_coc_panel import (
    assemble_msa_coc_panel,
    build_msa_coc_containment_spec,
    _collect_frame_records,
    _population_column,
    _source_year_frame,
)
from hhplab.recipe.executor_panel import assemble_panel
from hhplab.recipe.executor_panel_policies import collect_conformance_flags
from hhplab.recipe.manifest import AssetRecord, write_manifest
from hhplab.recipe.planner import ExecutionPlan
from hhplab.recipe.recipe_schema import ContainmentSpec
from hhplab.recipe.schema_common import expand_year_spec


def persist_outputs(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Collect joined intermediates and write panel output.

    Concatenates all ``("__joined__", year)`` intermediates into a
    single DataFrame, writes it to the canonical panel path, and
    attaches provenance metadata.
    """
    try:
        _pipeline, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
    except ExecutorError as exc:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=str(exc),
        )
    if target.msa_coc_panel is not None:
        return persist_msa_coc_panel(plan, ctx)

    assembled = assemble_panel(plan, ctx, step_kind="persist")
    if isinstance(assembled, StepResult):
        return assembled

    panel = assembled.panel
    frames = assembled.frames
    target_geo_type = assembled.target_geo_type
    boundary_vintage = assembled.boundary_vintage
    definition_version = assembled.definition_version
    target_selector_summary: dict[str, object] | None = None
    containment_filter_summary: dict[str, object] | None = None

    universe_years = expand_year_spec(ctx.recipe.universe)
    start_year = min(universe_years)
    end_year = max(universe_years)

    try:
        output_file = _resolve_panel_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )
    except ExecutorError as exc:
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=str(exc),
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Detect output filename collision from a prior pipeline in this run.
    if output_file.exists() and output_file in getattr(ctx, "_written_outputs", set()):
        return StepResult(
            step_kind="persist",
            detail="persist outputs",
            success=False,
            error=(
                f"Output collision: pipeline '{plan.pipeline_id}' resolves to "
                f"'{output_file}' which was "
                f"already written by another pipeline in this recipe. "
                f"Namespace targets or use distinct geometry vintages."
            ),
        )

    # Run conformance checks on the assembled panel.  All panel-policy
    # reads are centralised in ``collect_conformance_flags`` so assembly
    # and persistence share a single policy-read path.
    _, persist_target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
    if persist_target.selector_ids is not None:
        try:
            panel, target_selector_summary = _apply_target_panel_selector(
                panel,
                persist_target.selector_ids,
                target_geo_type=target_geo_type,
                ctx=ctx,
            )
        except ValueError as exc:
            return StepResult(
                step_kind="persist",
                detail="persist outputs",
                success=False,
                error=str(exc),
            )

    if persist_target.containment_filter is not None:
        try:
            panel, containment_filter_summary = _apply_containment_panel_filter(
                panel,
                persist_target.containment_filter,
                ctx,
            )
        except (ExecutorError, FileNotFoundError, ValueError) as exc:
            return StepResult(
                step_kind="persist",
                detail="persist outputs",
                success=False,
                error=str(exc),
            )

    conformance_flags = collect_conformance_flags(
        recipe=ctx.recipe,
        target=persist_target,
        panel=panel,
    )

    panel_request = PanelRequest(
        start_year=start_year,
        end_year=end_year,
        geo_type=target_geo_type,
        measure_columns=conformance_flags.measure_columns,
        acs_products=list(conformance_flags.acs_products),
        include_zori=conformance_flags.include_zori,
        include_laus=conformance_flags.include_laus,
        enforce_schema_contract=True,
    )
    conformance_report = run_conformance(panel, panel_request)
    if not ctx.quiet:
        print(conformance_report.summary(), file=sys.stderr)

    # Build provenance and write with metadata
    try:
        output_rel = str(output_file.relative_to(ctx.project_root))
    except ValueError:
        output_rel = str(output_file)
    provenance = _build_provenance(ctx.recipe, plan.pipeline_id, ctx)
    provenance["target_geometry"] = {
        "type": target_geo_type,
        **(
            {"vintage": boundary_vintage}
            if target_geo_type == "coc" and boundary_vintage is not None
            else {}
        ),
        **(
            {"source": definition_version}
            if target_geo_type in {"metro", "msa"} and definition_version is not None
            else {}
        ),
    }
    provenance["conformance"] = conformance_report.to_dict()
    if target_selector_summary is not None:
        provenance["target_selector"] = target_selector_summary
    if containment_filter_summary is not None:
        provenance["containment_filter"] = containment_filter_summary

    # Embed ZORI provenance and summary (coclab-gude.2).
    if assembled.zori_provenance is not None:
        provenance["zori"] = assembled.zori_provenance.to_dict()
        zori_summary = summarize_zori_eligibility(panel)
        if zori_summary.get("zori_integrated"):
            provenance["zori_summary"] = zori_summary

    table = pa.Table.from_pandas(panel)
    metadata = table.schema.metadata or {}
    metadata[b"hhplab_provenance"] = json.dumps(provenance).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, output_file)

    # Track written outputs for collision detection across pipelines.
    if not hasattr(ctx, "_written_outputs"):
        ctx._written_outputs = set()  # type: ignore[attr-defined]
    ctx._written_outputs.add(output_file)  # type: ignore[attr-defined]

    # Write manifest sidecar JSON
    manifest = _build_manifest(
        ctx.recipe,
        plan.pipeline_id,
        ctx,
        output_path=output_rel,
    )
    manifest_file = output_file.with_suffix(".manifest.json")
    write_manifest(manifest, manifest_file)

    detail = f"persist panel: {len(frames)} year(s), {len(panel)} rows → {output_rel}"
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist", detail=detail, success=True)


def persist_msa_coc_panel(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Build and persist an MSA-CoC containment panel parquet output."""
    try:
        _pipeline, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
        if target.msa_coc_panel is None:
            raise ExecutorError(f"Target '{target.id}' does not declare msa_coc_panel.")
        containment_spec = build_msa_coc_containment_spec(target.msa_coc_panel)
        coc_gdf, county_gdf, msa_county_membership = _load_containment_inputs(
            containment_spec,
            ctx,
        )
        if msa_county_membership is None:
            raise ExecutorError("MSA-CoC panel requires MSA county membership.")
        assembled = assemble_msa_coc_panel(
            plan,
            ctx,
            target=target,
            coc_gdf=coc_gdf,
            county_gdf=county_gdf,
            msa_county_membership=msa_county_membership,
        )
        output_file = _resolve_panel_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )
    except (ExecutorError, FileNotFoundError, ValueError) as exc:
        return StepResult(
            step_kind="persist",
            detail="persist msa-coc panel",
            success=False,
            error=str(exc),
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and output_file in getattr(ctx, "_written_outputs", set()):
        return StepResult(
            step_kind="persist",
            detail="persist msa-coc panel",
            success=False,
            error=(
                f"Output collision: pipeline '{plan.pipeline_id}' resolves to "
                f"'{output_file}' which was already written by another pipeline "
                "in this recipe."
            ),
        )

    try:
        output_rel = str(output_file.relative_to(ctx.project_root))
    except ValueError:
        output_rel = str(output_file)

    provenance = _build_provenance(ctx.recipe, plan.pipeline_id, ctx)
    provenance["target_geometry"] = target.geometry.model_dump(mode="json")
    provenance.update(assembled.provenance)

    table = pa.Table.from_pandas(assembled.panel)
    metadata = table.schema.metadata or {}
    metadata[b"hhplab_provenance"] = json.dumps(provenance).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, output_file)

    if not hasattr(ctx, "_written_outputs"):
        ctx._written_outputs = set()  # type: ignore[attr-defined]
    ctx._written_outputs.add(output_file)  # type: ignore[attr-defined]

    manifest = _build_manifest(
        ctx.recipe,
        plan.pipeline_id,
        ctx,
        output_path=output_rel,
    )
    manifest_file = output_file.with_suffix(".manifest.json")
    write_manifest(manifest, manifest_file)

    detail = f"persist msa-coc panel: {len(assembled.panel)} rows -> {output_rel}"
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist", detail=detail, success=True)


def persist_diagnostics(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Generate and persist diagnostics for the assembled panel.

    Runs the panel diagnostics report and writes a JSON sidecar file
    alongside the panel output.  The diagnostics file uses the same
    base name as the panel with a ``__diagnostics.json`` suffix.
    """
    assembled = assemble_panel(plan, ctx, step_kind="persist_diagnostics")
    if isinstance(assembled, StepResult):
        return assembled

    panel = assembled.panel

    try:
        panel_file = _resolve_panel_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )
    except ExecutorError as exc:
        return StepResult(
            step_kind="persist_diagnostics",
            detail="persist_diagnostics",
            success=False,
            error=str(exc),
        )
    diagnostics_file = panel_file.with_name(
        f"{panel_file.stem}__diagnostics.json",
    )
    diagnostics_file.parent.mkdir(parents=True, exist_ok=True)

    # Generate diagnostics
    report = generate_diagnostics_report(panel)

    # Write as JSON
    diagnostics_dict = report.to_dict()
    diagnostics_file.write_text(json.dumps(diagnostics_dict, indent=2, default=str) + "\n")

    try:
        diag_display = str(diagnostics_file.relative_to(ctx.project_root))
    except ValueError:
        diag_display = str(diagnostics_file)
    detail = f"persist diagnostics: {diag_display}"
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist_diagnostics", detail=detail, success=True)


def persist_containment(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Build and persist a containment-list parquet output."""
    try:
        _pipeline, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
        if target.containment_spec is None:
            raise ExecutorError(
                f"Target '{target.id}' declares containment output without containment_spec."
            )
        spec = target.containment_spec
        output_file = _resolve_containment_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )
        coc_gdf, county_gdf, msa_county_membership = _load_containment_inputs(spec, ctx)
        containment = build_containment_list(
            spec,
            coc_gdf=coc_gdf,
            county_gdf=county_gdf,
            msa_county_membership=msa_county_membership,
        )
    except (ExecutorError, FileNotFoundError, ValueError) as exc:
        return StepResult(
            step_kind="persist_containment",
            detail="persist containment",
            success=False,
            error=str(exc),
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and output_file in getattr(ctx, "_written_outputs", set()):
        return StepResult(
            step_kind="persist_containment",
            detail="persist containment",
            success=False,
            error=(
                f"Output collision: pipeline '{plan.pipeline_id}' resolves to "
                f"'{output_file}' which was already written by another pipeline "
                "in this recipe."
            ),
        )

    try:
        output_rel = str(output_file.relative_to(ctx.project_root))
    except ValueError:
        output_rel = str(output_file)

    provenance = _build_provenance(ctx.recipe, plan.pipeline_id, ctx)
    provenance["target_geometry"] = target.geometry.model_dump(mode="json")
    provenance["containment_spec"] = spec.model_dump(mode="json")
    provenance["containment"] = {
        "row_count": len(containment),
        "min_share": spec.min_share,
        "denominator": spec.denominator,
        "method": spec.method,
    }

    table = pa.Table.from_pandas(containment)
    metadata = table.schema.metadata or {}
    metadata[b"hhplab_provenance"] = json.dumps(provenance).encode()
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, output_file)

    if not hasattr(ctx, "_written_outputs"):
        ctx._written_outputs = set()  # type: ignore[attr-defined]
    ctx._written_outputs.add(output_file)  # type: ignore[attr-defined]

    manifest = _build_manifest(
        ctx.recipe,
        plan.pipeline_id,
        ctx,
        output_path=output_rel,
    )
    manifest_file = output_file.with_suffix(".manifest.json")
    write_manifest(manifest, manifest_file)

    detail = f"persist containment: {len(containment)} rows -> {output_rel}"
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist_containment", detail=detail, success=True)


def persist_msa_coc_coverage(
    plan: ExecutionPlan,
    ctx: ExecutionContext,
) -> StepResult:
    """Build and persist a recipe-native MSA-CoC coverage artifact."""
    try:
        _pipeline, target = _resolve_pipeline_target(ctx.recipe, plan.pipeline_id)
        if target.msa_coc_coverage is None:
            raise ExecutorError(
                f"Target '{target.id}' declares msa_coc_coverage output without "
                "msa_coc_coverage."
            )
        spec = target.msa_coc_coverage
        cfg = ctx.storage_config or load_config(project_root=ctx.project_root)
        data_root = cfg.asset_store_root

        output_file = _resolve_msa_coc_coverage_output_file(
            ctx.recipe,
            plan.pipeline_id,
            ctx.project_root,
            storage_config=ctx.storage_config,
        )

        coc_file = coc_base_path(str(spec.coc_boundary_vintage), data_root)
        county_file = county_path(spec.county_vintage, data_root)
        membership_file = msa_county_membership_path(spec.msa_definition_version, data_root)
        input_artifacts: dict[str, str] = {
            "coc_boundaries": _coverage_input_path(coc_file, ctx),
            "county_geometry": _coverage_input_path(county_file, ctx),
            "msa_county_membership": _coverage_input_path(membership_file, ctx),
        }
        for artifact in (coc_file, county_file, membership_file):
            _record_containment_asset(ctx, artifact)

        records = _collect_frame_records(plan, ctx)
        ranking_frame = _source_year_frame(
            records,
            source_token=spec.ranking_population_source,
            geo_type="msa",
            year=spec.ranking_reference_year,
            purpose="MSA coverage ranking population",
        )
        ranking_column = _population_column(ranking_frame, spec.ranking_population_source)

        acs5_population_df: pd.DataFrame | None = None
        tract_gdf: gpd.GeoDataFrame | None = None
        if "population" in spec.overlap_bases:
            if spec.acs5_population_vintage is None or spec.tract_vintage is None:
                raise ValueError(
                    "MSA-CoC coverage population overlap requires "
                    "acs5_population_vintage and tract_vintage."
                )
            tract_file = tract_path(spec.tract_vintage, data_root)
            acs_file = (
                data_root
                / "curated"
                / "acs"
                / acs5_tracts_filename(str(spec.acs5_population_vintage), spec.tract_vintage)
            )
            _record_containment_asset(ctx, tract_file)
            _record_containment_asset(ctx, acs_file)
            input_artifacts["tract_geometry"] = _coverage_input_path(tract_file, ctx)
            input_artifacts["acs5_population"] = _coverage_input_path(acs_file, ctx)
            tract_gdf = _read_geoparquet(tract_file, "tract geometry")
            acs5_population_df = _read_parquet(acs_file, "ACS5 tract population")

        coverage = build_msa_coc_coverage(
            _read_geoparquet(coc_file, "CoC boundary geometry"),
            _read_geoparquet(county_file, "county geometry"),
            _read_parquet(membership_file, "MSA county membership"),
            ranking_frame,
            year=spec.year,
            top_n=spec.top_n,
            ranking_population_source=spec.ranking_population_source,
            ranking_reference_year=spec.ranking_reference_year,
            boundary_vintage=str(spec.coc_boundary_vintage),
            county_vintage=str(spec.county_vintage),
            definition_version=spec.msa_definition_version,
            overlap_bases=tuple(spec.overlap_bases),
            acs5_population_df=acs5_population_df,
            tract_gdf=tract_gdf,
            acs5_population_vintage=spec.acs5_population_vintage,
            ranking_population_column=ranking_column,
            min_msa_area_coverage_share=spec.min_msa_area_coverage_share,
            min_msa_population_coverage_share=spec.min_msa_population_coverage_share,
        )
    except (ExecutorError, FileNotFoundError, ValueError, KeyError) as exc:
        return StepResult(
            step_kind="persist_msa_coc_coverage",
            detail="persist MSA-CoC coverage",
            success=False,
            error=str(exc),
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if output_file.exists() and output_file in getattr(ctx, "_written_outputs", set()):
        return StepResult(
            step_kind="persist_msa_coc_coverage",
            detail="persist MSA-CoC coverage",
            success=False,
            error=(
                f"Output collision: pipeline '{plan.pipeline_id}' resolves to "
                f"'{output_file}' which was already written by another pipeline "
                "in this recipe."
            ),
        )

    try:
        output_rel = str(output_file.relative_to(ctx.project_root))
    except ValueError:
        output_rel = str(output_file)

    save_msa_coc_coverage(
        coverage,
        output_file,
        year=spec.year,
        boundary_vintage=str(spec.coc_boundary_vintage),
        county_vintage=str(spec.county_vintage),
        definition_version=spec.msa_definition_version,
        overlap_bases=tuple(spec.overlap_bases),
        ranking_population_source=spec.ranking_population_source,
        ranking_reference_year=spec.ranking_reference_year,
        top_n=spec.top_n,
        acs5_population_vintage=spec.acs5_population_vintage,
        input_artifacts=input_artifacts,
    )
    if spec.csv_sidecar:
        coverage.to_csv(output_file.with_suffix(".csv"), index=False)

    if not hasattr(ctx, "_written_outputs"):
        ctx._written_outputs = set()  # type: ignore[attr-defined]
    ctx._written_outputs.add(output_file)  # type: ignore[attr-defined]

    manifest = _build_manifest(
        ctx.recipe,
        plan.pipeline_id,
        ctx,
        output_path=output_rel,
    )
    write_manifest(manifest, output_file.with_suffix(".manifest.json"))

    detail = f"persist MSA-CoC coverage: {len(coverage)} rows -> {output_rel}"
    _echo(ctx, f"  [persist] {detail}")
    return StepResult(step_kind="persist_msa_coc_coverage", detail=detail, success=True)


def _coverage_input_path(path: Path, ctx: ExecutionContext) -> str:
    _root, rel = _classify_path(path, ctx)
    return rel


def _apply_target_panel_selector(
    panel: pd.DataFrame,
    selector_ids: list[str],
    *,
    target_geo_type: str,
    ctx: ExecutionContext,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Filter a panel to explicit target geography IDs."""
    selector_set = set(str(selector_id) for selector_id in selector_ids)
    candidate_col = _panel_candidate_column(panel, target_geo_type)
    available_ids = set(panel[candidate_col].dropna().astype(str))
    missing = sorted(selector_set - available_ids)
    if missing:
        preview = ", ".join(missing[:5])
        suffix = ", ..." if len(missing) > 5 else ""
        raise ValueError(
            "Target selector_ids did not match panel geography IDs: "
            f"{preview}{suffix}. Check target.selector_ids, upstream datasets, "
            "or containment filters."
        )

    before_rows = len(panel)
    before_geographies = panel[candidate_col].nunique(dropna=True)
    filtered = panel[panel[candidate_col].astype(str).isin(selector_set)].reset_index(drop=True)
    after_geographies = filtered[candidate_col].nunique(dropna=True)
    summary = {
        "selector_ids": list(selector_ids),
        "selected_count": int(after_geographies),
        "panel_rows_before": before_rows,
        "panel_rows_after": len(filtered),
        "panel_geographies_before": int(before_geographies),
        "panel_geographies_after": int(after_geographies),
    }
    _echo(
        ctx,
        "  [target_selector] "
        f"{before_geographies} -> {after_geographies} geographies, "
        f"{before_rows} -> {len(filtered)} panel rows",
    )
    return filtered, summary


def _apply_containment_panel_filter(
    panel: pd.DataFrame,
    spec: ContainmentSpec,
    ctx: ExecutionContext,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Filter a panel to candidate geographies selected by a containment list."""
    coc_gdf, county_gdf, msa_county_membership = _load_containment_inputs(spec, ctx)
    containment = build_containment_list(
        spec,
        coc_gdf=coc_gdf,
        county_gdf=county_gdf,
        msa_county_membership=msa_county_membership,
    )
    candidate_ids = set(containment["candidate_id"].astype(str))
    candidate_col = _panel_candidate_column(panel, spec.candidate.type)
    before_rows = len(panel)
    before_geographies = panel[candidate_col].nunique(dropna=True)
    filtered = panel[panel[candidate_col].astype(str).isin(candidate_ids)].reset_index(drop=True)
    after_geographies = filtered[candidate_col].nunique(dropna=True)
    summary = {
        "spec": spec.model_dump(mode="json"),
        "containment_row_count": len(containment),
        "candidate_count": len(candidate_ids),
        "panel_rows_before": before_rows,
        "panel_rows_after": len(filtered),
        "panel_geographies_before": int(before_geographies),
        "panel_geographies_after": int(after_geographies),
    }
    _echo(
        ctx,
        "  [containment_filter] "
        f"{before_geographies} -> {after_geographies} geographies, "
        f"{before_rows} -> {len(filtered)} panel rows",
    )
    return filtered, summary


def _panel_candidate_column(panel: pd.DataFrame, candidate_type: str) -> str:
    preferred = {
        "coc": ("geo_id", "coc_id"),
        "county": ("geo_id", "county_fips", "GEOID", "geoid"),
        "msa": ("geo_id", "msa_id", "cbsa_code"),
        "metro": ("geo_id", "metro_id"),
    }
    for column in preferred.get(candidate_type, ("geo_id",)):
        if column in panel.columns:
            return column
    raise ValueError(
        "Containment filter cannot be applied because the panel does not "
        f"contain a candidate ID column for geometry type '{candidate_type}'."
    )


def _load_containment_inputs(
    spec: ContainmentSpec,
    ctx: ExecutionContext,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, pd.DataFrame | None]:
    cfg = ctx.storage_config or load_config(project_root=ctx.project_root)
    data_root = cfg.asset_store_root
    pair = (spec.container.type, spec.candidate.type)

    if pair == ("msa", "coc"):
        coc_file = coc_base_path(_required_vintage(spec.candidate, "candidate CoC"), data_root)
        county_file = county_path(_required_vintage(spec.container, "container MSA"), data_root)
        definition_version = spec.definition_version or spec.container.source
        if definition_version is None:
            raise ValueError(
                "MSA containment output requires containment_spec.definition_version "
                "or container.source."
            )
        membership_file = msa_county_membership_path(definition_version, data_root)
        _record_containment_asset(ctx, coc_file)
        _record_containment_asset(ctx, county_file)
        _record_containment_asset(ctx, membership_file)
        return (
            _read_geoparquet(coc_file, "CoC boundary geometry"),
            _read_geoparquet(county_file, "county geometry"),
            _read_parquet(membership_file, "MSA county membership"),
        )

    if pair == ("coc", "county"):
        coc_file = coc_base_path(_required_vintage(spec.container, "container CoC"), data_root)
        county_file = county_path(_required_vintage(spec.candidate, "candidate county"), data_root)
        _record_containment_asset(ctx, coc_file)
        _record_containment_asset(ctx, county_file)
        return (
            _read_geoparquet(coc_file, "CoC boundary geometry"),
            _read_geoparquet(county_file, "county geometry"),
            None,
        )

    raise ValueError(
        "Unsupported containment geometry pair "
        f"'{spec.container.type} -> {spec.candidate.type}'. "
        "Supported pairs: msa -> coc, coc -> county."
    )


def _record_containment_asset(ctx: ExecutionContext, path: Path) -> None:
    root, rel = _classify_path(path, ctx)
    ctx.consumed_assets.append(
        AssetRecord(
            role="geometry",
            path=rel,
            sha256="",
            size=path.stat().st_size if path.exists() else 0,
            root=root,
        )
    )


def _read_geoparquet(path: Path, label: str) -> gpd.GeoDataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} for containment output: {path}")
    return gpd.read_parquet(path)


def _read_parquet(path: Path, label: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {label} for containment output: {path}")
    return pd.read_parquet(path)


def _required_vintage(ref: object, label: str) -> int:
    vintage = ref.vintage  # type: ignore[attr-defined]
    if vintage is None:
        raise ValueError(f"Missing {label} vintage for containment output.")
    return vintage
