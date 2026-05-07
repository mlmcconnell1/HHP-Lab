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
from hhplab.naming import coc_base_path, county_path, msa_county_membership_path
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
    _resolve_panel_output_file,
    _resolve_pipeline_target,
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
    assembled = assemble_panel(plan, ctx, step_kind="persist")
    if isinstance(assembled, StepResult):
        return assembled

    panel = assembled.panel
    frames = assembled.frames
    target_geo_type = assembled.target_geo_type
    boundary_vintage = assembled.boundary_vintage
    definition_version = assembled.definition_version

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
