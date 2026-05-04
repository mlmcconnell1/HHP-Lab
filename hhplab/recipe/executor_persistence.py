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

import pyarrow as pa
import pyarrow.parquet as pq

from hhplab.panel.conformance import PanelRequest, run_conformance
from hhplab.panel.panel_diagnostics import generate_diagnostics_report
from hhplab.panel.zori_eligibility import summarize_zori_eligibility
from hhplab.recipe.executor_core import (
    ExecutionContext,
    ExecutorError,
    StepResult,
    _echo,
)
from hhplab.recipe.executor_manifest import (
    _build_manifest,
    _build_provenance,
    _resolve_panel_output_file,
    _resolve_pipeline_target,
)
from hhplab.recipe.executor_panel import assemble_panel
from hhplab.recipe.executor_panel_policies import collect_conformance_flags
from hhplab.recipe.manifest import write_manifest
from hhplab.recipe.planner import ExecutionPlan
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
