"""CLI commands for recipe-driven builds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from coclab.recipe.adapters import (
    ValidationDiagnostic,
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from coclab.recipe.cache import RecipeCache
from coclab.recipe.default_adapters import register_defaults
from coclab.recipe.executor import ExecutorError, execute_recipe
from coclab.recipe.loader import RecipeLoadError, load_recipe
from coclab.recipe.manifest import export_bundle as do_export_bundle
from coclab.recipe.manifest import read_manifest
from coclab.recipe.planner import PlannerError, resolve_plan
from coclab.recipe.preflight import PreflightReport, Severity, run_preflight
from coclab.recipe.recipe_schema import RecipeV1, expand_year_spec

# Common --json flag definition
_JSON_OPTION = Annotated[
    bool,
    typer.Option(
        "--json",
        help="Output machine-readable JSON instead of human text.",
    ),
]


def _json_out(data: dict) -> None:
    """Print a JSON response and exit."""
    typer.echo(json.dumps(data, indent=2))


def _json_error(message: str, *, code: int = 1) -> None:
    """Print a JSON error response and raise typer.Exit."""
    _json_out({"status": "error", "error": message})
    raise typer.Exit(code=code)


def _format_geometry(ref: object) -> str:
    """Render a GeometryRef-like object for human CLI output."""
    geo_type = getattr(ref, "type")
    vintage = getattr(ref, "vintage", None)
    source = getattr(ref, "source", None)
    if vintage is not None and source:
        return f"{geo_type}@{vintage}[{source}]"
    if vintage is not None:
        return f"{geo_type}@{vintage}"
    if source:
        return f"{geo_type}@{source}"
    return str(geo_type)


def _missing_file_level(
    ds_id: str,
    optional: bool,
    policy_default: str,
    policy_extra: dict[str, str],
) -> str:
    """Determine diagnostic level for a missing dataset file.

    Priority: per-dataset policy override > optional flag > policy default.
    Returns ``"error"`` or ``"warning"``.
    """
    if ds_id in policy_extra:
        return "warning" if policy_extra[ds_id] == "warn" else "error"
    if optional:
        return "warning"
    return "warning" if policy_default == "warn" else "error"


def _check_dataset_paths(
    parsed: RecipeV1, project_root: Path | None = None,
) -> list[ValidationDiagnostic]:
    """Check that all referenced dataset files exist on disk.

    Returns diagnostics for missing files.  Level (error/warning) is
    determined by the dataset's ``optional`` flag and the recipe's
    ``missing_dataset`` validation policy.

    Dataset paths in recipes are project-relative, so *project_root*
    (defaulting to ``Path.cwd()``) is used as the base for resolution.
    """
    if project_root is None:
        project_root = Path.cwd()

    results: list[ValidationDiagnostic] = []
    policy = parsed.validation.missing_dataset
    policy_extra: dict[str, str] = policy.model_extra or {}

    for ds_id, ds in parsed.datasets.items():
        level = _missing_file_level(
            ds_id, ds.optional, policy.default, policy_extra,
        )

        # Static path
        if ds.path is not None:
            resolved = project_root / ds.path
            if not resolved.exists():
                results.append(ValidationDiagnostic(
                    level=level,
                    message=f"Dataset '{ds_id}' path not found: {ds.path}",
                ))

        # File set: check template-expanded paths and overrides
        if ds.file_set is not None:
            for seg in ds.file_set.segments:
                seg_years = expand_year_spec(seg.years)
                for year in seg_years:
                    if year in seg.overrides:
                        p = seg.overrides[year]
                    else:
                        render_ctx: dict[str, object] = {"year": year}
                        render_ctx.update(seg.constants)
                        render_ctx.update(
                            {k: year + offset
                             for k, offset in seg.year_offsets.items()}
                        )
                        try:
                            p = ds.file_set.path_template.format(
                                **render_ctx,
                            )
                        except KeyError as exc:
                            results.append(ValidationDiagnostic(
                                level=level,
                                message=(
                                    f"Dataset '{ds_id}' file_set template "
                                    f"variable '{exc.args[0]}' not provided "
                                    f"for year {year}."
                                ),
                            ))
                            continue
                    resolved = project_root / p
                    if not resolved.exists():
                        results.append(ValidationDiagnostic(
                            level=level,
                            message=(
                                f"Dataset '{ds_id}' year {year} "
                                f"file not found: {p}"
                            ),
                        ))

    return results


def _validate_recipe(
    parsed: RecipeV1,
    *,
    use_json: bool = False,
) -> tuple[list[str], list[str]]:
    """Run structural validation and return (warnings, errors) as string lists.

    Runs adapter validation only.  Dataset path checks are deferred to
    the plan-scoped preflight analyzer so that missing-dataset failures
    are reported consistently through the shared preflight output
    (``status=blocked``) rather than the legacy ``validation.errors``
    path.

    When *use_json* is True, validation output is suppressed (caller
    will include it in the JSON response).
    """
    # Run adapter registry validation
    diagnostics = validate_recipe_adapters(
        parsed, geometry_registry, dataset_registry,
    )
    adapter_errors = [d for d in diagnostics if d.level == "error"]
    adapter_warnings = [d for d in diagnostics if d.level == "warning"]

    if not use_json:
        for w in adapter_warnings:
            typer.echo(f"  Warning: {w.message}", err=True)

    all_warnings = [d.message for d in adapter_warnings]
    all_errors = [d.message for d in adapter_errors]
    return all_warnings, all_errors


def recipe_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Validate and preflight only; do not execute the build.",
        ),
    ] = False,
    no_cache: Annotated[
        bool,
        typer.Option(
            "--no-cache",
            help="Disable asset caching (re-read every file from disk).",
        ),
    ] = False,
    skip_preflight: Annotated[
        bool,
        typer.Option(
            "--skip-preflight",
            help="Skip the preflight readiness check before execution.",
        ),
    ] = False,
    use_json: _JSON_OPTION = False,
) -> None:
    """Load, validate, preflight, and execute a build recipe.

    This is the normal entrypoint for recipe execution. It loads the
    recipe, runs validation, runs the readiness preflight, and then
    executes the pipelines when all prerequisites are satisfied.

    Use ``coclab build recipe-preflight`` when you want the readiness
    report without executing. Use ``coclab build recipe-plan`` when you
    need to inspect the resolved task graph while authoring or debugging
    a recipe. Use ``--dry-run`` to run the same validation/preflight
    path without execution.

    Use ``--skip-preflight`` only when you need to bypass the check
    for debugging purposes.

    Examples:

        # Normal human workflow
        coclab build recipe --recipe my_build.yaml

        # Automation / CI
        coclab build recipe-preflight --recipe my_build.yaml --json
        coclab build recipe --recipe my_build.yaml --json

        # Inspect resolved tasks while authoring/debugging
        coclab build recipe-plan --recipe my_build.yaml --json
    """
    # 0. Ensure built-in adapters are registered
    register_defaults()

    # 1. Load and structurally validate the recipe
    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    if not use_json:
        typer.echo(
            f"Loaded recipe: {parsed.name} (version {parsed.version})",
        )
        if not parsed.transforms:
            typer.echo(
                "  Warning: No transforms defined; "
                "no build output will be produced.",
                err=True,
            )
        if not parsed.pipelines:
            typer.echo(
                "  Warning: No pipelines defined; "
                "no build output will be produced.",
                err=True,
            )

    # 2. Validate (legacy path checks + adapter validation)
    all_warnings, all_errors = _validate_recipe(parsed, use_json=use_json)

    if all_errors:
        if use_json:
            _json_out({
                "status": "error",
                "recipe_name": parsed.name,
                "validation": {
                    "warnings": all_warnings,
                    "errors": all_errors,
                },
            })
            raise typer.Exit(code=1)
        for e in all_errors:
            typer.echo(f"  Error: {e}", err=True)
        typer.echo(
            f"\nRecipe validation failed with {len(all_errors)} error(s).",
            err=True,
        )
        raise typer.Exit(code=1)

    if not use_json:
        if all_warnings:
            typer.echo(
                f"Recipe validated with {len(all_warnings)} warning(s).",
            )
        else:
            typer.echo("Recipe validated successfully.")

    # 2b. Preflight readiness check
    if not skip_preflight:
        pf_report = run_preflight(parsed)
        if not use_json:
            if pf_report.is_ready:
                typer.echo(
                    f"Preflight: {len(pf_report.findings)} finding(s), "
                    "all clear."
                )
                # Show warnings so users know about non-blocking issues
                for f in pf_report.findings:
                    if f.severity == Severity.WARNING:
                        typer.echo(
                            f"  Warning: {f.message}", err=True,
                        )
            else:
                typer.echo(
                    f"\nPreflight found {pf_report.blocking_count} "
                    "blocker(s):",
                    err=True,
                )
                for f in pf_report.blocking_findings():
                    typer.echo(f"  {f.message}", err=True)
                    if f.remediation:
                        typer.echo(
                            f"    Fix: {f.remediation.hint}", err=True,
                        )
                        if f.remediation.command:
                            typer.echo(
                                f"    Run: {f.remediation.command}",
                                err=True,
                            )
                typer.echo(
                    "\nRun 'coclab build recipe-preflight --recipe "
                    f"{recipe}' for details.",
                    err=True,
                )

        if not pf_report.is_ready:
            if use_json:
                _json_out({
                    "status": "blocked",
                    "recipe_name": parsed.name,
                    "preflight": pf_report.to_dict(),
                })
            raise typer.Exit(code=1)

    if dry_run:
        if use_json:
            _json_out({
                "status": "ok",
                "recipe_name": parsed.name,
                "recipe_version": parsed.version,
                "validation": {
                    "warnings": all_warnings,
                    "errors": [],
                },
                "dry_run": True,
            })
        return

    # 3. Execute the build pipeline
    cache = RecipeCache(enabled=not no_cache)
    try:
        results = execute_recipe(
            parsed, cache=cache, quiet=use_json,
        )
    except ExecutorError as exc:
        if use_json:
            _json_error(str(exc))
        typer.echo(f"\nExecution error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if use_json:
        _json_out({
            "status": "ok",
            "recipe_name": parsed.name,
            "recipe_version": parsed.version,
            "validation": {
                "warnings": all_warnings,
                "errors": [],
            },
            "pipelines": [
                {
                    "pipeline_id": r.pipeline_id,
                    "success": r.success,
                    "steps": [
                        {
                            "step_kind": s.step_kind,
                            "detail": s.detail,
                            "success": s.success,
                            "error": s.error,
                        }
                        for s in r.steps
                    ],
                }
                for r in results
            ],
        })
        return

    total_steps = sum(len(r.steps) for r in results)
    typer.echo(
        f"\nRecipe '{parsed.name}' executed: "
        f"{len(results)} pipeline(s), {total_steps} steps completed."
    )


def recipe_plan_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    use_json: _JSON_OPTION = False,
) -> None:
    """Resolve the execution plan without executing.

    Shows all resolved tasks (materialize, resample, join), input
    paths, effective geometries, transform selections, and task
    counts. Useful while authoring or debugging a recipe.

    This command does not perform the full readiness checks used by
    ``recipe-preflight``. For a no-execute readiness gate, use
    ``coclab build recipe-preflight`` instead.

    Examples:

        coclab build recipe-plan --recipe my_build.yaml

        coclab build recipe-plan --recipe my_build.yaml --json
    """
    register_defaults()

    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    all_warnings, all_errors = _validate_recipe(parsed, use_json=use_json)

    if all_errors:
        if use_json:
            _json_out({
                "status": "error",
                "recipe_name": parsed.name,
                "validation": {
                    "warnings": all_warnings,
                    "errors": all_errors,
                },
            })
            raise typer.Exit(code=1)
        for e in all_errors:
            typer.echo(f"  Error: {e}", err=True)
        raise typer.Exit(code=1)

    plans = []
    for pipeline in parsed.pipelines:
        try:
            plan = resolve_plan(parsed, pipeline.id)
        except PlannerError as exc:
            if use_json:
                _json_error(
                    f"Pipeline '{pipeline.id}': {exc}",
                )
            typer.echo(f"Error: Pipeline '{pipeline.id}': {exc}", err=True)
            raise typer.Exit(code=1) from exc
        plans.append(plan)

    if use_json:
        _json_out({
            "status": "ok",
            "recipe_name": parsed.name,
            "recipe_version": parsed.version,
            "validation": {
                "warnings": all_warnings,
                "errors": [],
            },
            "pipelines": [p.to_dict() for p in plans],
        })
        return

    # Human-readable output
    typer.echo(f"Recipe: {parsed.name} (version {parsed.version})")
    if all_warnings:
        typer.echo(f"  {len(all_warnings)} validation warning(s)")

    for plan in plans:
        total = (
            len(plan.materialize_tasks)
            + len(plan.resample_tasks)
            + len(plan.join_tasks)
        )
        typer.echo(f"\nPipeline '{plan.pipeline_id}' ({total} tasks):")

        for mt in plan.materialize_tasks:
            typer.echo(
                f"  [materialize] transforms: {mt.transform_ids}",
            )

        for rt in plan.resample_tasks:
            geo = rt.effective_geometry
            geo_str = _format_geometry(geo)
            to_geo_str = _format_geometry(rt.to_geometry)
            line = (
                f"  [resample] {rt.dataset_id} year={rt.year} "
                f"method={rt.method} geometry={geo_str} "
                f"to={to_geo_str}"
            )
            if rt.transform_id:
                line += f" via={rt.transform_id}"
            if rt.input_path:
                line += f" path={rt.input_path}"
            typer.echo(line)

        for jt in plan.join_tasks:
            typer.echo(
                f"  [join] datasets={jt.datasets} "
                f"year={jt.year} on={jt.join_on}",
            )


def recipe_provenance_cmd(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            "-m",
            help="Path to a .manifest.json file produced by a recipe build.",
        ),
    ],
    use_json: _JSON_OPTION = False,
) -> None:
    """Show provenance from a recipe build manifest.

    Displays the recipe identity, consumed assets (with SHA-256 hashes
    and sizes), and output path recorded during the build.

    Examples:

        coclab build recipe-provenance \\
            --manifest panel__Y2020-2021@B2025.manifest.json
    """
    if not manifest.exists():
        if use_json:
            _json_error(f"Manifest not found: {manifest}")
        typer.echo(f"Error: Manifest not found: {manifest}", err=True)
        raise typer.Exit(code=1)

    m = read_manifest(manifest)

    if use_json:
        _json_out({"status": "ok", **m.to_dict()})
        return

    typer.echo(f"Recipe: {m.recipe_name} (v{m.recipe_version})")
    typer.echo(f"Pipeline: {m.pipeline_id}")
    typer.echo(f"Executed: {m.executed_at}")
    if m.output_path:
        typer.echo(f"Output: {m.output_path}")

    if m.assets:
        typer.echo(f"\nConsumed assets ({len(m.assets)}):")
        for a in m.assets:
            label = a.dataset_id or a.transform_id or ""
            size_kb = a.size / 1024
            typer.echo(
                f"  [{a.role}] {a.path}"
                f"  ({size_kb:.1f} KB, sha256:{a.sha256[:12]}...)"
            )
            if label:
                typer.echo(f"         id: {label}")


def recipe_export_cmd(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            "-m",
            help="Path to a .manifest.json file produced by a recipe build.",
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Destination directory for the replication bundle.",
        ),
    ],
    use_json: _JSON_OPTION = False,
) -> None:
    """Export a replication bundle from a recipe build manifest.

    Copies all consumed assets (datasets, crosswalks) into a
    self-contained directory alongside the manifest, so a replicator
    can reproduce the build without the original project tree.

    Examples:

        coclab build recipe-export \\
            --manifest panel.manifest.json --output /tmp/bundle
    """
    if not manifest.exists():
        if use_json:
            _json_error(f"Manifest not found: {manifest}")
        typer.echo(f"Error: Manifest not found: {manifest}", err=True)
        raise typer.Exit(code=1)

    m = read_manifest(manifest)
    project_root = Path.cwd()

    if not use_json:
        typer.echo(
            f"Exporting bundle for '{m.recipe_name}' "
            f"pipeline '{m.pipeline_id}'...",
        )

    do_export_bundle(m, project_root, output)

    if use_json:
        _json_out({
            "status": "ok",
            "recipe_name": m.recipe_name,
            "pipeline_id": m.pipeline_id,
            "assets_copied": len(m.assets),
            "bundle_path": str(output),
        })
        return

    typer.echo(f"  {len(m.assets)} asset(s) copied")
    typer.echo(f"  Manifest written to {output / 'manifest.json'}")
    typer.echo(f"Bundle: {output}")


def _render_preflight_human(report: PreflightReport) -> None:
    """Render a preflight report as human-readable text."""
    typer.echo(
        f"Recipe: {report.recipe_name} (version {report.recipe_version})"
    )
    typer.echo(
        f"Universe: {min(report.universe_years)}-"
        f"{max(report.universe_years)} "
        f"({len(report.universe_years)} years)"
    )

    for ps in report.pipelines:
        if ps.plan_error:
            typer.echo(
                f"\nPipeline '{ps.pipeline_id}': "
                f"PLAN ERROR - {ps.plan_error}",
                err=True,
            )
        else:
            typer.echo(
                f"\nPipeline '{ps.pipeline_id}': "
                f"{ps.task_count} tasks resolved"
            )

    if not report.findings:
        typer.echo("\nAll prerequisites satisfied. Ready to build.")
        return

    blockers = [f for f in report.findings if f.is_blocking]
    warnings = [
        f for f in report.findings if f.severity == Severity.WARNING
    ]

    if blockers:
        typer.echo(f"\nBlockers ({len(blockers)}):", err=True)
        for f in blockers:
            typer.echo(f"  ERROR: {f.message}", err=True)
            if f.remediation:
                typer.echo(
                    f"    Fix: {f.remediation.hint}", err=True,
                )
                if f.remediation.command:
                    typer.echo(
                        f"    Run: {f.remediation.command}", err=True,
                    )

    if warnings:
        typer.echo(f"\nWarnings ({len(warnings)}):", err=True)
        for f in warnings:
            typer.echo(f"  WARNING: {f.message}", err=True)

    if blockers:
        typer.echo(
            f"\nPreflight FAILED: {len(blockers)} blocker(s), "
            f"{len(warnings)} warning(s).",
            err=True,
        )
    else:
        typer.echo(
            f"\nPreflight passed with {len(warnings)} warning(s). "
            "Ready to build."
        )


def recipe_preflight_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    use_json: _JSON_OPTION = False,
    gaps: Annotated[
        bool,
        typer.Option(
            "--gaps",
            help="Emit only the data-gaps manifest (implies --json).",
        ),
    ] = False,
) -> None:
    """Check all recipe prerequisites in one pass without executing.

    Resolves execution plans, inspects dataset paths, transform
    artifacts, dataset schemas, and support-dataset requirements for
    weighted transforms.  Only checks dataset-years required by the
    resolved plan (recipe-scoped).  Reports all issues at once with
    actionable fix suggestions rather than failing on the first
    missing prerequisite.

    Use --json for machine-readable output suitable for automation or CI.
    Use --gaps for a focused data-gaps manifest with per-gap metadata,
    severity classification, and remediation hints.

    Examples:

        coclab build recipe-preflight --recipe my_build.yaml

        coclab build recipe-preflight --recipe my_build.yaml --json

        coclab build recipe-preflight --recipe my_build.yaml --gaps
    """
    register_defaults()

    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json or gaps:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    report = run_preflight(parsed)

    if gaps:
        _json_out({"status": "ok", **report.gaps_manifest()})
        raise typer.Exit(code=1 if report.blocking_count > 0 else 0)

    if use_json:
        _json_out({
            "status": "ok" if report.is_ready else "blocked",
            **report.to_dict(),
        })
        raise typer.Exit(code=1 if not report.is_ready else 0)

    _render_preflight_human(report)
    if not report.is_ready:
        raise typer.Exit(code=1)
