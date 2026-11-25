"""Noninteractive CLI for reproducible RecipeV2 result bundles."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Annotated

import typer

from hhplab.recipe.loader import load_recipe
from hhplab.recipe.results_plan import plan_results
from hhplab.recipe.schema_results import RecipeV2
from hhplab.schema.results import canonical_json
from hhplab.storage.config import load_config
from hhplab.storage.run_ledger import RunLedger

results_app = typer.Typer(help="Check, run, verify and publish reproducible results.")
RecipeOption = Annotated[Path, typer.Option("--recipe", "-r", help="RecipeV2 YAML file.")]
JsonOption = Annotated[bool, typer.Option("--json", help="Emit structured JSON.")]
RootOption = Annotated[Path | None, typer.Option("--outputs-root", help="Results and ledger root.")]
NonInteractive = Annotated[bool, typer.Option("--non-interactive", help="Run without prompts.")]
PublicationOption = Annotated[Path, typer.Option("--publication", help="Publication YAML file.")]
DestinationOption = Annotated[Path, typer.Option("--dest", help="Site content directory.")]
PublicBundleOption = Annotated[Path | None, typer.Option("--public-bundle")]
BundleOption = Annotated[Path | None, typer.Option("--bundle", help="Portable results bundle.")]
StrictOption = Annotated[bool, typer.Option("--strict", help="Treat narrative lint as an error.")]
SkipReproduceOption = Annotated[bool, typer.Option("--skip-reproduce")]
ReplayFailureOption = Annotated[bool, typer.Option("--allow-reproducibility-failure")]


def _load(path: Path):
    original = path.read_bytes()
    recipe = load_recipe(path)
    if not isinstance(recipe, RecipeV2):
        raise ValueError("Results commands require version: 2; use hhplab build recipe for V1.")
    return recipe, original


def _root(path: Path | None, recipe: RecipeV2 | None = None) -> Path:
    return load_config(
        output_root=path,
        recipe_output_root=recipe.output_root if recipe is not None else None,
        recipe_base_dir=recipe.source_base_dir if recipe is not None else None,
    ).output_root


def _emit(payload, use_json: bool) -> None:
    if use_json:
        typer.echo(canonical_json(payload))
    else:
        typer.echo(canonical_json(payload))


def _error(exc: Exception, use_json: bool) -> None:
    findings = getattr(exc, "findings", ())
    _emit(
        {
            "status": "error",
            "error": str(exc),
            "findings": [finding.to_dict() for finding in findings],
        },
        use_json,
    )
    raise typer.Exit(2) from exc


@results_app.command("check")
def check_cmd(
    recipe: RecipeOption,
    use_json: JsonOption = False,
    outputs_root: RootOption = None,
    readiness: Annotated[bool, typer.Option("--readiness")] = False,
    non_interactive: NonInteractive = False,
) -> None:
    from hhplab.results.runner import check_results

    try:
        parsed, original = _load(recipe)
        resolved_root = _root(outputs_root, parsed)
        payload = check_results(
            parsed,
            original_recipe_bytes=original,
            readiness=readiness,
            project_root=Path.cwd(),
            outputs_root=resolved_root,
        )
        payload["output_root"] = str(resolved_root)
    except Exception as exc:
        _error(exc, use_json)
    _emit(payload, use_json)
    if payload.get("status") not in {"ok", "ready"}:
        raise typer.Exit(1)


@results_app.command("run")
def run_cmd(
    recipe: RecipeOption,
    use_json: JsonOption = False,
    outputs_root: RootOption = None,
    private: Annotated[bool, typer.Option("--private")] = False,
    allow_unknown_aggregates: Annotated[bool, typer.Option("--allow-unknown-aggregates")] = False,
    non_interactive: NonInteractive = False,
) -> None:
    from hhplab.results.runner import run_results

    try:
        parsed, original = _load(recipe)
        payload = run_results(
            parsed,
            original_recipe_bytes=original,
            outputs_root=_root(outputs_root, parsed),
            project_root=Path.cwd(),
            private=private,
            allow_unknown_aggregates=allow_unknown_aggregates,
        )
    except Exception as exc:
        _error(exc, use_json)
    _emit(payload, use_json)
    execution = payload.get("statuses", {}).get("execution_status", payload.get("execution_status"))
    if execution != "complete":
        raise typer.Exit(1)


@results_app.command("lock")
def lock_cmd(
    recipe: RecipeOption,
    use_json: JsonOption = False,
    outputs_root: RootOption = None,
    export: Annotated[Path | None, typer.Option("--export")] = None,
    non_interactive: NonInteractive = False,
) -> None:
    try:
        parsed, original = _load(recipe)
        plan = plan_results(parsed, original_recipe_bytes=original)
        record = RunLedger(_root(outputs_root, parsed)).create_lock(
            recipe_id=parsed.id,
            recipe_sha256=hashlib.sha256(original).hexdigest(),
            components=plan.component_hashes,
            spec_hash=plan.plan_sha256,
            export_path=export,
        )
    except Exception as exc:
        _error(exc, use_json)
    _emit({"status": "ok", "lock": record}, use_json)


@results_app.command("history")
def history_cmd(
    recipe_id: Annotated[str | None, typer.Option("--recipe-id")] = None,
    use_json: JsonOption = False,
    outputs_root: RootOption = None,
    non_interactive: NonInteractive = False,
) -> None:
    try:
        payload = RunLedger(_root(outputs_root)).history()
        if recipe_id is not None:
            payload["runs"] = [
                run for run in payload["runs"] if run["start"]["recipe_id"] == recipe_id
            ]
    except Exception as exc:
        _error(exc, use_json)
    _emit({"status": "ok", **payload}, use_json)


@results_app.command("verify")
def verify_cmd(
    bundle: Annotated[Path, typer.Argument(help="Portable bundle directory.")],
    use_json: JsonOption = False,
    non_interactive: NonInteractive = False,
) -> None:
    from hhplab.results.verify import verify_bundle

    try:
        report = verify_bundle(bundle)
    except Exception as exc:
        _error(exc, use_json)
    _emit(report.to_dict(), use_json)
    raise typer.Exit(report.exit_code)


def _publish_command(
    publication: Path,
    dest: Path,
    use_json: bool,
    *,
    bundle: Path | None,
    public_bundle: Path | None,
    strict: bool,
    skip_reproduce: bool,
    allow_reproducibility_failure: bool,
    dry_run: bool,
) -> None:
    from hhplab.results.publish import PublicationError, publish

    try:
        result = publish(
            publication,
            dest,
            bundle=bundle,
            public_bundle=public_bundle,
            strict=strict,
            skip_reproduce=skip_reproduce,
            allow_reproducibility_failure=allow_reproducibility_failure,
            dry_run=dry_run,
        )
    except (OSError, PublicationError) as exc:
        _publication_error(exc, use_json)
    _emit(result, use_json)


def _publication_error(exc: Exception, use_json: bool) -> None:
    _emit(
        {
            "status": "error",
            "code": getattr(exc, "code", "publication_io_error"),
            "error": (
                "Could not access the publication source or destination."
                if isinstance(exc, OSError) else str(exc)
            ),
            "findings": [],
            "warnings": [],
        },
        use_json,
    )
    raise typer.Exit(2) from exc


@results_app.command("publish")
def publish_cmd(
    publication: PublicationOption,
    dest: DestinationOption,
    bundle: BundleOption = None,
    public_bundle: PublicBundleOption = None,
    strict: StrictOption = False,
    skip_reproduce: SkipReproduceOption = False,
    allow_reproducibility_failure: ReplayFailureOption = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
    use_json: JsonOption = False,
    non_interactive: NonInteractive = False,
) -> None:
    _publish_command(
        publication, dest, use_json, bundle=bundle, public_bundle=public_bundle,
        strict=strict, skip_reproduce=skip_reproduce,
        allow_reproducibility_failure=allow_reproducibility_failure, dry_run=dry_run,
    )


@results_app.command("publish-check")
def publish_check_cmd(
    publication: PublicationOption,
    dest: DestinationOption,
    bundle: BundleOption = None,
    public_bundle: PublicBundleOption = None,
    strict: StrictOption = False,
    skip_reproduce: SkipReproduceOption = False,
    allow_reproducibility_failure: ReplayFailureOption = False,
    use_json: JsonOption = False,
    non_interactive: NonInteractive = False,
) -> None:
    _publish_command(
        publication, dest, use_json, bundle=bundle, public_bundle=public_bundle,
        strict=strict, skip_reproduce=skip_reproduce,
        allow_reproducibility_failure=allow_reproducibility_failure, dry_run=True,
    )


@results_app.command("publish-index")
def publish_index_cmd(
    dest: DestinationOption,
    use_json: JsonOption = False,
    non_interactive: NonInteractive = False,
) -> None:
    from hhplab.results.publish import PublicationError, publish_index

    try:
        result = publish_index(dest)
    except (OSError, PublicationError) as exc:
        _publication_error(exc, use_json)
    _emit(result, use_json)
