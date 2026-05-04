"""CLI commands for HHP-Lab using Typer.

Provides commands for ingesting CoC boundary data, building crosswalks,
computing measures, and visualizing boundaries.
"""

import os
import sys
import warnings
from pathlib import Path
from typing import Annotated

import typer

from hhplab import __version__
from hhplab.cli.commands import register_commands

# Suppress known PyArrow warnings on macOS (sysctlbyname failures in sandboxed environments)
# These are harmless warnings about CPU cache detection that don't affect functionality.
warnings.filterwarnings(
    "ignore",
    message=".*sysctlbyname failed.*",
    category=UserWarning,
)


def _is_non_interactive(ctx: typer.Context | None = None) -> bool:
    """Return True when CLI should avoid all interactive prompts."""
    env = os.getenv("HHPLAB_NON_INTERACTIVE", "").strip().lower()
    env_true = env in {"1", "true", "yes", "on"}
    argv_flag = "--non-interactive" in sys.argv[1:]

    if ctx is None:
        return bool(env_true or argv_flag)
    obj = ctx.obj if isinstance(ctx.obj, dict) else {}
    return bool(obj.get("non_interactive", False) or env_true or argv_flag)


def _check_working_directory(*, non_interactive: bool = False) -> None:
    """Warn if the current directory doesn't look like the HHP-Lab project root."""
    cwd = Path.cwd()
    expected_markers = [
        cwd / "pyproject.toml",
        cwd / "hhplab",
        cwd / "data",
    ]
    missing = [p for p in expected_markers if not p.exists()]

    if missing:
        missing_names = ", ".join(p.name for p in missing)
        typer.echo(
            f"Warning: Current directory may not be the HHP-Lab project root. "
            f"Missing: {missing_names}",
            err=True,
        )
        if sys.stdin.isatty() and not non_interactive:
            if not typer.confirm("Do you still want to continue?", default=False):
                raise typer.Exit(0)


def _version_callback(value: bool) -> None:
    """Print the CLI version and exit."""
    if value:
        typer.echo(f"hhplab {__version__}")
        raise typer.Exit()


app = typer.Typer(
    name="hhplab",
    help="HHP-Lab - Continuum of Care boundary data infrastructure CLI",
    no_args_is_help=True,
)

ingest_app = typer.Typer(
    name="ingest",
    help="Ingest raw and curated datasets",
    no_args_is_help=True,
)
list_app = typer.Typer(
    name="list",
    help="List available datasets",
    no_args_is_help=True,
)
validate_app = typer.Typer(
    name="validate",
    help="Validate datasets and registries",
    no_args_is_help=True,
)
diagnostics_app = typer.Typer(
    name="diagnostics",
    help="Run diagnostics on datasets",
    no_args_is_help=True,
)
migrate_app = typer.Typer(
    name="migrate",
    help="Run data migration utilities",
    no_args_is_help=True,
)
generate_app = typer.Typer(
    name="generate",
    help="Generate crosswalks and metro definitions",
    no_args_is_help=True,
)
build_app = typer.Typer(
    name="build",
    help="Run recipe builds and bundle utilities",
    no_args_is_help=True,
)
show_app = typer.Typer(
    name="show",
    help="Display and visualize data",
    no_args_is_help=True,
)
registry_app = typer.Typer(
    name="registry",
    help="Manage boundary and source registries",
    no_args_is_help=True,
)

@app.callback()
def main_callback(
    ctx: typer.Context,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            callback=_version_callback,
            help="Show the HHP-Lab version and exit.",
            is_eager=True,
        ),
    ] = None,
    non_interactive: Annotated[
        bool,
        typer.Option(
            "--non-interactive",
            help=(
                "Disable interactive prompts. Can also be enabled with HHPLAB_NON_INTERACTIVE=1."
            ),
        ),
    ] = False,
) -> None:
    """Check working directory before running any command."""
    _ = version
    if not isinstance(ctx.obj, dict):
        ctx.obj = {}
    ctx.obj["non_interactive"] = bool(non_interactive)
    _check_working_directory(non_interactive=_is_non_interactive(ctx))


register_commands(
    app=app,
    ingest_app=ingest_app,
    list_app=list_app,
    validate_app=validate_app,
    diagnostics_app=diagnostics_app,
    migrate_app=migrate_app,
    generate_app=generate_app,
    build_app=build_app,
    show_app=show_app,
    registry_app=registry_app,
)


if __name__ == "__main__":
    app()
