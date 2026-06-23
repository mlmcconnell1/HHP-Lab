"""Shared CLI output helpers for JSON-capable commands."""

from __future__ import annotations

import json
from typing import Annotated, Any, NoReturn

import typer

JsonOutput = Annotated[
    bool,
    typer.Option("--json", help="Output structured JSON instead of human-readable text."),
]


def emit_result(
    payload: dict[str, Any],
    json_output: bool,
    *,
    indent: int | None = None,
    default: Any | None = None,
) -> bool:
    """Emit a JSON payload when requested and report whether output was handled."""
    if not json_output:
        return False
    typer.echo(json.dumps(payload, indent=indent, default=default))
    return True


def cli_error(
    error: BaseException | str,
    json_output: bool,
    *,
    code: int = 1,
    human_prefix: str = "Error",
    json_error: str | None = None,
    json_payload: dict[str, Any] | None = None,
) -> NoReturn:
    """Emit a consistent CLI error payload/message and exit."""
    message = str(error)
    if json_output:
        typer.echo(json.dumps(json_payload or {"status": "error", "error": json_error or message}))
    else:
        human_message = f"{human_prefix}: {message}" if human_prefix else message
        typer.echo(human_message, err=True)
    if isinstance(error, BaseException):
        raise typer.Exit(code) from error
    raise typer.Exit(code)
