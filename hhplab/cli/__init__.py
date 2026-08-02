"""CLI package for HHP-Lab.

Keep package initialization passive so importing command submodules does not
initialize the complete Typer application.  The lazy attribute preserves the
historical ``from hhplab.cli import app`` API.
"""

from typing import Any

__all__ = ["app"]


def __getattr__(name: str) -> Any:
    if name != "app":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from hhplab.cli.main import app

    return app
