"""Diagnostics command registration."""

import typer

from hhplab.cli.diagnostics.hic_coverage import hic_coverage_diagnostics
from hhplab.cli.diagnostics.panel import panel_diagnostics
from hhplab.cli.diagnostics.xwalk import diagnostics
from hhplab.cli.ingest.zori import zori_diagnostics


def register_commands(app: typer.Typer) -> None:
    """Register diagnostics commands."""
    app.command("panel")(panel_diagnostics)
    app.command("xwalk")(diagnostics)
    app.command("zori")(zori_diagnostics)
    app.command("hic-coverage")(hic_coverage_diagnostics)
