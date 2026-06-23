"""Generate command registration."""

import typer

from hhplab.cli.generate.metro import generate_metro, generate_metro_universe
from hhplab.cli.generate.metro_boundaries import generate_metro_boundaries
from hhplab.cli.generate.msa import generate_msa
from hhplab.cli.generate.msa_xwalk import generate_msa_xwalk
from hhplab.cli.generate.xwalks import build_xwalks


def register_commands(app: typer.Typer) -> None:
    """Register generate commands."""
    app.command("xwalks")(build_xwalks)
    app.command("metro")(generate_metro)
    app.command("metro-universe")(generate_metro_universe)
    app.command("metro-boundaries")(generate_metro_boundaries)
    app.command("msa")(generate_msa)
    app.command("msa-xwalk")(generate_msa_xwalk)
