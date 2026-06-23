"""Validate command registration."""

import typer

from hhplab.cli.shared.boundaries import validate_boundaries
from hhplab.cli.validate.curated import validate_curated_layout_cmd
from hhplab.cli.validate.metro import validate_metro, validate_metro_universe
from hhplab.cli.validate.msa import validate_msa
from hhplab.cli.validate.pit_vintages import validate_pit_vintages
from hhplab.cli.validate.population import validate_population
from hhplab.cli.validate.schema_contract import validate_schema_contract_cmd


def register_commands(app: typer.Typer) -> None:
    """Register validate commands."""
    app.command("boundaries")(validate_boundaries)
    app.command("metro")(validate_metro)
    app.command("metro-universe")(validate_metro_universe)
    app.command("msa")(validate_msa)
    app.command("pit-vintages")(validate_pit_vintages)
    app.command("population")(validate_population)
    app.command("curated-layout")(validate_curated_layout_cmd)
    app.command("schema-contract")(validate_schema_contract_cmd)
