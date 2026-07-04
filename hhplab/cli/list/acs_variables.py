"""CLI command for listing supported ACS variables and output columns."""

from __future__ import annotations

import json
from typing import Annotated, Any

import typer

from hhplab.acs.variables import (
    ACS5_COVARIATE_REGISTRY,
    ACS_VARIABLES,
    acs5_registry_tables,
)
from hhplab.acs.variables_acs1 import (
    ACS1_TABLES,
    ACS1_VARIABLE_NAMES,
    DERIVED_ACS1_MEASURES,
    acs1_measure_names,
)

ACS5_ARTIFACT_TEMPLATE = "data/curated/acs/acs5_tracts__A{acs_end}xT{tract}.parquet"
ACS1_METRO_ARTIFACT_TEMPLATE = (
    "data/curated/acs/acs1_metro__A{acs1_vintage}@D{definition_version}.parquet"
)
ACS1_COUNTY_ARTIFACT_TEMPLATE = "data/curated/acs/acs1_county__A{acs1_vintage}.parquet"


def _acs5_table_inventory() -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for table in acs5_registry_tables():
        specs = [spec for spec in ACS5_COVARIATE_REGISTRY if spec.table == table]
        output_columns = sorted({column for spec in specs for column in spec.output_columns})
        source_variables = sorted(
            {variable for spec in specs for variable in spec.source_variables}
        )
        tables.append(
            {
                "table": table,
                "registry_names": [spec.name for spec in specs],
                "source_variables": source_variables,
                "output_columns": output_columns,
            }
        )
    return tables


def _acs5_column_inventory() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in ACS5_COVARIATE_REGISTRY:
        for output_column in spec.output_columns:
            source_variables = [
                variable
                for variable in spec.source_variables
                if ACS_VARIABLES.get(variable) == output_column
            ]
            if not source_variables:
                source_variables = list(spec.source_variables)
            rows.append(
                {
                    "ingest_path": "acs5-tract",
                    "command": "hhplab ingest acs5-tract",
                    "artifact_family": "acs5_tracts",
                    "artifact_template": ACS5_ARTIFACT_TEMPLATE,
                    "table": spec.table,
                    "source_variables": source_variables,
                    "output_column": output_column,
                    "registry_name": spec.name,
                    "registry_source": "ACS5_COVARIATE_REGISTRY",
                    "covariate_registry_member": True,
                }
            )
    return rows


def _acs1_table_inventory() -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for table in ACS1_TABLES:
        source_variables = [
            variable
            for variable in ACS1_VARIABLE_NAMES
            if variable.startswith(f"{table}_")
        ]
        output_columns = [ACS1_VARIABLE_NAMES[variable] for variable in source_variables]
        tables.append(
            {
                "table": table,
                "registry_names": [table],
                "source_variables": source_variables,
                "output_columns": output_columns,
            }
        )
    return tables


def _acs1_column_inventory(
    *,
    ingest_path: str,
    command: str,
    artifact_family: str,
    artifact_template: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for variable, output_column in ACS1_VARIABLE_NAMES.items():
        table = variable.split("_", maxsplit=1)[0]
        rows.append(
            {
                "ingest_path": ingest_path,
                "command": command,
                "artifact_family": artifact_family,
                "artifact_template": artifact_template,
                "table": table,
                "source_variables": [variable],
                "output_column": output_column,
                "registry_name": table,
                "registry_source": "ACS1_TABLE_COLUMN_NAMES",
                "covariate_registry_member": False,
            }
        )
    for output_column, description in DERIVED_ACS1_MEASURES.items():
        rows.append(
            {
                "ingest_path": ingest_path,
                "command": command,
                "artifact_family": artifact_family,
                "artifact_template": artifact_template,
                "table": "B23025",
                "source_variables": ["B23025_005E", "B23025_003E"],
                "output_column": output_column,
                "registry_name": output_column,
                "registry_source": "DERIVED_ACS1_MEASURES",
                "description": description,
                "covariate_registry_member": False,
            }
        )
    return rows


def build_acs_variables_inventory() -> dict[str, Any]:
    """Return supported ACS tables and output columns by ingest path."""
    acs5_columns = _acs5_column_inventory()
    acs1_metro_columns = _acs1_column_inventory(
        ingest_path="acs1-metro",
        command="hhplab ingest acs1-metro",
        artifact_family="acs1_metro",
        artifact_template=ACS1_METRO_ARTIFACT_TEMPLATE,
    )
    acs1_county_columns = _acs1_column_inventory(
        ingest_path="acs1-county",
        command="hhplab ingest acs1-county",
        artifact_family="acs1_county",
        artifact_template=ACS1_COUNTY_ARTIFACT_TEMPLATE,
    )
    ingest_paths = [
        {
            "ingest_path": "acs5-tract",
            "command": "hhplab ingest acs5-tract",
            "artifact_family": "acs5_tracts",
            "artifact_template": ACS5_ARTIFACT_TEMPLATE,
            "registry_source": "ACS5_COVARIATE_REGISTRY",
            "tables": _acs5_table_inventory(),
            "output_columns": acs5_columns,
        },
        {
            "ingest_path": "acs1-metro",
            "command": "hhplab ingest acs1-metro",
            "artifact_family": "acs1_metro",
            "artifact_template": ACS1_METRO_ARTIFACT_TEMPLATE,
            "registry_source": "ACS1_TABLE_COLUMN_NAMES",
            "tables": _acs1_table_inventory(),
            "output_columns": acs1_metro_columns,
            "measure_columns": acs1_measure_names(),
        },
        {
            "ingest_path": "acs1-county",
            "command": "hhplab ingest acs1-county",
            "artifact_family": "acs1_county",
            "artifact_template": ACS1_COUNTY_ARTIFACT_TEMPLATE,
            "registry_source": "ACS1_TABLE_COLUMN_NAMES",
            "tables": _acs1_table_inventory(),
            "output_columns": acs1_county_columns,
            "measure_columns": acs1_measure_names(),
        },
    ]
    output_columns = [
        column
        for ingest_path in ingest_paths
        for column in ingest_path["output_columns"]
    ]
    return {
        "status": "ok",
        "ingest_path_count": len(ingest_paths),
        "output_column_count": len(output_columns),
        "ingest_paths": ingest_paths,
        "output_columns": output_columns,
    }


def list_acs_variables(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """List supported ACS tables and output columns by ingest path."""
    payload = build_acs_variables_inventory()
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return

    typer.echo("Supported ACS Variables:\n")
    typer.echo(f"{'Ingest path':<14} {'Tables':<7} {'Output columns':<14} Artifact")
    typer.echo("-" * 80)
    for path in payload["ingest_paths"]:
        typer.echo(
            f"{path['ingest_path']:<14} {len(path['tables']):<7} "
            f"{len(path['output_columns']):<14} {path['artifact_template']}"
        )
    typer.echo("")
    typer.echo("Use --json for table codes, source variables, output columns, and registry names.")
