"""Build cited right-to-shelter MSA and CoC tables."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from hhplab.sources.right_to_shelter import (
    CODING_RULES,
    DEFAULT_MSA_DEFINITION_VERSION,
    GEOGRAPHY_RULES,
    RightToShelterError,
    build_curated_outputs,
    curated_source_table_path,
)

_JSON_OPTION = Annotated[
    bool,
    typer.Option("--json", help="Output machine-readable JSON instead of human text."),
]


def build_right_to_shelter_cmd(
    county_population: Annotated[
        Path,
        typer.Option(
            "--county-population",
            help="CSV of county-year population weights for all three populations.",
        ),
    ],
    coc_boundary_vintage: Annotated[
        str,
        typer.Option("--coc-boundary-vintage", help="Curated CoC boundary vintage."),
    ],
    county_vintage: Annotated[
        str,
        typer.Option("--county-vintage", help="Curated TIGER county geometry vintage."),
    ],
    source_table: Annotated[
        Path | None,
        typer.Option(
            "--source-table",
            help="Cited jurisdiction-year CSV; defaults to the canonical curated path.",
        ),
    ] = None,
    data_root: Annotated[
        Path,
        typer.Option("--data-root", help="Data root containing curated geography files."),
    ] = Path("data"),
    msa_definition_version: Annotated[
        str,
        typer.Option("--msa-definition-version", help="MSA membership definition version."),
    ] = DEFAULT_MSA_DEFINITION_VERSION,
    coding_rule: Annotated[
        str,
        typer.Option(
            "--coding-rule",
            help="Population coding rule: strict or broad (weather-only requires broad).",
        ),
    ] = "strict",
    include_weather_only: Annotated[
        bool,
        typer.Option(
            "--include-weather-only/--exclude-weather-only",
            help=(
                "Count weather-only protections as rights; requires --coding-rule broad. "
                "Rejected with strict coding."
            ),
        ),
    ] = False,
    geography_rule: Annotated[
        str,
        typer.Option("--geography-rule", help="Geography flag rule: majority or any."),
    ] = "majority",
    majority_threshold: Annotated[
        float,
        typer.Option("--majority-threshold", min=0.0, max=1.0),
    ] = 0.5,
    json_output: _JSON_OPTION = False,
) -> None:
    """Build `right_to_shelter__msa` and `right_to_shelter__coc` outputs."""
    if coding_rule not in CODING_RULES:
        _emit_error(f"--coding-rule must be one of {CODING_RULES}.", json_output)
    if coding_rule == "strict" and include_weather_only:
        _emit_error(
            "--include-weather-only requires --coding-rule broad; use broad coding or "
            "disable --include-weather-only.",
            json_output,
        )
    if geography_rule not in GEOGRAPHY_RULES:
        _emit_error(f"--geography-rule must be one of {GEOGRAPHY_RULES}.", json_output)
    try:
        outputs = build_curated_outputs(
            source_table_path=source_table or curated_source_table_path(),
            county_population_path=county_population,
            data_root=data_root,
            msa_definition_version=msa_definition_version,
            coc_boundary_vintage=coc_boundary_vintage,
            county_vintage=county_vintage,
            coding_rule=coding_rule,
            include_weather_only=include_weather_only,
            geography_rule=geography_rule,
            majority_threshold=majority_threshold,
        )
    except (FileNotFoundError, RightToShelterError, ValueError) as exc:
        _emit_error(str(exc), json_output)
    payload = {
        "status": "ok",
        "outputs": {name: str(path) for name, path in outputs.items()},
        "coding_rule": coding_rule,
        "include_weather_only": include_weather_only,
        "geography_rule": geography_rule,
        "majority_threshold": majority_threshold,
    }
    if json_output:
        typer.echo(json.dumps(payload, indent=2))
        return
    typer.echo("Built right-to-shelter outputs:")
    for name, path in outputs.items():
        typer.echo(f"  {name.upper()}: {path}")


def _emit_error(message: str, json_output: bool) -> None:
    if json_output:
        typer.echo(json.dumps({"status": "error", "error": message}, indent=2))
    else:
        typer.echo(f"Error: {message}", err=True)
    raise typer.Exit(code=1)
