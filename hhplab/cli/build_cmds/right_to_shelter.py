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
    curated_baseline_path,
    curated_source_table_path,
)

_JSON_OPTION = Annotated[
    bool,
    typer.Option("--json", help="Output machine-readable JSON instead of human text."),
]


def build_right_to_shelter_cmd(
    first_year: Annotated[
        int,
        typer.Option("--first-year", min=1900, max=2200, help="First requested annual year."),
    ],
    last_year: Annotated[
        int,
        typer.Option("--last-year", min=1900, max=2200, help="Last requested annual year."),
    ],
    coc_boundary_vintage: Annotated[
        str,
        typer.Option("--coc-boundary-vintage", help="Curated CoC boundary vintage."),
    ],
    county_vintage: Annotated[
        str,
        typer.Option("--county-vintage", help="Curated TIGER county geometry vintage."),
    ],
    county_population: Annotated[
        Path | None,
        typer.Option(
            "--county-population",
            help=(
                "Optional county-year CSV/Parquet with resident_population or population-specific "
                "weights; only mixed status needs complete support."
            ),
        ),
    ] = None,
    weight_definition: Annotated[
        str | None,
        typer.Option(
            "--weight-definition",
            help="Required when weights are supplied; identify population basis and vintage.",
        ),
    ] = None,
    exceptions: Annotated[
        Path | None,
        typer.Option(
            "--exceptions",
            help="Cited policy-exception CSV; defaults to the canonical curated path.",
        ),
    ] = None,
    substate_county_membership: Annotated[
        Path | None,
        typer.Option(
            "--substate-county-membership",
            help=(
                "Optional additive CSV with jurisdiction_id,jurisdiction_type,county_fips; "
                "default NYC borough assignments remain active."
            ),
        ),
    ] = None,
    baseline: Annotated[
        Path | None,
        typer.Option(
            "--baseline",
            help="Reviewed baseline JSON; defaults to the canonical curated path.",
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
    msa_ids: Annotated[
        list[str] | None,
        typer.Option("--msa-id", help="Optional MSA ID selector; repeat to select several."),
    ] = None,
    coc_ids: Annotated[
        list[str] | None,
        typer.Option("--coc-id", help="Optional CoC ID selector; repeat to select several."),
    ] = None,
    json_output: _JSON_OPTION = False,
) -> None:
    """Build requested-year MSA and CoC outputs from reviewed policy inputs."""
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
    if first_year > last_year:
        _emit_error("--first-year must be less than or equal to --last-year.", json_output)
    if msa_ids == [] or coc_ids == []:
        _emit_error(
            "Geography selectors cannot be empty; omit the option or pass at least one ID.",
            json_output,
        )
    try:
        outputs = build_curated_outputs(
            exceptions_path=exceptions or curated_source_table_path(data_root),
            baseline_path=baseline or curated_baseline_path(data_root),
            county_population_path=county_population,
            data_root=data_root,
            msa_definition_version=msa_definition_version,
            coc_boundary_vintage=coc_boundary_vintage,
            county_vintage=county_vintage,
            coding_rule=coding_rule,
            include_weather_only=include_weather_only,
            geography_rule=geography_rule,
            majority_threshold=majority_threshold,
            first_year=first_year,
            last_year=last_year,
            msa_ids=msa_ids,
            coc_ids=coc_ids,
            weight_definition=weight_definition,
            substate_county_membership_path=substate_county_membership,
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
        "first_year": first_year,
        "last_year": last_year,
        "msa_ids": msa_ids,
        "coc_ids": coc_ids,
        "substate_county_membership": (
            str(substate_county_membership) if substate_county_membership else None
        ),
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
