"""CLI commands for recipe-driven builds."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
import yaml

from hhplab.config import load_config
from hhplab.recipe.adapters import (
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from hhplab.recipe.cache import RecipeCache
from hhplab.recipe.default_adapters import register_defaults
from hhplab.recipe.executor import (
    ExecutorError,
    execute_recipe,
    resolve_pipeline_artifacts,
)
from hhplab.recipe.loader import RecipeLoadError, load_recipe
from hhplab.recipe.manifest import export_bundle as do_export_bundle
from hhplab.recipe.manifest import read_manifest
from hhplab.recipe.planner import PlannerError, resolve_plan
from hhplab.recipe.preflight import PreflightReport, Severity, run_preflight
from hhplab.recipe.recipe_schema import RecipeV1

# Common --json flag definition
_JSON_OPTION = Annotated[
    bool,
    typer.Option(
        "--json",
        help="Output machine-readable JSON instead of human text.",
    ),
]


def _json_out(data: dict) -> None:
    """Print a JSON response and exit."""
    typer.echo(json.dumps(data, indent=2))


def _json_error(message: str, *, code: int = 1) -> None:
    """Print a JSON error response and raise typer.Exit."""
    _json_out({"status": "error", "error": message})
    raise typer.Exit(code=code)


def _msa_coc_coverage_recipe_dict(
    *,
    name: str,
    year: int,
    top_n: int,
    ranking_population_source: str,
    ranking_reference_year: int,
    coc_boundary_vintage: int,
    msa_definition_version: str,
    county_vintage: int,
    overlap_bases: list[str],
    acs5_population_vintage: int | None,
    acs5_population_reference_year: int | None,
    tract_vintage: int | None,
    min_msa_area_coverage_share: float | None,
    min_msa_population_coverage_share: float | None,
    csv_sidecar: bool,
    pep_msa_path: str,
) -> dict:
    spec: dict[str, object] = {
        "year": year,
        "top_n": top_n,
        "ranking_population_source": ranking_population_source,
        "ranking_reference_year": ranking_reference_year,
        "coc_boundary_vintage": coc_boundary_vintage,
        "msa_definition_version": msa_definition_version,
        "county_vintage": county_vintage,
        "overlap_bases": overlap_bases,
    }
    if acs5_population_vintage is not None:
        spec["acs5_population_vintage"] = acs5_population_vintage
    if acs5_population_reference_year is not None:
        spec["acs5_population_reference_year"] = acs5_population_reference_year
    if tract_vintage is not None:
        spec["tract_vintage"] = tract_vintage
    if min_msa_area_coverage_share is not None:
        spec["min_msa_area_coverage_share"] = min_msa_area_coverage_share
    if min_msa_population_coverage_share is not None:
        spec["min_msa_population_coverage_share"] = min_msa_population_coverage_share
    if csv_sidecar:
        spec["csv_sidecar"] = True

    return {
        "version": 1,
        "name": name,
        "description": (
            "Recipe-native MSA-CoC overlap coverage artifact. "
            "Use recipe-preflight before execution to validate prerequisites."
        ),
        "universe": {"years": [year]},
        "targets": [
            {
                "id": "msa_coc_coverage",
                "geometry": {"type": "coc", "vintage": coc_boundary_vintage},
                "outputs": ["msa_coc_coverage"],
                "msa_coc_coverage": spec,
            }
        ],
        "datasets": {
            "pep_msa": {
                "provider": "census",
                "product": "pep",
                "version": 1,
                "native_geometry": {"type": "msa", "source": msa_definition_version},
                "years": {"years": [ranking_reference_year]},
                "year_column": "year",
                "geo_column": "msa_id",
                "path": pep_msa_path,
            }
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "coverage",
                "target": "msa_coc_coverage",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pep_msa",
                            "to_geometry": {
                                "type": "msa",
                                "source": msa_definition_version,
                            },
                            "method": "identity",
                            "measures": ["population"],
                        }
                    }
                ],
            }
        ],
    }


def _filename_definition_token(definition_version: str) -> str:
    return "".join(c for c in definition_version.lower() if c.isalnum())


def _year_segment(start_year: int, end_year: int, *, exclude_years: set[int]) -> list[int]:
    return [
        year
        for year in range(start_year, end_year + 1)
        if year not in exclude_years
    ]


def _pit_msa_file_set_segments(
    *,
    start_year: int,
    end_year: int,
    msa_definition_version: str,
    county_vintage: int,
    exclude_years: set[int],
) -> list[dict[str, object]]:
    eras = [
        (start_year, min(end_year, 2019), 2018),
        (max(start_year, 2020), min(end_year, 2020), 2020),
        (max(start_year, 2022), end_year, 2024),
    ]
    segments: list[dict[str, object]] = []
    for era_start, era_end, boundary_vintage in eras:
        years = _year_segment(era_start, era_end, exclude_years=exclude_years)
        if not years:
            continue
        segments.append(
            {
                "years": {"years": years},
                "geometry": {"type": "msa", "source": msa_definition_version},
                "constants": {
                    "boundary": boundary_vintage,
                    "county": county_vintage,
                },
            }
        )
    return segments


def _longitudinal_msa_panel_recipe_dict(
    *,
    name: str,
    start_year: int,
    end_year: int,
    exclude_years: set[int],
    top_n: int,
    msa_definition_version: str,
    county_vintage: int,
) -> dict:
    years = _year_segment(start_year, end_year, exclude_years=exclude_years)
    if not years:
        raise ValueError(
            "longitudinal-msa-panel requires at least one included year after "
            "applying --start-year, --end-year, and --exclude-year."
        )
    if start_year < 2015:
        raise ValueError(
            "longitudinal-msa-panel starts at 2015 because the canonical MSA "
            "ZORI yearly artifact starts in 2015. Use --start-year 2015 or later."
        )
    definition_token = _filename_definition_token(msa_definition_version)
    pit_segments = _pit_msa_file_set_segments(
        start_year=start_year,
        end_year=end_year,
        msa_definition_version=msa_definition_version,
        county_vintage=county_vintage,
        exclude_years=exclude_years,
    )
    if not pit_segments:
        raise ValueError(
            "longitudinal-msa-panel PIT MSA eras currently cover years through "
            "2019, 2020, and 2022 onward. Adjust years or add an explicit "
            "pit_msa file_set segment for the missing era."
        )
    covered_pit_years = {
        year
        for segment in pit_segments
        for year in segment["years"]["years"]  # type: ignore[index]
    }
    missing_pit_years = sorted(set(years) - covered_pit_years)
    if missing_pit_years:
        raise ValueError(
            "longitudinal-msa-panel has no canonical PIT MSA file_set era for "
            f"year(s) {missing_pit_years}. Exclude 2021 with --exclude-year 2021 "
            "or add an explicit pit_msa segment to the generated recipe."
        )

    zori_start = max(start_year, 2015)
    zori_end = end_year
    return {
        "version": 1,
        "name": name,
        "description": (
            "Longitudinal MSA panel from pre-materialized PIT MSA yearly rollups, "
            "MSA ZORI yearly panel, and MSA PEP population artifacts."
        ),
        "universe": {"years": years},
        "targets": [
            {
                "id": "msa_longitudinal_panel",
                "geometry": {"type": "msa", "source": msa_definition_version},
                "outputs": ["panel"],
                "cohort": {
                    "method": "top_n",
                    "n": top_n,
                    "rank_by": "population",
                    "reference_year": 2020 if 2020 in years else years[0],
                },
                "panel_policy": {
                    "source_label": name,
                    "column_aliases": {
                        "coverage_ratio": "zori_coverage_ratio",
                    },
                    "derived_measures": [
                        {
                            "type": "ratio",
                            "numerator": "pit_unsheltered",
                            "denominator": "population",
                            "scale": 1000.0,
                            "output_column": "unshelt_per_1000",
                        },
                        {
                            "type": "log",
                            "column": "pit_unsheltered",
                            "output_column": "log_pit_unsheltered",
                        },
                        {
                            "type": "log",
                            "column": "unshelt_per_1000",
                            "output_column": "log_unshelt_per_1000",
                        },
                        {
                            "type": "log",
                            "column": "zori",
                            "output_column": "log_zori",
                        },
                        {
                            "type": "difference",
                            "column": "pit_unsheltered",
                            "output_column": "d_pit_unsheltered",
                        },
                        {
                            "type": "difference",
                            "column": "unshelt_per_1000",
                            "output_column": "d_unshelt_per_1000",
                        },
                        {
                            "type": "difference",
                            "column": "log_unshelt_per_1000",
                            "output_column": "d_log_unshelt_per_1000",
                        },
                        {
                            "type": "difference",
                            "column": "zori",
                            "output_column": "d_zori",
                        },
                        {
                            "type": "difference",
                            "column": "log_zori",
                            "output_column": "d_log_zori",
                        },
                        {
                            "type": "lag",
                            "column": "zori",
                            "output_column": "zori_lag_1",
                        },
                        {
                            "type": "lead",
                            "column": "zori",
                            "output_column": "zori_lead_1",
                        },
                    ],
                    "output_columns": [
                        "msa_id",
                        "msa_name",
                        "cbsa_code",
                        "geo_type",
                        "geo_id",
                        "year",
                        "pit_total",
                        "pit_sheltered",
                        "pit_unsheltered",
                        "population",
                        "zori",
                        "zori_coverage_ratio",
                        "unshelt_per_1000",
                        "log_pit_unsheltered",
                        "log_unshelt_per_1000",
                        "log_zori",
                        "d_pit_unsheltered",
                        "d_unshelt_per_1000",
                        "d_log_unshelt_per_1000",
                        "d_zori",
                        "d_log_zori",
                        "zori_lag_1",
                        "zori_lead_1",
                        "definition_version_used",
                        "source",
                    ],
                },
            }
        ],
        "datasets": {
            "pit_msa": {
                "provider": "hhplab",
                "product": "msa_pit_rollup",
                "version": 1,
                "native_geometry": {"type": "msa", "source": msa_definition_version},
                "geo_column": "msa_id",
                "year_column": "year",
                "file_set": {
                    "path_template": (
                        "data/curated/pit/pit__msa__P{year}@M"
                        f"{definition_token}xB{{boundary}}xC{{county}}.parquet"
                    ),
                    "segments": pit_segments,
                },
            },
            "zori_msa": {
                "provider": "zillow",
                "product": "zori",
                "version": 1,
                "native_geometry": {"type": "msa", "source": msa_definition_version},
                "years": {"years": years},
                "geo_column": "msa_id",
                "year_column": "year",
                "path": (
                    "data/curated/zori/"
                    f"zori__msa__Y{zori_start}-{zori_end}@M{definition_token}"
                    f"xC{county_vintage}__wpopulation__mpit_january__balanced.parquet"
                ),
            },
            "pep_msa": {
                "provider": "census",
                "product": "pep",
                "version": 1,
                "native_geometry": {"type": "msa", "source": msa_definition_version},
                "geo_column": "msa_id",
                "year_column": "year",
                "file_set": {
                    "path_template": (
                        "data/curated/pep/"
                        f"pep__msa__Y{{year}}@M{definition_token}"
                        f"xC{county_vintage}__wpopulation.parquet"
                    ),
                    "segments": [
                        {
                            "years": {"years": years},
                            "geometry": {"type": "msa", "source": msa_definition_version},
                        }
                    ],
                },
            },
        },
        "transforms": [],
        "pipelines": [
            {
                "id": "longitudinal",
                "target": "msa_longitudinal_panel",
                "steps": [
                    {
                        "resample": {
                            "dataset": "pit_msa",
                            "to_geometry": {"type": "msa", "source": msa_definition_version},
                            "method": "identity",
                            "measures": ["pit_total", "pit_sheltered", "pit_unsheltered"],
                        }
                    },
                    {
                        "resample": {
                            "dataset": "zori_msa",
                            "to_geometry": {"type": "msa", "source": msa_definition_version},
                            "method": "identity",
                            "measures": ["zori", "coverage_ratio"],
                        }
                    },
                    {
                        "resample": {
                            "dataset": "pep_msa",
                            "to_geometry": {"type": "msa", "source": msa_definition_version},
                            "method": "identity",
                            "measures": ["population"],
                        }
                    },
                    {
                        "join": {
                            "datasets": ["pit_msa", "zori_msa", "pep_msa"],
                            "join_on": ["geo_id", "year"],
                        }
                    },
                ],
            }
        ],
    }


def recipe_init_cmd(
    template: Annotated[
        str,
        typer.Argument(
            help=(
                "Recipe template to scaffold. Supported: msa-coc-overlap, "
                "msa-coc-coverage, longitudinal-msa-panel."
            ),
        ),
    ],
    output: Annotated[
        Path,
        typer.Option(
            "--output",
            "-o",
            help="Path to write the scaffolded recipe YAML.",
        ),
    ],
    year: Annotated[int, typer.Option("--year", help="Coverage artifact year.")] = 2024,
    start_year: Annotated[
        int,
        typer.Option("--start-year", help="First year for longitudinal panel templates."),
    ] = 2015,
    end_year: Annotated[
        int,
        typer.Option("--end-year", help="Last year for longitudinal panel templates."),
    ] = 2025,
    exclude_year: Annotated[
        list[int] | None,
        typer.Option(
            "--exclude-year",
            help="Analysis year to exclude from longitudinal panel templates. Repeatable.",
        ),
    ] = None,
    top_n: Annotated[int, typer.Option("--top-n", help="Number of MSAs to retain.")] = 100,
    ranking_population_source: Annotated[
        str,
        typer.Option("--ranking-population-source", help="MSA ranking source: pep or acs5."),
    ] = "pep",
    ranking_reference_year: Annotated[
        int | None,
        typer.Option("--ranking-reference-year", help="Year used to rank MSAs."),
    ] = None,
    coc_boundary_vintage: Annotated[
        int,
        typer.Option("--coc-boundary-vintage", help="CoC boundary vintage."),
    ] = 2025,
    msa_definition_version: Annotated[
        str,
        typer.Option("--msa-definition-version", help="MSA definition version."),
    ] = "census_msa_2023",
    county_vintage: Annotated[
        int,
        typer.Option("--county-vintage", help="County geometry vintage."),
    ] = 2023,
    overlap_basis: Annotated[
        list[str] | None,
        typer.Option(
            "--overlap-basis",
            help="Overlap basis to include. Repeat for area and population.",
        ),
    ] = None,
    acs5_population_vintage: Annotated[
        int | None,
        typer.Option("--acs5-population-vintage", help="ACS5 vintage for population basis."),
    ] = None,
    acs5_population_reference_year: Annotated[
        int | None,
        typer.Option(
            "--acs5-population-reference-year",
            help="Reference year documented for the ACS5 denominator.",
        ),
    ] = None,
    tract_vintage: Annotated[
        int | None,
        typer.Option("--tract-vintage", help="Tract vintage for population basis."),
    ] = None,
    min_msa_area_coverage_share: Annotated[
        float | None,
        typer.Option("--min-msa-area-coverage-share", help="Area-basis MSA threshold."),
    ] = None,
    min_msa_population_coverage_share: Annotated[
        float | None,
        typer.Option(
            "--min-msa-population-coverage-share",
            help="Population-basis MSA threshold.",
        ),
    ] = None,
    csv_sidecar: Annotated[
        bool,
        typer.Option("--csv-sidecar", help="Request a CSV sidecar next to Parquet."),
    ] = False,
    pep_msa_path: Annotated[
        str | None,
        typer.Option("--pep-msa-path", help="Path to the MSA-level PEP ranking artifact."),
    ] = None,
    name: Annotated[
        str | None,
        typer.Option("--name", help="Recipe name. Defaults to msa_coc_coverage_<year>."),
    ] = None,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite an existing recipe file."),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Print/validate scaffold metadata without writing."),
    ] = False,
    allow_existing_artifact: Annotated[
        bool,
        typer.Option(
            "--allow-existing-artifact",
            help="Allow scaffolding when the resolved coverage artifact already exists.",
        ),
    ] = False,
    use_json: _JSON_OPTION = False,
) -> None:
    """Scaffold recipe-first workflow files for common HHP-Lab outputs."""
    register_defaults()
    supported_templates = {
        "msa-coc-overlap",
        "msa-coc-coverage",
        "longitudinal-msa-panel",
    }
    if template not in supported_templates:
        message = (
            f"Unsupported recipe template '{template}'. "
            "Supported templates: longitudinal-msa-panel, msa-coc-overlap, "
            "msa-coc-coverage."
        )
        if use_json:
            _json_error(message, code=2)
        raise typer.BadParameter(message, param_hint="template")

    if template == "longitudinal-msa-panel":
        if start_year > end_year:
            message = "--start-year must be less than or equal to --end-year."
            if use_json:
                _json_error(message, code=2)
            raise typer.BadParameter(message, param_hint="--start-year")
        excluded_years = set(exclude_year or [2021])
        recipe_name = name or f"top{top_n}_msa_longitudinal_{start_year}_{end_year}"
        try:
            recipe_data = _longitudinal_msa_panel_recipe_dict(
                name=recipe_name,
                start_year=start_year,
                end_year=end_year,
                exclude_years=excluded_years,
                top_n=top_n,
                msa_definition_version=msa_definition_version,
                county_vintage=county_vintage,
            )
            parsed = load_recipe(recipe_data)
            artifacts = resolve_pipeline_artifacts(parsed, "longitudinal")
        except (RecipeLoadError, ExecutorError, PlannerError, ValueError) as exc:
            if use_json:
                _json_error(str(exc), code=2)
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(code=2) from exc

        recipe_exists = output.exists()
        if recipe_exists and not force and not dry_run:
            payload = {
                "status": "error",
                "error": f"Recipe file already exists: {output}. Use --force to overwrite.",
                "recipe_path": str(output),
                "recipe_exists": True,
                "artifacts": artifacts,
            }
            if use_json:
                _json_out(payload)
            else:
                typer.echo(payload["error"], err=True)
            raise typer.Exit(code=1)

        if not dry_run:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                yaml.safe_dump(recipe_data, sort_keys=False, default_flow_style=False),
                encoding="utf-8",
            )

        payload = {
            "status": "ok",
            "template": template,
            "recipe_path": str(output),
            "written": not dry_run,
            "recipe_exists": recipe_exists,
            "artifacts": artifacts,
            "included_years": _year_segment(
                start_year,
                end_year,
                exclude_years=excluded_years,
            ),
            "next_commands": {
                "preflight": f"hhplab build recipe-preflight --recipe {output} --json",
                "plan": f"hhplab build recipe-plan --recipe {output} --json",
                "execute": f"hhplab build recipe --recipe {output} --json",
            },
        }
        if use_json:
            _json_out(payload)
            return

        action = "Would write" if dry_run else "Wrote"
        typer.echo(f"{action} {output}")
        typer.echo(f"Panel artifact: {artifacts.get('panel_path')}")
        typer.echo(f"Next: {payload['next_commands']['preflight']}")
        return

    bases = list(overlap_basis or ["area"])
    invalid_bases = sorted(set(bases) - {"area", "population"})
    if invalid_bases:
        message = f"Unsupported overlap basis values {invalid_bases}; use area and/or population."
        if use_json:
            _json_error(message, code=2)
        raise typer.BadParameter(message, param_hint="--overlap-basis")

    if "population" in bases:
        missing = []
        if acs5_population_vintage is None:
            missing.append("--acs5-population-vintage")
        if tract_vintage is None:
            missing.append("--tract-vintage")
        if missing:
            message = (
                "Population overlap scaffolding requires "
                f"{', '.join(missing)} so the recipe can reference ACS5 tract "
                "total_population denominators."
            )
            if use_json:
                _json_error(message, code=2)
            raise typer.BadParameter(message, param_hint="--overlap-basis population")

    recipe_name = name or f"msa_coc_coverage_{year}"
    resolved_ranking_year = ranking_reference_year or year
    definition_token = "".join(c for c in msa_definition_version.lower() if c.isalnum())
    resolved_pep_path = (
        pep_msa_path
        or (
            "data/curated/pep/"
            f"pep__msa__Y{resolved_ranking_year}@M{definition_token}"
            f"xC{county_vintage}__wpopulation.parquet"
        )
    )
    recipe_data = _msa_coc_coverage_recipe_dict(
        name=recipe_name,
        year=year,
        top_n=top_n,
        ranking_population_source=ranking_population_source,
        ranking_reference_year=resolved_ranking_year,
        coc_boundary_vintage=coc_boundary_vintage,
        msa_definition_version=msa_definition_version,
        county_vintage=county_vintage,
        overlap_bases=bases,
        acs5_population_vintage=acs5_population_vintage,
        acs5_population_reference_year=acs5_population_reference_year,
        tract_vintage=tract_vintage,
        min_msa_area_coverage_share=min_msa_area_coverage_share,
        min_msa_population_coverage_share=min_msa_population_coverage_share,
        csv_sidecar=csv_sidecar,
        pep_msa_path=resolved_pep_path,
    )

    try:
        parsed = load_recipe(recipe_data)
        artifacts = resolve_pipeline_artifacts(parsed, "coverage")
    except (RecipeLoadError, ExecutorError, PlannerError) as exc:
        if use_json:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    coverage_path = Path(artifacts["msa_coc_coverage_path"])
    artifact_exists = coverage_path.exists()
    recipe_exists = output.exists()
    if recipe_exists and not force and not dry_run:
        payload = {
            "status": "error",
            "error": f"Recipe file already exists: {output}. Use --force to overwrite.",
            "recipe_path": str(output),
            "recipe_exists": True,
            "artifact_exists": artifact_exists,
            "artifacts": artifacts,
        }
        if use_json:
            _json_out(payload)
        else:
            typer.echo(payload["error"], err=True)
        raise typer.Exit(code=1)

    if artifact_exists and not allow_existing_artifact:
        payload = {
            "status": "error",
            "error": (
                f"Coverage artifact already exists: {coverage_path}. "
                "Use --allow-existing-artifact to scaffold anyway."
            ),
            "recipe_path": str(output),
            "recipe_exists": recipe_exists,
            "artifact_exists": True,
            "artifacts": artifacts,
        }
        if use_json:
            _json_out(payload)
        else:
            typer.echo(payload["error"], err=True)
        raise typer.Exit(code=1)

    if not dry_run:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            yaml.safe_dump(recipe_data, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )

    payload = {
        "status": "ok",
        "template": template,
        "recipe_path": str(output),
        "written": not dry_run,
        "recipe_exists": recipe_exists,
        "artifact_exists": artifact_exists,
        "artifacts": artifacts,
        "next_commands": {
            "preflight": f"hhplab build recipe-preflight --recipe {output} --json",
            "plan": f"hhplab build recipe-plan --recipe {output} --json",
            "execute": f"hhplab build recipe --recipe {output} --json",
        },
    }
    if use_json:
        _json_out(payload)
        return

    action = "Would write" if dry_run else "Wrote"
    typer.echo(f"{action} {output}")
    typer.echo(f"Coverage artifact: {artifacts['msa_coc_coverage_path']}")
    typer.echo(f"Next: {payload['next_commands']['preflight']}")


def _pipeline_payload(
    parsed: RecipeV1,
    results,
    *,
    storage_cfg,
) -> list[dict]:
    """Serialize pipeline execution results for JSON responses."""
    return [
        {
            "pipeline_id": r.pipeline_id,
            "success": r.success,
            "artifacts": resolve_pipeline_artifacts(
                parsed, r.pipeline_id, storage_config=storage_cfg,
            ),
            "steps": [
                {
                    "step_kind": s.step_kind,
                    "detail": s.detail,
                    "success": s.success,
                    "error": s.error,
                    "notes": s.notes,
                }
                for s in r.steps
            ],
        }
        for r in results
    ]


def _format_geometry(ref: object) -> str:
    """Render a GeometryRef-like object for human CLI output."""
    geo_type = ref.type
    vintage = getattr(ref, "vintage", None)
    source = getattr(ref, "source", None)
    if vintage is not None and source:
        return f"{geo_type}@{vintage}[{source}]"
    if vintage is not None:
        return f"{geo_type}@{vintage}"
    if source:
        return f"{geo_type}@{source}"
    return str(geo_type)


def _validate_recipe(
    parsed: RecipeV1,
    *,
    use_json: bool = False,
) -> tuple[list[str], list[str]]:
    """Run structural validation and return (warnings, errors) as string lists.

    Runs adapter validation only.  Dataset path checks are deferred to
    the plan-scoped preflight analyzer so that missing-dataset failures
    are reported consistently through the shared preflight output
    (``status=blocked``) rather than the legacy ``validation.errors``
    path.

    When *use_json* is True, validation output is suppressed (caller
    will include it in the JSON response).
    """
    # Run adapter registry validation
    diagnostics = validate_recipe_adapters(
        parsed, geometry_registry, dataset_registry,
    )
    adapter_errors = [d for d in diagnostics if d.level == "error"]
    adapter_warnings = [d for d in diagnostics if d.level == "warning"]

    if not use_json:
        for w in adapter_warnings:
            typer.echo(f"  Warning: {w.message}", err=True)

    all_warnings = [d.message for d in adapter_warnings]
    all_errors = [d.message for d in adapter_errors]
    return all_warnings, all_errors


def recipe_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-n",
            help="Validate and preflight only; do not execute the build.",
        ),
    ] = False,
    no_cache: Annotated[
        bool,
        typer.Option(
            "--no-cache",
            help="Disable asset caching (re-read every file from disk).",
        ),
    ] = False,
    skip_preflight: Annotated[
        bool,
        typer.Option(
            "--skip-preflight",
            help="Skip the preflight readiness check before execution.",
        ),
    ] = False,
    asset_store_root: Annotated[
        Path | None,
        typer.Option(
            "--asset-store-root",
            help="Override the asset store root directory.",
        ),
    ] = None,
    output_root: Annotated[
        Path | None,
        typer.Option(
            "--output-root",
            help="Override the output root directory for recipe products.",
        ),
    ] = None,
    use_json: _JSON_OPTION = False,
) -> None:
    """Load, validate, preflight, and execute a build recipe.

    This is the normal entrypoint for recipe execution. It loads the
    recipe, runs validation, runs the readiness preflight, and then
    executes the pipelines when all prerequisites are satisfied.

    Use ``hhplab build recipe-preflight`` when you want the readiness
    report without executing. Use ``hhplab build recipe-plan`` when you
    need to inspect the resolved task graph while authoring or debugging
    a recipe. Use ``--dry-run`` to run the same validation/preflight
    path without execution.

    Use ``--skip-preflight`` only when you need to bypass the check
    for debugging purposes.

    Examples:

        # Normal human workflow
        hhplab build recipe --recipe my_build.yaml

        # Automation / CI
        hhplab build recipe-preflight --recipe my_build.yaml --json
        hhplab build recipe --recipe my_build.yaml --json

        # Inspect resolved tasks while authoring/debugging
        hhplab build recipe-plan --recipe my_build.yaml --json
    """
    # 0. Ensure built-in adapters are registered
    register_defaults()

    # 1. Load and structurally validate the recipe
    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    if not use_json:
        typer.echo(
            f"Loaded recipe: {parsed.name} (version {parsed.version})",
        )
        if not parsed.transforms:
            typer.echo(
                "  Warning: No transforms defined; "
                "no build output will be produced.",
                err=True,
            )
        if not parsed.pipelines:
            typer.echo(
                "  Warning: No pipelines defined; "
                "no build output will be produced.",
                err=True,
            )

    # 2. Validate (legacy path checks + adapter validation)
    all_warnings, all_errors = _validate_recipe(parsed, use_json=use_json)

    if all_errors:
        if use_json:
            _json_out({
                "status": "error",
                "recipe_name": parsed.name,
                "validation": {
                    "warnings": all_warnings,
                    "errors": all_errors,
                },
            })
            raise typer.Exit(code=1)
        for e in all_errors:
            typer.echo(f"  Error: {e}", err=True)
        typer.echo(
            f"\nRecipe validation failed with {len(all_errors)} error(s).",
            err=True,
        )
        raise typer.Exit(code=1)

    if not use_json:
        if all_warnings:
            typer.echo(
                f"Recipe validated with {len(all_warnings)} warning(s).",
            )
        else:
            typer.echo("Recipe validated successfully.")

    # 2b. Preflight readiness check
    if not skip_preflight:
        pf_report = run_preflight(parsed)
        if not use_json:
            if pf_report.is_ready:
                typer.echo(
                    f"Preflight: {len(pf_report.findings)} finding(s), "
                    "all clear."
                )
                # Show warnings so users know about non-blocking issues
                for f in pf_report.findings:
                    if f.severity == Severity.WARNING:
                        typer.echo(
                            f"  Warning: {f.message}", err=True,
                        )
            else:
                typer.echo(
                    f"\nPreflight found {pf_report.blocking_count} "
                    "blocker(s):",
                    err=True,
                )
                for f in pf_report.blocking_findings():
                    typer.echo(f"  {f.message}", err=True)
                    if f.remediation:
                        typer.echo(
                            f"    Fix: {f.remediation.hint}", err=True,
                        )
                        if f.remediation.command:
                            typer.echo(
                                f"    Run: {f.remediation.command}",
                                err=True,
                            )
                typer.echo(
                    "\nRun 'hhplab build recipe-preflight --recipe "
                    f"{recipe}' for details.",
                    err=True,
                )

        if not pf_report.is_ready:
            if use_json:
                _json_out({
                    "status": "blocked",
                    "recipe_name": parsed.name,
                    "preflight": pf_report.to_dict(),
                })
            raise typer.Exit(code=1)

    if dry_run:
        if use_json:
            _json_out({
                "status": "ok",
                "recipe_name": parsed.name,
                "recipe_version": parsed.version,
                "validation": {
                    "warnings": all_warnings,
                    "errors": [],
                },
                "dry_run": True,
            })
        return

    # 3. Execute the build pipeline
    cache = RecipeCache(enabled=not no_cache)
    storage_cfg = load_config(
        asset_store_root=asset_store_root,
        output_root=output_root,
    )
    try:
        results = execute_recipe(
            parsed, cache=cache, quiet=use_json,
            storage_config=storage_cfg,
        )
    except ExecutorError as exc:
        if use_json:
            payload = {
                "status": "error",
                "recipe_name": parsed.name,
                "recipe_version": parsed.version,
                "error": str(exc),
            }
            if exc.partial_results:
                pipeline_items = _pipeline_payload(
                    parsed,
                    exc.partial_results,
                    storage_cfg=storage_cfg,
                )
                payload["pipelines"] = pipeline_items
                if len(pipeline_items) == 1:
                    payload["artifacts"] = pipeline_items[0]["artifacts"]
            _json_out(payload)
            raise typer.Exit(code=1) from None
        typer.echo(f"\nExecution error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if use_json:
        pipeline_items = _pipeline_payload(
            parsed,
            results,
            storage_cfg=storage_cfg,
        )
        payload = {
            "status": "ok",
            "recipe_name": parsed.name,
            "recipe_version": parsed.version,
            "validation": {
                "warnings": all_warnings,
                "errors": [],
            },
            "pipelines": pipeline_items,
        }
        if len(pipeline_items) == 1:
            payload["artifacts"] = pipeline_items[0]["artifacts"]
        _json_out(payload)
        return

    total_steps = sum(len(r.steps) for r in results)
    typer.echo(
        f"\nRecipe '{parsed.name}' executed: "
        f"{len(results)} pipeline(s), {total_steps} steps completed."
    )


def recipe_plan_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    use_json: _JSON_OPTION = False,
) -> None:
    """Resolve the execution plan without executing.

    Shows all resolved tasks (materialize, resample, join), input
    paths, effective geometries, transform selections, and task
    counts. Useful while authoring or debugging a recipe.

    This command does not perform the full readiness checks used by
    ``recipe-preflight``. For a no-execute readiness gate, use
    ``hhplab build recipe-preflight`` instead.

    Examples:

        hhplab build recipe-plan --recipe my_build.yaml

        hhplab build recipe-plan --recipe my_build.yaml --json
    """
    register_defaults()

    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    all_warnings, all_errors = _validate_recipe(parsed, use_json=use_json)

    if all_errors:
        if use_json:
            _json_out({
                "status": "error",
                "recipe_name": parsed.name,
                "validation": {
                    "warnings": all_warnings,
                    "errors": all_errors,
                },
            })
            raise typer.Exit(code=1)
        for e in all_errors:
            typer.echo(f"  Error: {e}", err=True)
        raise typer.Exit(code=1)

    plans = []
    for pipeline in parsed.pipelines:
        try:
            plan = resolve_plan(parsed, pipeline.id)
        except PlannerError as exc:
            if use_json:
                _json_error(
                    f"Pipeline '{pipeline.id}': {exc}",
                )
            typer.echo(f"Error: Pipeline '{pipeline.id}': {exc}", err=True)
            raise typer.Exit(code=1) from exc
        plans.append(plan)

    if use_json:
        storage_cfg = load_config()
        pipeline_payloads = []
        for plan in plans:
            payload = plan.to_dict()
            payload["artifacts"] = resolve_pipeline_artifacts(
                parsed,
                plan.pipeline_id,
                storage_config=storage_cfg,
            )
            pipeline_payloads.append(payload)
        _json_out({
            "status": "ok",
            "recipe_name": parsed.name,
            "recipe_version": parsed.version,
            "validation": {
                "warnings": all_warnings,
                "errors": [],
            },
            "pipelines": pipeline_payloads,
        })
        return

    # Human-readable output
    typer.echo(f"Recipe: {parsed.name} (version {parsed.version})")
    if all_warnings:
        typer.echo(f"  {len(all_warnings)} validation warning(s)")

    for plan in plans:
        total = (
            len(plan.materialize_tasks)
            + len(plan.resample_tasks)
            + len(plan.small_area_estimate_tasks)
            + len(plan.join_tasks)
        )
        typer.echo(f"\nPipeline '{plan.pipeline_id}' ({total} tasks):")

        for mt in plan.materialize_tasks:
            typer.echo(
                f"  [materialize] transforms: {mt.transform_ids}",
            )

        for rt in plan.resample_tasks:
            geo = rt.effective_geometry
            geo_str = _format_geometry(geo)
            to_geo_str = _format_geometry(rt.to_geometry)
            line = (
                f"  [resample] {rt.dataset_id} year={rt.year} "
                f"method={rt.method} geometry={geo_str} "
                f"to={to_geo_str}"
            )
            if rt.transform_id:
                line += f" via={rt.transform_id}"
            if rt.input_path:
                line += f" path={rt.input_path}"
            typer.echo(line)

        for st in plan.small_area_estimate_tasks:
            target_geo_str = _format_geometry(st.target_geometry)
            typer.echo(
                f"  [small_area_estimate] {st.output_dataset} year={st.year} "
                f"source={st.source_dataset} support={st.support_dataset} "
                f"target={target_geo_str} measures={st.measure_families}",
            )

        for jt in plan.join_tasks:
            typer.echo(
                f"  [join] datasets={jt.datasets} "
                f"year={jt.year} on={jt.join_on}",
            )


def recipe_provenance_cmd(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            "-m",
            help="Path to a .manifest.json file produced by a recipe build.",
        ),
    ],
    use_json: _JSON_OPTION = False,
) -> None:
    """Show provenance from a recipe build manifest.

    Displays the recipe identity, consumed assets (with SHA-256 hashes
    and sizes), and output path recorded during the build.

    Examples:

        hhplab build recipe-provenance \\
            --manifest panel__Y2020-2021@B2025.manifest.json
    """
    if not manifest.exists():
        if use_json:
            _json_error(f"Manifest not found: {manifest}")
        typer.echo(f"Error: Manifest not found: {manifest}", err=True)
        raise typer.Exit(code=1)

    m = read_manifest(manifest)

    if use_json:
        _json_out({"status": "ok", **m.to_dict()})
        return

    typer.echo(f"Recipe: {m.recipe_name} (v{m.recipe_version})")
    typer.echo(f"Pipeline: {m.pipeline_id}")
    typer.echo(f"Executed: {m.executed_at}")
    if m.output_path:
        typer.echo(f"Output: {m.output_path}")

    if m.assets:
        typer.echo(f"\nConsumed assets ({len(m.assets)}):")
        for a in m.assets:
            label = a.dataset_id or a.transform_id or ""
            size_kb = a.size / 1024
            typer.echo(
                f"  [{a.role}] {a.path}"
                f"  ({size_kb:.1f} KB, sha256:{a.sha256[:12]}...)"
            )
            if label:
                typer.echo(f"         id: {label}")


def recipe_export_cmd(
    manifest: Annotated[
        Path,
        typer.Option(
            "--manifest",
            "-m",
            help="Path to a .manifest.json file produced by a recipe build.",
        ),
    ],
    destination: Annotated[
        Path,
        typer.Option(
            "--destination",
            "-d",
            help="Destination directory for the replication bundle.",
        ),
    ],
    asset_store_root: Annotated[
        Path | None,
        typer.Option(
            "--asset-store-root",
            help="Override the asset store root directory.",
        ),
    ] = None,
    output_root: Annotated[
        Path | None,
        typer.Option(
            "--output-root",
            help="Override the output root directory.",
        ),
    ] = None,
    use_json: _JSON_OPTION = False,
) -> None:
    """Export a replication bundle from a recipe build manifest.

    Copies all consumed assets (datasets, crosswalks) into a
    self-contained directory alongside the manifest, so a replicator
    can reproduce the build without the original project tree.

    Examples:

        hhplab build recipe-export \\
            --manifest panel.manifest.json --destination /tmp/bundle
    """
    if not manifest.exists():
        if use_json:
            _json_error(f"Manifest not found: {manifest}")
        typer.echo(f"Error: Manifest not found: {manifest}", err=True)
        raise typer.Exit(code=1)

    m = read_manifest(manifest)
    project_root = Path.cwd()
    storage_cfg = load_config(
        asset_store_root=asset_store_root,
        output_root=output_root,
    )

    if not use_json:
        typer.echo(
            f"Exporting bundle for '{m.recipe_name}' "
            f"pipeline '{m.pipeline_id}'...",
        )

    do_export_bundle(m, project_root, destination, storage_config=storage_cfg)

    if use_json:
        _json_out({
            "status": "ok",
            "recipe_name": m.recipe_name,
            "pipeline_id": m.pipeline_id,
            "assets_copied": len(m.assets),
            "bundle_path": str(destination),
        })
        return

    typer.echo(f"  {len(m.assets)} asset(s) copied")
    typer.echo(f"  Manifest written to {destination / 'manifest.json'}")
    typer.echo(f"Bundle: {destination}")


def _render_preflight_human(report: PreflightReport) -> None:
    """Render a preflight report as human-readable text."""
    typer.echo(
        f"Recipe: {report.recipe_name} (version {report.recipe_version})"
    )
    typer.echo(
        f"Universe: {min(report.universe_years)}-"
        f"{max(report.universe_years)} "
        f"({len(report.universe_years)} years)"
    )

    for ps in report.pipelines:
        if ps.plan_error:
            typer.echo(
                f"\nPipeline '{ps.pipeline_id}': "
                f"PLAN ERROR - {ps.plan_error}",
                err=True,
            )
        else:
            typer.echo(
                f"\nPipeline '{ps.pipeline_id}': "
                f"{ps.task_count} tasks resolved"
            )

    if not report.findings:
        typer.echo("\nAll prerequisites satisfied. Ready to build.")
        return

    blockers = [f for f in report.findings if f.is_blocking]
    warnings = [
        f for f in report.findings if f.severity == Severity.WARNING
    ]

    if blockers:
        typer.echo(f"\nBlockers ({len(blockers)}):", err=True)
        for f in blockers:
            typer.echo(f"  ERROR: {f.message}", err=True)
            if f.remediation:
                typer.echo(
                    f"    Fix: {f.remediation.hint}", err=True,
                )
                if f.remediation.command:
                    typer.echo(
                        f"    Run: {f.remediation.command}", err=True,
                    )

    if warnings:
        typer.echo(f"\nWarnings ({len(warnings)}):", err=True)
        for f in warnings:
            typer.echo(f"  WARNING: {f.message}", err=True)

    if blockers:
        typer.echo(
            f"\nPreflight FAILED: {len(blockers)} blocker(s), "
            f"{len(warnings)} warning(s).",
            err=True,
        )
    else:
        typer.echo(
            f"\nPreflight passed with {len(warnings)} warning(s). "
            "Ready to build."
        )


def recipe_preflight_cmd(
    recipe: Annotated[
        Path,
        typer.Option(
            "--recipe",
            "-r",
            help="Path to a YAML recipe file.",
        ),
    ],
    use_json: _JSON_OPTION = False,
    gaps: Annotated[
        bool,
        typer.Option(
            "--gaps",
            help="Emit only the data-gaps manifest (implies --json).",
        ),
    ] = False,
    non_interactive: Annotated[
        bool,
        typer.Option(
            "--non-interactive",
            help=(
                "Accept the documented automation flag when invoking "
                "'hhplab build recipe-preflight' directly."
            ),
        ),
    ] = False,
) -> None:
    """Check all recipe prerequisites in one pass without executing.

    Resolves execution plans, inspects dataset paths, transform
    artifacts, dataset schemas, and support-dataset requirements for
    weighted transforms.  Only checks dataset-years required by the
    resolved plan (recipe-scoped).  Reports all issues at once with
    actionable fix suggestions rather than failing on the first
    missing prerequisite.

    Use --json for machine-readable output suitable for automation or CI.
    Use --gaps for a focused data-gaps manifest with per-gap metadata,
    severity classification, and remediation hints.

    Examples:

        hhplab build recipe-preflight --recipe my_build.yaml

        hhplab build recipe-preflight --recipe my_build.yaml --json

        hhplab build recipe-preflight --recipe my_build.yaml --gaps
    """
    _ = non_interactive
    register_defaults()

    try:
        parsed = load_recipe(recipe)
    except RecipeLoadError as exc:
        if use_json or gaps:
            _json_error(str(exc), code=2)
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=2) from exc

    report = run_preflight(parsed)

    if gaps:
        _json_out({"status": "ok", **report.gaps_manifest()})
        raise typer.Exit(code=1 if report.blocking_count > 0 else 0)

    if use_json:
        _json_out({
            "status": "ok" if report.is_ready else "blocked",
            **report.to_dict(),
        })
        raise typer.Exit(code=1 if not report.is_ready else 0)

    _render_preflight_human(report)
    if not report.is_ready:
        raise typer.Exit(code=1)
