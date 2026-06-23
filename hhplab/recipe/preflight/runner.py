"""Preflight runner and checker registry."""

from __future__ import annotations

import re
from pathlib import Path

from hhplab.recipe.default_adapters import register_defaults
from hhplab.recipe.planner import PlannerError, ResampleTask, resolve_plan
from hhplab.recipe.preflight.checks import (
    _check_acs1_temporal_alignment_guidance,
    _check_acs5_tract_schema_contracts,
    _check_adapter_validation,
    _check_containment_artifacts,
    _check_ct_county_alignment,
    _check_dataset_paths,
    _check_dataset_provenance,
    _check_dataset_schemas,
    _check_dataset_year_values,
    _check_map_artifacts,
    _check_msa_coc_coverage_artifacts,
    _check_msa_coc_coverage_sources,
    _check_msa_coc_panel_sources,
    _check_msa_fractional_rollup_artifacts,
    _check_pep_decennial_tract_mediated_inputs,
    _check_primary_msa_artifacts,
    _check_sae_task_paths,
    _check_sae_task_schemas,
    _check_support_datasets,
    _check_target_selectors,
    _check_temporal_alignment_guidance,
    _check_transforms,
)
from hhplab.recipe.preflight.models import (
    FindingKind,
    PipelineSummary,
    PreflightFinding,
    PreflightReport,
    Remediation,
    Severity,
)
from hhplab.recipe.recipe_schema import RecipeV1
from hhplab.recipe.schema_common import expand_year_spec


def run_preflight(
    recipe: RecipeV1,
    project_root: Path | None = None,
) -> PreflightReport:
    """Run a complete preflight analysis on a recipe.

    Resolves each pipeline plan, checks dataset paths, transform
    artifacts, and dataset schemas.  Returns a structured report
    without executing any build steps.

    Parameters
    ----------
    recipe : RecipeV1
        A structurally valid recipe.
    project_root : Path | None
        Project root for resolving paths.  Defaults to cwd.

    Returns
    -------
    PreflightReport
        Structured report with all findings.
    """
    if project_root is None:
        project_root = Path.cwd()

    register_defaults()

    universe_years = expand_year_spec(recipe.universe)
    report = PreflightReport(
        recipe_name=recipe.name,
        recipe_version=recipe.version,
        universe_years=universe_years,
    )

    # 1. Adapter validation
    report.findings.extend(_check_adapter_validation(recipe))
    report.findings.extend(_check_map_artifacts(recipe, project_root))
    report.findings.extend(_check_containment_artifacts(recipe, project_root))
    report.findings.extend(_check_msa_coc_coverage_artifacts(recipe, project_root))
    report.findings.extend(_check_msa_fractional_rollup_artifacts(recipe, project_root))
    report.findings.extend(_check_primary_msa_artifacts(recipe, project_root))
    report.findings.extend(_check_target_selectors(recipe, project_root))

    # 2. Resolve plans and collect tasks (before path checks so we
    #    can scope path checking to plan-required dataset-years only)
    report.findings.extend(_check_temporal_alignment_guidance(recipe))

    all_resample_tasks: list[ResampleTask] = []
    pipeline_resample_tasks: list[tuple[str, ResampleTask]] = []
    needed_transforms: set[str] = set()

    for pipeline in recipe.pipelines:
        try:
            plan = resolve_plan(recipe, pipeline.id)
            summary = PipelineSummary(
                pipeline_id=pipeline.id,
                plan=plan,
                task_count=(
                    len(plan.materialize_tasks)
                    + len(plan.resample_tasks)
                    + len(plan.join_tasks)
                    + len(plan.small_area_estimate_tasks)
                ),
            )
            report.pipelines.append(summary)

            # Collect transforms and resample tasks
            for mt in plan.materialize_tasks:
                needed_transforms.update(mt.transform_ids)
            for rt in plan.resample_tasks:
                if rt.transform_id:
                    needed_transforms.add(rt.transform_id)
                pipeline_resample_tasks.append((pipeline.id, rt))
            all_resample_tasks.extend(plan.resample_tasks)
            report.findings.extend(
                _check_sae_task_paths(
                    recipe,
                    project_root,
                    plan.small_area_estimate_tasks,
                )
            )
            report.findings.extend(
                _check_sae_task_schemas(
                    project_root,
                    plan.small_area_estimate_tasks,
                )
            )

        except PlannerError as exc:
            err_str = str(exc)
            summary = PipelineSummary(
                pipeline_id=pipeline.id,
                plan_error=err_str,
            )
            report.pipelines.append(summary)

            # Surface planner errors as both PLANNER_ERROR and
            # UNCOVERED_YEARS when they indicate year-coverage gaps,
            # so the gaps manifest includes them.
            report.findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.PLANNER_ERROR,
                    message=f"Pipeline '{pipeline.id}': {exc}",
                    pipeline_id=pipeline.id,
                )
            )
            if "not covered" in err_str or "no file_set segment" in err_str:
                # Extract dataset_id from common planner error patterns
                ds_id_from_err: str | None = None
                if "Dataset '" in err_str:
                    start = err_str.index("Dataset '") + 9
                    end = err_str.index("'", start)
                    ds_id_from_err = err_str[start:end]

                # Extract the specific missing year from the error
                _year_match = re.search(r"year (\d{4})", err_str)
                missing_years = [int(_year_match.group(1))] if _year_match else universe_years

                report.findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.UNCOVERED_YEARS,
                        message=f"Pipeline '{pipeline.id}': {exc}",
                        pipeline_id=pipeline.id,
                        dataset_id=ds_id_from_err,
                        years=missing_years,
                        remediation=Remediation(
                            hint=(
                                f"Year(s) {missing_years} not covered by "
                                f"dataset '{ds_id_from_err or '?'}'. "
                                f"Extend dataset year coverage or narrow "
                                f"the recipe universe "
                                f"({min(universe_years)}-{max(universe_years)})."
                            ),
                        ),
                    )
                )

    report.findings.extend(
        _check_acs1_temporal_alignment_guidance(
            recipe,
            project_root,
            pipeline_resample_tasks,
        ),
    )
    report.findings.extend(
        _check_msa_coc_panel_sources(
            recipe,
            pipeline_resample_tasks,
        ),
    )
    report.findings.extend(
        _check_msa_coc_coverage_sources(
            recipe,
            pipeline_resample_tasks,
        ),
    )

    # 3. Dataset path checks (plan-scoped: only checks paths required
    #    by the resolved execution plan)
    report.findings.extend(
        _check_dataset_paths(recipe, project_root, all_resample_tasks),
    )

    # 4. Dataset year-value checks catch files that exist but cannot satisfy
    #    executor filtering for a planned task year.
    report.findings.extend(
        _check_dataset_year_values(recipe, project_root, all_resample_tasks),
    )

    # 5. Dataset provenance and schema checks for ACS tract caches
    report.findings.extend(
        _check_dataset_provenance(recipe, project_root, all_resample_tasks),
    )
    report.findings.extend(
        _check_acs5_tract_schema_contracts(recipe, project_root, all_resample_tasks),
    )

    # 6. Transform artifact checks
    report.findings.extend(
        _check_transforms(recipe, project_root, needed_transforms),
    )

    # 7. Dataset schema probes
    report.findings.extend(
        _check_dataset_schemas(recipe, project_root, all_resample_tasks),
    )

    # 8. Support-dataset probes for weighted transforms
    report.findings.extend(
        _check_support_datasets(
            recipe,
            project_root,
            needed_transforms,
            universe_years,
        ),
    )

    # 8. PEP decennial tract-mediated baseline and denominator checks
    report.findings.extend(
        _check_pep_decennial_tract_mediated_inputs(
            recipe,
            project_root,
            all_resample_tasks,
        ),
    )

    # 9. Connecticut county-transition detection and bridge readiness
    report.findings.extend(
        _check_ct_county_alignment(recipe, project_root, pipeline_resample_tasks),
    )

    return report
