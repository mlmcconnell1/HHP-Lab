"""Preflight validator checks grouped behind the runner registry."""

from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from hhplab.artifacts.naming.naming import (
    acs5_tracts_filename,
    block_geometry_path,
    coc_base_filename,
    coc_base_path,
    coc_urban_fraction_path,
    county_path,
    decennial_tracts_filename,
    metro_boundaries_path,
    msa_boundaries_path,
    msa_coc_block_population_xwalk_path,
    msa_coc_xwalk_path,
    msa_county_membership_path,
    msa_definitions_path,
    pl_block_population_path,
    tract_path,
    tract_xwalk_path,
)
from hhplab.geographies.coc.coc_io import resolve_curated_boundary_path
from hhplab.geographies.coc.ct_planning_regions import (
    CT_LEGACY_COUNTY_VINTAGE,
    CT_PLANNING_REGION_VINTAGE,
    build_ct_county_planning_region_crosswalk,
    is_ct_legacy_county_fips,
    is_ct_planning_region_fips,
)
from hhplab.recipe.adapters import (
    dataset_registry,
    geometry_registry,
    validate_recipe_adapters,
)
from hhplab.recipe.default_adapters import register_defaults
from hhplab.recipe.executor.core import ExecutorError
from hhplab.recipe.executor.msa_coc_panel import build_msa_coc_containment_spec
from hhplab.recipe.executor.resample import derived_measure_required_columns
from hhplab.recipe.planner import (
    ResampleTask,
    SmallAreaEstimateTask,
    _resolve_dataset_year,
)
from hhplab.recipe.preflight.models import (
    FindingKind,
    PreflightFinding,
    Remediation,
    Severity,
)
from hhplab.recipe.probes import (
    get_weighted_transform_requirements,
    probe_acs5_tract_schema_contract,
    probe_acs5_tract_translation_provenance,
    probe_dataset_schema,
    probe_geo_column,
    probe_interpolate_to_month_data,
    probe_measures,
    probe_static_broadcast,
    probe_support_dataset,
    probe_temporal_filter,
    probe_transform_path,
    probe_year_column,
)
from hhplab.recipe.recipe_schema import (
    ContainmentSpec,
    JoinStep,
    RecipeV1,
    ResampleStep,
    SmallAreaEstimateStep,
    TemporalFilter,
)
from hhplab.recipe.schema_common import expand_year_spec
from hhplab.sources.census.acs.variables import ACS5_SAE_SUPPORT_COLUMNS
from hhplab.sources.census.acs.variables_acs1 import (
    ACS1_SAE_SOURCE_COLUMNS,
    ACS1_UNAVAILABLE_VINTAGES,
)
from hhplab.storage.config import load_config


def _check_dataset_paths(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Check that plan-required dataset files exist on disk.

    Only checks paths for (dataset_id, year) pairs that appear in the
    resolved execution plan, so years outside the recipe universe or
    datasets not referenced by any pipeline are ignored.
    """
    findings: list[PreflightFinding] = []
    policy = recipe.validation.missing_dataset
    policy_extra: dict[str, str] = policy.model_extra or {}

    # Group tasks by dataset to aggregate missing years
    by_dataset: dict[str, list[ResampleTask]] = {}
    for task in resample_tasks:
        by_dataset.setdefault(task.dataset_id, []).append(task)

    for ds_id, tasks in by_dataset.items():
        ds = recipe.datasets.get(ds_id)
        if ds is None:
            continue

        is_optional = ds.optional
        per_ds_policy = policy_extra.get(ds_id)
        severity = (
            Severity.WARNING
            if (
                per_ds_policy == "warn"
                or (per_ds_policy is None and is_optional)
                or (per_ds_policy is None and not is_optional and policy.default == "warn")
            )
            else Severity.ERROR
        )

        # Deduplicate: check each resolved path once
        checked_paths: set[str] = set()
        missing_years: list[int] = []
        missing_tasks: list[ResampleTask] = []
        missing_paths: set[str] = set()

        for task in tasks:
            path = task.input_path
            if path is None:
                # Static dataset with no file_set — use ds.path
                if ds.path is not None and ds.path not in checked_paths:
                    checked_paths.add(ds.path)
                    if not (project_root / ds.path).exists():
                        findings.append(
                            PreflightFinding(
                                severity=severity,
                                kind=FindingKind.MISSING_DATASET,
                                message=(f"Dataset '{ds_id}' path not found: {ds.path}"),
                                dataset_id=ds_id,
                                remediation=_dataset_remediation(ds_id, ds),
                            )
                        )
                continue

            if path in checked_paths:
                continue
            checked_paths.add(path)

            if not (project_root / path).exists():
                missing_years.append(task.year)
                missing_tasks.append(task)
                missing_paths.add(path)

        if missing_years:
            if ds.provider == "hud" and ds.product == "hic" and len(missing_years) == 1:
                year = missing_years[0]
                msg = (
                    f"No HIC data found for {year}; run hhplab ingest hic --year {year} "
                    f"or place the HUD file under data/raw/hic/{year}/."
                )
            elif len(missing_paths) == 1:
                msg = f"Dataset '{ds_id}' path not found: {next(iter(missing_paths))}"
            else:
                msg = (
                    f"Dataset '{ds_id}': {len(missing_years)} "
                    f"file(s) missing for years {missing_years}"
                )
            findings.append(
                PreflightFinding(
                    severity=severity,
                    kind=FindingKind.MISSING_DATASET,
                    message=msg,
                    dataset_id=ds_id,
                    years=missing_years,
                    remediation=_dataset_remediation(
                        ds_id,
                        ds,
                        years=_remediation_years_for_missing_dataset(ds, missing_tasks),
                    ),
                )
            )

    return findings


def _check_dataset_year_values(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Verify existing inputs contain rows for each planned task year.

    Path and schema checks can pass while execution later fails because the
    resolved year column has no rows for a specific task year. This check keeps
    the validation plan-scoped and reads only the resolved year column.
    """
    findings: list[PreflightFinding] = []
    tasks_by_input: dict[tuple[str, str], list[ResampleTask]] = {}

    for task in resample_tasks:
        if task.input_path is None:
            continue
        tasks_by_input.setdefault((task.dataset_id, task.input_path), []).append(task)

    for (dataset_id, input_path), tasks in tasks_by_input.items():
        ds = recipe.datasets.get(dataset_id)
        if ds is None:
            continue

        full_path = project_root / input_path
        if not full_path.exists():
            continue

        schema_result = probe_dataset_schema(full_path)
        if not schema_result.ok or schema_result.detail is None:
            continue

        columns = schema_result.detail["columns"]
        year_result = probe_year_column(columns, ds.year_column)
        if not year_result.ok or year_result.detail is None:
            continue

        year_column = year_result.detail.get("year_column")
        if year_column is None:
            continue

        try:
            df = pd.read_parquet(full_path, columns=[year_column])
        except (FileNotFoundError, OSError, ValueError, KeyError):
            continue

        if year_column not in df.columns:
            continue

        years = pd.to_numeric(df[year_column], errors="coerce")
        missing_years = sorted(
            {
                task.year
                for task in tasks
                if not bool((years == task.year).any())
            }
        )
        if not missing_years:
            continue

        if ds.provider == "hud" and ds.product == "hic" and len(missing_years) == 1:
            year = missing_years[0]
            message = (
                f"No HIC data found for {year}; run hhplab ingest hic --year {year} "
                f"or place the HUD file under data/raw/hic/{year}/."
            )
            remediation = _dataset_remediation(dataset_id, ds, years=missing_years)
        else:
            message = (
                f"Dataset '{dataset_id}' ({input_path}): no rows for planned "
                f"year(s) {missing_years} after filtering {year_column}."
            )
            remediation = Remediation(
                hint=(
                    f"Set year_column to the artifact column that contains the "
                    f"planned analysis/source years, or rebuild {input_path} so "
                    f"it includes rows where {year_column} is in {missing_years}."
                ),
            )

        findings.append(
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.UNCOVERED_YEARS,
                message=message,
                dataset_id=dataset_id,
                years=missing_years,
                remediation=remediation,
            )
        )

    return findings


def _check_dataset_provenance(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Block known-stale translated ACS tract caches via provenance checks."""
    findings: list[PreflightFinding] = []
    tasks_by_path: dict[tuple[str, str], list[ResampleTask]] = {}

    for task in resample_tasks:
        if task.input_path is None:
            continue
        tasks_by_path.setdefault((task.dataset_id, task.input_path), []).append(task)

    for (dataset_id, input_path), tasks in tasks_by_path.items():
        ds = recipe.datasets.get(dataset_id)
        if ds is None:
            continue

        full_path = project_root / input_path
        if not full_path.exists():
            continue

        result = probe_acs5_tract_translation_provenance(
            dataset_id=dataset_id,
            dataset_spec=ds,
            effective_geometry=tasks[0].effective_geometry,
            path=full_path,
            path_label=input_path,
        )
        if result.ok:
            continue

        remediation_command = (
            result.detail.get("remediation_command") if result.detail is not None else None
        )
        findings.append(
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.DATASET_PROVENANCE,
                message=result.message or "Dataset provenance validation failed.",
                dataset_id=dataset_id,
                years=sorted(task.year for task in tasks),
                remediation=Remediation(
                    hint=(
                        "Rebuild the translated ACS tract cache so its provenance "
                        "records the source and target tract vintages."
                    ),
                    command=(str(remediation_command) if remediation_command is not None else None),
                ),
            )
        )

    return findings


def _check_acs5_tract_schema_contracts(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Block stale ACS5 tract caches that predate the current canonical schema."""
    findings: list[PreflightFinding] = []
    tasks_by_path: dict[tuple[str, str], list[ResampleTask]] = {}

    for task in resample_tasks:
        if task.input_path is None:
            continue
        tasks_by_path.setdefault((task.dataset_id, task.input_path), []).append(task)

    for (dataset_id, input_path), tasks in tasks_by_path.items():
        ds = recipe.datasets.get(dataset_id)
        if ds is None:
            continue

        full_path = project_root / input_path
        if not full_path.exists():
            continue

        result = probe_acs5_tract_schema_contract(
            dataset_id=dataset_id,
            dataset_spec=ds,
            effective_geometry=tasks[0].effective_geometry,
            path=full_path,
            path_label=input_path,
        )
        if result.ok:
            continue

        schema = pq.read_schema(full_path)
        available_columns = set(schema.names)
        required_columns = {
            column
            for task in tasks
            for column in [
                *task.measures,
                *(
                    required
                    for config in (task.derived_measures or {}).values()
                    for required in derived_measure_required_columns(
                        config,
                        available_columns=available_columns,
                    )
                ),
            ]
        }
        has_derived_measures = any(task.derived_measures for task in tasks)
        if has_derived_measures and required_columns and required_columns <= available_columns:
            continue

        remediation_command = (
            result.detail.get("remediation_command") if result.detail is not None else None
        )
        findings.append(
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.MISSING_COLUMN,
                message=result.message or "Dataset schema validation failed.",
                dataset_id=dataset_id,
                years=sorted(task.year for task in tasks),
                remediation=Remediation(
                    hint=(
                        "Rebuild the ACS5 tract cache so it includes the current "
                        "canonical output schema."
                    ),
                    command=(str(remediation_command) if remediation_command is not None else None),
                ),
            )
        )

    return findings


def _dataset_remediation(ds_id: str, ds, *, years: list[int] | None = None) -> Remediation:
    """Build a remediation hint for a missing dataset."""
    from hhplab.sources.census.acs.variables_acs1 import ACS1_UNAVAILABLE_VINTAGES

    provider = ds.provider
    product = ds.product

    if provider == "hud" and product == "hic":
        year_note = ""
        if years:
            year_note = f" for year(s) {', '.join(str(year) for year in sorted(set(years)))}"
        return Remediation(
            hint=(
                f"Ingest HUD HIC data for dataset '{ds_id}'{year_note}, or place "
                "the HUD source file under data/raw/hic/<year>/ before parsing."
            ),
            command="hhplab ingest hic --year <year>",
        )

    # Detect ACS 1-year datasets that span unavailable vintages (e.g. 2020).
    if product == "acs1" and years:
        unavailable = sorted(set(years) & ACS1_UNAVAILABLE_VINTAGES)
        if unavailable:
            years_str = ", ".join(str(y) for y in unavailable)
            return Remediation(
                hint=(
                    f"ACS 1-year data for vintage(s) {years_str} is not "
                    f"available from Census (data collection was disrupted by "
                    f"COVID-19 in 2020). No ingest command can succeed for "
                    f"these vintages. Exclude {years_str}, choose an explicit "
                    "fallback vintage, or ingest the 2020 experimental tables "
                    "from a non-API source."
                ),
                command=None,
            )

    if provider == "census" and product == "acs1":
        native_type = getattr(ds.native_geometry, "type", None)
        if native_type == "county":
            return Remediation(
                hint=(
                    f"Ingest county-native Census ACS 1-year data for dataset "
                    f"'{ds_id}'. County ACS1 is sparse by Census publication "
                    "threshold; non-threshold counties are absent by design."
                ),
                command="hhplab ingest acs1-county --vintage <year>",
            )
        if native_type == "metro":
            return Remediation(
                hint=f"Ingest metro-native Census ACS 1-year data for dataset '{ds_id}'.",
                command="hhplab ingest acs1-metro --vintage <year>",
            )

    if provider == "census" and product in {"acs1_poverty", "acs1_imputation_target"}:
        unavailable_note = ""
        if years and sorted(set(years) & ACS1_UNAVAILABLE_VINTAGES):
            unavailable_note = (
                " Standard ACS1 vintage 2020 is unavailable through the Census API; "
                "use an explicit fallback or non-API experimental source for any "
                "file_set path that resolves to A2020."
            )
        return Remediation(
            hint=(
                f"Dataset '{ds_id}' requests ACS1 tract poverty artifacts, but "
                "direct Census ACS 1-year tract poverty tables are not published "
                "through the standard Census API. Build or provide the modeled "
                "acs1_poverty_tracts artifact from ACS1 county controls, ACS1 "
                "metro fallback controls, CoC/county overlap policy inputs, and "
                "ACS5 tract support; downstream recipes can then aggregate "
                "imputed poverty counts/universes, recompute rates, and retain "
                "county-vs-metro control metadata. "
                f"{unavailable_note}"
            ),
            command=None,
        )

    if provider == "census" and product in {"acs", "acs5"}:
        native_type = getattr(ds.native_geometry, "type", None)
        if native_type == "tract":
            tract_vintage = getattr(ds.native_geometry, "vintage", None)
            command = "hhplab ingest acs5-tract --acs <acs-years>"
            if tract_vintage is not None:
                command += f" --tracts {tract_vintage}"
            return Remediation(
                hint=(
                    f"Ingest Census ACS 5-year tract data for dataset '{ds_id}'. "
                    "SAE support artifacts must include the requested tract "
                    "distribution component columns."
                ),
                command=command,
            )

    if provider == "census" and product == "pep":
        native_type = getattr(ds.native_geometry, "type", None)
        if native_type == "msa":
            source = getattr(ds.native_geometry, "source", None) or "census_msa_2023"
            county_vintage = _pep_msa_county_vintage_for_remediation(ds)
            year_arg = _format_years_for_command(years)
            return Remediation(
                hint=(
                    f"Materialize MSA-level Census PEP population artifacts for "
                    f"dataset '{ds_id}' from county PEP estimates and the MSA "
                    "county membership table."
                ),
                command=(
                    "hhplab aggregate pep --target-geo msa "
                    f"--msa-definition-version {source} --counties {county_vintage} "
                    f"--years {year_arg}"
                ),
            )

    if provider == "medsl" and product == "president":
        return Remediation(
            hint=(
                f"Build MEDSL county presidential measures for dataset '{ds_id}'. "
                "Stage data/raw/medsl/countypres_2000-2024.tab, ingest the "
                "normalized county returns, then materialize the county-year "
                "political measures. In recipes, aggregate vote counts first "
                "and derive target-level shares or ratios from summed counts "
                "where possible."
            ),
            command=(
                "hhplab ingest medsl-presidential --force && "
                "hhplab build medsl-president-county --force"
            ),
        )

    if provider == "vera" and product == "incarceration_trends":
        return Remediation(
            hint=(
                f"Ingest Vera county incarceration trends for dataset '{ds_id}'. "
                "The curated artifact is county-year native. In recipes, aggregate "
                "count and denominator columns first, then derive incarceration "
                "rates at the target geography from the summed columns."
            ),
            command="hhplab ingest vera-incarceration --force",
        )

    if provider == "census" and product == "urban_fraction":
        boundary_vintage = ds.params.get("boundary_vintage") or getattr(
            ds.native_geometry,
            "vintage",
            None,
        )
        decennial_vintage = ds.params.get("decennial_vintage") or ds.params.get("decennial")
        urban_area_vintage = ds.params.get("urban_area_vintage") or decennial_vintage
        block_vintage = ds.params.get("block_vintage") or decennial_vintage
        if (
            boundary_vintage is not None
            and decennial_vintage is not None
            and urban_area_vintage is not None
            and block_vintage is not None
        ):
            artifact = coc_urban_fraction_path(
                boundary_vintage=boundary_vintage,
                urban_area_vintage=urban_area_vintage,
                block_vintage=block_vintage,
                decennial_vintage=decennial_vintage,
                base_dir="data",
            )
            command = (
                "hhplab build urban-fraction "
                f"--boundary {boundary_vintage} "
                f"--urban-area-vintage {urban_area_vintage} "
                f"--decennial {decennial_vintage} "
                f"--block-vintage {block_vintage}"
            )
            return Remediation(
                hint=(
                    f"Build the CoC urban population fraction artifact for dataset "
                    f"'{ds_id}' at {artifact}. Recipes should treat this as a "
                    "static CoC covariate and set params.broadcast_static=true "
                    "when spanning multiple years."
                ),
                command=command,
            )
        return Remediation(
            hint=(
                f"Dataset '{ds_id}' needs params.boundary_vintage or "
                "native_geometry.vintage plus params.decennial_vintage before "
                "HHP-Lab can resolve the urban fraction artifact path."
            ),
            command=(
                "hhplab build urban-fraction --boundary <year> "
                "--urban-area-vintage <year> --decennial <year>"
            ),
        )

    if provider == "bls" and product == "laus":
        return Remediation(
            hint=(
                f"Ingest BLS LAUS annual-average metro data for dataset '{ds_id}' "
                "for every requested panel year."
            ),
            command="hhplab ingest laus-metro --year <year>",
        )

    if provider == "prism" and product == "temperature":
        variable = ds.params.get("variable") or "<variable>"
        month = ds.params.get("month") or "<month>"
        county_vintage = getattr(ds.native_geometry, "vintage", None) or "<county-vintage>"
        year = years[0] if years and len(set(years)) == 1 else "<year>"
        return Remediation(
            hint=(
                f"Download the PRISM monthly temperature ZIP for dataset '{ds_id}', "
                "then materialize the county monthly temperature artifact."
            ),
            command=(
                f"hhplab ingest prism --variable {variable} --year {year} "
                f"--month {month} && hhplab build prism-county --variable {variable} "
                f"--year {year} --month {month} --county-vintage {county_vintage}"
            ),
        )

    return Remediation(
        hint=(f"Ingest {provider}/{product} data for dataset '{ds_id}'."),
        command=f"hhplab ingest {product}" if product else None,
    )


def _format_years_for_command(years: list[int] | None) -> str:
    """Return a compact year spec for remediation commands."""
    if not years:
        return "<years>"
    unique_years = sorted(set(years))
    if unique_years == list(range(unique_years[0], unique_years[-1] + 1)):
        return f"{unique_years[0]}-{unique_years[-1]}"
    return ",".join(str(year) for year in unique_years)


def _pep_msa_county_vintage_for_remediation(ds) -> str:
    """Return the county vintage needed to build a PEP MSA recipe artifact."""
    county_vintage = ds.params.get("county_vintage")
    if county_vintage is not None:
        return str(county_vintage)

    native_vintage = getattr(ds.native_geometry, "vintage", None)
    if native_vintage is not None:
        return str(native_vintage)

    if ds.path is not None:
        path_vintage = _county_vintage_from_encoded_path(ds.path)
        if path_vintage is not None:
            return path_vintage

    if ds.file_set is not None:
        for seg in ds.file_set.segments:
            for override_path in seg.overrides.values():
                override_vintage = _county_vintage_from_encoded_path(override_path)
                if override_vintage is not None:
                    return override_vintage

            years = sorted(expand_year_spec(seg.years))
            if not years:
                continue
            sample_year = years[0]
            render_ctx: dict[str, object] = {"year": sample_year}
            render_ctx.update(seg.constants)
            render_ctx.update(
                {key: sample_year + offset for key, offset in seg.year_offsets.items()}
            )
            try:
                rendered_path = ds.file_set.path_template.format(**render_ctx)
            except (KeyError, IndexError, ValueError):
                rendered_path = ds.file_set.path_template
            template_vintage = _county_vintage_from_encoded_path(rendered_path)
            if template_vintage is not None:
                return template_vintage

    return "<county-vintage>"


def _county_vintage_from_encoded_path(path: str) -> str | None:
    match = re.search(r"xC(?P<vintage>[A-Za-z0-9-]+)(?=__|[./@]|$)", path)
    if match is None:
        return None
    return match.group("vintage")


def _msa_transform_remediation(
    transform_id: str, transform, missing_inputs: list[str]
) -> Remediation:
    """Build an actionable remediation hint for an MSA transform artifact."""
    from pathlib import Path

    from hhplab.recipe.executor.transforms import (
        _county_vintage_from_msa_definition_version,
        _identify_msa_and_base,
    )

    msa_ref, base_ref = _identify_msa_and_base(transform.from_, transform.to)
    definition_version = msa_ref.source if msa_ref is not None else None

    if base_ref.type == "coc" and base_ref.vintage is not None and definition_version is not None:
        county_vintage = _county_vintage_from_msa_definition_version(definition_version)
        crosswalk_command = (
            "hhplab generate msa-xwalk "
            f"--boundary {base_ref.vintage} "
            f"--definition-version {definition_version} "
            f"--counties {county_vintage}"
        )
    else:
        crosswalk_command = None

    if not missing_inputs:
        return Remediation(
            hint=(
                f"Generate the cached CoC-to-MSA crosswalk artifact for transform '{transform_id}'."
            ),
            command=crosswalk_command,
        )

    details: list[str] = []
    commands: list[str] = []
    for missing in missing_inputs:
        name = Path(missing).name
        if name.startswith("msa_county_membership__") and definition_version is not None:
            details.append(f"missing MSA county membership artifact '{missing}'")
            commands.append(f"hhplab generate msa --definition-version {definition_version}")
            continue
        if name.startswith("msa_definitions__") and definition_version is not None:
            details.append(f"missing MSA definitions artifact '{missing}'")
            commands.append(f"hhplab generate msa --definition-version {definition_version}")
            continue
        if name.startswith("coc__B") and base_ref.type == "coc" and base_ref.vintage is not None:
            details.append(f"missing CoC boundary artifact '{missing}'")
            commands.append(
                f"hhplab ingest boundaries --source hud_exchange --vintage {base_ref.vintage}"
            )
            continue
        if (
            name.startswith("counties__C")
            and base_ref.type == "coc"
            and base_ref.vintage is not None
        ):
            details.append(f"missing county geometry artifact '{missing}'")
            commands.append(f"hhplab ingest tiger --year {base_ref.vintage} --type counties")
            continue
        if (
            name.startswith("tracts__T")
            and base_ref.type == "tract"
            and base_ref.vintage is not None
        ):
            details.append(f"missing tract geometry artifact '{missing}'")
            commands.append(f"hhplab ingest tiger --year {base_ref.vintage} --type tracts")
            continue
        details.append(f"missing prerequisite artifact '{missing}'")

    hint = (
        f"MSA transform '{transform_id}' can be generated once its prerequisites exist: "
        + "; ".join(details)
        + "."
    )
    return Remediation(
        hint=hint,
        command=commands[0] if commands else crosswalk_command,
    )


def _check_adapter_validation(
    recipe: RecipeV1,
) -> list[PreflightFinding]:
    """Run adapter registry validation and convert to findings."""
    register_defaults()
    diagnostics = validate_recipe_adapters(
        recipe,
        geometry_registry,
        dataset_registry,
    )
    findings: list[PreflightFinding] = []
    for d in diagnostics:
        findings.append(
            PreflightFinding(
                severity=(Severity.ERROR if d.level == "error" else Severity.WARNING),
                kind=FindingKind.ADAPTER_ERROR,
                message=d.message,
            )
        )
    return findings


def _check_temporal_alignment_guidance(
    recipe: RecipeV1,
) -> list[PreflightFinding]:
    """Warn on nonstandard recipe lag choices that need review."""
    findings: list[PreflightFinding] = []
    for ds_id, ds in recipe.datasets.items():
        if ds.provider != "census":
            continue

        # ACS5 lag check: warn on nonstandard acs_end offset.
        if ds.product in {"acs", "acs5"} and ds.file_set is not None:
            for seg in ds.file_set.segments:
                acs_end_offset = seg.year_offsets.get("acs_end")
                if acs_end_offset is None or acs_end_offset == -1:
                    continue
                years = expand_year_spec(seg.years)
                year_label = seg.years.range or seg.years.years
                findings.append(
                    PreflightFinding(
                        severity=Severity.WARNING,
                        kind=FindingKind.TEMPORAL_ALIGNMENT,
                        message=(
                            f"Dataset '{ds_id}' segment {year_label} uses "
                            f"acs_end offset {acs_end_offset}. HHP-Lab's standard "
                            "ACS lag for PIT/January-aligned panels is acs_end=-1; "
                            "review this offset against the ACS temporal guidance "
                            "before building."
                        ),
                        dataset_id=ds_id,
                        years=years,
                        remediation=Remediation(
                            hint=(
                                "Use year_offsets: { acs_end: -1 } for the standard "
                                "PIT/January-aligned ACS lag, or document why a "
                                "different offset is intentional."
                            ),
                        ),
                    )
                )

    return findings


def _map_artifact_remediation(
    geo_type: str, *, source: str | None, vintage: int | None
) -> Remediation:
    """Build actionable remediation for missing map boundary artifacts."""
    if geo_type == "coc":
        return Remediation(
            hint=(
                f"Ingest curated CoC boundary polygons for vintage {vintage} "
                "before rendering this map layer."
            ),
            command=(
                f"hhplab ingest boundaries --source hud_exchange --vintage {vintage}"
                if vintage is not None
                else None
            ),
        )

    if geo_type == "county":
        return Remediation(
            hint=(
                f"Ingest TIGER county polygons for vintage {vintage} before "
                "rendering this map layer."
            ),
            command=(
                f"hhplab ingest tiger --year {vintage} --type counties"
                if vintage is not None
                else None
            ),
        )

    if geo_type == "msa":
        return Remediation(
            hint=(
                f"Ingest official MSA boundary polygons for definition version "
                f"'{source}' aligned to county geometry vintage {vintage}."
            ),
            command=(
                f"hhplab ingest msa-boundaries --definition-version {source} --year {vintage}"
                if source is not None and vintage is not None
                else None
            ),
        )

    if geo_type == "metro":
        return Remediation(
            hint=(
                f"Generate metro boundary polygons for definition version "
                f"'{source}' using county geometry vintage {vintage}."
            ),
            command=(
                "hhplab generate metro-boundaries "
                f"--definition-version {source} --counties {vintage}"
                if source is not None and vintage is not None
                else None
            ),
        )

    return Remediation(hint="Create the missing boundary artifact required by the map layer.")


def _check_map_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check recipe-native map targets for required boundary artifacts."""
    findings: list[PreflightFinding] = []
    base_dir = project_root / "data"

    for target in recipe.targets:
        if "map" not in target.outputs or target.map_spec is None:
            continue

        for index, layer in enumerate(target.map_spec.layers, start=1):
            layer_name = layer.label or layer.group or f"layer {index}"
            geometry = layer.geometry
            geo_type = geometry.type
            vintage = geometry.vintage
            source = geometry.source

            artifact_path: Path | None = None
            if geo_type == "coc" and vintage is not None:
                try:
                    artifact_path = resolve_curated_boundary_path(str(vintage), base_dir=base_dir)
                except FileNotFoundError:
                    artifact_path = (
                        base_dir / "curated" / "coc_boundaries" / coc_base_filename(str(vintage))
                    )
            elif geo_type == "msa" and source is not None and vintage is not None:
                artifact_path = msa_boundaries_path(source, base_dir)
            elif geo_type == "metro" and source is not None and vintage is not None:
                artifact_path = metro_boundaries_path(source, vintage, base_dir)
            elif geo_type == "county" and vintage is not None:
                artifact_path = county_path(vintage, base_dir)
            else:
                continue

            if artifact_path.exists():
                continue

            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MAP_ARTIFACT,
                    message=(
                        f"Map target '{target.id}' layer '{layer_name}' requires missing "
                        f"{geo_type.upper()} boundary artifact: {artifact_path}"
                    ),
                    geometry=geo_type,
                    remediation=_map_artifact_remediation(
                        geo_type,
                        source=source,
                        vintage=vintage,
                    ),
                )
            )

    return findings


def _containment_artifact_remediation(
    label: str,
    geo_type: str,
    *,
    vintage: int | None = None,
    definition_version: str | None = None,
) -> Remediation:
    """Build actionable remediation for missing containment prerequisites."""
    if geo_type == "coc":
        return Remediation(
            hint=f"Ingest curated CoC boundary polygons for vintage {vintage}.",
            command=(
                f"hhplab ingest boundaries --source hud_exchange --vintage {vintage}"
                if vintage is not None
                else None
            ),
        )
    if geo_type == "county":
        return Remediation(
            hint=f"Ingest TIGER county polygons for vintage {vintage}.",
            command=(
                f"hhplab ingest tiger --year {vintage} --type counties"
                if vintage is not None
                else None
            ),
        )
    if geo_type == "msa":
        return Remediation(
            hint=(
                f"Generate MSA definition and county-membership artifacts for "
                f"definition version '{definition_version}'."
            ),
            command=(
                f"hhplab generate msa --definition-version {definition_version}"
                if definition_version is not None
                else "hhplab generate msa"
            ),
        )
    return Remediation(hint=f"Create the missing containment prerequisite: {label}.")


def _containment_missing_artifact_finding(
    *,
    target_id: str,
    label: str,
    path: Path,
    geo_type: str,
    vintage: int | None = None,
    definition_version: str | None = None,
) -> PreflightFinding:
    return PreflightFinding(
        severity=Severity.ERROR,
        kind=FindingKind.MISSING_CONTAINMENT_ARTIFACT,
        message=(f"Containment target '{target_id}' requires missing {label}: {path}"),
        geometry=geo_type,
        remediation=_containment_artifact_remediation(
            label,
            geo_type,
            vintage=vintage,
            definition_version=definition_version,
        ),
    )


def _read_parquet_id_values(path: Path, candidate_columns: tuple[str, ...]) -> set[str] | None:
    """Read the first available ID column from a parquet artifact."""
    try:
        columns = list(pq.read_schema(path).names)
    except (FileNotFoundError, OSError, ValueError):
        return None

    id_col = next((col for col in candidate_columns if col in columns), None)
    if id_col is None:
        return None

    try:
        df = pd.read_parquet(path, columns=[id_col])
    except (FileNotFoundError, OSError, ValueError, KeyError):
        return None
    return set(df[id_col].dropna().astype(str))


def _check_containment_selector(
    *,
    target_id: str,
    selector_label: str,
    requested: list[str] | None,
    available: set[str] | None,
    geo_type: str,
) -> list[PreflightFinding]:
    if requested is None or available is None:
        return []
    missing = sorted(set(requested) - available)
    if not missing:
        return []
    preview = ", ".join(missing[:5])
    suffix = ", ..." if len(missing) > 5 else ""
    return [
        PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.CONTAINMENT_SELECTOR,
            message=(
                f"Containment target '{target_id}' {selector_label} did not match "
                f"available {geo_type.upper()} IDs: {preview}{suffix}."
            ),
            geometry=geo_type,
            remediation=Remediation(
                hint=(
                    "Check containment selector IDs against the curated geometry "
                    "artifacts, or build the artifacts for the intended vintage."
                ),
            ),
        )
    ]


def _check_containment_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check recipe-native containment targets for required artifacts and selectors."""
    findings: list[PreflightFinding] = []
    data_root = load_config(project_root=project_root).asset_store_root

    for target in recipe.targets:
        specs: list[tuple[str, ContainmentSpec]] = []
        if "containment" in target.outputs and target.containment_spec is not None:
            specs.append(("containment_spec", target.containment_spec))
        if target.containment_filter is not None:
            specs.append(("containment_filter", target.containment_filter))
        if target.msa_coc_panel is not None:
            try:
                specs.append(
                    (
                        "msa_coc_panel",
                        build_msa_coc_containment_spec(target.msa_coc_panel),
                    )
                )
            except (ExecutorError, ValueError) as exc:
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_CONTAINMENT_ARTIFACT,
                        message=f"MSA-CoC panel target '{target.id}' is invalid: {exc}",
                        geometry="msa",
                        remediation=Remediation(
                            hint=(
                                "Set msa_coc_panel.msa_definition_version to a version "
                                "that includes the county geometry year, for example "
                                "'census_msa_2023'."
                            ),
                        ),
                    )
                )
        if not specs:
            continue

        for spec_field, spec in specs:
            pair = (spec.container.type, spec.candidate.type)
            artifact_paths: dict[str, Path] = {}
            selectors: dict[str, tuple[Path, tuple[str, ...], str, list[str] | None]] = {}

            if pair == ("msa", "coc"):
                coc_vintage = spec.candidate.vintage
                county_vintage = spec.container.vintage
                definition_version = spec.definition_version or spec.container.source

                if coc_vintage is not None:
                    artifact_paths["candidate CoC boundary artifact"] = coc_base_path(
                        coc_vintage,
                        data_root,
                    )
                if county_vintage is not None:
                    artifact_paths["container MSA county geometry artifact"] = county_path(
                        county_vintage,
                        data_root,
                    )
                if definition_version is not None:
                    artifact_paths["MSA definitions artifact"] = msa_definitions_path(
                        definition_version,
                        data_root,
                    )
                    artifact_paths["MSA county membership artifact"] = msa_county_membership_path(
                        definition_version,
                        data_root,
                    )
                else:
                    findings.append(
                        PreflightFinding(
                            severity=Severity.ERROR,
                            kind=FindingKind.MISSING_CONTAINMENT_ARTIFACT,
                            message=(
                                f"Containment target '{target.id}' {spec_field} requires "
                                "definition_version or container.source for MSA containment."
                            ),
                            geometry="msa",
                            remediation=Remediation(
                                hint=(
                                    f"Set {spec_field}.definition_version or "
                                    f"{spec_field}.container.source to the MSA "
                                    "definition version used by curated artifacts."
                                ),
                            ),
                        )
                    )

                membership_path = (
                    artifact_paths.get("MSA county membership artifact")
                    if definition_version is not None
                    else None
                )
                coc_path = (
                    artifact_paths.get("candidate CoC boundary artifact")
                    if coc_vintage is not None
                    else None
                )
                if membership_path is not None:
                    selectors["container selector_ids"] = (
                        membership_path,
                        ("msa_id", "cbsa_code"),
                        "msa",
                        spec.selector_ids,
                    )
                if coc_path is not None:
                    selectors["candidate_selector_ids"] = (
                        coc_path,
                        ("coc_id", "geo_id"),
                        "coc",
                        spec.candidate_selector_ids,
                    )

            elif pair == ("coc", "county"):
                coc_vintage = spec.container.vintage
                county_vintage = spec.candidate.vintage
                if coc_vintage is not None:
                    artifact_paths["container CoC boundary artifact"] = coc_base_path(
                        coc_vintage,
                        data_root,
                    )
                if county_vintage is not None:
                    artifact_paths["candidate county geometry artifact"] = county_path(
                        county_vintage,
                        data_root,
                    )

                coc_path = (
                    artifact_paths.get("container CoC boundary artifact")
                    if coc_vintage is not None
                    else None
                )
                county_file = (
                    artifact_paths.get("candidate county geometry artifact")
                    if county_vintage is not None
                    else None
                )
                if coc_path is not None:
                    selectors["container selector_ids"] = (
                        coc_path,
                        ("coc_id", "geo_id"),
                        "coc",
                        spec.selector_ids,
                    )
                if county_file is not None:
                    selectors["candidate_selector_ids"] = (
                        county_file,
                        ("GEOID", "geoid", "county_fips", "geo_id"),
                        "county",
                        spec.candidate_selector_ids,
                    )

            else:
                continue

            missing_labels = {label for label, path in artifact_paths.items() if not path.exists()}
            for label in sorted(missing_labels):
                path = artifact_paths[label]
                if "CoC" in label:
                    geo_type = "coc"
                    vintage = (
                        spec.candidate.vintage if pair == ("msa", "coc") else spec.container.vintage
                    )
                    definition_version = None
                elif "county" in label and "MSA" not in label:
                    geo_type = "county"
                    vintage = spec.candidate.vintage
                    definition_version = None
                elif "geometry" in label:
                    geo_type = "county"
                    vintage = spec.container.vintage
                    definition_version = None
                else:
                    geo_type = "msa"
                    vintage = spec.container.vintage
                    definition_version = spec.definition_version or spec.container.source
                findings.append(
                    _containment_missing_artifact_finding(
                        target_id=target.id,
                        label=f"{spec_field} {label}",
                        path=path,
                        geo_type=geo_type,
                        vintage=vintage,
                        definition_version=definition_version,
                    )
                )

            for selector_label, (path, columns, geo_type, requested) in selectors.items():
                if path.exists():
                    findings.extend(
                        _check_containment_selector(
                            target_id=target.id,
                            selector_label=f"{spec_field} {selector_label}",
                            requested=requested,
                            available=_read_parquet_id_values(path, columns),
                            geo_type=geo_type,
                        )
                    )

    return findings


def _check_msa_coc_coverage_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check recipe-native MSA-CoC coverage targets for required artifacts."""
    findings: list[PreflightFinding] = []
    data_root = load_config(project_root=project_root).asset_store_root

    for target in recipe.targets:
        spec = target.msa_coc_coverage
        if "msa_coc_coverage" not in target.outputs or spec is None:
            continue

        artifact_paths: dict[str, tuple[Path, str, object | None, str | None, str | None]] = {
            "CoC boundary artifact": (
                coc_base_path(str(spec.coc_boundary_vintage), data_root),
                "coc",
                spec.coc_boundary_vintage,
                None,
                "hhplab ingest boundaries --source hud_exchange "
                f"--vintage {spec.coc_boundary_vintage}",
            ),
            "county geometry artifact": (
                county_path(spec.county_vintage, data_root),
                "county",
                spec.county_vintage,
                None,
                f"hhplab ingest tiger --year {spec.county_vintage} --type counties",
            ),
            "MSA definitions artifact": (
                msa_definitions_path(spec.msa_definition_version, data_root),
                "msa",
                spec.county_vintage,
                spec.msa_definition_version,
                f"hhplab generate msa --definition-version {spec.msa_definition_version}",
            ),
            "MSA county membership artifact": (
                msa_county_membership_path(spec.msa_definition_version, data_root),
                "msa",
                spec.county_vintage,
                spec.msa_definition_version,
                f"hhplab generate msa --definition-version {spec.msa_definition_version}",
            ),
        }

        if "population" in spec.overlap_bases:
            if spec.tract_vintage is not None:
                artifact_paths["tract geometry artifact"] = (
                    tract_path(spec.tract_vintage, data_root),
                    "tract",
                    spec.tract_vintage,
                    None,
                    f"hhplab ingest tiger --year {spec.tract_vintage} --type tracts",
                )
            if spec.acs5_population_vintage is not None and spec.tract_vintage is not None:
                artifact_paths["ACS5 tract population artifact"] = (
                    data_root
                    / "curated"
                    / "acs"
                    / acs5_tracts_filename(
                        str(spec.acs5_population_vintage),
                        spec.tract_vintage,
                    ),
                    "tract",
                    spec.tract_vintage,
                    None,
                    "hhplab ingest acs5-tract "
                    f"--acs {spec.acs5_population_vintage} "
                    f"--tracts {spec.tract_vintage}",
                )

        for label, (path, geo_type, _vintage, _definition_version, command) in sorted(
            artifact_paths.items()
        ):
            if path.exists():
                continue
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MSA_COC_COVERAGE_ARTIFACT,
                    message=(
                        f"MSA-CoC coverage target '{target.id}' requires missing "
                        f"{label}: {path}"
                    ),
                    geometry=geo_type,
                    remediation=Remediation(
                        hint=(
                            f"Build or ingest the {label} for MSA-CoC coverage "
                            f"target '{target.id}'."
                        ),
                        command=command,
                    ),
                )
            )

    return findings


def _check_msa_fractional_rollup_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check CoC-to-MSA fractional rollup prerequisites."""
    findings: list[PreflightFinding] = []
    data_root = load_config(project_root=project_root).asset_store_root

    for target in recipe.targets:
        spec = target.msa_fractional_rollup
        if "panel" not in target.outputs or spec is None:
            continue

        generate_xwalk_command = (
            "hhplab generate msa-xwalk "
            f"--allocation-basis {spec.allocation_basis} "
            f"--boundary {spec.coc_boundary_vintage} "
            f"--definition-version {spec.msa_definition_version} "
            f"--counties {spec.county_vintage}"
        )
        if spec.allocation_basis == "block_population":
            generate_xwalk_command = (
                f"{generate_xwalk_command} "
                f"--blocks {spec.block_vintage} "
                f"--decennial {spec.decennial_population_vintage}"
            )
            crosswalk_path = msa_coc_block_population_xwalk_path(
                spec.coc_boundary_vintage,
                spec.msa_definition_version,
                spec.county_vintage,
                spec.block_vintage,
                spec.decennial_population_vintage,
                data_root,
            )
        else:
            crosswalk_path = msa_coc_xwalk_path(
                str(spec.coc_boundary_vintage),
                spec.msa_definition_version,
                spec.county_vintage,
                data_root,
            )

        artifact_paths: dict[
            str,
            tuple[Path, str, str, tuple[tuple[str, ...], ...]],
        ] = {
            "CoC boundary artifact": (
                coc_base_path(str(spec.coc_boundary_vintage), data_root),
                "coc",
                "hhplab ingest boundaries --source hud_exchange "
                f"--vintage {spec.coc_boundary_vintage}",
                (("coc_id", "geo_id"),),
            ),
            "county geometry artifact": (
                county_path(spec.county_vintage, data_root),
                "county",
                f"hhplab ingest tiger --year {spec.county_vintage} --type counties",
                (("GEOID", "geoid", "county_fips"),),
            ),
            "MSA definitions artifact": (
                msa_definitions_path(spec.msa_definition_version, data_root),
                "msa",
                f"hhplab generate msa --definition-version {spec.msa_definition_version}",
                (("msa_id",),),
            ),
            "MSA county membership artifact": (
                msa_county_membership_path(spec.msa_definition_version, data_root),
                "msa",
                f"hhplab generate msa --definition-version {spec.msa_definition_version}",
                (("msa_id",), ("county_fips",)),
            ),
            "CoC-to-MSA fractional rollup crosswalk artifact": (
                crosswalk_path,
                "msa",
                generate_xwalk_command,
                (("coc_id",), ("msa_id",), ("allocation_share",), ("allocation_basis",)),
            ),
        }

        if spec.allocation_basis == "block_population":
            artifact_paths["block geometry artifact"] = (
                block_geometry_path(spec.block_vintage, data_root),
                "block",
                f"hhplab ingest tiger --year {spec.block_vintage} --type blocks",
                (("GEOID", "geoid", "block_geoid"),),
            )
            artifact_paths["PL block population artifact"] = (
                pl_block_population_path(
                    spec.decennial_population_vintage,
                    spec.block_vintage,
                    data_root,
                ),
                "block",
                "hhplab ingest pl-blocks "
                f"--decennial {spec.decennial_population_vintage} "
                f"--blocks {spec.block_vintage}",
                (("block_geoid", "GEOID", "geoid"), ("total_population",)),
            )

        for label, (path, geo_type, command, required) in sorted(artifact_paths.items()):
            if not path.exists():
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_MSA_FRACTIONAL_ROLLUP_ARTIFACT,
                        message=(
                            f"MSA fractional rollup target '{target.id}' requires "
                            f"missing {label}: {path}"
                        ),
                        geometry=geo_type,
                        remediation=Remediation(
                            hint=(
                                f"Build or ingest the {label} for MSA fractional "
                                f"rollup target '{target.id}'."
                            ),
                            command=command,
                        ),
                    )
                )
                continue

            findings.extend(
                _check_msa_fractional_rollup_artifact_columns(
                    target_id=target.id,
                    label=label,
                    path=path,
                    required_column_groups=required,
                    geometry=geo_type,
                )
            )

    return findings


def _check_msa_fractional_rollup_artifact_columns(
    *,
    target_id: str,
    label: str,
    path: Path,
    required_column_groups: tuple[tuple[str, ...], ...],
    geometry: str,
) -> list[PreflightFinding]:
    try:
        columns = set(pq.read_schema(path).names)
    except (FileNotFoundError, OSError, ValueError) as exc:
        return [
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.SCHEMA_UNREADABLE,
                message=(
                    f"MSA fractional rollup target '{target_id}' could not read "
                    f"{label} schema at {path}: {exc}"
                ),
                geometry=geometry,
            )
        ]

    missing = [
        " or ".join(group)
        for group in required_column_groups
        if not any(column in columns for column in group)
    ]
    if not missing:
        return []
    return [
        PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.MISSING_COLUMN,
            message=(
                f"MSA fractional rollup target '{target_id}' requires {label} "
                f"columns {missing}. Available columns: {sorted(columns)}"
            ),
            geometry=geometry,
            remediation=Remediation(
                hint=(
                    f"Rebuild the {label} so it satisfies the fractional MSA "
                    "rollup schema contract."
                ),
            ),
        )
    ]


def _check_primary_msa_artifacts(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check CoC panel primary-MSA annotation prerequisites."""
    findings: list[PreflightFinding] = []
    data_root = load_config(project_root=project_root).asset_store_root

    for target in recipe.targets:
        policy = target.panel_policy
        spec = policy.primary_msa if policy is not None else None
        if "panel" not in target.outputs or spec is None:
            continue

        boundary_vintage = target.geometry.vintage
        if boundary_vintage is None:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_PRIMARY_MSA_ARTIFACT,
                    message=(
                        f"Target '{target.id}' panel_policy.primary_msa requires "
                        "target.geometry.vintage so CoC boundary artifacts can be resolved."
                    ),
                    geometry="coc",
                    remediation=Remediation(
                        hint=(
                            "Set target.geometry.vintage to the CoC boundary vintage used "
                            "for primary-MSA annotation."
                        ),
                    ),
                )
            )
            continue

        artifact_paths: dict[
            str,
            tuple[Path, str, str, str | None, str, tuple[tuple[str, ...], ...]],
        ] = {
            "CoC boundary artifact": (
                coc_base_path(str(boundary_vintage), data_root),
                "coc",
                str(boundary_vintage),
                None,
                "hhplab ingest boundaries --source hud_exchange "
                f"--vintage {boundary_vintage}",
                (("coc_id", "geo_id"),),
            ),
            "county geometry artifact": (
                county_path(spec.county_vintage, data_root),
                "county",
                str(spec.county_vintage),
                None,
                f"hhplab ingest tiger --year {spec.county_vintage} --type counties",
                (("GEOID", "geoid", "county_fips"),),
            ),
            "MSA definitions artifact": (
                msa_definitions_path(spec.definition_version, data_root),
                "msa",
                str(spec.county_vintage),
                spec.definition_version,
                f"hhplab generate msa --definition-version {spec.definition_version}",
                (("msa_id",), ("msa_name",)),
            ),
            "MSA county membership artifact": (
                msa_county_membership_path(spec.definition_version, data_root),
                "msa",
                str(spec.county_vintage),
                spec.definition_version,
                f"hhplab generate msa --definition-version {spec.definition_version}",
                (("msa_id",), ("county_fips",)),
            ),
            "CoC-to-MSA crosswalk artifact": (
                msa_coc_xwalk_path(
                    str(boundary_vintage),
                    spec.definition_version,
                    spec.county_vintage,
                    data_root,
                ),
                "msa",
                str(spec.county_vintage),
                spec.definition_version,
                "hhplab generate msa-xwalk "
                f"--boundary {boundary_vintage} "
                f"--definition-version {spec.definition_version} "
                f"--counties {spec.county_vintage}",
                (("coc_id",), ("msa_id",), ("allocation_share",)),
            ),
        }

        if spec.overlap_basis == "population":
            if spec.tract_vintage is not None:
                artifact_paths["tract geometry artifact"] = (
                    tract_path(spec.tract_vintage, data_root),
                    "tract",
                    str(spec.tract_vintage),
                    None,
                    f"hhplab ingest tiger --year {spec.tract_vintage} --type tracts",
                    (("GEOID", "geoid", "tract_geoid"),),
                )
            if (
                spec.population_source == "acs5"
                and spec.acs5_population_vintage is not None
                and spec.tract_vintage is not None
            ):
                artifact_paths["ACS5 tract population artifact"] = (
                    data_root
                    / "curated"
                    / "acs"
                    / acs5_tracts_filename(
                        str(spec.acs5_population_vintage),
                        spec.tract_vintage,
                    ),
                    "tract",
                    str(spec.tract_vintage),
                    None,
                    "hhplab ingest acs5-tract "
                    f"--acs {spec.acs5_population_vintage} "
                    f"--tracts {spec.tract_vintage}",
                    (("GEOID", "tract_geoid"), ("total_population",)),
                )
            if (
                spec.population_source == "decennial"
                and spec.decennial_population_vintage is not None
                and spec.tract_vintage is not None
            ):
                artifact_paths["decennial tract population artifact"] = (
                    data_root
                    / "curated"
                    / "census"
                    / decennial_tracts_filename(
                        spec.decennial_population_vintage,
                        spec.tract_vintage,
                    ),
                    "tract",
                    str(spec.tract_vintage),
                    None,
                    "hhplab ingest decennial-tracts "
                    f"--decennial {spec.decennial_population_vintage} "
                    f"--tracts {spec.tract_vintage}",
                    (("GEOID", "tract_geoid"), ("total_population",)),
                )

        for label, (path, geo_type, _vintage, _definition_version, command, required) in sorted(
            artifact_paths.items()
        ):
            if not path.exists():
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_PRIMARY_MSA_ARTIFACT,
                        message=(
                            f"Target '{target.id}' panel_policy.primary_msa requires "
                            f"missing {label}: {path}"
                        ),
                        geometry=geo_type,
                        remediation=Remediation(
                            hint=(
                                f"Build or ingest the {label} for primary-MSA "
                                f"annotations on target '{target.id}'."
                            ),
                            command=command,
                        ),
                    )
                )
                continue
            findings.extend(
                _check_primary_msa_artifact_columns(
                    target_id=target.id,
                    label=label,
                    path=path,
                    required_column_groups=required,
                    geometry=geo_type,
                )
            )

    return findings


def _check_primary_msa_artifact_columns(
    *,
    target_id: str,
    label: str,
    path: Path,
    required_column_groups: tuple[tuple[str, ...], ...],
    geometry: str,
) -> list[PreflightFinding]:
    try:
        columns = set(pq.read_schema(path).names)
    except (FileNotFoundError, OSError, ValueError) as exc:
        return [
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.SCHEMA_UNREADABLE,
                message=(
                    f"Target '{target_id}' panel_policy.primary_msa could not read "
                    f"{label} schema at {path}: {exc}"
                ),
                geometry=geometry,
            )
        ]

    missing = [
        " or ".join(group)
        for group in required_column_groups
        if not any(column in columns for column in group)
    ]
    if not missing:
        return []
    return [
        PreflightFinding(
            severity=Severity.ERROR,
            kind=FindingKind.MISSING_COLUMN,
            message=(
                f"Target '{target_id}' panel_policy.primary_msa requires {label} "
                f"columns {missing}. Available columns: {sorted(columns)}"
            ),
            geometry=geometry,
            remediation=Remediation(
                hint=(
                    f"Rebuild the {label} so it satisfies the primary-MSA annotation "
                    "schema contract."
                ),
            ),
        )
    ]


def _check_target_selectors(
    recipe: RecipeV1,
    project_root: Path,
) -> list[PreflightFinding]:
    """Check explicit panel target selectors against available geography artifacts."""
    findings: list[PreflightFinding] = []
    data_root = load_config(project_root=project_root).asset_store_root

    for target in recipe.targets:
        if "panel" not in target.outputs or target.selector_ids is None:
            continue

        selector_artifact: Path | None = None
        candidate_columns: tuple[str, ...] = ()
        geo_type = target.geometry.type
        if geo_type == "coc" and target.geometry.vintage is not None:
            selector_artifact = coc_base_path(target.geometry.vintage, data_root)
            candidate_columns = ("coc_id", "geo_id")
        elif geo_type == "msa" and target.geometry.source is not None:
            selector_artifact = msa_definitions_path(target.geometry.source, data_root)
            candidate_columns = ("msa_id", "cbsa_code", "geo_id")

        if selector_artifact is None or not selector_artifact.exists():
            continue

        available = _read_parquet_id_values(selector_artifact, candidate_columns)
        if available is None:
            continue

        missing = sorted(set(target.selector_ids) - available)
        if not missing:
            continue
        preview = ", ".join(missing[:5])
        suffix = ", ..." if len(missing) > 5 else ""
        findings.append(
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.TARGET_SELECTOR,
                message=(
                    f"Target '{target.id}' selector_ids did not match available "
                    f"{geo_type.upper()} IDs: {preview}{suffix}."
                ),
                geometry=geo_type,
                remediation=Remediation(
                    hint=(
                        "Check target.selector_ids against the curated target "
                        "geometry artifact, or build the artifact for the intended vintage."
                    ),
                ),
            )
        )

    return findings


def _pipeline_dataset_ids(recipe: RecipeV1, pipeline) -> set[str]:
    """Return dataset ids referenced by a pipeline."""
    dataset_ids: set[str] = set()
    for step in pipeline.steps:
        if isinstance(step, ResampleStep):
            dataset_ids.add(step.dataset)
        elif isinstance(step, SmallAreaEstimateStep):
            dataset_ids.add(step.source_dataset)
            dataset_ids.add(step.support_dataset)
        elif isinstance(step, JoinStep):
            dataset_ids.update(ds_id for ds_id in step.datasets if ds_id in recipe.datasets)
    return dataset_ids


def _dataset_is_january_aligned(recipe: RecipeV1, dataset_id: str) -> bool:
    """Return True when a dataset is used with January/PIT temporal alignment."""
    ds = recipe.datasets[dataset_id]
    filt = recipe.filters.get(dataset_id)

    if ds.provider == "hud" and ds.product == "pit":
        return ds.params.get("align", "point_in_time_jan") == "point_in_time_jan"

    if ds.params.get("align") == "point_in_time_jan":
        return True

    if (
        filt is not None
        and filt.method in {"point_in_time", "interpolate_to_month"}
        and filt.month == 1
    ):
        return True

    return False


def _january_aligned_pipeline_ids(recipe: RecipeV1) -> set[str]:
    """Return pipeline ids that include January/PIT-aligned inputs."""
    pipeline_ids: set[str] = set()
    for pipeline in recipe.pipelines:
        dataset_ids = _pipeline_dataset_ids(recipe, pipeline)
        if any(_dataset_is_january_aligned(recipe, ds_id) for ds_id in dataset_ids):
            pipeline_ids.add(pipeline.id)
    return pipeline_ids


def _detect_year_column(path: Path, declared_year_column: str | None) -> str | None:
    """Resolve the effective year column for a parquet input."""
    if declared_year_column is not None:
        return declared_year_column

    schema_probe = probe_dataset_schema(path)
    if not schema_probe.ok or schema_probe.detail is None:
        return None

    year_probe = probe_year_column(schema_probe.detail["columns"], None)
    if not year_probe.ok or year_probe.detail is None:
        return None
    return year_probe.detail["year_column"]


def _path_contains_same_year_acs1(
    *,
    path: Path,
    analysis_year: int,
    declared_year_column: str | None,
) -> bool:
    """Return True when a parquet input contains rows for the analysis year."""
    if not path.exists():
        return False

    year_column = _detect_year_column(path, declared_year_column)
    if year_column is None:
        return False

    try:
        df = pd.read_parquet(path, columns=[year_column])
    except (FileNotFoundError, OSError, ValueError, KeyError):
        return False

    if year_column not in df.columns:
        return False

    years = pd.to_numeric(df[year_column], errors="coerce")
    return bool((years == analysis_year).any())


def _file_set_segment_for_year(ds, year: int):
    """Return the file_set segment that covers a given analysis year."""
    if ds.file_set is None:
        return None
    for seg in ds.file_set.segments:
        if year in expand_year_spec(seg.years):
            return seg
    return None


def _resolved_acs1_vintage_for_task(ds, task: ResampleTask) -> int | None:
    """Return the ACS1 vintage a resample task resolves to, when discoverable."""
    if task.input_path is not None:
        match = re.search(r"__A(\d{4})", task.input_path)
        if match:
            return int(match.group(1))

    seg = _file_set_segment_for_year(ds, task.year)
    if seg is None:
        return None

    override_path = seg.overrides.get(task.year)
    if override_path is not None:
        match = re.search(r"__A(\d{4})", override_path)
        if match:
            return int(match.group(1))

    for key in ("acs1_end", "acs_end"):
        if key in seg.year_offsets:
            return task.year + int(seg.year_offsets[key])

    if ds.file_set is not None:
        template = ds.file_set.path_template
        if re.search(r"__A\{year(?::[^}]*)?\}", template):
            return task.year

    return None


def _remediation_years_for_missing_dataset(ds, tasks: list[ResampleTask]) -> list[int]:
    """Return source-vintage years to use in missing-dataset remediation."""
    if ds.provider == "census" and ds.product in {"acs1", "acs1_poverty"}:
        vintages = [
            vintage
            for task in tasks
            if (vintage := _resolved_acs1_vintage_for_task(ds, task)) is not None
        ]
        if vintages:
            return sorted(set(vintages))
    return sorted({task.year for task in tasks})


def _task_uses_same_year_acs1(
    *,
    recipe: RecipeV1,
    task: ResampleTask,
    project_root: Path,
) -> bool:
    """Return True when an ACS1 task resolves to same-year data."""
    ds = recipe.datasets[task.dataset_id]

    if task.input_path is not None:
        match = re.search(r"__A(\d{4})", task.input_path)
        if match and int(match.group(1)) == task.year:
            return True

        full_path = project_root / task.input_path
        if _path_contains_same_year_acs1(
            path=full_path,
            analysis_year=task.year,
            declared_year_column=task.year_column,
        ):
            return True

    seg = _file_set_segment_for_year(ds, task.year)
    if seg is None:
        return False

    acs1_end_offset = seg.year_offsets.get("acs1_end")
    if acs1_end_offset is None:
        acs1_end_offset = seg.year_offsets.get("acs_end")
    if acs1_end_offset == 0:
        return True

    override_path = seg.overrides.get(task.year)
    if override_path is not None:
        match = re.search(r"__A(\d{4})", override_path)
        if match and int(match.group(1)) == task.year:
            return True

    template = ds.file_set.path_template
    if (
        re.search(r"__A\{year(?::[^}]*)?\}", template)
        and "{acs1_end}" not in template
        and "{acs_end}" not in template
    ):
        return True

    return False


def _check_acs1_temporal_alignment_guidance(
    recipe: RecipeV1,
    project_root: Path,
    pipeline_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Warn when PIT/January-aligned pipelines use same-year ACS1 vintages."""
    findings: list[PreflightFinding] = []
    january_pipeline_ids = _january_aligned_pipeline_ids(recipe)
    same_year_usage: dict[tuple[str, str], set[int]] = {}

    for pipeline_id, task in pipeline_tasks:
        if pipeline_id not in january_pipeline_ids:
            continue

        ds = recipe.datasets[task.dataset_id]
        if ds.provider != "census" or ds.product != "acs1":
            continue

        if _task_uses_same_year_acs1(
            recipe=recipe,
            task=task,
            project_root=project_root,
        ):
            same_year_usage.setdefault((pipeline_id, task.dataset_id), set()).add(
                task.year,
            )

    for (pipeline_id, dataset_id), years in sorted(same_year_usage.items()):
        findings.append(
            PreflightFinding(
                severity=Severity.WARNING,
                kind=FindingKind.TEMPORAL_ALIGNMENT,
                message=(
                    f"Pipeline '{pipeline_id}': dataset '{dataset_id}' uses "
                    f"same-year ACS1 vintage for analysis year(s) {sorted(years)}. "
                    "ACS1 data for year Y is published in fall of year Y and is "
                    "not available at January-aligned observation dates; use "
                    "prior-year ACS1 vintages or document why same-year ACS1 is "
                    "intentional."
                ),
                pipeline_id=pipeline_id,
                dataset_id=dataset_id,
                years=sorted(years),
                remediation=Remediation(
                    hint=(
                        "For PIT/January-aligned pipelines, use lagged ACS1 "
                        "vintages (for example year_offsets: { acs1_end: -1 } or "
                        "a prior-year static artifact path), or document why "
                        "same-year ACS1 is intentional."
                    ),
                ),
            )
        )

    return findings


def _check_transforms(
    recipe: RecipeV1,
    project_root: Path,
    needed_transforms: set[str],
) -> list[PreflightFinding]:
    """Check that required transform artifacts exist."""
    findings: list[PreflightFinding] = []
    for tid in sorted(needed_transforms):
        result = probe_transform_path(tid, recipe, project_root)
        if not result.ok:
            can_generate = result.detail.get("can_generate", False) if result.detail else False
            generation_ready = (
                result.detail.get("generation_ready", False) if result.detail else False
            )
            if can_generate and generation_ready:
                continue
            transform = None
            for t in recipe.transforms:
                if t.id == tid:
                    transform = t
                    break
            is_metro = transform is not None and (
                transform.from_.type == "metro" or transform.to.type == "metro"
            )
            is_msa = transform is not None and (
                transform.from_.type == "msa" or transform.to.type == "msa"
            )
            is_tract_mediated = (
                transform is not None
                and getattr(transform.spec.weighting, "scheme", None) == "tract_mediated"
            )
            if is_metro:
                missing_inputs = result.detail.get("missing_inputs", []) if result.detail else []
                cmd = None
                if transform is not None:
                    metro_ref = transform.to if transform.to.type == "metro" else transform.from_
                    metro_definition_version = metro_ref.resolved_metro_definition_version()
                    subset_definition_version = metro_ref.resolved_metro_subset_definition_version()
                    if any("msa_county_membership__" in item for item in missing_inputs):
                        cmd = (
                            f"hhplab generate msa --definition-version {metro_definition_version}"
                            if metro_definition_version is not None
                            else "hhplab generate msa"
                        )
                    elif any("metro_subset_membership__" in item for item in missing_inputs):
                        cmd = (
                            "hhplab generate metro-universe "
                            f"--definition-version {metro_definition_version} "
                            f"--profile-definition-version {subset_definition_version}"
                            if metro_definition_version is not None
                            and subset_definition_version is not None
                            else "hhplab generate metro-universe"
                        )
                    elif any(
                        "metro_coc_membership__" in item or "metro_county_membership__" in item
                        for item in missing_inputs
                    ):
                        cmd = "hhplab generate metro"
                    elif any("coc__B" in item for item in missing_inputs):
                        coc_ref = (
                            transform.to
                            if transform.to.type == "coc"
                            else transform.from_
                            if transform.from_.type == "coc"
                            else None
                        )
                        vintage = coc_ref.vintage if coc_ref is not None else None
                        cmd = (
                            None
                            if vintage is None
                            else (
                                "hhplab ingest boundaries --source hud_exchange "
                                f"--vintage {vintage}"
                            )
                        )
                if missing_inputs:
                    hint = (
                        f"Metro transform '{tid}' can be generated once its "
                        f"source artifacts exist. Missing: {missing_inputs}"
                    )
                else:
                    hint = (
                        f"Metro transform '{tid}' can be generated. "
                        f"Ensure metro definition artifacts exist."
                    )
                remediation = Remediation(hint=hint, command=cmd)
            elif is_msa:
                missing_inputs = result.detail.get("missing_inputs", []) if result.detail else []
                remediation = _msa_transform_remediation(tid, transform, missing_inputs)
            elif is_tract_mediated and transform is not None:
                weighting = transform.spec.weighting
                coc_ref = (
                    transform.to
                    if transform.to.type == "coc"
                    else transform.from_
                    if transform.from_.type == "coc"
                    else None
                )
                county_ref = (
                    transform.to
                    if transform.to.type == "county"
                    else transform.from_
                    if transform.from_.type == "county"
                    else None
                )
                command = None
                if (
                    coc_ref is not None
                    and county_ref is not None
                    and coc_ref.vintage is not None
                    and county_ref.vintage is not None
                    and weighting.tract_vintage is not None
                ):
                    denominator_args = ""
                    if weighting.denominator_source == "decennial":
                        denominator_args = (
                            " --denominator-source decennial "
                            f"--denominator-vintage {weighting.resolved_denominator_vintage}"
                        )
                    elif weighting.acs_vintage is not None:
                        denominator_args = f" --acs {weighting.acs_vintage}"
                    command = (
                        "hhplab generate xwalks "
                        f"--boundary {coc_ref.vintage} "
                        "--type tract-mediated "
                        f"--counties {county_ref.vintage} "
                        f"--tracts {weighting.tract_vintage}"
                        f"{denominator_args}"
                    )
                remediation = Remediation(
                    hint=(
                        f"Generate tract-mediated county crosswalk artifact "
                        f"for transform '{tid}'. This requires the CoC-tract "
                        "crosswalk and tract denominator cache."
                    ),
                    command=command,
                )
            else:
                remediation = Remediation(
                    hint=f"Generate crosswalk artifacts for transform '{tid}'.",
                    command="hhplab generate xwalks",
                )
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_TRANSFORM,
                    message=result.message,
                    transform_id=tid,
                    remediation=remediation,
                )
            )
    return findings


def _check_dataset_schemas(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Inspect dataset schemas for column issues without loading data."""
    findings: list[PreflightFinding] = []
    universe_years = expand_year_spec(recipe.universe)
    distinct_paths_by_dataset: dict[str, set[str]] = {}
    for task in resample_tasks:
        if task.input_path is not None:
            distinct_paths_by_dataset.setdefault(task.dataset_id, set()).add(task.input_path)

    # Deduplicate: check each (dataset_id, path) once
    checked: set[tuple[str, str]] = set()

    for task in resample_tasks:
        if task.input_path is None:
            continue
        key = (task.dataset_id, task.input_path)
        if key in checked:
            continue
        checked.add(key)

        full_path = project_root / task.input_path
        schema_result = probe_dataset_schema(full_path)
        if not schema_result.ok:
            # File missing or unreadable — already covered by path checks
            continue

        columns = schema_result.detail["columns"]
        column_types = schema_result.detail.get("column_types", {})
        ds = recipe.datasets.get(task.dataset_id)
        if ds is None:
            continue

        # Year column
        year_result = probe_year_column(columns, ds.year_column)
        if not year_result.ok:
            sev = Severity.ERROR
            if "Ambiguous" in (year_result.message or ""):
                kind = FindingKind.AMBIGUOUS_COLUMN
            else:
                kind = FindingKind.MISSING_COLUMN
            findings.append(
                PreflightFinding(
                    severity=sev,
                    kind=kind,
                    message=(
                        f"Dataset '{task.dataset_id}' ({task.input_path}): {year_result.message}"
                    ),
                    dataset_id=task.dataset_id,
                )
            )

        # Geo column
        geo_result = probe_geo_column(columns, ds.geo_column)
        if not geo_result.ok:
            if "Ambiguous" in (geo_result.message or ""):
                kind = FindingKind.AMBIGUOUS_COLUMN
            else:
                kind = FindingKind.MISSING_COLUMN
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=kind,
                    message=(
                        f"Dataset '{task.dataset_id}' ({task.input_path}): {geo_result.message}"
                    ),
                    dataset_id=task.dataset_id,
                )
            )

        # Measures
        measure_result = probe_measures(
            columns,
            task.measures,
            task.dataset_id,
        )
        if not measure_result.ok:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MEASURE,
                    message=(
                        f"Dataset '{task.dataset_id}' ({task.input_path}): {measure_result.message}"
                    ),
                    dataset_id=task.dataset_id,
                )
            )
        derived_required_columns = sorted(
            {
                column_name
                for config in (task.derived_measures or {}).values()
                for column_name in derived_measure_required_columns(
                    config,
                    available_columns=columns,
                )
            }
        )
        derived_missing = [column for column in derived_required_columns if column not in columns]
        if derived_missing:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MEASURE,
                    message=(
                        f"Dataset '{task.dataset_id}' ({task.input_path}): "
                        "derived measures require missing source column(s): "
                        f"{derived_missing}"
                    ),
                    dataset_id=task.dataset_id,
                )
            )

        # Temporal filter
        filt = recipe.filters.get(task.dataset_id)
        if filt is not None and isinstance(filt, TemporalFilter):
            resolved_year_column = (
                year_result.detail.get("year_column")
                if year_result.ok and year_result.detail is not None
                else None
            )
            tf_result = probe_temporal_filter(
                columns,
                filt,
                task.dataset_id,
                year_column=resolved_year_column,
                column_types=column_types,
            )
            if not tf_result.ok:
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.TEMPORAL_FILTER,
                        message=(
                            f"Dataset '{task.dataset_id}' ({task.input_path}): {tf_result.message}"
                        ),
                        dataset_id=task.dataset_id,
                    )
                )
            elif filt.method == "interpolate_to_month":
                tf_data_result = probe_interpolate_to_month_data(
                    full_path,
                    filt,
                    task.dataset_id,
                )
                if not tf_data_result.ok:
                    findings.append(
                        PreflightFinding(
                            severity=Severity.ERROR,
                            kind=FindingKind.TEMPORAL_FILTER,
                            message=(
                                f"Dataset '{task.dataset_id}' ({task.input_path}): "
                                f"{tf_data_result.message}"
                            ),
                            dataset_id=task.dataset_id,
                        )
                    )

        # Static broadcast
        year_col_found = (
            year_result.ok
            and year_result.detail is not None
            and year_result.detail.get("year_column") is not None
        )
        broadcast_result = probe_static_broadcast(
            ds,
            task.dataset_id,
            year_column_found=year_col_found,
            universe_year_count=len(universe_years),
            distinct_paths=(
                len(distinct_paths_by_dataset.get(task.dataset_id, set()))
                if ds.file_set is not None
                else None
            ),
        )
        if not broadcast_result.ok:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.STATIC_BROADCAST,
                    message=(f"Dataset '{task.dataset_id}': {broadcast_result.message}"),
                    dataset_id=task.dataset_id,
                    years=universe_years,
                    remediation=Remediation(
                        hint=(
                            "Add year_column, use file_set for per-year files, "
                            "or set params.broadcast_static=true."
                        ),
                    ),
                )
            )

    return findings


_SAE_FAMILY_PREFIXES: dict[str, tuple[str, ...]] = {
    "household_income_bins": ("household_income_",),
    "contract_rent_bins": ("contract_rent_distribution_",),
    "gross_rent_bins": ("gross_rent_distribution_",),
    "rent_burden": ("gross_rent_pct_income_",),
    "owner_cost_burden": ("owner_costs_pct_income_",),
    "tenure_income": ("tenure_income_",),
}

_SAE_FAMILY_EXACT_COLUMNS: dict[str, tuple[str, ...]] = {
    "labor_force": ("civilian_labor_force", "unemployed_count"),
}

_SAE_OUTPUT_PREFIXES: dict[str, tuple[str, ...]] = {
    "household_income_bins": ("sae_household_income_",),
    "contract_rent_bins": ("sae_contract_rent_distribution_",),
    "gross_rent_bins": ("sae_gross_rent_distribution_",),
    "rent_burden": ("sae_rent_burden_",),
    "owner_cost_burden": ("sae_owner_cost_burden_",),
    "tenure_income": ("sae_tenure_income_",),
    "labor_force": (
        "sae_civilian_labor_force",
        "sae_unemployed_count",
        "sae_unemployment_rate",
    ),
}

_SAE_OUTPUT_EXACT_COLUMNS: dict[str, tuple[str, ...]] = {
    "household_income_bins": (
        "sae_household_income_median",
        "sae_household_income_quintile_cutoff_20",
        "sae_household_income_quintile_cutoff_40",
        "sae_household_income_quintile_cutoff_60",
        "sae_household_income_quintile_cutoff_80",
    ),
    "gross_rent_bins": ("sae_gross_rent_median",),
    "contract_rent_bins": ("sae_contract_rent_median",),
}


def _sae_family_columns(family: str, available: list[str]) -> list[str]:
    exact = set(_SAE_FAMILY_EXACT_COLUMNS.get(family, ()))
    prefixes = _SAE_FAMILY_PREFIXES.get(family, ())
    return [
        column
        for column in available
        if column in exact or any(column.startswith(prefix) for prefix in prefixes)
    ]


def _sae_output_allowed(family: str, output: str) -> bool:
    if output in _SAE_OUTPUT_EXACT_COLUMNS.get(family, ()):
        return True
    prefixes = _SAE_OUTPUT_PREFIXES.get(family, ())
    return any(output.startswith(prefix) for prefix in prefixes)


def _check_sae_task_paths(
    recipe: RecipeV1,
    project_root: Path,
    tasks: list[SmallAreaEstimateTask],
) -> list[PreflightFinding]:
    """Check SAE source/support artifacts and target crosswalk prerequisites."""
    findings: list[PreflightFinding] = []
    checked_paths: set[tuple[str, str | None, str]] = set()

    for task in tasks:
        if task.year in ACS1_UNAVAILABLE_VINTAGES:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message=(
                        f"SAE source dataset '{task.source_dataset}' requests ACS1 "
                        f"vintage {task.year}, but Census ACS 1-year data is "
                        "unavailable for that vintage."
                    ),
                    dataset_id=task.source_dataset,
                    years=[task.year],
                    remediation=_dataset_remediation(
                        task.source_dataset,
                        recipe.datasets[task.source_dataset],
                        years=[task.year],
                    ),
                )
            )

        for role, dataset_id, path in (
            ("source", task.source_dataset, task.source_path),
            ("support", task.support_dataset, task.support_path),
        ):
            key = (dataset_id, path, role)
            if key in checked_paths:
                continue
            checked_paths.add(key)
            ds = recipe.datasets[dataset_id]
            if path is None:
                path = ds.path
            if path is None:
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_DATASET,
                        message=(
                            f"SAE {role} dataset '{dataset_id}' does not resolve "
                            "to an artifact path."
                        ),
                        dataset_id=dataset_id,
                        years=[task.year],
                        remediation=_dataset_remediation(dataset_id, ds, years=[task.year]),
                    )
                )
                continue
            if not (project_root / path).exists():
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_DATASET,
                        message=f"SAE {role} dataset '{dataset_id}' path not found: {path}",
                        dataset_id=dataset_id,
                        years=[task.year],
                        remediation=_dataset_remediation(dataset_id, ds, years=[task.year]),
                    )
                )

        if task.target_geometry.type == "coc" and task.target_geometry.vintage is not None:
            xwalk_path = tract_xwalk_path(
                str(task.target_geometry.vintage),
                task.tract_vintage,
            )
            if not (project_root / xwalk_path).exists():
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_TRANSFORM,
                        message=(
                            "SAE target rollup requires a CoC-to-tract crosswalk "
                            f"artifact: {xwalk_path}"
                        ),
                        years=[task.year],
                        geometry=f"coc@{task.target_geometry.vintage}",
                        remediation=Remediation(
                            hint=(
                                "Generate the tract crosswalk for the SAE target "
                                "geometry and ACS5 tract support vintage."
                            ),
                            command=(
                                "hhplab generate xwalks "
                                f"--boundary {task.target_geometry.vintage} "
                                "--type tract "
                                f"--tracts {task.tract_vintage}"
                            ),
                        ),
                    )
                )

    return findings


def _check_sae_task_schemas(
    project_root: Path,
    tasks: list[SmallAreaEstimateTask],
) -> list[PreflightFinding]:
    """Inspect SAE source/support schemas for required component columns."""
    findings: list[PreflightFinding] = []
    checked: set[tuple[str, str | None, tuple[str, ...], tuple[tuple[str, str], ...]]] = set()

    for task in tasks:
        if task.support_geometry.vintage is not None and (
            str(task.support_geometry.vintage) != task.tract_vintage
        ):
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.DATASET_PROVENANCE,
                    message=(
                        f"SAE support dataset '{task.support_dataset}' resolved to "
                        f"tract vintage {task.support_geometry.vintage}, but the "
                        f"step requires tract_vintage {task.tract_vintage}."
                    ),
                    dataset_id=task.support_dataset,
                    years=[task.year],
                    remediation=Remediation(
                        hint=(
                            "Use an ACS5 tract SAE support artifact whose tract "
                            "vintage matches the SAE step tract_vintage."
                        ),
                    ),
                )
            )

        task_key = (
            task.output_dataset,
            task.source_path,
            task.support_path,
            tuple(task.measure_families),
            tuple(sorted(task.denominators.items())),
        )
        if task_key in checked:
            continue
        checked.add(task_key)

        source_path = None if task.source_path is None else project_root / task.source_path
        support_path = None if task.support_path is None else project_root / task.support_path
        if source_path is None or not source_path.exists():
            continue
        if support_path is None or not support_path.exists():
            continue

        source_schema = probe_dataset_schema(source_path)
        support_schema = probe_dataset_schema(support_path)
        if not source_schema.ok or not support_schema.ok:
            continue

        source_columns = list(source_schema.detail["columns"])
        support_columns = list(support_schema.detail["columns"])

        source_required = ["county_fips", "acs1_vintage"]
        support_required = ["tract_geoid", "county_fips", "acs_vintage", "tract_vintage"]
        for family in task.measure_families:
            source_required.extend(_sae_family_columns(family, ACS1_SAE_SOURCE_COLUMNS))
            support_required.extend(_sae_family_columns(family, ACS5_SAE_SUPPORT_COLUMNS))
            bad_outputs = [
                output
                for output in task.derived_outputs.get(family, [])
                if not _sae_output_allowed(family, output)
            ]
            if bad_outputs:
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.MISSING_MEASURE,
                        message=(
                            f"SAE measure family '{family}' cannot produce outputs "
                            f"{bad_outputs}. Use outputs derived from the matching "
                            "component distribution; do not request direct medians "
                            "or unrelated sae_* columns."
                        ),
                        dataset_id=task.output_dataset,
                        years=[task.year],
                        remediation=Remediation(
                            hint=(
                                "Move the output to the correct SAE measure family "
                                "or remove unsupported median/quintile configuration."
                            ),
                        ),
                    )
                )

        missing_source = [
            column for column in dict.fromkeys(source_required) if column not in source_columns
        ]
        if missing_source:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MEASURE,
                    message=(
                        f"SAE source dataset '{task.source_dataset}' "
                        f"({task.source_path}) is missing columns: {missing_source}"
                    ),
                    dataset_id=task.source_dataset,
                    years=[task.year],
                    remediation=Remediation(
                        hint=(
                            "Rebuild the ACS1 county SAE source artifact so it "
                            "contains the requested measure family components."
                        ),
                        command="hhplab ingest acs1-county --vintage <year>",
                    ),
                )
            )

        missing_support = [
            column for column in dict.fromkeys(support_required) if column not in support_columns
        ]
        if missing_support:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MEASURE,
                    message=(
                        f"SAE support dataset '{task.support_dataset}' "
                        f"({task.support_path}) is missing columns: {missing_support}"
                    ),
                    dataset_id=task.support_dataset,
                    years=[task.year],
                    remediation=Remediation(
                        hint=(
                            "Rebuild the ACS5 tract SAE support artifact so it "
                            "contains the requested distribution components."
                        ),
                        command=(
                            "hhplab ingest acs5-tract "
                            f"--acs {task.terminal_acs5_vintage} "
                            f"--tracts {task.tract_vintage}"
                        ),
                    ),
                )
            )

        missing_denominators = [
            column for column in task.denominators.values() if column not in support_columns
        ]
        if missing_denominators:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_MEASURE,
                    message=(
                        f"SAE support dataset '{task.support_dataset}' "
                        f"({task.support_path}) is missing denominator columns: "
                        f"{missing_denominators}"
                    ),
                    dataset_id=task.support_dataset,
                    years=[task.year],
                    remediation=Remediation(
                        hint=(
                            "Use denominator columns from the ACS5 tract SAE "
                            "support schema or rebuild the support artifact."
                        ),
                    ),
                )
            )

    return findings


def _check_support_datasets(
    recipe: RecipeV1,
    project_root: Path,
    needed_transforms: set[str],
    universe_years: list[int],
) -> list[PreflightFinding]:
    """Check support-dataset prerequisites for weighted transforms.

    When a transform uses population weighting, the referenced
    population_source dataset must exist and contain the declared
    population_field.  These checks run without loading data.
    """
    findings: list[PreflightFinding] = []

    for tid in sorted(needed_transforms):
        transform = None
        for t in recipe.transforms:
            if t.id == tid:
                transform = t
                break
        if transform is None:
            continue

        reqs = get_weighted_transform_requirements(transform)
        if reqs is None:
            continue

        population_source, population_field = reqs
        probe_results = probe_support_dataset(
            population_source=population_source,
            population_field=population_field,
            transform_id=tid,
            recipe=recipe,
            project_root=project_root,
            years=universe_years,
        )
        for r in probe_results:
            finding_kind = FindingKind.MISSING_SUPPORT_DATASET
            remediation_hint = (
                f"Ensure dataset '{population_source}' is "
                f"available with field '{population_field}' "
                f"for the required years."
            )
            remediation_command = (
                f"hhplab ingest {recipe.datasets[population_source].product}"
                if population_source in recipe.datasets
                and recipe.datasets[population_source].product
                else None
            )
            if (
                r.detail is not None
                and r.detail.get("finding_kind") == FindingKind.DATASET_PROVENANCE.value
            ):
                finding_kind = FindingKind.DATASET_PROVENANCE
                remediation_hint = (
                    "Rebuild the translated ACS tract cache so its provenance "
                    "records the source and target tract vintages."
                )
                remediation_command = r.detail.get("remediation_command")
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=finding_kind,
                    message=r.message,
                    transform_id=tid,
                    dataset_id=population_source,
                    years=(r.detail.get("missing_years") if r.detail else None),
                    remediation=Remediation(
                        hint=remediation_hint,
                        command=(
                            str(remediation_command) if remediation_command is not None else None
                        ),
                    ),
                )
            )

    return findings


def _check_pep_decennial_tract_mediated_inputs(
    recipe: RecipeV1,
    project_root: Path,
    resample_tasks: list[ResampleTask],
) -> list[PreflightFinding]:
    """Validate PEP baseline and crosswalk columns for decennial tract-mediated population."""
    findings: list[PreflightFinding] = []

    for task in resample_tasks:
        ds = recipe.datasets.get(task.dataset_id)
        if (
            ds is None
            or ds.provider != "census"
            or ds.product != "pep"
            or task.transform_id is None
            or task.weight_column != "population_weight"
        ):
            continue
        transform = next((t for t in recipe.transforms if t.id == task.transform_id), None)
        if transform is None:
            continue
        weighting = getattr(transform.spec, "weighting", None)
        if (
            weighting is None
            or weighting.scheme != "tract_mediated"
            or weighting.denominator_source != "decennial"
        ):
            continue
        baseline_year = int(weighting.resolved_denominator_vintage)

        if task.input_path is not None:
            pep_path = project_root / task.input_path
            if pep_path.exists():
                try:
                    pep_years = pd.read_parquet(pep_path, columns=["year"])["year"]
                except (OSError, ValueError, KeyError):
                    pep_years = pd.Series(dtype="int64")
                available_years = set(
                    pd.to_numeric(pep_years, errors="coerce").dropna().astype(int)
                )
                if baseline_year not in available_years:
                    findings.append(
                        PreflightFinding(
                            severity=Severity.ERROR,
                            kind=FindingKind.MISSING_SUPPORT_DATASET,
                            message=(
                                f"Pipeline PEP dataset '{task.dataset_id}' for transform "
                                f"'{task.transform_id}' requires baseline-year PEP "
                                f"county estimates for {baseline_year}."
                            ),
                            dataset_id=task.dataset_id,
                            transform_id=task.transform_id,
                            years=[baseline_year],
                            remediation=Remediation(
                                hint=(
                                    "Use a PEP county vintage that includes the decennial "
                                    "baseline year required by the tract-mediated denominator."
                                ),
                                command="hhplab ingest pep",
                            ),
                        )
                    )

        try:
            from hhplab.recipe.executor.transforms import _resolve_transform_path

            xwalk_path = _resolve_transform_path(task.transform_id, recipe, project_root)
        except Exception:
            continue
        if not xwalk_path.exists():
            continue
        try:
            columns = set(pq.read_schema(xwalk_path).names)
        except (OSError, ValueError):
            continue
        required_columns = {
            "population_weight",
            "county_population_total",
            "denominator_source",
            "denominator_vintage",
        }
        missing_columns = sorted(required_columns - columns)
        if missing_columns:
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_COLUMN,
                    message=(
                        f"Transform '{task.transform_id}' decennial PEP population "
                        f"aggregation requires crosswalk columns {missing_columns}."
                    ),
                    transform_id=task.transform_id,
                    remediation=Remediation(
                        hint=(
                            "Regenerate the tract-mediated county crosswalk with "
                            "decennial denominators so baseline scaling columns are present."
                        ),
                    ),
                )
            )

    return findings


def _filter_to_year(
    df: pd.DataFrame,
    year_col: str,
    year: int,
) -> pd.DataFrame:
    """Filter a DataFrame to a requested year, tolerating string year values."""
    series = df[year_col]
    if pd.api.types.is_numeric_dtype(series):
        mask = series == year
    else:
        coerced = pd.to_numeric(series, errors="coerce")
        if coerced.notna().any():
            mask = coerced == year
        else:
            mask = series.astype(str) == str(year)
    return df[mask].copy()


def _read_geo_values_for_year(
    *,
    path: Path,
    declared_geo_column: str | None,
    declared_year_column: str | None,
    year: int,
) -> pd.Series | None:
    """Load the relevant geo-ID column, filtered to the requested year when possible."""
    try:
        schema_columns = list(pq.read_schema(path).names)
    except (FileNotFoundError, OSError, ValueError):
        return None

    geo_result = probe_geo_column(schema_columns, declared_geo_column)
    if not geo_result.ok or not geo_result.detail:
        return None
    geo_col = geo_result.detail["geo_column"]

    year_result = probe_year_column(schema_columns, declared_year_column)
    year_col = year_result.detail["year_column"] if year_result.ok and year_result.detail else None

    read_columns = [geo_col]
    if year_col is not None and year_col not in read_columns:
        read_columns.append(year_col)
    try:
        df = pd.read_parquet(path, columns=read_columns)
    except (FileNotFoundError, OSError, ValueError):
        return None

    if year_col is not None and year_col in df.columns:
        df = _filter_to_year(df, year_col, year)
    return df[geo_col]


def _needs_ct_planning_to_legacy_alignment(
    *,
    xwalk_values: pd.Series,
    source_values: pd.Series | None,
) -> bool:
    """Return True when CT planning-region inputs need the legacy-county bridge."""
    if source_values is None:
        return False

    xwalk_has_ct_legacy = (
        xwalk_values.dropna()
        .astype(str)
        .map(
            is_ct_legacy_county_fips,
        )
        .any()
    )
    source_has_ct_planning = (
        source_values.dropna()
        .astype(str)
        .map(
            is_ct_planning_region_fips,
        )
        .any()
    )
    return bool(xwalk_has_ct_legacy and source_has_ct_planning)


def _check_ct_county_alignment(
    recipe: RecipeV1,
    project_root: Path,
    pipeline_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Detect CT legacy/planning mismatches and report the special-case path."""
    findings: list[PreflightFinding] = []
    bridge_status: dict[int, str | None] = {}
    source_events: dict[tuple[str, str, int], set[int]] = {}
    support_events: dict[tuple[str, str, str, int], set[int]] = {}

    def ensure_bridge_ready(legacy_vintage: int) -> str | None:
        cached = bridge_status.get(legacy_vintage)
        if legacy_vintage in bridge_status:
            return cached
        try:
            build_ct_county_planning_region_crosswalk(
                legacy_county_vintage=legacy_vintage,
                planning_region_vintage=CT_PLANNING_REGION_VINTAGE,
            )
        except (FileNotFoundError, OSError, ValueError) as exc:
            bridge_status[legacy_vintage] = (
                "Connecticut county alignment needs the authoritative bridge "
                f"derived from {county_path(legacy_vintage)} and "
                f"{county_path(CT_PLANNING_REGION_VINTAGE)}: {exc}"
            )
        else:
            bridge_status[legacy_vintage] = None
        return bridge_status[legacy_vintage]

    for pipeline_id, task in pipeline_tasks:
        if task.method != "aggregate" or task.transform_id is None:
            continue
        if task.effective_geometry.type != "county":
            continue

        transform_probe = probe_transform_path(task.transform_id, recipe, project_root)
        transform_path = (
            project_root / transform_probe.detail["path"]
            if transform_probe.ok and transform_probe.detail
            else None
        )
        if transform_path is None or not transform_path.exists():
            continue

        try:
            xwalk = pd.read_parquet(transform_path, columns=["county_fips"])
        except (FileNotFoundError, OSError, ValueError, KeyError):
            continue

        legacy_vintage = (
            int(task.effective_geometry.vintage)
            if task.effective_geometry.vintage is not None
            else CT_LEGACY_COUNTY_VINTAGE
        )

        if task.input_path is not None:
            source_series = _read_geo_values_for_year(
                path=project_root / task.input_path,
                declared_geo_column=task.geo_column,
                declared_year_column=task.year_column,
                year=task.year,
            )
            if _needs_ct_planning_to_legacy_alignment(
                xwalk_values=xwalk["county_fips"],
                source_values=source_series,
            ):
                bridge_error = ensure_bridge_ready(legacy_vintage)
                if bridge_error is None:
                    source_events.setdefault(
                        (pipeline_id, task.dataset_id, legacy_vintage),
                        set(),
                    ).add(task.year)
                else:
                    findings.append(
                        PreflightFinding(
                            severity=Severity.ERROR,
                            kind=FindingKind.CT_COUNTY_ALIGNMENT,
                            message=(
                                f"Pipeline '{pipeline_id}': dataset '{task.dataset_id}' "
                                "uses Connecticut planning-region county IDs against "
                                f"legacy county crosswalk '{task.transform_id}' for "
                                f"year {task.year}. {bridge_error}"
                            ),
                            pipeline_id=pipeline_id,
                            dataset_id=task.dataset_id,
                            transform_id=task.transform_id,
                            years=[task.year],
                            remediation=Remediation(
                                hint=(
                                    "Materialize the CT planning-region county geometry "
                                    "before running this recipe so the bridge can be built."
                                ),
                                command="hhplab ingest tiger --year 2023 --type counties",
                            ),
                        )
                    )

        transform = next((t for t in recipe.transforms if t.id == task.transform_id), None)
        reqs = get_weighted_transform_requirements(transform) if transform is not None else None
        if reqs is None:
            continue

        population_source, _population_field = reqs
        resolved = _resolve_dataset_year(population_source, task.year, recipe)
        if resolved.path is None:
            continue
        support_path = project_root / resolved.path
        if not support_path.exists():
            continue

        support_ds = recipe.datasets.get(population_source)
        if support_ds is None:
            continue
        support_series = _read_geo_values_for_year(
            path=support_path,
            declared_geo_column=support_ds.geo_column,
            declared_year_column=support_ds.year_column,
            year=task.year,
        )
        if _needs_ct_planning_to_legacy_alignment(
            xwalk_values=xwalk["county_fips"],
            source_values=support_series,
        ):
            bridge_error = ensure_bridge_ready(legacy_vintage)
            if bridge_error is None:
                support_events.setdefault(
                    (pipeline_id, task.transform_id, population_source, legacy_vintage),
                    set(),
                ).add(task.year)
            else:
                findings.append(
                    PreflightFinding(
                        severity=Severity.ERROR,
                        kind=FindingKind.CT_COUNTY_ALIGNMENT,
                        message=(
                            f"Pipeline '{pipeline_id}': population_source "
                            f"'{population_source}' for transform '{task.transform_id}' "
                            "uses Connecticut planning-region county IDs against a "
                            f"legacy county crosswalk for year {task.year}. {bridge_error}"
                        ),
                        pipeline_id=pipeline_id,
                        dataset_id=population_source,
                        transform_id=task.transform_id,
                        years=[task.year],
                        remediation=Remediation(
                            hint=(
                                "Materialize the CT planning-region county geometry "
                                "before running this recipe so the bridge can be built."
                            ),
                            command="hhplab ingest tiger --year 2023 --type counties",
                        ),
                    )
                )

    for (pipeline_id, dataset_id, legacy_vintage), years in sorted(source_events.items()):
        findings.append(
            PreflightFinding(
                severity=Severity.WARNING,
                kind=FindingKind.CT_COUNTY_ALIGNMENT,
                message=(
                    f"Pipeline '{pipeline_id}': Connecticut special-case alignment "
                    f"will translate planning-region dataset '{dataset_id}' to "
                    f"legacy counties for years {sorted(years)} using "
                    f"{county_path(legacy_vintage)} and "
                    f"{county_path(CT_PLANNING_REGION_VINTAGE)}."
                ),
                pipeline_id=pipeline_id,
                dataset_id=dataset_id,
                years=sorted(years),
            )
        )

    for key, years in sorted(support_events.items()):
        pipeline_id, transform_id, dataset_id, legacy_vintage = key
        findings.append(
            PreflightFinding(
                severity=Severity.WARNING,
                kind=FindingKind.CT_COUNTY_ALIGNMENT,
                message=(
                    f"Pipeline '{pipeline_id}': Connecticut special-case alignment "
                    f"will translate planning-region population_source '{dataset_id}' "
                    f"for transform '{transform_id}' to legacy counties for years "
                    f"{sorted(years)} using {county_path(legacy_vintage)} and "
                    f"{county_path(CT_PLANNING_REGION_VINTAGE)}."
                ),
                pipeline_id=pipeline_id,
                dataset_id=dataset_id,
                transform_id=transform_id,
                years=sorted(years),
            )
        )

    return findings


def _check_msa_coc_panel_sources(
    recipe: RecipeV1,
    pipeline_resample_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Check that MSA-CoC panel targets have the required source steps."""
    findings: list[PreflightFinding] = []
    tasks_by_pipeline: dict[str, list[ResampleTask]] = {}
    for pipeline_id, task in pipeline_resample_tasks:
        tasks_by_pipeline.setdefault(pipeline_id, []).append(task)

    for pipeline in recipe.pipelines:
        target = next((target for target in recipe.targets if target.id == pipeline.target), None)
        if target is None or target.msa_coc_panel is None:
            continue
        panel_spec = target.msa_coc_panel
        tasks = tasks_by_pipeline.get(pipeline.id, [])
        source_geometries = {
            (_preflight_source_token(recipe, task.dataset_id), task.to_geometry.type)
            for task in tasks
        }
        dataset_ids_by_source = {
            _preflight_source_token(recipe, task.dataset_id): task.dataset_id for task in tasks
        }

        required: list[tuple[str, str, str, str | None]] = [
            ("acs5", "msa", "ACS5 MSA covariate source", None),
            (
                panel_spec.msa_population_source,
                "msa",
                f"{panel_spec.msa_population_source.upper()} MSA population source",
                None,
            ),
            ("pit", "coc", "CoC PIT source", None),
        ]
        if panel_spec.unemployment_source == "laus":
            required.append(
                (
                    "laus",
                    "msa",
                    "BLS LAUS MSA unemployment source",
                    "hhplab ingest laus-metro --year <year>",
                )
            )
        else:
            required.append(("acs5", "msa", "ACS5 MSA unemployment source", None))

        for source_token, geo_type, label, command in required:
            if (source_token, geo_type) in source_geometries:
                continue
            findings.append(
                PreflightFinding(
                    severity=Severity.ERROR,
                    kind=FindingKind.MISSING_DATASET,
                    message=(
                        f"MSA-CoC panel target '{target.id}' requires a recipe "
                        f"resample step for {label} at {geo_type.upper()} geometry."
                    ),
                    pipeline_id=pipeline.id,
                    dataset_id=dataset_ids_by_source.get(source_token),
                    geometry=geo_type,
                    remediation=Remediation(
                        hint=(
                            "Add a dataset and resample step that materializes this "
                            "source for the MSA-CoC panel before persistence."
                        ),
                        command=command,
                    ),
                )
            )

    return findings


def _check_msa_coc_coverage_sources(
    recipe: RecipeV1,
    pipeline_resample_tasks: list[tuple[str, ResampleTask]],
) -> list[PreflightFinding]:
    """Check that MSA-CoC coverage targets have a ranking source step."""
    findings: list[PreflightFinding] = []
    tasks_by_pipeline: dict[str, list[ResampleTask]] = {}
    for pipeline_id, task in pipeline_resample_tasks:
        tasks_by_pipeline.setdefault(pipeline_id, []).append(task)

    for pipeline in recipe.pipelines:
        target = next((target for target in recipe.targets if target.id == pipeline.target), None)
        if target is None or target.msa_coc_coverage is None:
            continue
        spec = target.msa_coc_coverage
        tasks = tasks_by_pipeline.get(pipeline.id, [])
        matching = [
            task
            for task in tasks
            if _preflight_source_token(recipe, task.dataset_id) == spec.ranking_population_source
            and task.to_geometry.type == "msa"
            and task.year == spec.ranking_reference_year
        ]
        if matching:
            continue
        findings.append(
            PreflightFinding(
                severity=Severity.ERROR,
                kind=FindingKind.MISSING_DATASET,
                message=(
                    f"MSA-CoC coverage target '{target.id}' requires a recipe "
                    f"resample step for {spec.ranking_population_source.upper()} "
                    f"MSA ranking population in year {spec.ranking_reference_year}."
                ),
                pipeline_id=pipeline.id,
                geometry="msa",
                remediation=Remediation(
                    hint=(
                        "Add a dataset and resample step that materializes the "
                        "ranking population source at MSA geometry before "
                        "msa_coc_coverage persistence."
                    ),
                ),
            )
        )

    return findings


def _preflight_source_token(recipe: RecipeV1, dataset_id: str) -> str:
    ds = recipe.datasets.get(dataset_id)
    if ds is None:
        return dataset_id
    if ds.provider == "census" and ds.product in {"acs", "acs5"}:
        return "acs5"
    if ds.provider == "census" and ds.product == "pep":
        return "pep"
    if ds.provider == "bls" and ds.product == "laus":
        return "laus"
    if ds.provider == "hud" and ds.product == "pit":
        return "pit"
    return ds.product


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
