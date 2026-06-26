"""Built-in dataset adapters for recipe validation."""

from __future__ import annotations

from hhplab.recipe.adapters import (
    DatasetAdapterRegistry,
    ValidationDiagnostic,
)
from hhplab.recipe.recipe_schema import DatasetSpec
from hhplab.schema import (
    ACS1_IMPUTATION_REQUIRED_ACS1_SOURCE_COLUMNS,
    ACS1_IMPUTATION_REQUIRED_ACS5_SUPPORT_COLUMNS,
)


def _uses_materialized_artifact(spec: DatasetSpec) -> bool:
    """Whether the recipe points at a concrete on-disk artifact."""
    return spec.path is not None or spec.file_set is not None


def _validate_hud_pit(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate HUD PIT dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/pit: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "coc" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/pit: expected native_geometry type 'coc', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"vintage", "align"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"hud/pit: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params:
        valid_aligns = ("point_in_time_jan", "to_calendar_year")
        if spec.params["align"] not in valid_aligns:
            diags.append(
                ValidationDiagnostic(
                    "warning",
                    f"hud/pit: unknown align mode '{spec.params['align']}'; "
                    f"expected one of {valid_aligns}.",
                )
            )
    return diags


def _validate_hud_hic(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate HUD HIC dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/hic: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "coc" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hud/hic: expected native_geometry type 'coc', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"vintage", "align"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"hud/hic: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params:
        valid_aligns = ("point_in_time_jan", "to_calendar_year")
        if spec.params["align"] not in valid_aligns:
            diags.append(
                ValidationDiagnostic(
                    "warning",
                    f"hud/hic: unknown align mode '{spec.params['align']}'; "
                    f"expected one of {valid_aligns}.",
                )
            )
    return diags


def _validate_census_acs5(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census ACS5 dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    return diags


def _validate_hhplab_msa_pit_rollup(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate pre-materialized HHP-Lab MSA PIT rollup artifacts."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"hhplab/msa_pit_rollup: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "msa":
        diags.append(
            ValidationDiagnostic(
                "error",
                "hhplab/msa_pit_rollup: expected native_geometry type 'msa', "
                f"got '{spec.native_geometry.type}'.",
            )
        )
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                "hhplab/msa_pit_rollup: path or file_set is required because this "
                "adapter validates pre-materialized rollup artifacts.",
            )
        )
    return diags


def _validate_census_acs5_contract_rent_bins(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate ACS5 tract contract-rent bin artifacts."""
    diags = _validate_census_acs5(spec)
    if spec.native_geometry.type != "tract":
        return diags
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                "census/acs5_contract_rent_bins: path or file_set is required for "
                "the pre-materialized contract-rent bin artifact.",
            )
        )
    return diags


def _validate_census_acs5_household_income_bins(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate ACS5 tract household-income bin artifacts."""
    diags = _validate_census_acs5(spec)
    if spec.native_geometry.type != "tract":
        return diags
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                "census/acs5_household_income_bins: path or file_set is required for "
                "the pre-materialized household-income bin artifact.",
            )
        )
    return diags


def _validate_census_acs(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census ACS dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    return diags


def _validate_census_acs1(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census ACS 1-year dataset specification."""
    diags: list[ValidationDiagnostic] = []
    supported_native_geometries = {"metro", "county"}
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs1: unsupported version {spec.version}; expected 1.",
            )
        )
    if (
        spec.native_geometry.type not in supported_native_geometries
        and not _uses_materialized_artifact(spec)
    ):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs1: expected native_geometry type 'metro' or 'county', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    if spec.native_geometry.type in supported_native_geometries and not spec.native_geometry.source:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/acs1: {spec.native_geometry.type}-native geometry has no source set; "
                "consider setting source for provenance tracking.",
            )
        )
    known_params = {"vintage", "align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/acs1: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def _validate_census_acs1_poverty(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate pre-materialized ACS1 poverty-rate artifacts."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs1_poverty: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs1_poverty: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/acs1_poverty: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def _validate_census_acs1_imputation_target(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate ACS1 target inputs for modeled tract imputation."""
    diags: list[ValidationDiagnostic] = []
    supported_native_geometries = {"county", "place", "metro"}
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs1_imputation_target: unsupported version {spec.version}; expected 1.",
            )
        )
    if (
        spec.native_geometry.type not in supported_native_geometries
        and not _uses_materialized_artifact(spec)
    ):
        if spec.native_geometry.type == "tract":
            message = (
                "census/acs1_imputation_target: direct ACS1 tract data is unavailable "
                "from Census. Use an ACS1 target geography such as county, place, or "
                "metro and pair it with ACS5 tract support, or set path/file_set to a "
                "pre-materialized modeled tract artifact."
            )
        else:
            message = (
                "census/acs1_imputation_target: expected native_geometry type "
                "'county', 'place', or 'metro', got "
                f"'{spec.native_geometry.type}'."
            )
        diags.append(ValidationDiagnostic("error", message))
    if spec.native_geometry.type in supported_native_geometries and not spec.native_geometry.source:
        diags.append(
            ValidationDiagnostic(
                "warning",
                "census/acs1_imputation_target: target geometry has no source set; "
                "set source for target matching provenance.",
            )
        )
    known_params = {
        "vintage",
        "align",
        "broadcast_static",
        "target_id_col",
        "measure_specs",
        "control_policy",
        "control_preference",
        "control_geo_type_column",
        "control_geo_id_column",
        "fallback_reason_column",
    }
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/acs1_imputation_target: unrecognized params {sorted(unknown)}.",
            )
        )
    if "measure_specs" not in spec.params:
        diags.append(
            ValidationDiagnostic(
                "warning",
                "census/acs1_imputation_target: measure_specs not declared; default "
                "imputation specs require ACS1 count columns "
                f"{list(ACS1_IMPUTATION_REQUIRED_ACS1_SOURCE_COLUMNS)}.",
            )
        )
    return diags


def _validate_census_acs5_imputation_support(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate ACS5 tract support inputs for modeled ACS1 imputation."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5_imputation_support: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "tract" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/acs5_imputation_support: expected native_geometry type 'tract', "
                f"got '{spec.native_geometry.type}'. ACS5 support must provide tract "
                "counts from the ACS5 vintage ending in the analysis year.",
            )
        )
    known_params = {"vintage", "tract_vintage", "align", "target_id_col", "measure_specs"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/acs5_imputation_support: unrecognized params {sorted(unknown)}.",
            )
        )
    if "measure_specs" not in spec.params:
        diags.append(
            ValidationDiagnostic(
                "warning",
                "census/acs5_imputation_support: measure_specs not declared; default "
                "imputation specs require ACS5 tract count columns "
                f"{list(ACS1_IMPUTATION_REQUIRED_ACS5_SUPPORT_COLUMNS)}.",
            )
        )
    return diags


def _validate_census_pep(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Census PEP dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/pep: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/pep: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"series", "vintage", "align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/pep: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params:
        valid_aligns = ("point_in_time_jan",)
        if spec.params["align"] not in valid_aligns:
            diags.append(
                ValidationDiagnostic(
                    "warning",
                    f"census/pep: unknown align mode '{spec.params['align']}'; "
                    f"expected one of {valid_aligns}.",
                )
            )
    return diags


def _validate_medsl_president(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate MEDSL county presidential political measure artifacts."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"medsl/president: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county":
        diags.append(
            ValidationDiagnostic(
                "error",
                f"medsl/president: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Materialize MEDSL county "
                "presidential returns before using them in recipes.",
            )
        )
    if spec.geo_column not in {"county_fips", "geo_id"}:
        diags.append(
            ValidationDiagnostic(
                "error",
                "medsl/president: set geo_column to 'county_fips' or 'geo_id' "
                "for the county presidential artifact.",
            )
        )
    if spec.year_column != "year":
        diags.append(
            ValidationDiagnostic(
                "error",
                "medsl/president: set year_column to 'year' for the county "
                "presidential artifact.",
            )
        )

    known_params = {"county_vintage", "align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"medsl/president: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params and spec.params["align"] != "presidential_election_year":
        diags.append(
            ValidationDiagnostic(
                "warning",
                "medsl/president: params.align should be "
                "'presidential_election_year' when declared.",
            )
        )
    return diags


def _validate_vera_incarceration_trends(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Vera county incarceration trends measure artifacts."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"vera/incarceration_trends: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county":
        diags.append(
            ValidationDiagnostic(
                "error",
                f"vera/incarceration_trends: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. The Vera source is county-native.",
            )
        )
    if spec.geo_column not in {"county_fips", "geo_id"}:
        diags.append(
            ValidationDiagnostic(
                "error",
                "vera/incarceration_trends: set geo_column to 'county_fips' or "
                "'geo_id' for the county-year artifact.",
            )
        )
    if spec.year_column != "year":
        diags.append(
            ValidationDiagnostic(
                "error",
                "vera/incarceration_trends: set year_column to 'year' for the "
                "county-year artifact.",
            )
        )

    known_params = {"county_vintage", "align", "broadcast_static"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"vera/incarceration_trends: unrecognized params {sorted(unknown)}.",
            )
        )
    if "align" in spec.params and spec.params["align"] != "calendar_year":
        diags.append(
            ValidationDiagnostic(
                "warning",
                "vera/incarceration_trends: params.align should be 'calendar_year' "
                "when declared.",
            )
        )
    return diags


def _validate_census_urban_fraction(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate CoC urban population fraction covariate artifacts."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/urban_fraction: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "coc" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"census/urban_fraction: expected native_geometry type 'coc', "
                f"got '{spec.native_geometry.type}'. Urban fraction artifacts "
                "are CoC-native static covariates.",
            )
        )

    known_params = {
        "boundary_vintage",
        "urban_area_vintage",
        "block_vintage",
        "decennial_vintage",
        "decennial",
        "broadcast_static",
    }
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"census/urban_fraction: unrecognized params {sorted(unknown)}.",
            )
        )

    if not _uses_materialized_artifact(spec):
        boundary_vintage = spec.params.get("boundary_vintage") or spec.native_geometry.vintage
        decennial_vintage = spec.params.get("decennial_vintage") or spec.params.get("decennial")
        if boundary_vintage is None:
            diags.append(
                ValidationDiagnostic(
                    "error",
                    "census/urban_fraction: default path resolution requires "
                    "native_geometry.vintage or params.boundary_vintage.",
                )
            )
        if decennial_vintage is None:
            diags.append(
                ValidationDiagnostic(
                    "error",
                    "census/urban_fraction: default path resolution requires "
                    "params.decennial_vintage.",
                )
            )
        if spec.params.get("broadcast_static") is not True:
            diags.append(
                ValidationDiagnostic(
                    "warning",
                    "census/urban_fraction: artifact is a static CoC covariate; "
                    "set params.broadcast_static=true for multi-year recipes.",
                )
            )
    return diags


def _validate_zillow_zori(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate Zillow ZORI dataset specification."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"zillow/zori: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"zillow/zori: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Recipes that point to "
                "pre-materialized derived artifacts must set path or file_set.",
            )
        )
    known_params = {"align", "series", "vintage"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"zillow/zori: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def _validate_prism_temperature(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate PRISM county-month temperature artifact specifications."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"prism/temperature: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "county":
        diags.append(
            ValidationDiagnostic(
                "error",
                f"prism/temperature: expected native_geometry type 'county', "
                f"got '{spec.native_geometry.type}'. Materialize PRISM rasters with "
                "`hhplab build prism-county` and set native_geometry.type to 'county'.",
            )
        )
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: recipes must set path or file_set to a "
                "pre-materialized county artifact. Run `hhplab build prism-county "
                "--variable <tmin|tmean|tmax> --year <YEAR> --month <MONTH>` first.",
            )
        )
    if spec.geo_column is None:
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: set geo_column to 'geo_id' or 'county_fips' "
                "so recipe joins can identify county rows.",
            )
        )
    if spec.year_column is None:
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: set year_column to 'year' for the county artifact.",
            )
        )

    known_params = {"variable", "month", "align"}
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"prism/temperature: unrecognized params {sorted(unknown)}.",
            )
        )

    variable = spec.params.get("variable")
    if variable not in {"tmin", "tmean", "tmax"}:
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: params.variable must be one of 'tmin', 'tmean', or 'tmax'.",
            )
        )

    month = spec.params.get("month")
    if not isinstance(month, int) or month < 1 or month > 12:
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: params.month must be an integer from 1 to 12.",
            )
        )

    align = spec.params.get("align", "calendar_month")
    valid_aligns = {"calendar_month", "point_in_time_jan"}
    if align not in valid_aligns:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"prism/temperature: params.align must be one of {sorted(valid_aligns)}.",
            )
        )
    if align == "point_in_time_jan" and month != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                "prism/temperature: params.align='point_in_time_jan' requires params.month=1.",
            )
        )
    return diags


def _validate_bls_laus(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate BLS LAUS metro dataset specification.

    BLS LAUS annual-average metro datasets must have metro native geometry
    and should reference a pre-materialized curated artifact (path is
    required because LAUS is fetched separately via 'hhplab ingest laus-metro').

    The expected path pattern is:
        data/curated/laus/laus_metro__A{year}@D{definition}.parquet

    Run the ingest command before using LAUS in a recipe:
        hhplab ingest laus-metro --year YEAR
    """
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"bls/laus: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "metro" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"bls/laus: expected native_geometry type 'metro', "
                f"got '{spec.native_geometry.type}'. BLS LAUS data is "
                "metro-native; set native_geometry.type to 'metro'.",
            )
        )
    if spec.native_geometry.type == "metro" and not spec.native_geometry.source:
        diags.append(
            ValidationDiagnostic(
                "warning",
                "bls/laus: metro-native geometry has no source set; "
                "consider setting source (e.g. 'glynn_fox_v1') for provenance.",
            )
        )
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "warning",
                "bls/laus: no path set. LAUS data must be ingested before "
                "recipe execution. Run: hhplab ingest laus-metro --year YEAR",
            )
        )
    known_params: set[str] = set()
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"bls/laus: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def _validate_bls_cpi_u(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    """Validate BLS CPI-U annual index dataset specifications."""
    diags: list[ValidationDiagnostic] = []
    if spec.version != 1:
        diags.append(
            ValidationDiagnostic(
                "error",
                f"bls/cpi_u: unsupported version {spec.version}; expected 1.",
            )
        )
    if spec.native_geometry.type != "national" and not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "error",
                f"bls/cpi_u: expected native_geometry type 'national', "
                f"got '{spec.native_geometry.type}'. CPI-U is a national index.",
            )
        )
    if not _uses_materialized_artifact(spec):
        diags.append(
            ValidationDiagnostic(
                "warning",
                "bls/cpi_u: no path set. CPI-U data must be ingested before "
                "recipe execution. Run: hhplab ingest cpi-u --start-year START --end-year END",
            )
        )
    known_params: set[str] = set()
    unknown = set(spec.params.keys()) - known_params
    if unknown:
        diags.append(
            ValidationDiagnostic(
                "warning",
                f"bls/cpi_u: unrecognized params {sorted(unknown)}.",
            )
        )
    return diags


def register_dataset_defaults(registry: DatasetAdapterRegistry) -> None:
    """Register built-in dataset adapters."""
    registry.register("hhplab", "msa_pit_rollup", _validate_hhplab_msa_pit_rollup)
    registry.register("hud", "pit", _validate_hud_pit)
    registry.register("hud", "hic", _validate_hud_hic)
    registry.register("census", "acs5", _validate_census_acs5)
    registry.register(
        "census",
        "acs5_contract_rent_bins",
        _validate_census_acs5_contract_rent_bins,
    )
    registry.register(
        "census",
        "acs5_household_income_bins",
        _validate_census_acs5_household_income_bins,
    )
    registry.register("census", "acs", _validate_census_acs)
    registry.register("census", "acs1", _validate_census_acs1)
    registry.register("census", "acs1_poverty", _validate_census_acs1_poverty)
    registry.register("census", "acs1_imputation_target", _validate_census_acs1_imputation_target)
    registry.register("census", "acs5_imputation_support", _validate_census_acs5_imputation_support)
    registry.register("census", "pep", _validate_census_pep)
    registry.register("census", "urban_fraction", _validate_census_urban_fraction)
    registry.register("medsl", "president", _validate_medsl_president)
    registry.register("vera", "incarceration_trends", _validate_vera_incarceration_trends)
    registry.register("zillow", "zori", _validate_zillow_zori)
    registry.register("prism", "temperature", _validate_prism_temperature)
    registry.register("bls", "laus", _validate_bls_laus)
    registry.register("bls", "cpi_u", _validate_bls_cpi_u)
