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


def register_dataset_defaults(registry: DatasetAdapterRegistry) -> None:
    """Register built-in dataset adapters."""
    registry.register("hud", "pit", _validate_hud_pit)
    registry.register("census", "acs5", _validate_census_acs5)
    registry.register("census", "acs", _validate_census_acs)
    registry.register("census", "acs1", _validate_census_acs1)
    registry.register("census", "acs1_poverty", _validate_census_acs1_poverty)
    registry.register("census", "acs1_imputation_target", _validate_census_acs1_imputation_target)
    registry.register("census", "acs5_imputation_support", _validate_census_acs5_imputation_support)
    registry.register("census", "pep", _validate_census_pep)
    registry.register("zillow", "zori", _validate_zillow_zori)
    registry.register("bls", "laus", _validate_bls_laus)
