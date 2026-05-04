"""
hhplab recipe schema (v1) - Pydantic v2 models

Goal:
- Represent a declarative build recipe that can target multiple geometries
  (CoC, county, state, tract, zcta, etc.)
- Support extensible datasets (e.g., MIT Election Data and others) via
  provider/product/version + free-form params
- Express a small, stable set of transform operators (crosswalk, rollup)
  and pipeline steps (materialize, resample, join)

Notes:
- Geometry types are strings (open set). Runtime plugin layer should
  validate whether a geometry adapter exists.
- Dataset params are free-form. Runtime dataset adapter validates params
  for a given provider/product/version.
- This file is the *structural* schema; semantic validation
  (e.g., allocatability of measures) belongs in a compiler/adapters layer.
"""

from __future__ import annotations

from pathlib import Path
from string import Formatter
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from hhplab.recipe.schema_common import (
    GeometryRef,
    VintageSetSpec,
    YearSpec,
    expand_year_spec,
)

# -----------------------------
# File set (time-banded dataset paths)
# -----------------------------


class FileSetSegment(BaseModel):
    """A time-banded segment mapping years to a geometry vintage and optional path overrides."""

    model_config = ConfigDict(extra="forbid")

    years: YearSpec
    geometry: GeometryRef
    overrides: dict[int, str] = Field(default_factory=dict)
    constants: dict[str, str | int] = Field(
        default_factory=dict,
        description=(
            "Optional constant template variables for path rendering, for example {'tract': 2010}."
        ),
    )
    year_offsets: dict[str, int] = Field(
        default_factory=dict,
        description=(
            "Optional year-derived template variables, where each value is "
            "added to the analysis year, for example {'acs_end': -1}."
        ),
    )


class FileSetSpec(BaseModel):
    """Path template + segments for datasets whose geometry vintage varies by year."""

    model_config = ConfigDict(extra="forbid")

    path_template: str = Field(
        ...,
        description=(
            "Template for dataset paths. Supports {year} and optional segment "
            "variables from constants/year_offsets."
        ),
    )
    segments: list[FileSetSegment] = Field(..., min_length=1)

    @field_validator("path_template")
    @classmethod
    def _validate_path_template(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("FileSetSpec.path_template must be non-empty.")
        return value


# -----------------------------
# Datasets (extensible)
# -----------------------------


class DatasetSpec(BaseModel):
    """
    Extensible dataset declaration. Adapter resolution is via (provider, product, version).
    native_geometry indicates the dataset's base spatial granularity.
    params is free-form and validated by the dataset adapter.
    path optionally points to a project-relative on-disk artifact to use for this dataset.
    file_set provides time-banded paths and geometry vintages for multi-segment datasets.
    """

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(
        ...,
        description=("Dataset provider namespace, e.g. 'hud', 'census', 'zillow', 'mit-election'."),
    )
    product: str = Field(
        ...,
        description=(
            "Dataset product name within provider, e.g. 'pit', 'acs5', 'pep', 'county-returns'."
        ),
    )
    version: int = Field(
        ...,
        description="Adapter version for this dataset product (schema evolution control).",
        ge=1,
    )
    native_geometry: GeometryRef = Field(..., description="Native geometry of the dataset.")
    years: YearSpec | None = Field(
        default=None,
        description=(
            "Temporal coverage of this dataset."
            " For file_set datasets, coverage is implicit from segments."
        ),
    )
    params: dict[str, Any] = Field(default_factory=dict, description="Free-form adapter params.")
    path: str | None = Field(
        default=None,
        description="Optional project-relative file path for a pre-materialized dataset artifact.",
        examples=["data/curated/pit/pit_vintage__P2024.parquet"],
    )
    file_set: FileSetSpec | None = Field(
        default=None,
        description="Time-banded file set with per-segment geometry vintages.",
    )
    year_column: str | None = Field(
        default=None,
        description="Column name containing the year dimension. Auto-detected if omitted.",
    )
    geo_column: str | None = Field(
        default=None,
        description="Column name containing the geo-ID. Auto-detected if omitted.",
    )
    optional: bool = Field(
        default=False,
        description=("If true, missing dataset does not fail the build (policy still applies)."),
    )

    @field_validator("path")
    @classmethod
    def _validate_path_is_relative(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.strip():
            raise ValueError("DatasetSpec.path must be a non-empty relative path.")
        if Path(value).is_absolute():
            raise ValueError("DatasetSpec.path must be a relative path, not absolute.")
        return value


# -----------------------------
# Targets / outputs
# -----------------------------

# -----------------------------
# Filters (temporal)
# -----------------------------

TemporalMethod = Literal[
    "point_in_time",
    "calendar_mean",
    "calendar_median",
    "interpolate_to_month",
]


class TemporalFilter(BaseModel):
    """Temporal filter applied before geographic resampling."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["temporal"] = "temporal"
    column: str = Field(
        ...,
        description="Column containing the temporal dimension (e.g. 'month', 'date').",
    )
    method: TemporalMethod = Field(..., description="Temporal aggregation method.")
    month: int | None = Field(
        default=None,
        description="Month for point_in_time filter (1-12).",
        ge=1,
        le=12,
    )

    @model_validator(mode="after")
    def _validate_method_requires_month(self) -> TemporalFilter:
        if self.method in ("point_in_time", "interpolate_to_month") and self.month is None:
            raise ValueError(f"Temporal filter method '{self.method}' requires 'month'.")
        return self


FilterSpec = TemporalFilter  # Extensible: Union[TemporalFilter, ...] in future


# -----------------------------
# Targets / outputs
# -----------------------------

OutputKind = Literal["panel", "diagnostics", "map"]

CohortMethod = Literal["top_n", "bottom_n", "percentile"]


class ZoriPolicy(BaseModel):
    """Declarative ZORI eligibility and provenance policy.

    When present on a target, the recipe executor applies ZORI eligibility
    rules and provenance column generation using these settings instead of
    relying on implicit ``build_panel`` defaults.
    """

    model_config = ConfigDict(extra="forbid")

    min_coverage: float = Field(
        default=0.90,
        ge=0.0,
        le=1.0,
        description="Minimum coverage ratio for ZORI eligibility.",
    )


class Acs1Policy(BaseModel):
    """Declarative ACS 1-year merge policy for metro panels.

    When present, the executor merges ACS 1-year metro-native measures
    (e.g. unemployment_rate_acs1) into the panel alongside ACS 5-year
    measures.
    """

    model_config = ConfigDict(extra="forbid")

    include: bool = Field(
        default=False,
        description="If true, merge ACS 1-year metro-native measures into the panel.",
    )


class LausPolicy(BaseModel):
    """Declarative BLS LAUS merge policy for metro panels.

    When present, the executor annotates LAUS provenance columns and enables
    LAUS-aware conformance checks.  LAUS annual-average data must already be
    loaded as a dataset in the recipe pipeline (provider: bls, product: laus);
    this policy records the provenance and signals downstream conformance to
    validate the LAUS columns (labor_force, employed, unemployed,
    unemployment_rate) instead of treating them as missing.

    LAUS is distinct from ACS1: BLS publishes official labor-force estimates
    for metro statistical areas, while ACS1 provides survey-based estimates.
    Do not confuse the BLS unemployment_rate with unemployment_rate_acs1.
    """

    model_config = ConfigDict(extra="forbid")

    include: bool = Field(
        default=False,
        description=(
            "If true, treat BLS LAUS metro measures as present and annotate "
            "laus_vintage_used provenance column in the output panel."
        ),
    )


class PanelPolicy(BaseModel):
    """Declarative panel output and finalization policy.

    Allows recipes to explicitly declare panel-specific semantics that
    were previously implicit in ``build_panel``, including ZORI eligibility
    thresholds, ACS 1-year merge behavior, LAUS labor-market merge behavior,
    source labeling, and column rename aliases.
    """

    model_config = ConfigDict(extra="forbid")

    source_label: str | None = Field(
        default=None,
        description="Override the default source label for the panel (e.g. 'hhplab_panel').",
    )
    zori: ZoriPolicy | None = Field(
        default=None,
        description="ZORI eligibility and provenance policy. Null means no ZORI integration.",
    )
    acs1: Acs1Policy | None = Field(
        default=None,
        description="ACS 1-year merge policy (metro targets only).",
    )
    laus: LausPolicy | None = Field(
        default=None,
        description=(
            "BLS LAUS metro-native labor-market merge policy (metro targets only). "
            "Declares that the pipeline includes a bls/laus dataset and enables "
            "LAUS-aware conformance checks."
        ),
    )
    column_aliases: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Column rename mapping for output. "
            "E.g. {'total_population': 'acs_total_population', 'population': 'pep_population'}."
        ),
    )


class MapLayerStyle(BaseModel):
    """Declarative style controls for a rendered map overlay layer."""

    model_config = ConfigDict(extra="forbid")

    fill_color: str = Field(
        default="#3388ff",
        description="Polygon fill color.",
    )
    stroke_color: str = Field(
        default="#3388ff",
        description="Polygon outline color.",
    )
    fill_opacity: float = Field(
        default=0.3,
        ge=0.0,
        le=1.0,
        description="Polygon fill opacity.",
    )
    stroke_opacity: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Polygon outline opacity.",
    )
    line_weight: float = Field(
        default=2.0,
        ge=0.0,
        description="Polygon outline width in pixels.",
    )
    z_order: int = Field(
        default=0,
        ge=0,
        description="Relative overlay draw order.",
    )


class MapViewportSpec(BaseModel):
    """Viewport policy for a rendered recipe map."""

    model_config = ConfigDict(extra="forbid")

    fit_layers: bool = Field(
        default=True,
        description="If true, fit map bounds to the selected overlay layers.",
    )
    center: tuple[float, float] | None = Field(
        default=None,
        description="Optional explicit map center as (lat, lon).",
    )
    zoom: int | None = Field(
        default=None,
        ge=0,
        description="Optional explicit zoom level.",
    )
    padding: int = Field(
        default=20,
        ge=0,
        description="Padding in pixels when fitting layer bounds.",
    )

    @model_validator(mode="after")
    def _validate_explicit_view(self) -> MapViewportSpec:
        if not self.fit_layers and (self.center is None or self.zoom is None):
            raise ValueError(
                "MapViewportSpec with fit_layers=false requires both 'center' and 'zoom'."
            )
        return self


MapStyleMode = Literal["uniform", "distinct"]


class MapLayerSpec(BaseModel):
    """One overlay layer within a recipe-native map target."""

    model_config = ConfigDict(extra="forbid")

    geometry: GeometryRef = Field(
        ...,
        description="Geometry universe to render for this layer (e.g. coc, county, msa, metro).",
    )
    selector_ids: list[str] = Field(
        ...,
        min_length=1,
        description="Explicit geography identifiers to draw in this layer.",
    )
    style: MapLayerStyle = Field(
        default_factory=MapLayerStyle,
        description="Render style for the selected features.",
    )
    style_mode: MapStyleMode = Field(
        default="uniform",
        description=(
            "How styles are applied within the layer. 'uniform' uses one style "
            "for every feature; 'distinct' assigns deterministic per-feature colors."
        ),
    )
    tooltip_fields: list[str] = Field(
        default_factory=list,
        description="Explicit allowlist of feature fields exposed in the tooltip.",
    )
    label: str | None = Field(
        default=None,
        description="Optional human-readable layer label.",
    )
    group: str | None = Field(
        default=None,
        description="Optional legend/group name.",
    )
    initial_visibility: bool = Field(
        default=True,
        description="Whether the layer is visible on initial render.",
    )

    @field_validator("selector_ids", "tooltip_fields")
    @classmethod
    def _validate_non_blank_items(cls, value: list[str]) -> list[str]:
        if any(not item.strip() for item in value):
            raise ValueError("Map layer string lists may not contain blank items.")
        return value


class MapSpec(BaseModel):
    """Declarative map artifact definition for a target."""

    model_config = ConfigDict(extra="forbid")

    layers: list[MapLayerSpec] = Field(
        ...,
        min_length=1,
        description="Overlay layers rendered into one HTML map artifact.",
    )
    basemap: str = Field(
        default="cartodbpositron",
        description="Deterministic basemap identifier for the rendered HTML map.",
    )
    viewport: MapViewportSpec = Field(
        default_factory=MapViewportSpec,
        description="Viewport behavior for the rendered map.",
    )


class CohortSelector(BaseModel):
    """Declarative cohort filter that ranks geographies by a measure column
    at a reference year and keeps only the selected subset."""

    model_config = ConfigDict(extra="forbid")

    rank_by: str = Field(
        ...,
        description="Measure column to rank geographies on (e.g. 'total_population').",
    )
    method: CohortMethod = Field(
        ...,
        description="Selection method: top_n, bottom_n, or percentile.",
    )
    n: int | None = Field(
        default=None,
        ge=1,
        description="Number of geographies to keep (for top_n / bottom_n).",
    )
    threshold: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Percentile threshold (for 'percentile' method). 0.75 keeps the top 25%%.",
    )
    reference_year: int = Field(
        ...,
        description="Year whose values are used for ranking (must be in universe).",
    )

    @model_validator(mode="after")
    def _validate_method_params(self) -> CohortSelector:
        if self.method in ("top_n", "bottom_n"):
            if self.n is None:
                raise ValueError(f"CohortSelector method '{self.method}' requires 'n'.")
        if self.method == "percentile":
            if self.threshold is None:
                raise ValueError("CohortSelector method 'percentile' requires 'threshold'.")
        return self


class TargetSpec(BaseModel):
    """
    A recipe can define multiple targets (e.g., CoC panel + county panel + state rollup).
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Unique target identifier.")
    geometry: GeometryRef = Field(..., description="Target geometry for the pipeline.")
    outputs: list[OutputKind] = Field(
        default_factory=lambda: ["panel"],
        description="Requested outputs for this target.",
    )
    cohort: CohortSelector | None = Field(
        default=None,
        description="Optional cohort selector to filter to a ranked subset of geographies.",
    )
    panel_policy: PanelPolicy | None = Field(
        default=None,
        description="Declarative panel output and finalization policy.",
    )
    map_spec: MapSpec | None = Field(
        default=None,
        description="Declarative recipe-native map artifact configuration.",
    )

    @model_validator(mode="after")
    def _validate_output_policies(self) -> TargetSpec:
        outputs = set(self.outputs)
        if "map" in outputs and self.map_spec is None:
            raise ValueError("Target outputs including 'map' require 'map_spec'.")
        if "map" not in outputs and self.map_spec is not None:
            raise ValueError("Target 'map_spec' requires outputs to include 'map'.")
        return self


# -----------------------------
# Transforms (v1 operators)
# -----------------------------

WeightScheme = Literal["area", "population"]


class CrosswalkWeighting(BaseModel):
    """
    Weighting spec for spatial crosswalk shares.
    If scheme == 'population', a population source/field may be referenced
    (optional here; required by adapter).
    """

    model_config = ConfigDict(extra="forbid")

    scheme: WeightScheme = Field(..., description="Crosswalk share weighting scheme.")
    population_source: str | None = Field(
        default=None,
        description="Dataset id to source population weights from (when scheme=population).",
        examples=["acs"],
    )
    population_field: str | None = Field(
        default=None,
        description="Field in the population dataset used for weights.",
        examples=["total_population"],
    )


class CrosswalkSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    weighting: CrosswalkWeighting = Field(..., description="Weighting config for crosswalk shares.")


class RollupKeys(BaseModel):
    model_config = ConfigDict(extra="forbid")
    from_key: str = Field(..., description="Key in source geometry table (e.g., 'geoid').")
    to_key: str = Field(..., description="Key in target geometry table (e.g., 'state_fips').")


class RollupSpec(BaseModel):
    """
    Deterministic administrative rollup mapping.
    derive: optional expression strings to derive the target key from
    the source key. The expression language is intentionally unspecified
    here; implement a safe evaluator in compiler layer.
    """

    model_config = ConfigDict(extra="forbid")
    keys: RollupKeys = Field(..., description="Key mapping config.")
    derive: dict[str, str] = Field(
        default_factory=dict,
        description="Optional derived fields/keys expressions.",
    )


class TransformBase(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(..., description="Unique transform identifier.")
    type: str = Field(..., description="Transform operator type (discriminated union key).")
    from_: GeometryRef = Field(..., alias="from", description="Source geometry.")
    to: GeometryRef = Field(..., description="Target geometry.")


class CrosswalkTransform(TransformBase):
    type: Literal["crosswalk"] = "crosswalk"
    spec: CrosswalkSpec = Field(..., description="Crosswalk operator spec.")


class RollupTransform(TransformBase):
    type: Literal["rollup"] = "rollup"
    spec: RollupSpec = Field(..., description="Rollup operator spec.")


TransformSpec = Annotated[CrosswalkTransform | RollupTransform, Field(discriminator="type")]


# -----------------------------
# Pipeline steps (v1)
# -----------------------------

ResampleMethod = Literal["identity", "allocate", "aggregate"]
AggregationMethod = Literal["sum", "mean", "weighted_mean"]


class MaterializeStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["materialize"] = "materialize"
    transforms: list[str] = Field(..., description="Transform ids to ensure exist/materialized.")


class MeasureAggConfig(BaseModel):
    """Per-measure aggregation configuration."""

    model_config = ConfigDict(extra="forbid")
    aggregation: AggregationMethod = Field(..., description="Aggregation method for this measure.")


class ResampleStep(BaseModel):
    """
    Resample a dataset to the target geometry.
    - identity: dataset already at to_geometry (via not required)
    - allocate: few -> many, requires crosswalk shares (via required)
    - aggregate: many -> few, via required (crosswalk or rollup)

    ``measures`` accepts either:
    - a list of strings (uniform aggregation via top-level ``aggregation``)
    - a dict mapping measure name → MeasureAggConfig (per-measure aggregation)
    """

    model_config = ConfigDict(extra="forbid")

    kind: Literal["resample"] = "resample"
    dataset: str = Field(..., description="Dataset id to resample.")
    to_geometry: GeometryRef = Field(
        ...,
        description="Destination geometry for this dataset output.",
    )
    method: ResampleMethod = Field(
        ...,
        description="Resampling method.",
    )
    via: str | None = Field(
        default=None,
        description="Transform id or 'auto' for allocate/aggregate.",
    )
    measures: dict[str, MeasureAggConfig] = Field(
        ...,
        description="Map of measure name → aggregation config.",
    )
    aggregation: AggregationMethod | None = Field(
        default=None,
        description="Deprecated: uniform aggregation method. Use per-measure config instead.",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_measures_list(cls, data: Any) -> Any:
        """Support legacy list-of-strings format for measures."""
        if not isinstance(data, dict):
            return data
        measures = data.get("measures")
        if isinstance(measures, list):
            agg = data.get("aggregation")
            config: dict[str, Any] = {}
            if agg is not None:
                config = {m: {"aggregation": agg} for m in measures}
            else:
                config = {m: {"aggregation": "sum"} for m in measures}
            data = {**data, "measures": config}
        return data

    @model_validator(mode="after")
    def _validate_via_requirement(self) -> ResampleStep:
        if self.method in ("allocate", "aggregate") and not self.via:
            raise ValueError("ResampleStep.method in {allocate,aggregate} requires 'via'.")
        if self.method == "identity" and self.via is not None:
            raise ValueError("ResampleStep.method=identity must not set 'via'.")
        return self

    @property
    def measure_names(self) -> list[str]:
        """Return ordered list of measure names."""
        return list(self.measures.keys())


class JoinStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["join"] = "join"
    datasets: list[str] = Field(..., description="Dataset ids to join into a target panel.")
    join_on: list[str] = Field(default_factory=lambda: ["geo_id", "year"], description="Join keys.")


StepSpec = Annotated[MaterializeStep | ResampleStep | JoinStep, Field(discriminator="kind")]


_STEP_KINDS = frozenset({"materialize", "resample", "join"})


def _unwrap_step(raw: Any) -> Any:
    """Rewrite ``{"resample": {...}}`` → ``{"kind": "resample", ...}`` etc."""
    if not isinstance(raw, dict):
        return raw
    # Already in canonical form
    if "kind" in raw:
        return raw
    # Look for a single wrapper key that matches a known step kind
    keys = set(raw.keys()) & _STEP_KINDS
    if len(keys) == 1:
        kind = keys.pop()
        inner = raw[kind]
        if isinstance(inner, dict):
            return {"kind": kind, **inner}
    return raw


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Unique pipeline identifier.")
    target: str = Field(..., description="Target id that this pipeline materializes.")
    steps: list[StepSpec] = Field(..., description="Ordered steps for the pipeline.")

    @model_validator(mode="before")
    @classmethod
    def _unwrap_step_shorthands(cls, data: Any) -> Any:
        if isinstance(data, dict) and "steps" in data:
            data = {**data, "steps": [_unwrap_step(s) for s in data["steps"]]}
        return data


# -----------------------------
# Validation / policy blocks
# -----------------------------


class MissingDatasetPolicy(BaseModel):
    """
    Controls behavior when required datasets are missing.
    Keys are dataset ids; 'default' applies when dataset not explicitly listed.
    Values are 'fail' or 'warn'.
    """

    model_config = ConfigDict(extra="allow")  # allow arbitrary dataset keys

    default: Literal["fail", "warn"] = Field(default="fail")


class CrosswalkCoveragePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    warn_below: float = Field(default=0.95, ge=0.0, le=1.0)
    fail_below: float = Field(default=0.90, ge=0.0, le=1.0)


class ValidationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    missing_dataset: MissingDatasetPolicy = Field(default_factory=MissingDatasetPolicy)
    crosswalk_coverage: CrosswalkCoveragePolicy = Field(default_factory=CrosswalkCoveragePolicy)


# -----------------------------
# The full recipe model
# -----------------------------


class RecipeV1(BaseModel):
    """
    Top-level recipe.
    Notes:
    - Uses open sets for geometry types and dataset providers/products.
    - Performs referential integrity checks across ids (targets, datasets, transforms, pipelines).
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    version: Literal[1] = 1
    name: str
    description: str | None = None

    universe: YearSpec

    targets: list[TargetSpec]
    datasets: dict[str, DatasetSpec]
    filters: dict[str, FilterSpec] = Field(
        default_factory=dict,
        description="Temporal filters keyed by dataset_id, applied before resampling.",
    )
    transforms: list[TransformSpec] = Field(default_factory=list)
    pipelines: list[PipelineSpec] = Field(default_factory=list)
    validation: ValidationPolicy = Field(default_factory=ValidationPolicy)
    vintage_sets: dict[str, VintageSetSpec] = Field(
        default_factory=dict,
        description="Named vintage tuple sets for terse multi-year dataset resolution.",
    )

    @model_validator(mode="after")
    def _validate_references(self) -> RecipeV1:
        # Ensure unique target ids
        target_ids = [t.id for t in self.targets]
        if len(set(target_ids)) != len(target_ids):
            raise ValueError("targets[].id must be unique.")

        dataset_ids = set(self.datasets.keys())

        # Ensure unique transform ids
        transform_ids = [tr.id for tr in self.transforms]
        if len(set(transform_ids)) != len(transform_ids):
            raise ValueError("transforms[].id must be unique.")
        transform_id_set = set(transform_ids)

        # Pipelines reference valid targets
        target_id_set = set(target_ids)
        pipeline_ids = [p.id for p in self.pipelines]
        if len(set(pipeline_ids)) != len(pipeline_ids):
            raise ValueError("pipelines[].id must be unique.")

        for p in self.pipelines:
            if p.target not in target_id_set:
                raise ValueError(f"Pipeline '{p.id}' references unknown target '{p.target}'.")

            for step in p.steps:
                if isinstance(step, MaterializeStep):
                    missing = [x for x in step.transforms if x not in transform_id_set]
                    if missing:
                        raise ValueError(
                            f"Pipeline '{p.id}' materialize step references"
                            f" unknown transforms: {missing}"
                        )

                elif isinstance(step, ResampleStep):
                    if step.dataset not in dataset_ids:
                        raise ValueError(
                            f"Pipeline '{p.id}' resample step references"
                            f" unknown dataset '{step.dataset}'."
                        )
                    if step.via and step.via != "auto" and step.via not in transform_id_set:
                        raise ValueError(
                            f"Pipeline '{p.id}' resample step references"
                            f" unknown transform '{step.via}'."
                        )

                elif isinstance(step, JoinStep):
                    missing = [d for d in step.datasets if d not in dataset_ids]
                    if missing:
                        raise ValueError(
                            f"Pipeline '{p.id}' join step references unknown datasets: {missing}"
                        )

        return self

    @model_validator(mode="after")
    def _validate_file_sets(self) -> RecipeV1:
        """Semantic validation for dataset file_set segments."""
        formatter = Formatter()

        def _template_fields(template: str) -> set[str]:
            fields: set[str] = set()
            for _literal, field_name, _format_spec, _conversion in formatter.parse(template):
                if field_name:
                    fields.add(field_name)
            return fields

        for ds_id, ds in self.datasets.items():
            if ds.file_set is None:
                continue
            all_years: set[int] = set()
            template_fields = _template_fields(ds.file_set.path_template)
            for seg in ds.file_set.segments:
                seg_years = set(expand_year_spec(seg.years))
                # Check override keys fall within segment years
                for override_year in seg.overrides:
                    if override_year not in seg_years:
                        raise ValueError(
                            f"Dataset '{ds_id}' segment years "
                            f"{seg.years.range or seg.years.years} has override "
                            f"for year {override_year} (not in segment)."
                        )
                # Check segment geometry type matches dataset native_geometry.type
                if seg.geometry.type != ds.native_geometry.type:
                    raise ValueError(
                        f"Dataset '{ds_id}' segment geometry type "
                        f"'{seg.geometry.type}' does not match "
                        f"native_geometry type '{ds.native_geometry.type}'."
                    )

                # Check that dynamic variables can be resolved.
                duplicate_keys = set(seg.constants) & set(seg.year_offsets)
                if duplicate_keys:
                    raise ValueError(
                        f"Dataset '{ds_id}' segment defines keys in both "
                        f"constants and year_offsets: {sorted(duplicate_keys)}."
                    )

                non_override_years = sorted(seg_years - set(seg.overrides.keys()))
                if non_override_years:
                    sample_year = non_override_years[0]
                    render_ctx: dict[str, Any] = {"year": sample_year}
                    render_ctx.update(seg.constants)
                    render_ctx.update(
                        {k: sample_year + offset for k, offset in seg.year_offsets.items()}
                    )
                    missing = sorted(field for field in template_fields if field not in render_ctx)
                    if missing:
                        raise ValueError(
                            f"Dataset '{ds_id}' file_set.path_template requires "
                            f"variables {missing} but segment does not provide them "
                            f"(available: {sorted(render_ctx.keys())})."
                        )

                # Check for overlapping years across segments
                overlap = all_years & seg_years
                if overlap:
                    raise ValueError(
                        f"Dataset '{ds_id}' file_set segments overlap on years: {sorted(overlap)}."
                    )
                all_years |= seg_years
        return self

    @model_validator(mode="after")
    def _validate_vintage_sets(self) -> RecipeV1:
        """Semantic validation for vintage_sets declarations."""
        for vs_name, vs in self.vintage_sets.items():
            all_years: set[int] = set()
            for rule in vs.rules:
                rule_years = set(expand_year_spec(rule.years))
                overlap = all_years & rule_years
                if overlap:
                    raise ValueError(
                        f"Vintage set '{vs_name}' rules overlap on years: {sorted(overlap)}."
                    )
                all_years |= rule_years

                # No duplicate keys between constants and year_offsets
                duplicate_keys = set(rule.constants) & set(rule.year_offsets)
                if duplicate_keys:
                    raise ValueError(
                        f"Vintage set '{vs_name}' rule defines keys in both "
                        f"constants and year_offsets: {sorted(duplicate_keys)}."
                    )

                # Every declared dimension must be provided
                provided = set(rule.constants) | set(rule.year_offsets)
                missing = [d for d in vs.dimensions if d not in provided]
                if missing:
                    raise ValueError(
                        f"Vintage set '{vs_name}' rule (years "
                        f"{rule.years.range or rule.years.years}) does not "
                        f"cover dimension(s): {sorted(missing)}. Each dimension "
                        f"must appear in constants or year_offsets."
                    )
        return self

    @model_validator(mode="after")
    def _validate_filters(self) -> RecipeV1:
        """Validate that filter keys reference declared datasets."""
        dataset_ids = set(self.datasets.keys())
        unknown = set(self.filters.keys()) - dataset_ids
        if unknown:
            raise ValueError(
                f"Filters reference unknown dataset(s): {sorted(unknown)}. "
                f"Available: {sorted(dataset_ids)}"
            )
        acs_products = {"acs", "acs1", "acs5"}
        for ds_id, filt in self.filters.items():
            ds = self.datasets[ds_id]
            if ds.provider == "census" and ds.product in acs_products:
                raise ValueError(
                    f"Dataset '{ds_id}' is {ds.provider}/{ds.product}; ACS "
                    "estimates are annual and use a rolling reference period, "
                    "so recipe temporal filters are not supported. Select the "
                    "desired ACS vintage via file_set/year_offsets instead."
                )
            if filt.method == "interpolate_to_month":
                if not (ds.provider == "census" and ds.product == "pep"):
                    raise ValueError(
                        f"Dataset '{ds_id}' is {ds.provider}/{ds.product}; "
                        "interpolate_to_month is only supported for "
                        "census/pep datasets."
                    )
                if filt.month != 1:
                    raise ValueError(
                        f"Dataset '{ds_id}' uses interpolate_to_month with "
                        f"month={filt.month}; census/pep interpolation must "
                        "target January (month=1)."
                    )
        return self

    @model_validator(mode="after")
    def _validate_missing_dataset_policy(self) -> RecipeV1:
        """Validate that missing_dataset policy extra keys reference declared datasets."""
        policy_extra = self.validation.missing_dataset.model_extra or {}
        if policy_extra:
            dataset_ids = set(self.datasets.keys())
            unknown = set(policy_extra.keys()) - dataset_ids
            if unknown:
                raise ValueError(
                    f"missing_dataset policy references unknown dataset(s): "
                    f"{sorted(unknown)}. Available: {sorted(dataset_ids)}"
                )
        return self

    @model_validator(mode="after")
    def _validate_cohort_selectors(self) -> RecipeV1:
        """Validate that cohort reference_year falls within the recipe universe."""
        universe_years = set(expand_year_spec(self.universe))
        for t in self.targets:
            if t.cohort is not None and t.cohort.reference_year not in universe_years:
                raise ValueError(
                    f"Target '{t.id}' cohort reference_year "
                    f"{t.cohort.reference_year} is not in the recipe universe "
                    f"({min(universe_years)}-{max(universe_years)})."
                )
        return self

    @model_validator(mode="after")
    def _validate_static_path_multi_year(self) -> RecipeV1:
        """Error when a static-path dataset spans multiple universe years
        without years declaration."""
        universe_years = expand_year_spec(self.universe)
        if len(universe_years) <= 1:
            return self

        for ds_id, ds in self.datasets.items():
            if ds.file_set is None and ds.path is not None and ds.years is None:
                raise ValueError(
                    f"Dataset '{ds_id}' uses a static path but does not declare "
                    f"'years', and the recipe universe spans "
                    f"{len(universe_years)} years. Add a 'years' declaration to "
                    f"confirm intentional broadcast, use 'file_set' for "
                    f"time-banded paths, or narrow the universe to a single year."
                )
        return self
