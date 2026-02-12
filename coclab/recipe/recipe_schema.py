"""
coclab recipe schema (v1) - Pydantic v2 models

Goal:
- Represent a declarative build recipe that can target multiple geometries (CoC, county, state, tract, zcta, etc.)
- Support extensible datasets (e.g., MIT Election Data and others) via provider/product/version + free-form params
- Express a small, stable set of transform operators (crosswalk, rollup) and pipeline steps (materialize, resample, join)

Notes:
- Geometry types are strings (open set). Runtime plugin layer should validate whether a geometry adapter exists.
- Dataset params are free-form. Runtime dataset adapter validates params for a given provider/product/version.
- This file is the *structural* schema; semantic validation (e.g., allocatability of measures) belongs in a compiler/adapters layer.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union, Annotated
from string import Formatter
from pydantic import BaseModel, Field, ConfigDict, model_validator, field_validator


# -----------------------------
# Utility / value objects
# -----------------------------

class YearSpec(BaseModel):
    """
    Represents a year domain.
    Exactly one of:
      - range: "2018-2024"
      - years: [2018, 2019, ...]
    Also accepts a bare string like "2018-2024" as shorthand for {range: "2018-2024"}.
    """
    model_config = ConfigDict(extra="forbid")

    range: Optional[str] = Field(
        default=None,
        description="Year range in inclusive form 'YYYY-YYYY'.",
        examples=["2018-2024"],
    )
    years: Optional[List[int]] = Field(
        default=None,
        description="Explicit list of years.",
        examples=[[2018, 2019, 2020]],
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_bare_string(cls, data: Any) -> Any:
        if isinstance(data, str):
            return {"range": data}
        return data

    @model_validator(mode="after")
    def _validate_one_of(self) -> "YearSpec":
        if (self.range is None) == (self.years is None):
            raise ValueError("YearSpec must set exactly one of 'range' or 'years'.")
        return self


def expand_year_spec(spec: YearSpec | str | list[int]) -> list[int]:
    """Expand a YearSpec (or shorthand) to a sorted list of year ints.

    Accepts:
      - YearSpec model instance
      - str range like "2018-2024"
      - explicit list of ints
    """
    if isinstance(spec, list):
        return sorted(spec)
    if isinstance(spec, str):
        spec = YearSpec(range=spec)
    if spec.years is not None:
        return sorted(spec.years)
    if spec.range is not None:
        parts = spec.range.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid year range format: '{spec.range}'. Expected 'YYYY-YYYY'.")
        start, end = int(parts[0]), int(parts[1])
        return list(range(start, end + 1))
    raise ValueError("YearSpec has neither 'range' nor 'years'.")


class GeometryRef(BaseModel):
    """
    Reference to a geometry universe.
    type: open string set (e.g., 'coc', 'tract', 'county', 'state', 'zcta', 'zip')
    vintage: required for most polygonal types in practice, but schema keeps it optional
            and lets runtime adapters enforce requirements.
    source: optional provenance hint (e.g., 'hud_exchange', 'tiger', 'nhgis').
    """
    model_config = ConfigDict(extra="forbid")

    type: str = Field(..., description="Geometry type identifier (open set).", examples=["coc", "county", "state", "zcta"])
    vintage: Optional[int] = Field(default=None, description="Vintage year for geometry (if applicable).", examples=[2025, 2023, 2020])
    source: Optional[str] = Field(default=None, description="Geometry source hint (optional).", examples=["hud_exchange", "tiger", "nhgis"])


# -----------------------------
# Vintage sets (named tuple-set declarations)
# -----------------------------


class VintageSetRule(BaseModel):
    """A rule within a vintage set that maps years to dimension values."""
    model_config = ConfigDict(extra="forbid")

    years: YearSpec
    constants: Dict[str, Union[str, int]] = Field(
        default_factory=dict,
        description="Fixed dimension values for this rule's year band.",
    )
    year_offsets: Dict[str, int] = Field(
        default_factory=dict,
        description="Dimension values derived from analysis year (value = year + offset).",
    )


class VintageSetSpec(BaseModel):
    """A named set of vintage tuples, expanded from compact range rules."""
    model_config = ConfigDict(extra="forbid")

    dimensions: List[str] = Field(
        ..., min_length=1,
        description="Ordered list of dimension names in each tuple.",
    )
    rules: List[VintageSetRule] = Field(
        ..., min_length=1,
        description="Year-banded rules that expand into tuples.",
    )


# -----------------------------
# File set (time-banded dataset paths)
# -----------------------------

class FileSetSegment(BaseModel):
    """A time-banded segment mapping years to a geometry vintage and optional path overrides."""
    model_config = ConfigDict(extra="forbid")

    years: YearSpec
    geometry: GeometryRef
    overrides: Dict[int, str] = Field(default_factory=dict)
    constants: Dict[str, Union[str, int]] = Field(
        default_factory=dict,
        description=(
            "Optional constant template variables for path rendering, "
            "for example {'tract': 2010}."
        ),
    )
    year_offsets: Dict[str, int] = Field(
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
    segments: List[FileSetSegment] = Field(..., min_length=1)

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

    provider: str = Field(..., description="Dataset provider namespace, e.g. 'hud', 'census', 'zillow', 'mit-election'.")
    product: str = Field(..., description="Dataset product name within provider, e.g. 'pit', 'acs5', 'pep', 'county-returns'.")
    version: int = Field(..., description="Adapter version for this dataset product (schema evolution control).", ge=1)
    native_geometry: GeometryRef = Field(..., description="Native geometry of the dataset.")
    years: Optional[YearSpec] = Field(
        default=None,
        description="Temporal coverage of this dataset. For file_set datasets, coverage is implicit from segments.",
    )
    params: Dict[str, Any] = Field(default_factory=dict, description="Free-form adapter params.")
    path: Optional[str] = Field(
        default=None,
        description="Optional project-relative file path for a pre-materialized dataset artifact.",
        examples=["data/curated/pit/pit_vintage__P2024.parquet"],
    )
    file_set: Optional[FileSetSpec] = Field(
        default=None,
        description="Time-banded file set with per-segment geometry vintages.",
    )
    optional: bool = Field(default=False, description="If true, missing dataset does not fail the build (policy still applies).")

    @field_validator("path")
    @classmethod
    def _validate_path_is_relative(cls, value: Optional[str]) -> Optional[str]:
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

OutputKind = Literal["panel", "diagnostics", "export"]


class TargetSpec(BaseModel):
    """
    A recipe can define multiple targets (e.g., CoC panel + county panel + state rollup).
    """
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="Unique target identifier.")
    geometry: GeometryRef = Field(..., description="Target geometry for the pipeline.")
    outputs: List[OutputKind] = Field(default_factory=lambda: ["panel"], description="Requested outputs for this target.")


# -----------------------------
# Transforms (v1 operators)
# -----------------------------

WeightScheme = Literal["area", "population"]


class CrosswalkWeighting(BaseModel):
    """
    Weighting spec for spatial crosswalk shares.
    If scheme == 'population', a population source/field may be referenced (optional here; required by adapter).
    """
    model_config = ConfigDict(extra="forbid")

    scheme: WeightScheme = Field(..., description="Crosswalk share weighting scheme.")
    population_source: Optional[str] = Field(
        default=None,
        description="Dataset id to source population weights from (when scheme=population).",
        examples=["acs"],
    )
    population_field: Optional[str] = Field(
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
    derive: optional expression strings to derive the target key from the source key.
    The expression language is intentionally unspecified here; implement a safe evaluator in compiler layer.
    """
    model_config = ConfigDict(extra="forbid")
    keys: RollupKeys = Field(..., description="Key mapping config.")
    derive: Dict[str, str] = Field(default_factory=dict, description="Optional derived fields/keys expressions.")


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


TransformSpec = Annotated[Union[CrosswalkTransform, RollupTransform], Field(discriminator="type")]


# -----------------------------
# Pipeline steps (v1)
# -----------------------------

ResampleMethod = Literal["identity", "allocate", "aggregate"]
AggregationMethod = Literal["sum", "mean", "weighted_mean"]


class MaterializeStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["materialize"] = "materialize"
    transforms: List[str] = Field(..., description="Transform ids to ensure exist/materialized.")


class ResampleStep(BaseModel):
    """
    Resample a dataset to the target geometry.
    - identity: dataset already at to_geometry (via not required)
    - allocate: few -> many, requires crosswalk shares (via required)
    - aggregate: many -> few, via required (crosswalk or rollup)
    """
    model_config = ConfigDict(extra="forbid")

    kind: Literal["resample"] = "resample"
    dataset: str = Field(..., description="Dataset id to resample.")
    to_geometry: GeometryRef = Field(..., description="Destination geometry for this dataset output.")
    method: ResampleMethod = Field(..., description="Resampling method.")
    via: Optional[str] = Field(default=None, description="Transform id or 'auto' for allocate/aggregate.")
    measures: List[str] = Field(..., description="List of measure/field names to carry through.")
    aggregation: Optional[AggregationMethod] = Field(
        default=None,
        description="Aggregation method for method=aggregate (compiler may infer per-measure).",
    )

    @model_validator(mode="after")
    def _validate_via_requirement(self) -> "ResampleStep":
        if self.method in ("allocate", "aggregate") and not self.via:
            raise ValueError("ResampleStep.method in {allocate,aggregate} requires 'via'.")
        if self.method == "identity" and self.via is not None:
            raise ValueError("ResampleStep.method=identity must not set 'via'.")
        return self


class JoinStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["join"] = "join"
    datasets: List[str] = Field(..., description="Dataset ids to join into a target panel.")
    join_on: List[str] = Field(default_factory=lambda: ["geo_id", "year"], description="Join keys.")


StepSpec = Annotated[Union[MaterializeStep, ResampleStep, JoinStep], Field(discriminator="kind")]


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
    steps: List[StepSpec] = Field(..., description="Ordered steps for the pipeline.")

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
    description: Optional[str] = None

    universe: YearSpec

    targets: List[TargetSpec]
    datasets: Dict[str, DatasetSpec]
    transforms: List[TransformSpec] = Field(default_factory=list)
    pipelines: List[PipelineSpec] = Field(default_factory=list)
    validation: ValidationPolicy = Field(default_factory=ValidationPolicy)
    vintage_sets: Dict[str, VintageSetSpec] = Field(
        default_factory=dict,
        description="Named vintage tuple sets for terse multi-year dataset resolution.",
    )

    @model_validator(mode="after")
    def _validate_references(self) -> "RecipeV1":
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
                        raise ValueError(f"Pipeline '{p.id}' materialize step references unknown transforms: {missing}")

                elif isinstance(step, ResampleStep):
                    if step.dataset not in dataset_ids:
                        raise ValueError(f"Pipeline '{p.id}' resample step references unknown dataset '{step.dataset}'.")
                    if step.via and step.via != "auto" and step.via not in transform_id_set:
                        raise ValueError(f"Pipeline '{p.id}' resample step references unknown transform '{step.via}'.")

                elif isinstance(step, JoinStep):
                    missing = [d for d in step.datasets if d not in dataset_ids]
                    if missing:
                        raise ValueError(f"Pipeline '{p.id}' join step references unknown datasets: {missing}")

        return self

    @model_validator(mode="after")
    def _validate_file_sets(self) -> "RecipeV1":
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
                        f"Dataset '{ds_id}' file_set segments overlap "
                        f"on years: {sorted(overlap)}."
                    )
                all_years |= seg_years
        return self

    @model_validator(mode="after")
    def _validate_vintage_sets(self) -> "RecipeV1":
        """Semantic validation for vintage_sets declarations."""
        for vs_name, vs in self.vintage_sets.items():
            all_years: set[int] = set()
            for rule in vs.rules:
                rule_years = set(expand_year_spec(rule.years))
                overlap = all_years & rule_years
                if overlap:
                    raise ValueError(
                        f"Vintage set '{vs_name}' rules overlap "
                        f"on years: {sorted(overlap)}."
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
