"""Closed RecipeV2 models for reproducible results specifications."""

from __future__ import annotations

import math
from typing import Annotated, Any, Literal

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

from hhplab.recipe.recipe_schema import RecipeV1
from hhplab.schema.columns import HIC_PANEL_MEASURE_COLUMNS
from hhplab.schema.contracts import (
    AnnotationEvidence as AnnotationEvidence,
)
from hhplab.schema.contracts import (
    AnnotationScope as AnnotationScope,
)
from hhplab.schema.contracts import (
    DataAnnotationSpec as DataAnnotationSpec,
)
from hhplab.schema.contracts import (
    validate_annotation_revisions as _validate_annotation_supersedes_graph,
)

PIT_SITE_YEAR_MEASURE_COLUMNS = frozenset(
    {"pit_total", "pit_sheltered", "pit_unsheltered"}
)
HIC_SITE_YEAR_MEASURE_COLUMNS = frozenset(HIC_PANEL_MEASURE_COLUMNS)


class ClosedModel(BaseModel):
    """Base for result-recipe records with no unrecognized fields."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class UnresolvedMarker(ClosedModel):
    """A deliberately unfinished value that static preflight can report."""

    unresolved: StrictStr = Field(min_length=1)


class ParameterReference(ClosedModel):
    """A reference to a named recipe parameter."""

    param: StrictStr = Field(min_length=1)


class YearInterval(ClosedModel):
    """Inclusive calendar interval used by samples and temporal operations."""

    from_year: StrictInt = Field(alias="from")
    to_year: StrictInt = Field(alias="to")
    step: StrictInt = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_interval(self) -> YearInterval:
        if self.from_year > self.to_year:
            raise ValueError("year interval 'from' must be less than or equal to 'to'")
        return self


FiniteNumber = StrictInt | Annotated[StrictFloat, Field(allow_inf_nan=False)]
NumericValue = FiniteNumber | ParameterReference | UnresolvedMarker
YearSelection = YearInterval | list[StrictInt]


class PanelReference(ClosedModel):
    """One named panel output produced by an exact build pipeline."""

    pipeline: StrictStr = Field(min_length=1)
    output: Literal["panel"] = "panel"


class SiteUniverseSource(ClosedModel):
    """One saved MSA/CoC site artifact, either local files or a portable bundle."""

    artifact_id: StrictStr = Field(min_length=1)
    bundle_path: StrictStr | None = Field(default=None, min_length=1)
    sites_path: StrictStr | None = Field(default=None, min_length=1)
    memberships_path: StrictStr | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def validate_locator(self) -> SiteUniverseSource:
        bundle = self.bundle_path is not None
        files = self.sites_path is not None or self.memberships_path is not None
        if bundle == files:
            raise ValueError(
                "site_universe.source must use bundle_path or both sites_path and "
                "memberships_path, but not both"
            )
        if files and (self.sites_path is None or self.memberships_path is None):
            raise ValueError(
                "site_universe.source file form requires both sites_path and memberships_path"
            )
        return self


class SiteUniverseSpec(ClosedModel):
    """Explicit reference to one threshold-selected MSA/CoC artifact."""

    source: SiteUniverseSource
    threshold: FiniteNumber
    site_key: Literal["msa_id"]

    @field_validator("threshold")
    @classmethod
    def validate_threshold(cls, value: int | float) -> int | float:
        numeric = float(value)
        if not math.isfinite(numeric) or not 0.5 < numeric <= 1:
            raise ValueError("site_universe.threshold must be finite and in (0.5, 1]")
        return value


class SiteYearOutcomeBinding(ClosedModel):
    """One CoC/year source panel and its explicitly selected additive measures."""

    panel: StrictStr = Field(min_length=1)
    measures: list[StrictStr] = Field(min_length=1)

    @field_validator("measures")
    @classmethod
    def validate_unique_measures(cls, values: list[str]) -> list[str]:
        if len(set(values)) != len(values):
            raise ValueError("site_year outcome measures must be unique")
        return values


class SiteYearOutcomes(ClosedModel):
    pit: SiteYearOutcomeBinding
    hic: SiteYearOutcomeBinding

    @model_validator(mode="after")
    def validate_measure_registry(self) -> SiteYearOutcomes:
        for name, binding, allowed in (
            ("pit", self.pit, PIT_SITE_YEAR_MEASURE_COLUMNS),
            ("hic", self.hic, HIC_SITE_YEAR_MEASURE_COLUMNS),
        ):
            invalid = sorted(set(binding.measures) - allowed)
            if invalid:
                raise ValueError(
                    f"site_year {name} measures are not registered additive columns: {invalid}"
                )
        return self


class BoundaryEvidenceBinding(ClosedModel):
    path: StrictStr = Field(min_length=1)


class SiteYearPanelBinding(ClosedModel):
    """Derived site-year panel sourced from frozen memberships and PIT/HIC panels."""

    universe: Literal["site_universe"] = "site_universe"
    boundary_evidence: BoundaryEvidenceBinding
    outcomes: SiteYearOutcomes


class PanelBinding(ClosedModel):
    """Named analysis panel and its required key-column contract."""

    ref: PanelReference | UnresolvedMarker | None = None
    site_year: SiteYearPanelBinding | None = None
    geo_key: StrictStr | None = None
    time_key: StrictStr | None = None

    @model_validator(mode="after")
    def validate_keys(self) -> PanelBinding:
        if isinstance(self.ref, UnresolvedMarker):
            if self.site_year is not None:
                raise ValueError("unresolved panel refs cannot also declare site_year")
            return self
        if (self.ref is None) == (self.site_year is None):
            raise ValueError("panel binding must declare exactly one of ref or site_year")
        if self.site_year is not None:
            if self.geo_key != "msa_id" or self.time_key != "year":
                raise ValueError("site_year panels require geo_key=msa_id and time_key=year")
            return self
        if not self.geo_key or not self.time_key:
            raise ValueError("resolved panel references require geo_key and time_key")
        if self.geo_key == self.time_key:
            raise ValueError("panel geo_key and time_key must be distinct")
        return self


class MaskGeographies(ClosedModel):
    include: list[StrictStr] | None = None


class MaskSpec(ClosedModel):
    id: StrictStr = Field(min_length=1)
    panels: list[StrictStr] = Field(default_factory=list)
    columns: list[StrictStr] = Field(min_length=1)
    years: list[StrictInt] = Field(min_length=1)
    reason: StrictStr = Field(min_length=1)
    geographies: MaskGeographies | None = None


class DeriveBase(ClosedModel):
    id: StrictStr = Field(min_length=1)
    panels: list[StrictStr] = Field(default_factory=list)


class SumDerivation(DeriveBase):
    op: Literal["sum"]
    of: list[StrictStr] = Field(min_length=1)
    require_all: bool = False


class RatioDerivation(DeriveBase):
    op: Literal["ratio"]
    num: StrictStr
    den: StrictStr
    zero_den: Literal["null", "error"] = "null"


class PerCapitaDerivation(DeriveBase):
    op: Literal["per_capita"]
    of: StrictStr
    pop: StrictStr
    scale: NumericValue


class TimeDerivation(DeriveBase):
    op: Literal["diff", "lag"]
    of: StrictStr
    years: StrictInt = Field(ge=1)


class GrowthDerivation(DeriveBase):
    op: Literal["growth"]
    of: StrictStr
    years: StrictInt = Field(ge=1)
    kind: Literal["pct", "log"]


class LogDerivation(DeriveBase):
    op: Literal["log"]
    of: StrictStr
    offset: NumericValue = 0


class IndicatorDerivation(DeriveBase):
    op: Literal["indicator"]
    of: StrictStr
    values: list[NumericValue] | None = Field(default=None, alias="in")
    gte: NumericValue | None = None
    lte: NumericValue | None = None

    @model_validator(mode="after")
    def validate_condition(self) -> IndicatorDerivation:
        choices = (
            int(self.values is not None) + int(self.gte is not None) + int(self.lte is not None)
        )
        if choices != 1:
            raise ValueError("indicator requires exactly one of 'in', 'gte', or 'lte'")
        return self


class ShareDerivation(DeriveBase):
    op: Literal["share"]
    of: StrictStr
    total: StrictStr
    zero_den: Literal["null", "error"] = "null"


DerivationSpec = Annotated[
    SumDerivation
    | RatioDerivation
    | PerCapitaDerivation
    | TimeDerivation
    | GrowthDerivation
    | LogDerivation
    | IndicatorDerivation
    | ShareDerivation,
    Field(discriminator="op"),
]


class GeographyPredicate(ClosedModel):
    column: StrictStr = Field(min_length=1)
    equals: NumericValue


class GeographySelection(ClosedModel):
    include: list[StrictStr] | None = None
    exclude: GeographyPredicate | None = None


class SampleSpec(ClosedModel):
    years: YearSelection | None = None
    exclude_years: list[StrictInt] = Field(default_factory=list)
    geographies: GeographySelection | None = None
    require: list[StrictStr] = Field(default_factory=list)
    min_obs_per_geography: StrictInt = Field(default=1, ge=1)
    drop_singletons: bool = True


class SampleGroupReference(ClosedModel):
    group: StrictStr = Field(min_length=1)


SampleSelection = SampleSpec | SampleGroupReference


class SampleGroupSpec(ClosedModel):
    sample: SampleSpec


class ShareStatistic(ClosedModel):
    share_gte: dict[StrictStr, NumericValue] | None = None
    share_lte: dict[StrictStr, NumericValue] | None = None

    @model_validator(mode="after")
    def validate_one_statistic(self) -> ShareStatistic:
        if (self.share_gte is None) == (self.share_lte is None):
            raise ValueError("share statistic requires exactly one of share_gte or share_lte")
        selected = self.share_gte if self.share_gte is not None else self.share_lte
        if len(selected or {}) != 1:
            raise ValueError("share statistic requires exactly one column and threshold")
        return self


Statistic = (
    Literal[
        "n",
        "mean",
        "sd",
        "min",
        "p10",
        "p25",
        "median",
        "p75",
        "p90",
        "max",
    ]
    | ShareStatistic
)


class AnalysisBase(ClosedModel):
    id: StrictStr = Field(min_length=1)
    input: StrictStr = Field(min_length=1)
    sample: SampleSelection
    weights: StrictStr | Literal["none"] | None = None
    report: list[StrictStr] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def validate_result_key_id(cls, value: str) -> str:
        return _validate_result_key_id(value, kind="analysis")


class DescriptiveAnalysis(AnalysisBase):
    method: Literal["descriptive"]
    columns: list[StrictStr] = Field(min_length=1)
    stats: list[Statistic] = Field(min_length=1)
    by: list[StrictStr] = Field(default_factory=list)

    @model_validator(mode="after")
    def reject_weighted_descriptive(self) -> DescriptiveAnalysis:
        if self.weights not in (None, "none"):
            raise ValueError(
                "capability_unavailable: weighted_descriptive is not supported in Phase 1"
            )
        return self


class CorrelationAnalysis(AnalysisBase):
    method: Literal["correlation"]
    columns: list[StrictStr] = Field(min_length=2)
    kind: Literal["pearson", "spearman"] = "pearson"

    @model_validator(mode="after")
    def reject_weighted_correlation(self) -> CorrelationAnalysis:
        if self.weights not in (None, "none"):
            raise ValueError(
                "capability_unavailable: weighted_correlation is not supported in Phase 1"
            )
        return self


class InferenceSpec(ClosedModel):
    method: Literal["classical", "hc1", "clustered", "wild_cluster_bootstrap"] = "classical"
    cluster_by: StrictStr | None = None
    reps: StrictInt | None = Field(default=None, ge=1)
    seed: StrictInt | None = None
    weight_type: Literal["rademacher", "webb"] | None = None

    @model_validator(mode="after")
    def validate_inference(self) -> InferenceSpec:
        if self.method == "clustered" and not self.cluster_by:
            raise ValueError("clustered inference requires cluster_by")
        if self.method == "wild_cluster_bootstrap":
            raise ValueError(
                "capability_unavailable: wild_cluster_bootstrap is not supported in Phase 1"
            )
        return self


class RegressionAnalysis(AnalysisBase):
    method: Literal["regression"]
    estimator: Literal["ols", "2sls"] = "ols"
    outcome: StrictStr = Field(min_length=1)
    predictors: list[StrictStr] = Field(min_length=1)
    controls: list[StrictStr] = Field(default_factory=list)
    fixed_effects: list[StrictStr] = Field(default_factory=list)
    inference: InferenceSpec = Field(default_factory=InferenceSpec)
    ci_level: Annotated[StrictFloat, Field(gt=0.0, lt=1.0, allow_inf_nan=False)] = 0.95

    @model_validator(mode="after")
    def validate_regression(self) -> RegressionAnalysis:
        if self.estimator == "2sls":
            raise ValueError("capability_unavailable: 2sls is not supported in Phase 1")
        if self.weights == "none":
            object.__setattr__(self, "weights", None)
        terms = [self.outcome, *self.predictors, *self.controls]
        if len(set(terms)) != len(terms):
            raise ValueError("regression outcome, predictors, and controls must be distinct")
        return self


class LaggedAssociationAnalysis(AnalysisBase):
    method: Literal["lagged_association"]
    outcome: StrictStr = Field(min_length=1)
    predictors: list[StrictStr] = Field(min_length=1)
    years: StrictInt = Field(default=1, ge=1)

    @model_validator(mode="after")
    def reject_lagged_association(self) -> LaggedAssociationAnalysis:
        raise ValueError("capability_unavailable: lagged_association is not supported in Phase 1")


AnalysisSpec = Annotated[
    DescriptiveAnalysis | CorrelationAnalysis | RegressionAnalysis | LaggedAssociationAnalysis,
    Field(discriminator="method"),
]


class VariantOverride(ClosedModel):
    override: dict[StrictStr, Any] = Field(default_factory=dict)
    substitute: dict[StrictStr, StrictStr] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_override_paths(self) -> VariantOverride:
        allowed_paths = {
            "input",
            "sample",
            "sample.years",
            "sample.exclude_years",
            "sample.geographies",
            "sample.require",
            "sample.min_obs_per_geography",
            "sample.drop_singletons",
            "weights",
            "outcome",
            "predictors",
            "controls",
            "fixed_effects",
            "inference",
            "inference.method",
            "inference.cluster_by",
            "inference.reps",
            "inference.seed",
            "inference.weight_type",
            "ci_level",
        }
        invalid = sorted(
            path for path in self.override if path not in allowed_paths or not path.strip()
        )
        if invalid:
            raise ValueError(f"unsupported variant override path(s): {invalid}")
        return self


class VariantSpec(ClosedModel):
    id: StrictStr = Field(min_length=1)
    applies_to: list[StrictStr] = Field(min_length=1)
    override: dict[StrictStr, Any] = Field(default_factory=dict)
    substitute: dict[StrictStr, StrictStr] = Field(default_factory=dict)

    @field_validator("id")
    @classmethod
    def validate_result_key_id(cls, value: str) -> str:
        return _validate_result_key_id(value, kind="variant", reserved={"base"})

    @model_validator(mode="after")
    def validate_variant(self) -> VariantSpec:
        VariantOverride(override=self.override, substitute=self.substitute)
        if len(set(self.applies_to)) != len(self.applies_to):
            raise ValueError("variant applies_to entries must be unique")
        if any(
            not source.strip() or not destination.strip()
            for source, destination in self.substitute.items()
        ):
            raise ValueError(
                "variant substitutions require non-empty source and destination columns"
            )
        if len(set(self.substitute.values())) != len(self.substitute.values()):
            raise ValueError("variant substitution destinations must be unique")
        return self


class LinearIdentityTerm(ClosedModel):
    analysis: StrictStr = Field(min_length=1)
    term: StrictStr = Field(min_length=1)
    sign: StrictInt


class LinearIdentityDiagnostic(ClosedModel):
    id: StrictStr = Field(min_length=1)
    op: Literal["linear_identity"]
    terms: list[LinearIdentityTerm] = Field(min_length=2)
    variants: Literal["all"] | list[StrictStr] = "all"
    require_same_design: bool = False


class ColumnIdentityRows(ClosedModel):
    group: StrictStr = Field(min_length=1)


class ColumnIdentityDiagnostic(ClosedModel):
    id: StrictStr = Field(min_length=1)
    op: Literal["column_identity"]
    panel: StrictStr = Field(min_length=1)
    lhs: StrictStr = Field(min_length=1)
    rhs: list[StrictStr] = Field(min_length=1)
    rows: Literal["unmasked"] | ColumnIdentityRows = "unmasked"


DiagnosticSpec = Annotated[
    LinearIdentityDiagnostic | ColumnIdentityDiagnostic,
    Field(discriminator="op"),
]


class CriterionSource(ClosedModel):
    analysis: StrictStr = Field(min_length=1)
    term: StrictStr | None = None
    stat: StrictStr | None = None

    @model_validator(mode="after")
    def validate_source(self) -> CriterionSource:
        if (self.term is None) == (self.stat is None):
            raise ValueError("criterion source requires exactly one of term or stat")
        return self


class StatCondition(ClosedModel):
    stat: StrictStr = Field(min_length=1)
    value: NumericValue


class ValueMarginCondition(ClosedModel):
    value: NumericValue
    margin: NumericValue = 0


class BandCondition(ClosedModel):
    equivalent_to: ValueMarginCondition | None = None
    ci_above: ValueMarginCondition | None = None
    ci_below: ValueMarginCondition | None = None
    ci_excludes: NumericValue | None = None
    stat_gte: StatCondition | None = None
    stat_lte: StatCondition | None = None

    @model_validator(mode="after")
    def validate_conditions(self) -> BandCondition:
        selected = [
            self.equivalent_to,
            self.ci_above,
            self.ci_below,
            self.ci_excludes,
            self.stat_gte,
            self.stat_lte,
        ]
        if not any(value is not None for value in selected):
            raise ValueError("a finding band must declare at least one condition")
        return self


class FindingBand(ClosedModel):
    label: StrictStr = Field(min_length=1)
    equivalent_to: ValueMarginCondition | None = None
    ci_above: ValueMarginCondition | None = None
    ci_below: ValueMarginCondition | None = None
    ci_excludes: NumericValue | None = None
    stat_gte: StatCondition | None = None
    stat_lte: StatCondition | None = None

    @model_validator(mode="after")
    def validate_band(self) -> FindingBand:
        BandCondition(
            equivalent_to=self.equivalent_to,
            ci_above=self.ci_above,
            ci_below=self.ci_below,
            ci_excludes=self.ci_excludes,
            stat_gte=self.stat_gte,
            stat_lte=self.stat_lte,
        )
        for name in ("equivalent_to", "ci_above", "ci_below"):
            values = getattr(self, name)
            if values is not None:
                margin = values.margin
                if isinstance(margin, (int, float)) and margin < 0:
                    raise ValueError(f"{name}.margin must be nonnegative")
        return self


class RobustnessSpec(ClosedModel):
    variants: list[StrictStr] = Field(min_length=1)


class FindingSpec(ClosedModel):
    id: StrictStr = Field(min_length=1)
    claim: StrictStr = Field(min_length=1)
    criteria_source: dict[StrictStr, Any]
    reference_values: list[FiniteNumber] = Field(default_factory=list)
    bands: list[FindingBand] = Field(min_length=1)
    otherwise: Literal["inconclusive"] = "inconclusive"
    robustness: RobustnessSpec | None = None
    limitations: list[StrictStr] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_finding(self) -> FindingSpec:
        labels = [band.label for band in self.bands]
        if len(set(labels)) != len(labels):
            raise ValueError("finding band labels must be unique")
        if not isinstance(self.criteria_source, dict) or set(self.criteria_source) - {
            "estimate",
            "stats",
        }:
            raise ValueError("criteria_source permits only estimate and stats")
        if "estimate" not in self.criteria_source and not self.criteria_source.get("stats"):
            raise ValueError("criteria_source requires estimate or at least one stat")
        if "estimate" in self.criteria_source:
            CriterionSource.model_validate(self.criteria_source["estimate"])
        stats = self.criteria_source.get("stats", {})
        if not isinstance(stats, dict):
            raise ValueError("criteria_source.stats must be a mapping")
        for source in stats.values():
            CriterionSource.model_validate(source)
        return self


class ResultsReport(ClosedModel):
    title: StrictStr = Field(min_length=1)


class ResultsSpec(ClosedModel):
    bundle_dir: StrictStr = Field(min_length=1)
    include_panels: list[StrictStr] = Field(min_length=1)
    tables: list[
        Literal[
            "coefficients",
            "fit",
            "sample_summary",
            "exclusions",
            "variant_comparison",
            "diagnostics",
        ]
    ] = Field(default_factory=list)
    report: ResultsReport


class RecipeV2(RecipeV1):
    """RecipeV1 build plan extended with a closed Phase-1 analysis contract."""

    version: Literal[2] = 2
    name: StrictStr = Field(validation_alias=AliasChoices("id", "name"), serialization_alias="id")
    title: StrictStr = Field(min_length=1)
    authors: list[StrictStr] = Field(min_length=1)
    panels: dict[StrictStr, PanelBinding] = Field(min_length=1)
    parameters: dict[StrictStr, FiniteNumber | UnresolvedMarker] = Field(default_factory=dict)
    masks: list[MaskSpec] = Field(default_factory=list)
    annotations: list[DataAnnotationSpec] = Field(default_factory=list)
    derive: list[DerivationSpec] = Field(default_factory=list)
    sample_groups: dict[StrictStr, SampleGroupSpec] = Field(default_factory=dict)
    analyses: list[AnalysisSpec] = Field(min_length=1)
    variants: list[VariantSpec] = Field(default_factory=list)
    diagnostics: list[DiagnosticSpec] = Field(default_factory=list)
    findings: list[FindingSpec] = Field(default_factory=list)
    results: ResultsSpec
    site_universe: SiteUniverseSpec | None = None

    @property
    def id(self) -> str:
        """Return the serialized V2 identity (stored in the inherited name field)."""
        return self.name

    @model_validator(mode="after")
    def validate_results_recipe(self) -> RecipeV2:
        for label, records in (
            ("masks", self.masks),
            ("derive", self.derive),
            ("analyses", self.analyses),
            ("variants", self.variants),
            ("diagnostics", self.diagnostics),
            ("findings", self.findings),
        ):
            ids = [record.id for record in records]
            if len(set(ids)) != len(ids):
                raise ValueError(f"{label} ids must be unique")

        identity_keys = [(record.id, record.revision) for record in self.annotations]
        if len(set(identity_keys)) != len(identity_keys):
            raise ValueError("annotations must be unique per (id, revision)")
        _validate_annotation_supersedes_graph(
            [record.model_dump(mode="python") for record in self.annotations],
            require_exists=False,
        )

        mask_ids = {mask.id for mask in self.masks}
        for annotation in self.annotations:
            for mask_id in annotation.applies_to_masks:
                if mask_id not in mask_ids:
                    raise ValueError(
                        f"annotation {annotation.id!r} references unknown mask "
                        f"{mask_id!r}"
                    )

        pipeline_by_id = {pipeline.id: pipeline for pipeline in self.pipelines}
        panel_refs: set[tuple[str, str]] = set()
        for panel_name, panel in self.panels.items():
            if panel.site_year is not None:
                continue
            if isinstance(panel.ref, UnresolvedMarker):
                continue
            if panel.ref is None:
                raise ValueError(f"panel {panel_name!r} requires a build reference")
            pipeline = pipeline_by_id.get(panel.ref.pipeline)
            if pipeline is None:
                raise ValueError(
                    f"panel {panel_name!r} references unknown pipeline {panel.ref.pipeline!r}"
                )
            target = next(target for target in self.targets if target.id == pipeline.target)
            if "panel" not in target.outputs:
                raise ValueError(
                    f"panel {panel_name!r} pipeline {pipeline.id!r} does not produce panel output"
                )
            identity = (panel.ref.pipeline, panel.ref.output)
            if identity in panel_refs:
                raise ValueError(f"panel output {identity!r} is assigned to more than one name")
            panel_refs.add(identity)

        panel_names = set(self.panels)
        site_year_panels = [
            (name, panel)
            for name, panel in self.panels.items()
            if panel.site_year is not None
        ]
        if self.site_universe is None and site_year_panels:
            raise ValueError("site_year panel requires top-level site_universe")
        if self.site_universe is not None and len(site_year_panels) != 1:
            raise ValueError("site_universe requires exactly one derived site_year panel")
        if self.site_universe is None and len(site_year_panels) > 1:
            raise ValueError("only one derived site_year panel is supported")
        for _panel_name, panel in site_year_panels:
            assert panel.site_year is not None
            outcome_panels = panel.site_year.outcomes
            for source_name, binding in (
                ("pit", outcome_panels.pit),
                ("hic", outcome_panels.hic),
            ):
                source = self.panels.get(binding.panel)
                if source is None or source.site_year is not None:
                    raise ValueError(
                        f"site_year {source_name} source references unknown or derived panel "
                        f"{binding.panel!r}"
                    )
                if isinstance(source.ref, UnresolvedMarker):
                    raise ValueError(
                        f"site_year {source_name} source panel {binding.panel!r} is unresolved"
                    )
                if source.geo_key != "coc_id" or source.time_key != "year":
                    raise ValueError(
                        f"site_year {source_name} source panel {binding.panel!r} requires "
                        "geo_key=coc_id and time_key=year"
                    )
                source_pipeline = pipeline_by_id[source.ref.pipeline]
                source_target = next(
                    target for target in self.targets if target.id == source_pipeline.target
                )
                if source_target.geometry.type != "coc":
                    raise ValueError(
                        f"site_year {source_name} source panel {binding.panel!r} "
                        "must target CoC geometry"
                    )
        analysis_ids = {analysis.id for analysis in self.analyses}
        variant_ids = {variant.id for variant in self.variants}
        group_ids = set(self.sample_groups)
        for analysis in self.analyses:
            input_name = analysis.input.removeprefix("panels.")
            if input_name not in panel_names:
                raise ValueError(
                    f"analysis {analysis.id!r} references unknown panel {analysis.input!r}"
                )
            if (
                isinstance(analysis.sample, SampleGroupReference)
                and analysis.sample.group not in group_ids
            ):
                raise ValueError(
                    f"analysis {analysis.id!r} references unknown sample group "
                    f"{analysis.sample.group!r}"
                )
        for variant in self.variants:
            missing = sorted(set(variant.applies_to) - analysis_ids)
            if missing:
                raise ValueError(f"variant {variant.id!r} references unknown analyses: {missing}")
        for mask in self.masks:
            missing = sorted(set(mask.panels) - panel_names)
            if missing:
                raise ValueError(f"mask {mask.id!r} references unknown panels: {missing}")
        for derivation in self.derive:
            missing = sorted(set(derivation.panels) - panel_names)
            if missing:
                raise ValueError(
                    f"derivation {derivation.id!r} references unknown panels: {missing}"
                )
        for diagnostic in self.diagnostics:
            if isinstance(diagnostic, LinearIdentityDiagnostic):
                missing = sorted({term.analysis for term in diagnostic.terms} - analysis_ids)
                if missing:
                    raise ValueError(
                        f"diagnostic {diagnostic.id!r} references unknown analyses: {missing}"
                    )
            elif diagnostic.panel not in panel_names:
                raise ValueError(
                    f"diagnostic {diagnostic.id!r} references unknown panel {diagnostic.panel!r}"
                )
        for finding in self.findings:
            sources = []
            if "estimate" in finding.criteria_source:
                sources.append(CriterionSource.model_validate(finding.criteria_source["estimate"]))
            sources.extend(
                CriterionSource.model_validate(source)
                for source in finding.criteria_source.get("stats", {}).values()
            )
            for source in sources:
                if source.analysis not in analysis_ids:
                    raise ValueError(
                        f"finding {finding.id!r} references unknown analysis {source.analysis!r}"
                    )
            if finding.robustness is not None:
                missing = sorted(set(finding.robustness.variants) - variant_ids)
                if missing:
                    raise ValueError(
                        f"finding {finding.id!r} references unknown variants: {missing}"
                    )
        missing_result_panels = sorted(set(self.results.include_panels) - panel_names)
        if missing_result_panels:
            raise ValueError(f"results include unknown panels: {missing_result_panels}")

        self._validate_derivation_graph()
        self._validate_parameter_references()
        self._validate_resolved_margins()
        return self

    def unresolved_values(self) -> tuple[tuple[str, str], ...]:
        """Return unresolved markers as ``(model path, description)`` pairs."""
        found: list[tuple[str, str]] = []

        def visit(value: Any, path: str) -> None:
            if isinstance(value, UnresolvedMarker):
                found.append((path, value.unresolved))
            elif isinstance(value, BaseModel):
                for field_name in type(value).model_fields:
                    visit(getattr(value, field_name), f"{path}.{field_name}")
            elif isinstance(value, dict):
                for key, item in value.items():
                    visit(item, f"{path}.{key}")
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    visit(item, f"{path}[{index}]")

        visit(self, "recipe")
        return tuple(found)

    def _validate_parameter_references(self) -> None:
        parameter_names = set(self.parameters)

        def visit(value: Any, path: str) -> None:
            if isinstance(value, ParameterReference) and value.param not in parameter_names:
                raise ValueError(f"{path} references unknown parameter {value.param!r}")
            if isinstance(value, BaseModel):
                for field_name in type(value).model_fields:
                    visit(getattr(value, field_name), f"{path}.{field_name}")
            elif isinstance(value, dict):
                for key, item in value.items():
                    visit(item, f"{path}.{key}")
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    visit(item, f"{path}[{index}]")

        visit(self, "recipe")

    def _validate_resolved_margins(self) -> None:
        """Check margins after resolving parameter references when possible."""
        for finding in self.findings:
            for band in finding.bands:
                for name in ("equivalent_to", "ci_above", "ci_below"):
                    condition = getattr(band, name)
                    if condition is None:
                        continue
                    margin = condition.margin
                    if isinstance(margin, ParameterReference):
                        margin = self.parameters.get(margin.param)
                    if isinstance(margin, (int, float)) and margin < 0:
                        raise ValueError(
                            f"finding {finding.id!r} {name}.margin must be nonnegative"
                        )

    def _validate_derivation_graph(self) -> None:
        dependencies: dict[str, set[str]] = {}
        for derivation in self.derive:
            if isinstance(
                derivation,
                (
                    SumDerivation,
                    TimeDerivation,
                    GrowthDerivation,
                    LogDerivation,
                    IndicatorDerivation,
                    ShareDerivation,
                ),
            ):
                columns = [derivation.of]
                if isinstance(derivation, SumDerivation):
                    columns = derivation.of
                elif isinstance(derivation, ShareDerivation):
                    columns.append(derivation.total)
            elif isinstance(derivation, RatioDerivation):
                columns = [derivation.num, derivation.den]
            elif isinstance(derivation, PerCapitaDerivation):
                columns = [derivation.of, derivation.pop]
            else:
                columns = []
            dependencies[derivation.id] = set(columns) & {item.id for item in self.derive}

        visiting: list[str] = []
        visited: set[str] = set()

        def visit(node: str) -> None:
            if node in visiting:
                cycle = visiting[visiting.index(node) :] + [node]
                raise ValueError(f"derive dependency cycle: {' -> '.join(cycle)}")
            if node in visited:
                return
            visiting.append(node)
            for dependency in sorted(dependencies[node]):
                visit(dependency)
            visiting.pop()
            visited.add(node)

        for node in sorted(dependencies):
            visit(node)


def is_finite_number(value: object) -> bool:
    """Return whether ``value`` is a finite, non-boolean JSON number."""
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value)


def _validate_result_key_id(
    value: str,
    *,
    kind: str,
    reserved: set[str] | None = None,
) -> str:
    """Keep IDs safe for the documented ``<analysis>@<variant>`` result key."""
    if not value.strip():
        raise ValueError(f"{kind} id must contain a non-whitespace character")
    if "@" in value:
        raise ValueError(f"{kind} id may not contain the reserved '@' result-key delimiter")
    if value in (reserved or set()):
        raise ValueError(f"{kind} id {value!r} is reserved")
    return value
