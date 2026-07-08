"""Canonical measure definitions for analysis-ready artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from hhplab.schema.columns import (
    ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
    ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
    ACS1_IMPUTATION_LINEAGE_COLUMNS,
    ACS1_MEASURE_COLUMNS,
    ACS_MEASURE_COLUMNS,
    HIC_EXPANDED_MEASURE_SCHEMA,
    HIC_PANEL_MEASURE_COLUMNS,
    LAUS_MEASURE_COLUMNS,
    POPULATION_DENSITY_COLUMN,
    SAE_MEASURE_COLUMNS,
    TOTAL_POPULATION,
)


@dataclass(frozen=True)
class MeasureDefinition:
    """Stable definition for a canonical analysis measure."""

    name: str
    family: Literal["population", "pit", "rent", "labor", "demographic"]
    unit: str
    default_aggregation: Literal["sum", "mean", "weighted_mean"]


MeasureRole = Literal["outcome", "candidate_driver", "control", "denominator", "diagnostic"]


@dataclass(frozen=True)
class PanelMeasureDictionaryEntry:
    """Machine-readable semantics for a panel measure column."""

    measure_id: str
    column: str
    definition: str
    units: str
    source_provider: str
    source_product: str
    native_geometry: str
    first_year: int
    last_year: int | None
    role_hint: MeasureRole
    panel_contexts: tuple[str, ...] = ()
    caveats: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        data = asdict(self)
        data["coverage_years"] = {
            "first": self.first_year,
            "last": self.last_year,
            "last_is_ongoing": self.last_year is None,
        }
        del data["first_year"]
        del data["last_year"]
        return data


@dataclass(frozen=True)
class ACS1ImputationMeasureSpec:
    """Declarative contract for an ACS1-controlled / ACS5-share measure."""

    name: str
    family: Literal["population", "poverty", "labor", "housing", "income"]
    target_geo_type: str
    value_kind: Literal["count", "rate"]
    acs1_source_columns: tuple[str, ...]
    acs5_support_columns: tuple[str, ...]
    output_column: str
    numerator_source_columns: tuple[str, ...] = ()
    denominator_source_column: str | None = None
    numerator_output_column: str | None = None
    denominator_output_column: str | None = None
    allocation_method: str = "acs1_controlled_acs5_tract_share"
    denominator_source: str = "acs5_tract_support"
    zero_denominator_policy: Literal["null_rate", "zero_count"] = "null_rate"
    validation_abs_tolerance: float = 1e-6
    validation_rel_tolerance: float = 1e-9

    @property
    def output_columns(self) -> tuple[str, ...]:
        columns = [
            self.numerator_output_column,
            self.denominator_output_column,
            self.output_column,
        ]
        return tuple(column for column in columns if column is not None)

    @property
    def modeled_flag_column(self) -> str:
        return "is_modeled"

    @property
    def synthetic_flag_column(self) -> str:
        return "is_synthetic"

    @property
    def provenance_columns(self) -> tuple[str, ...]:
        return tuple(ACS1_IMPUTATION_LINEAGE_COLUMNS)

    def validate(self) -> None:
        """Raise a clear error if the measure declaration is internally invalid."""
        if not self.acs1_source_columns:
            raise ValueError(f"{self.name} must declare ACS1 source columns.")
        if not self.acs5_support_columns:
            raise ValueError(f"{self.name} must declare ACS5 tract support columns.")
        if self.value_kind == "rate":
            missing = [
                label
                for label, value in (
                    ("numerator_source_columns", self.numerator_source_columns),
                    ("denominator_source_column", self.denominator_source_column),
                    ("numerator_output_column", self.numerator_output_column),
                    ("denominator_output_column", self.denominator_output_column),
                )
                if not value
            ]
            if missing:
                raise ValueError(f"{self.name} rate spec is missing required fields: {missing}.")
            if len(self.numerator_source_columns) != 1:
                raise ValueError(
                    f"{self.name} rate spec must declare exactly one numerator_source_columns "
                    "entry because ACS1 rate imputation allocates one numerator component."
                )
            invalid_numerators = [
                column
                for column in self.numerator_source_columns
                if column not in self.acs1_source_columns
            ]
            if invalid_numerators:
                raise ValueError(
                    f"{self.name} numerator_source_columns must be present in "
                    f"acs1_source_columns: {invalid_numerators}."
                )
            if self.denominator_source_column not in self.acs5_support_columns:
                raise ValueError(
                    f"{self.name} denominator_source_column must be present in "
                    "acs5_support_columns."
                )
        if self.value_kind == "count" and (
            self.numerator_source_columns
            or self.denominator_source_column is not None
            or self.numerator_output_column is not None
            or self.denominator_output_column is not None
        ):
            raise ValueError(
                f"{self.name} count spec must not declare numerator/denominator fields."
            )


ACS1_IMPUTED_POVERTY_SPEC = ACS1ImputationMeasureSpec(
    name="poverty_rate",
    family="poverty",
    target_geo_type="tract",
    value_kind="rate",
    acs1_source_columns=("population_below_poverty", "poverty_universe"),
    acs5_support_columns=("population_below_poverty", "poverty_universe"),
    numerator_source_columns=("population_below_poverty",),
    denominator_source_column="poverty_universe",
    numerator_output_column="acs1_imputed_population_below_poverty",
    denominator_output_column="acs1_imputed_poverty_universe",
    output_column="acs1_imputed_poverty_rate",
)

ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC = ACS1ImputationMeasureSpec(
    name="total_households",
    family="housing",
    target_geo_type="tract",
    value_kind="count",
    acs1_source_columns=("total_households",),
    acs5_support_columns=("total_households",),
    output_column="acs1_imputed_total_households",
    zero_denominator_policy="zero_count",
)

ACS1_IMPUTATION_MEASURE_SPECS: tuple[ACS1ImputationMeasureSpec, ...] = (
    ACS1_IMPUTED_POVERTY_SPEC,
    ACS1_IMPUTED_TOTAL_HOUSEHOLDS_SPEC,
)

ACS1_IMPUTATION_MEASURE_COLUMNS: list[str] = list(
    dict.fromkeys(
        column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.output_columns
    )
)

ACS1_IMPUTATION_OUTPUT_COLUMNS: list[str] = [
    *ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
    *ACS1_IMPUTATION_MEASURE_COLUMNS,
    *ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
]

ACS1_IMPUTATION_MEASURES: tuple[str, ...] = tuple(ACS1_IMPUTATION_MEASURE_COLUMNS)

ACS1_IMPUTATION_REQUIRED_ACS1_SOURCE_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.acs1_source_columns
    )
)

ACS1_IMPUTATION_REQUIRED_ACS5_SUPPORT_COLUMNS: tuple[str, ...] = tuple(
    dict.fromkeys(
        column for spec in ACS1_IMPUTATION_MEASURE_SPECS for column in spec.acs5_support_columns
    )
)


def acs1_imputation_output_columns(
    specs: tuple[ACS1ImputationMeasureSpec, ...] = ACS1_IMPUTATION_MEASURE_SPECS,
) -> list[str]:
    """Return canonical output columns for a set of ACS1 imputation specs."""
    measure_columns = [column for spec in specs for column in spec.output_columns]
    return [
        *ACS1_IMPUTATION_BASE_OUTPUT_COLUMNS,
        *dict.fromkeys(measure_columns),
        *ACS1_IMPUTATION_DIAGNOSTIC_COLUMNS,
    ]


TOTAL_POPULATION_MEASURE = MeasureDefinition(
    name=TOTAL_POPULATION,
    family="population",
    unit="persons",
    default_aggregation="sum",
)

PIT_MEASURES: tuple[MeasureDefinition, ...] = (
    MeasureDefinition("pit_total", "pit", "persons", "sum"),
    MeasureDefinition("pit_sheltered", "pit", "persons", "sum"),
    MeasureDefinition("pit_unsheltered", "pit", "persons", "sum"),
)

ACS5_MEASURES: tuple[str, ...] = tuple(ACS_MEASURE_COLUMNS)
ACS1_MEASURES: tuple[str, ...] = tuple(ACS1_MEASURE_COLUMNS)
LAUS_MEASURES: tuple[str, ...] = tuple(LAUS_MEASURE_COLUMNS)
SAE_MEASURES: tuple[str, ...] = tuple(SAE_MEASURE_COLUMNS)


def _entry(
    column: str,
    definition: str,
    units: str,
    source_provider: str,
    source_product: str,
    native_geometry: str,
    first_year: int,
    role_hint: MeasureRole,
    *caveats: str,
    last_year: int | None = None,
    measure_id: str | None = None,
    panel_contexts: tuple[str, ...] = (),
) -> PanelMeasureDictionaryEntry:
    return PanelMeasureDictionaryEntry(
        measure_id=measure_id or f"{source_product}:{column}",
        column=column,
        definition=definition,
        units=units,
        source_provider=source_provider,
        source_product=source_product,
        native_geometry=native_geometry,
        first_year=first_year,
        last_year=last_year,
        role_hint=role_hint,
        panel_contexts=panel_contexts,
        caveats=tuple(caveats),
    )


ACS5_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        TOTAL_POPULATION,
        "Total resident population apportioned from ACS 5-year tract estimates.",
        "persons",
        "census",
        "acs5",
        "tract",
        2009,
        "denominator",
        "ACS vintage follows the PIT lag convention: PIT year Y uses ACS vintage Y-1.",
        "Tract-to-target allocation assumes the selected crosswalk denominator is representative.",
    ),
    _entry(
        POPULATION_DENSITY_COLUMN,
        "Population per square kilometer for the analysis geography.",
        "persons_per_square_kilometer",
        "census",
        "acs5",
        "tract",
        2009,
        "control",
        "Depends on target geography area and the ACS population vintage used in the panel.",
    ),
    _entry(
        "adult_population",
        "Population age 18 and older derived from ACS age-by-sex counts.",
        "persons",
        "census",
        "acs5",
        "tract",
        2009,
        "denominator",
        "Derived from B01001 components before target-geography apportionment.",
    ),
    _entry(
        "population_below_poverty",
        "Population below the federal poverty threshold.",
        "persons",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "ACS poverty status excludes some group-quarter populations.",
    ),
    _entry(
        "poverty_rate",
        "Share of the poverty-universe population below the federal poverty threshold.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Derived as population_below_poverty / poverty_universe after aggregation.",
    ),
    _entry(
        "median_household_income",
        "Median household income in nominal dollars.",
        "nominal_usd",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Nominal dollars are not inflation-adjusted; use CPI-derived transforms for real dollars.",
    ),
    _entry(
        "median_gross_rent",
        "Median monthly gross rent for renter-occupied units paying cash rent.",
        "nominal_usd_per_month",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Nominal dollars are not inflation-adjusted.",
    ),
    _entry(
        "vacancy_rate",
        "Vacant housing units divided by total housing units.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Derived after ACS tract measures are allocated to the target geography.",
    ),
    _entry(
        "rent_burden_30_plus",
        "Share of renter households with gross rent at least 30 percent of income.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Excludes rent-burden rows where income percentage is not computed.",
    ),
    _entry(
        "rent_burden_40_plus",
        "Share of renter households with gross rent at least 40 percent of income.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Uses a computed denominator that excludes rent-burden rows where income "
        "percentage is not computed.",
    ),
    _entry(
        "rent_burden_50_plus",
        "Share of renter households with gross rent at least 50 percent of income.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "Uses a computed denominator that excludes rent-burden rows where income "
        "percentage is not computed.",
    ),
    _entry(
        "unemployment_rate",
        "Unemployed civilian labor force divided by civilian labor force.",
        "share",
        "census",
        "acs5",
        "tract",
        2009,
        "candidate_driver",
        "When LAUS is also present, prefer provenance fields to distinguish ACS5 and LAUS rates.",
        measure_id="acs5:unemployment_rate",
        panel_contexts=("coc", "acs5", "msa"),
    ),
)

PIT_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "pit_total",
        "Total people experiencing homelessness counted in the annual January PIT count.",
        "persons",
        "hud",
        "pit",
        "coc",
        2007,
        "outcome",
        "PIT is a point-in-time count and is known to undercount unsheltered homelessness.",
        "The 2021 PIT count was disrupted by COVID-19 and is not fully comparable everywhere.",
    ),
    _entry(
        "pit_sheltered",
        "Sheltered people experiencing homelessness counted in the annual January PIT count.",
        "persons",
        "hud",
        "pit",
        "coc",
        2007,
        "outcome",
        "HUD methodology and local enumeration practices can change across years.",
    ),
    _entry(
        "pit_unsheltered",
        "Unsheltered people experiencing homelessness counted in the annual January PIT count.",
        "persons",
        "hud",
        "pit",
        "coc",
        2007,
        "outcome",
        "Unsheltered counts are especially sensitive to weather, outreach coverage, "
        "and methodology.",
    ),
)

ACS1_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "rent_burden_40_plus",
        "ACS 1-year share of renter households with computed gross rent at least 40% of income.",
        "share",
        "census",
        "acs1",
        "metro",
        2005,
        "candidate_driver",
        "Denominator excludes renter households where gross rent as a percentage of "
        "income is not computed.",
    ),
    _entry(
        "rent_burden_50_plus",
        "ACS 1-year share of renter households with computed gross rent at least 50% of income.",
        "share",
        "census",
        "acs1",
        "metro",
        2005,
        "candidate_driver",
        "Denominator excludes renter households where gross rent as a percentage of "
        "income is not computed.",
    ),
    _entry(
        "unemployment_rate_acs1",
        "ACS 1-year unemployment rate for metro-native analysis.",
        "share",
        "census",
        "acs1",
        "metro",
        2005,
        "candidate_driver",
        "ACS1 is available only for geographies meeting Census population thresholds.",
    ),
    _entry(
        "renter_moved_share",
        "ACS 1-year share of the renter-housed population that moved in the past "
        "year, including within-county moves (B07013).",
        "share",
        "census",
        "acs1",
        "metro",
        2010,
        "candidate_driver",
        "Universe is population 1 year and over in housing units classified by "
        "current householder tenure; person-weighted, not household-weighted.",
    ),
    _entry(
        "owner_moved_share",
        "ACS 1-year share of the owner-housed population that moved in the past "
        "year, including within-county moves (B07013).",
        "share",
        "census",
        "acs1",
        "metro",
        2010,
        "candidate_driver",
        "Universe is population 1 year and over in housing units classified by "
        "current householder tenure; person-weighted, not household-weighted.",
    ),
    _entry(
        "work_from_home_share",
        "ACS 1-year share of workers 16 years and over who worked from home (B08301).",
        "share",
        "census",
        "acs1",
        "metro",
        2005,
        "candidate_driver",
        "Commute mode is worker-weighted and does not directly measure housing-space demand.",
    ),
    _entry(
        "seasonal_recreational_vacancy_share",
        "ACS 1-year share of vacant housing units for seasonal, recreational, "
        "or occasional use (B25004).",
        "share",
        "census",
        "acs1",
        "metro",
        2005,
        "candidate_driver",
        "Denominator is vacant housing units, not all housing units; interpret "
        "as a free proxy for vacation-home or STR-adjacent stock.",
    ),
)

LAUS_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "labor_force",
        "Annual-average civilian labor force.",
        "persons",
        "bls",
        "laus",
        "metro",
        1990,
        "denominator",
        "LAUS metro definitions and series availability can change across vintages.",
    ),
    _entry(
        "employed",
        "Annual-average employed civilian labor force.",
        "persons",
        "bls",
        "laus",
        "metro",
        1990,
        "candidate_driver",
        "Use with labor_force to interpret employment scale across differently sized metros.",
    ),
    _entry(
        "unemployed",
        "Annual-average unemployed civilian labor force.",
        "persons",
        "bls",
        "laus",
        "metro",
        1990,
        "candidate_driver",
        "Use with labor_force to interpret unemployment scale across differently sized metros.",
    ),
    _entry(
        "unemployment_rate",
        "Annual-average LAUS unemployment rate.",
        "share",
        "bls",
        "laus",
        "metro",
        1990,
        "candidate_driver",
        "Column name can also appear in ACS5 contexts; inspect provenance and panel geography.",
        measure_id="laus:unemployment_rate",
        panel_contexts=("metro", "laus"),
    ),
)

MSA_COC_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "msa_population",
        "MSA resident population used for MSA-CoC panel covariates.",
        "persons",
        "census",
        "acs5_or_pep",
        "msa",
        2009,
        "denominator",
        "Population source is controlled by the MSA-CoC panel recipe policy.",
        "Check msa_population_source to distinguish ACS5 from PEP population.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_median_rent",
        "MSA median monthly gross rent used as a metro-context covariate.",
        "nominal_usd_per_month",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Nominal dollars are not inflation-adjusted.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_vacancy_rate",
        "MSA vacant housing units divided by total housing units.",
        "share",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Derived from ACS5 housing counts before joining to CoC rows.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_poverty_rate",
        "MSA share of poverty-universe population below the federal poverty threshold.",
        "share",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "ACS poverty status excludes some group-quarter populations.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_unemployment",
        "MSA unemployment rate selected for the MSA-CoC panel.",
        "share",
        "census_or_bls",
        "acs5_or_laus",
        "msa",
        2009,
        "candidate_driver",
        "Check unemployment_source to distinguish ACS5-derived and LAUS unemployment.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_income",
        "MSA median household income in nominal dollars.",
        "nominal_usd",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Nominal dollars are not inflation-adjusted; use CPI-derived transforms for real dollars.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_rent_burden",
        "MSA share of renter households with gross rent at least 30 percent of income.",
        "share",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Excludes rent-burden rows where income percentage is not computed.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_rent_burden_40_plus",
        "MSA share of renter households with gross rent at least 40 percent of income.",
        "share",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Uses a computed denominator that excludes rent-burden rows where income "
        "percentage is not computed.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "msa_rent_burden_50_plus",
        "MSA share of renter households with gross rent at least 50 percent of income.",
        "share",
        "census",
        "acs5",
        "msa",
        2009,
        "candidate_driver",
        "Uses a computed denominator that excludes rent-burden rows where income "
        "percentage is not computed.",
        panel_contexts=("msa_coc",),
    ),
    _entry(
        "coc_population",
        "CoC resident population used for ranking and containment in MSA-CoC panels.",
        "persons",
        "census",
        "acs5_or_pep",
        "coc",
        2009,
        "denominator",
        "Population source follows the recipe input selected for CoC population.",
        panel_contexts=("msa_coc",),
    ),
)

PEP_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "population",
        "County postcensal population estimate aggregated to the analysis geography.",
        "persons",
        "census",
        "pep",
        "county",
        2010,
        "denominator",
        "PEP vintages are revised; record the source vintage when comparing outputs.",
    ),
)

ZORI_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = (
    _entry(
        "zori_coc",
        "Zillow Observed Rent Index aggregated from counties to the CoC analysis geography.",
        "nominal_usd_per_month",
        "zillow",
        "zori",
        "county",
        2015,
        "candidate_driver",
        "ZORI All Homes starts in January 2015.",
        "Panel PIT alignment usually uses January observations or a declared annual alignment.",
    ),
    _entry(
        "zori_coverage_ratio",
        "Share of target-geography rent weight covered by available ZORI counties.",
        "share",
        "zillow",
        "zori",
        "county",
        2015,
        "diagnostic",
        "Low coverage means zori_coc may be missing or should be interpreted cautiously.",
    ),
    _entry(
        "zori_is_eligible",
        "Whether the CoC-year passes the configured ZORI coverage threshold.",
        "boolean",
        "zillow",
        "zori",
        "county",
        2015,
        "diagnostic",
        "Eligibility depends on the configured zori_min_coverage threshold.",
    ),
    _entry(
        "zori_excluded_reason",
        "Reason ZORI was excluded for an otherwise requested CoC-year.",
        "category",
        "zillow",
        "zori",
        "county",
        2015,
        "diagnostic",
        "Null or empty values indicate no ZORI exclusion reason was applied.",
    ),
    _entry(
        "rent_to_income",
        "Monthly ZORI divided by monthly median household income.",
        "ratio",
        "derived",
        "zori_acs5",
        "mixed",
        2015,
        "candidate_driver",
        "Combines ZORI county rents with ACS5 income; inflation and ACS lag "
        "conventions still apply.",
    ),
)

SAE_ROLE_HINTS: dict[str, MeasureRole] = {
    "sae_civilian_labor_force": "denominator",
}

SAE_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = tuple(
    _entry(
        column,
        f"Small-area-estimated ACS-derived measure: {column}.",
        "varies",
        "census",
        "acs1_acs5_sae",
        "tract",
        2009,
        SAE_ROLE_HINTS.get(column, "candidate_driver"),
        "SAE measures combine ACS1 controls with ACS5 tract support; inspect SAE lineage columns.",
    )
    for column in SAE_MEASURE_COLUMNS
)

HIC_PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = tuple(
    _entry(
        str(spec["column"]),
        str(spec["semantics"]),
        str(spec["unit"]),
        "hud",
        "hic",
        "coc",
        2007,
        "candidate_driver",
        str(spec["historical_missing"]),
    )
    for spec in HIC_EXPANDED_MEASURE_SCHEMA
    if str(spec["column"]) in HIC_PANEL_MEASURE_COLUMNS
)

_PANEL_MEASURE_DICTIONARY_CANDIDATES: tuple[PanelMeasureDictionaryEntry, ...] = (
    *PIT_PANEL_MEASURE_DICTIONARY,
    *ACS5_PANEL_MEASURE_DICTIONARY,
    *ACS1_PANEL_MEASURE_DICTIONARY,
    *LAUS_PANEL_MEASURE_DICTIONARY,
    *MSA_COC_PANEL_MEASURE_DICTIONARY,
    *PEP_PANEL_MEASURE_DICTIONARY,
    *ZORI_PANEL_MEASURE_DICTIONARY,
    *SAE_PANEL_MEASURE_DICTIONARY,
    *HIC_PANEL_MEASURE_DICTIONARY,
)

PANEL_MEASURE_DICTIONARY: tuple[PanelMeasureDictionaryEntry, ...] = tuple(
    sorted(_PANEL_MEASURE_DICTIONARY_CANDIDATES, key=lambda entry: entry.measure_id)
)

PANEL_MEASURE_DICTIONARY_BY_ID: dict[str, PanelMeasureDictionaryEntry] = {
    entry.measure_id: entry for entry in PANEL_MEASURE_DICTIONARY
}

PANEL_MEASURE_DICTIONARY_BY_COLUMN: dict[str, PanelMeasureDictionaryEntry] = {}
for _entry_by_column in PANEL_MEASURE_DICTIONARY:
    PANEL_MEASURE_DICTIONARY_BY_COLUMN.setdefault(_entry_by_column.column, _entry_by_column)


def resolve_panel_measure_entry(
    column: str,
    *,
    panel_columns: set[str] | list[str] | tuple[str, ...] | None = None,
) -> PanelMeasureDictionaryEntry | None:
    """Return the best dictionary entry for a panel column in context."""
    if column != "unemployment_rate":
        return PANEL_MEASURE_DICTIONARY_BY_COLUMN.get(column)

    columns = set(panel_columns or ())
    if {"labor_force", "employed", "unemployed", "laus_vintage_used"} & columns:
        return PANEL_MEASURE_DICTIONARY_BY_ID["laus:unemployment_rate"]
    return PANEL_MEASURE_DICTIONARY_BY_ID["acs5:unemployment_rate"]


def panel_measure_dictionary() -> list[dict[str, object]]:
    """Return the canonical machine-readable panel measure dictionary."""
    return [entry.to_dict() for entry in PANEL_MEASURE_DICTIONARY]
