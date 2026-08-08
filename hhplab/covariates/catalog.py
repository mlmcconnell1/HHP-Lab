"""Catalog of expanded hidden-cause covariate sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from hhplab.sources.bls.qcew.contract import (
    QCEW_FIRST_YEAR,
    QCEW_MEASURE_COLUMNS,
    QCEW_PRODUCT,
    QCEW_PROVIDER,
    QCEW_SOURCE_ID,
    QCEW_SOURCE_PAGE,
    QCEW_SOURCE_URL,
)
from hhplab.sources.cdc.overdose_contract import (
    CDC_OVERDOSE_FIRST_YEAR,
    CDC_OVERDOSE_MEASURE_COLUMNS,
    CDC_OVERDOSE_PRODUCT,
    CDC_OVERDOSE_PROVIDER,
    CDC_OVERDOSE_REQUIRED_COLUMNS,
    CDC_OVERDOSE_SOURCE_ID,
    CDC_OVERDOSE_SOURCE_PAGE,
    CDC_OVERDOSE_SOURCE_URL,
)
from hhplab.sources.census.bps.census_bps_contract import (
    CENSUS_BPS_MEASURE_COLUMNS,
    CENSUS_BPS_PRODUCT,
    CENSUS_BPS_PROVIDER,
    CENSUS_BPS_SOURCE_ID,
    CENSUS_BPS_SOURCE_PAGE,
)
from hhplab.sources.irs.irs_soi_contract import (
    IRS_SOI_COUNTY_MEASURE_COLUMNS,
    IRS_SOI_FIRST_YEAR,
    IRS_SOI_PRODUCT,
    IRS_SOI_PROVIDER,
    IRS_SOI_SOURCE_ID,
    IRS_SOI_SOURCE_PAGE,
    IRS_SOI_SOURCE_URL,
)
from hhplab.sources.mpi.mpi_contract import (
    MPI_MEASURE_COLUMNS,
    MPI_METHODOLOGY_NOTE,
    MPI_PRODUCT,
    MPI_PROVIDER,
    MPI_REQUIRED_CURATED_COLUMNS,
    MPI_SOURCE_ID,
    MPI_SOURCE_PAGE,
    MPI_SOURCE_URL,
)
from hhplab.sources.saiz.saiz_contract import (
    SAIZ_ESTIMATE_YEAR,
    SAIZ_MEASURE_COLUMNS,
    SAIZ_PRODUCT,
    SAIZ_PROVIDER,
    SAIZ_REQUIRED_CURATED_COLUMNS,
    SAIZ_SOURCE_ID,
    SAIZ_SOURCE_PAGE,
    SAIZ_SOURCE_URL,
)
from hhplab.sources.vera.incarceration_contract import (
    VERA_JAIL_MEASURE_COLUMNS,
    VERA_JAIL_PRODUCT,
    VERA_JAIL_RELIABLE_FIRST_YEAR,
    VERA_JAIL_RELIABLE_LAST_YEAR,
    VERA_JAIL_SOURCE_ID,
    VERA_PRISON_LAST_YEAR,
    VERA_PRISON_MEASURE_COLUMNS,
    VERA_PRISON_PRODUCT,
    VERA_PRISON_RELIABLE_FIRST_YEAR,
    VERA_PRISON_SOURCE_ID,
    VERA_PROVIDER,
    VERA_REQUIRED_CURATED_COLUMNS,
    VERA_SOURCE_PAGE,
    VERA_SOURCE_URL,
)

GeoType = Literal["county", "coc", "state", "msa"]
MeasureAggregation = Literal[
    "extensive_sum",
    "intensive_pop_weighted_mean",
    "rate",
]


@dataclass(frozen=True)
class CovariateSourceSpec:
    """Machine-readable source metadata and canonical schema contract."""

    source_id: str
    provider: str
    product: str
    topic: str
    native_geo: GeoType
    first_year: int
    last_year: int | None
    source_page: str
    source_url: str
    required_columns: tuple[str, ...]
    measure_columns: tuple[str, ...]
    measure_aggregations: dict[str, MeasureAggregation]
    recommended_align: str
    notes: str

    def to_dict(self) -> dict:
        """Serialize to a JSON-ready dictionary."""
        return asdict(self)


COVARIATE_SOURCE_SPECS: dict[str, CovariateSourceSpec] = {
    "eviction_lab": CovariateSourceSpec(
        source_id="eviction_lab",
        provider="eviction_lab",
        product="eviction_filings",
        topic="evictions",
        native_geo="county",
        first_year=2000,
        last_year=None,
        source_page="https://evictionlab.org/map/",
        source_url="https://evictionlab.org/map/",
        required_columns=("county_fips", "year"),
        measure_columns=("eviction_filings", "eviction_rate"),
        measure_aggregations={
            "eviction_filings": "extensive_sum",
            "eviction_rate": "intensive_pop_weighted_mean",
        },
        recommended_align="calendar_year",
        notes=(
            "County-year eviction filings/rates are useful housing-instability "
            "covariates; verify local coverage before causal interpretation."
        ),
    ),
    "eviction_lab_national": CovariateSourceSpec(
        source_id="eviction_lab_national",
        provider="eviction_lab",
        product="national_eviction_map",
        topic="evictions",
        native_geo="county",
        first_year=2000,
        last_year=2018,
        source_page="https://evictionlab.org/map/",
        source_url="https://evictionlab.org/map/",
        required_columns=("county_fips", "year"),
        measure_columns=("eviction_filings", "eviction_rate"),
        measure_aggregations={
            "eviction_filings": "extensive_sum",
            "eviction_rate": "intensive_pop_weighted_mean",
        },
        recommended_align="calendar_year",
        notes=(
            "National county-year Eviction Lab map data cover 2000-2018 and "
            "can be population-weighted to MSA panels for historical mechanism tests."
        ),
    ),
    CENSUS_BPS_SOURCE_ID: CovariateSourceSpec(
        source_id=CENSUS_BPS_SOURCE_ID,
        provider=CENSUS_BPS_PROVIDER,
        product=CENSUS_BPS_PRODUCT,
        topic="housing_supply",
        native_geo="county",
        first_year=1980,
        last_year=None,
        source_page=CENSUS_BPS_SOURCE_PAGE,
        source_url=CENSUS_BPS_SOURCE_PAGE,
        required_columns=("county_fips", "year"),
        measure_columns=CENSUS_BPS_MEASURE_COLUMNS,
        measure_aggregations={
            column: "extensive_sum" for column in CENSUS_BPS_MEASURE_COLUMNS
        },
        recommended_align="calendar_year",
        notes=(
            "Census Building Permits Survey county annual files measure newly "
            "authorized privately owned housing supply. HHP-Lab retains class-level "
            "unit and permit-valuation totals so MSA panels can derive "
            "bps_mix_adjusted_permit_value_per_unit_thousands using fixed base-year "
            "structure-class unit weights."
        ),
    ),
    QCEW_SOURCE_ID: CovariateSourceSpec(
        source_id=QCEW_SOURCE_ID,
        provider=QCEW_PROVIDER,
        product=QCEW_PRODUCT,
        topic="labor_market",
        native_geo="county",
        first_year=QCEW_FIRST_YEAR,
        last_year=None,
        source_page=QCEW_SOURCE_PAGE,
        source_url=QCEW_SOURCE_URL,
        required_columns=("county_fips", "year"),
        measure_columns=QCEW_MEASURE_COLUMNS,
        measure_aggregations={
            "annual_avg_emplvl": "extensive_sum",
            "total_annual_wages": "extensive_sum",
            "annual_avg_estabs": "extensive_sum",
        },
        recommended_align="calendar_year",
        notes=(
            "BLS QCEW county high-level annual files should be filtered to Total Covered "
            "(Own=0) and total all-industries (NAICS=10) rows. MSA wage-per-worker "
            "measures should be derived after summing county employment and wage totals."
        ),
    ),
    "hud_fmr": CovariateSourceSpec(
        source_id="hud_fmr",
        provider="hud",
        product="fair_market_rents",
        topic="rents",
        native_geo="county",
        first_year=2000,
        last_year=None,
        source_page="https://www.huduser.gov/portal/datasets/fmr.html",
        source_url="https://www.huduser.gov/portal/datasets/fmr.html",
        required_columns=("county_fips", "year"),
        measure_columns=("fmr_0br", "fmr_1br", "fmr_2br", "fmr_3br", "fmr_4br"),
        measure_aggregations={
            "fmr_0br": "intensive_pop_weighted_mean",
            "fmr_1br": "intensive_pop_weighted_mean",
            "fmr_2br": "intensive_pop_weighted_mean",
            "fmr_3br": "intensive_pop_weighted_mean",
            "fmr_4br": "intensive_pop_weighted_mean",
        },
        recommended_align="fiscal_year",
        notes=(
            "HUD Fair Market Rents are fiscal-year rent ceilings used by voucher "
            "and homelessness programs."
        ),
    ),
    "hud_psh": CovariateSourceSpec(
        source_id="hud_psh",
        provider="hud",
        product="picture_of_subsidized_households",
        topic="subsidized_housing",
        native_geo="county",
        first_year=2000,
        last_year=None,
        source_page="https://www.huduser.gov/portal/datasets/assthsg.html",
        source_url="https://www.huduser.gov/portal/datasets/assthsg.html",
        required_columns=("county_fips", "year"),
        measure_columns=("subsidized_households", "housing_choice_vouchers"),
        measure_aggregations={
            "subsidized_households": "extensive_sum",
            "housing_choice_vouchers": "extensive_sum",
        },
        recommended_align="calendar_year",
        notes=(
            "Picture of Subsidized Households reports HUD-assisted housing "
            "availability and voucher counts."
        ),
    ),
    "hud_spm": CovariateSourceSpec(
        source_id="hud_spm",
        provider="hud",
        product="system_performance_measures",
        topic="homeless_system_performance",
        native_geo="coc",
        first_year=2015,
        last_year=None,
        source_page="https://www.hudexchange.info/programs/coc/system-performance-measures/",
        source_url="https://www.hudexchange.info/programs/coc/system-performance-measures/",
        required_columns=("coc_number", "year"),
        measure_columns=(
            "spm_first_time_homeless",
            "spm_returns_to_homelessness",
            "spm_successful_exits",
        ),
        measure_aggregations={
            "spm_first_time_homeless": "extensive_sum",
            "spm_returns_to_homelessness": "extensive_sum",
            "spm_successful_exits": "extensive_sum",
        },
        recommended_align="fiscal_year",
        notes=(
            "HUD System Performance Measures are CoC-native annual HMIS system "
            "performance metrics and should not be treated as independent of PIT."
        ),
    ),
    "kff_medicaid_expansion": CovariateSourceSpec(
        source_id="kff_medicaid_expansion",
        provider="kff",
        product="medicaid_expansion_status",
        topic="state_policy",
        native_geo="state",
        first_year=2014,
        last_year=None,
        source_page="https://www.kff.org/medicaid/status-of-state-medicaid-expansion-decisions/",
        source_url="https://www.kff.org/medicaid/status-of-state-medicaid-expansion-decisions/",
        required_columns=("state", "year"),
        measure_columns=("medicaid_expansion_adopted",),
        measure_aggregations={
            "medicaid_expansion_adopted": "intensive_pop_weighted_mean",
        },
        recommended_align="effective_year",
        notes=(
            "State policy indicator for ACA Medicaid expansion adoption; agents "
            "must avoid assigning sub-state variation that is not in the source."
        ),
    ),
    "prism_tmin_january": CovariateSourceSpec(
        source_id="prism_tmin_january",
        provider="prism",
        product="temperature",
        topic="climate",
        native_geo="county",
        first_year=1895,
        last_year=None,
        source_page="https://prism.oregonstate.edu/",
        source_url=(
            "https://data.prism.oregonstate.edu/time_series/us/an/4km/"
            "tmin/monthly/"
        ),
        required_columns=("county_fips", "year"),
        measure_columns=(
            "tmin_c",
            "tmin_below_freezing",
            "tmin_code_blue_band",
            "tmin_above_code_blue",
        ),
        measure_aggregations={
            "tmin_c": "intensive_pop_weighted_mean",
            "tmin_below_freezing": "intensive_pop_weighted_mean",
            "tmin_code_blue_band": "intensive_pop_weighted_mean",
            "tmin_above_code_blue": "intensive_pop_weighted_mean",
        },
        recommended_align="point_in_time_jan",
        notes=(
            "January county PRISM minimum temperature in Celsius, with policy-threshold "
            "hinge basis columns at freezing and common 40F Code Blue activation."
        ),
    ),
    CDC_OVERDOSE_SOURCE_ID: CovariateSourceSpec(
        source_id=CDC_OVERDOSE_SOURCE_ID,
        provider=CDC_OVERDOSE_PROVIDER,
        product=CDC_OVERDOSE_PRODUCT,
        topic="substance_use",
        native_geo="county",
        first_year=CDC_OVERDOSE_FIRST_YEAR,
        last_year=None,
        source_page=CDC_OVERDOSE_SOURCE_PAGE,
        source_url=CDC_OVERDOSE_SOURCE_URL,
        required_columns=CDC_OVERDOSE_REQUIRED_COLUMNS,
        measure_columns=CDC_OVERDOSE_MEASURE_COLUMNS,
        measure_aggregations={
            "overdose_deaths_12mo": "extensive_sum",
        },
        recommended_align="point_in_time_jan_trailing_12_months",
        notes=(
            "CDC NCHS VSRR county-residence provisional drug overdose death "
            "counts for 12-month-ending periods. HHP-Lab curated artifacts use "
            "January rows for PIT alignment; recent periods are provisional and "
            "subject to reporting lag and suppression."
        ),
    ),
    VERA_JAIL_SOURCE_ID: CovariateSourceSpec(
        source_id=VERA_JAIL_SOURCE_ID,
        provider=VERA_PROVIDER,
        product=VERA_JAIL_PRODUCT,
        topic="criminal_legal_system",
        native_geo="county",
        first_year=VERA_JAIL_RELIABLE_FIRST_YEAR,
        last_year=VERA_JAIL_RELIABLE_LAST_YEAR,
        source_page=VERA_SOURCE_PAGE,
        source_url=VERA_SOURCE_URL,
        required_columns=VERA_REQUIRED_CURATED_COLUMNS,
        measure_columns=VERA_JAIL_MEASURE_COLUMNS,
        measure_aggregations={
            column: "extensive_sum" for column in VERA_JAIL_MEASURE_COLUMNS
        },
        recommended_align="calendar_year",
        notes=(
            "Vera county jail measures from Incarceration Trends. The raw file "
            "contains earlier census/sample years and 2024-2026 rows, but "
            "1999-2023 is the dependable annual county coverage window; "
            "Connecticut county jail coverage is structurally absent because "
            "jails are state-run."
        ),
    ),
    VERA_PRISON_SOURCE_ID: CovariateSourceSpec(
        source_id=VERA_PRISON_SOURCE_ID,
        provider=VERA_PROVIDER,
        product=VERA_PRISON_PRODUCT,
        topic="criminal_legal_system",
        native_geo="county",
        first_year=VERA_PRISON_RELIABLE_FIRST_YEAR,
        last_year=VERA_PRISON_LAST_YEAR,
        source_page=VERA_SOURCE_PAGE,
        source_url=VERA_SOURCE_URL,
        required_columns=VERA_REQUIRED_CURATED_COLUMNS,
        measure_columns=VERA_PRISON_MEASURE_COLUMNS,
        measure_aggregations={
            column: "extensive_sum" for column in VERA_PRISON_MEASURE_COLUMNS
        },
        recommended_align="calendar_year",
        notes=(
            "Vera county prison measures from Incarceration Trends. Prison "
            "coverage is present through 2019; pre-1984 rows are sparse enough "
            "that the catalog advertises 1984-2019 as the usable coverage window."
        ),
    ),
    MPI_SOURCE_ID: CovariateSourceSpec(
        source_id=MPI_SOURCE_ID,
        provider=MPI_PROVIDER,
        product=MPI_PRODUCT,
        topic="immigration_status",
        native_geo="county",
        first_year=2023,
        last_year=2023,
        source_page=MPI_SOURCE_PAGE,
        source_url=MPI_SOURCE_URL,
        required_columns=MPI_REQUIRED_CURATED_COLUMNS,
        measure_columns=MPI_MEASURE_COLUMNS,
        measure_aggregations={
            "unauthorized_immigrant_population": "extensive_sum",
            "unauthorized_immigrant_share_of_us_total": "extensive_sum",
        },
        recommended_align="point_in_time_mid_year",
        notes=(
            "Migration Policy Institute mid-2023 estimates for unauthorized "
            f"immigrant populations. {MPI_METHODOLOGY_NOTE}"
        ),
    ),
    IRS_SOI_SOURCE_ID: CovariateSourceSpec(
        source_id=IRS_SOI_SOURCE_ID,
        provider=IRS_SOI_PROVIDER,
        product=IRS_SOI_PRODUCT,
        topic="migration",
        native_geo="county",
        first_year=IRS_SOI_FIRST_YEAR,
        last_year=None,
        source_page=IRS_SOI_SOURCE_PAGE,
        source_url=IRS_SOI_SOURCE_URL,
        required_columns=("county_fips", "year"),
        measure_columns=IRS_SOI_COUNTY_MEASURE_COLUMNS,
        measure_aggregations={
            column: "extensive_sum" for column in IRS_SOI_COUNTY_MEASURE_COLUMNS
        },
        recommended_align="filing_year_later_year",
        notes=(
            "IRS Statistics of Income county-to-county migration flows. Curated "
            "county-year measures use the later filing year, keep gross inflows "
            "and outflows separate, and retain suppressed/other-flow remainder "
            "columns for coverage diagnostics."
        ),
    ),
    SAIZ_SOURCE_ID: CovariateSourceSpec(
        source_id=SAIZ_SOURCE_ID,
        provider=SAIZ_PROVIDER,
        product=SAIZ_PRODUCT,
        topic="housing_supply",
        native_geo="msa",
        first_year=SAIZ_ESTIMATE_YEAR,
        last_year=SAIZ_ESTIMATE_YEAR,
        source_page=SAIZ_SOURCE_PAGE,
        source_url=SAIZ_SOURCE_URL,
        required_columns=SAIZ_REQUIRED_CURATED_COLUMNS,
        measure_columns=SAIZ_MEASURE_COLUMNS,
        measure_aggregations={
            column: "intensive_pop_weighted_mean" for column in SAIZ_MEASURE_COLUMNS
        },
        recommended_align="static_cross_section",
        notes=(
            "Saiz (2010) metro housing supply elasticity and land-unavailability "
            "measures, matched from 1999 MSA/NECMA names to current MSA definitions. "
            "Use as a descriptive static MSA covariate, not as a strong 2015-2025 "
            "rent-growth instrument without first-stage validation."
        ),
    ),
}


def covariate_source_spec(source_id: str) -> CovariateSourceSpec:
    """Return a supported covariate source spec."""
    try:
        return COVARIATE_SOURCE_SPECS[source_id]
    except KeyError as exc:
        supported = ", ".join(sorted(COVARIATE_SOURCE_SPECS))
        raise KeyError(
            f"Unsupported covariate source '{source_id}'. Supported: {supported}"
        ) from exc
