"""Catalog of expanded hidden-cause covariate sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

from hhplab.covariates.mpi_contract import (
    MPI_MEASURE_COLUMNS,
    MPI_METHODOLOGY_NOTE,
    MPI_PRODUCT,
    MPI_PROVIDER,
    MPI_REQUIRED_CURATED_COLUMNS,
    MPI_SOURCE_ID,
    MPI_SOURCE_PAGE,
    MPI_SOURCE_URL,
)

GeoType = Literal["county", "coc", "state"]


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
        recommended_align="calendar_year",
        notes=(
            "County-year eviction filings/rates are useful housing-instability "
            "covariates; verify local coverage before causal interpretation."
        ),
    ),
    "census_bps": CovariateSourceSpec(
        source_id="census_bps",
        provider="census",
        product="building_permits_survey",
        topic="housing_supply",
        native_geo="county",
        first_year=1980,
        last_year=None,
        source_page="https://www.census.gov/permits",
        source_url="https://www.census.gov/permits",
        required_columns=("county_fips", "year"),
        measure_columns=("permitted_units", "permitted_buildings"),
        recommended_align="calendar_year",
        notes=(
            "Census Building Permits Survey county annual files measure newly "
            "authorized privately owned housing supply."
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
        recommended_align="point_in_time_jan",
        notes=(
            "January county PRISM minimum temperature in Celsius, with policy-threshold "
            "hinge basis columns at freezing and common 40F Code Blue activation."
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
        recommended_align="point_in_time_mid_year",
        notes=(
            "Migration Policy Institute mid-2023 estimates for unauthorized "
            f"immigrant populations. {MPI_METHODOLOGY_NOTE}"
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
