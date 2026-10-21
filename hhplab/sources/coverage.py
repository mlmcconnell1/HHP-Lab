"""Machine-readable temporal coverage for core panel sources."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from hhplab.schema.redistribution import (
    PUBLIC_DATA_TERMS,
    UNKNOWN_TERMS,
    RedistributionEvidence,
    SourceRedistribution,
)

CENSUS_DATA_EVIDENCE = RedistributionEvidence(
    "https://www2.census.gov/foia/ds_policies/ds027.pdf",
    "2026-10-20",
    "Public Census-produced statistical data only; "
    "excludes third-party products and restricted microdata.",
)
HUD_COUNT_EVIDENCE = RedistributionEvidence(
    "https://www.hud.gov/sites/documents/webpubstandards.pdf",
    "2026-10-20",
    "HUD-published PIT/HIC statistical workbooks on HUD User AHAR pages; "
    "section 1.D public-domain policy; excludes separately copyrighted material.",
)
HUD_COUNT_PRODUCT_EVIDENCE = RedistributionEvidence(
    "https://www.huduser.gov/portal/datasets/ahar/"
    "2024-ahar-part-1-pit-estimates-of-homelessness-in-the-us.html",
    "2026-10-20",
    "Official 2007-2024 PIT and HIC counts by CoC/state, linked as HUD report data.",
)


@dataclass(frozen=True)
class SourceCoverageSpec:
    """Core source temporal coverage and native geometry metadata."""

    source_id: str
    provider: str
    product: str
    native_geo: str
    first_year: int
    last_year: int | None
    notes: str
    redistribution: SourceRedistribution = UNKNOWN_TERMS
    redistribution_evidence: tuple[RedistributionEvidence, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Serialize to a JSON-ready dictionary."""
        return asdict(self)


CORE_SOURCE_COVERAGE_SPECS: dict[str, SourceCoverageSpec] = {
    "pit": SourceCoverageSpec(
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(HUD_COUNT_EVIDENCE, HUD_COUNT_PRODUCT_EVIDENCE),
        source_id="pit",
        provider="hud",
        product="pit",
        native_geo="coc",
        first_year=2007,
        last_year=None,
        notes="Annual January point-in-time count. HUD workbooks are cumulative by vintage.",
    ),
    "hic": SourceCoverageSpec(
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(HUD_COUNT_EVIDENCE, HUD_COUNT_PRODUCT_EVIDENCE),
        source_id="hic",
        provider="hud",
        product="hic",
        native_geo="coc",
        first_year=2007,
        last_year=None,
        notes="Annual January housing inventory count aligned to PIT years.",
    ),
    "acs5": SourceCoverageSpec(
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(CENSUS_DATA_EVIDENCE,),
        source_id="acs5",
        provider="census",
        product="acs5",
        native_geo="tract",
        first_year=2009,
        last_year=None,
        notes=(
            "ACS 5-year estimates; vintage is the end year. Panels conventionally "
            "use ACS vintage Y-1 for PIT year Y."
        ),
    ),
    "acs1": SourceCoverageSpec(
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(CENSUS_DATA_EVIDENCE,),
        source_id="acs1",
        provider="census",
        product="acs1",
        native_geo="metro_county",
        first_year=2005,
        last_year=None,
        notes=(
            "ACS 1-year estimates at native metro and county geographies. Specific "
            "table availability varies by vintage; inspect hhplab list acs-variables."
        ),
    ),
    "pep": SourceCoverageSpec(
        redistribution=PUBLIC_DATA_TERMS,
        redistribution_evidence=(CENSUS_DATA_EVIDENCE,),
        source_id="pep",
        provider="census",
        product="pep",
        native_geo="county",
        first_year=2010,
        last_year=None,
        notes=(
            "County postcensal estimates. Vintage 2020 covers 2010-2020; current "
            "postcensal vintages cover 2020 onward."
        ),
    ),
    "zori": SourceCoverageSpec(
        source_id="zori",
        provider="zillow",
        product="zori",
        native_geo="county",
        first_year=2015,
        last_year=None,
        notes="ZORI All Homes county monthly series begins in January 2015.",
    ),
}


def list_core_source_coverage() -> list[SourceCoverageSpec]:
    """Return core source coverage specs in stable source_id order."""
    return [
        CORE_SOURCE_COVERAGE_SPECS[source_id] for source_id in sorted(CORE_SOURCE_COVERAGE_SPECS)
    ]
