# Landlord Concentration And Institutional Ownership Channel Assessment

Date: 2026-07-09

This note resolves bead `coclab-mzpm7.10`: whether any credible public or
licensable panel source can measure institutional SFR ownership, landlord
concentration, or algorithmic rent-setting exposure at usable geography and
time resolution for the rent-growth roadmap.

## Decision

There is no ready public source that currently supports a reproducible annual
MSA panel for this channel.

The viable path is a licensable property-record dataset with ownership history
and parcel or property geography. For HHP-Lab's purposes, that means
parcel-/property-native ingest followed by county and then canonical MSA
aggregation, not a direct MSA download.

Algorithmic rent-setting exposure is weaker still. As of 2026-07-09, there is
no credible public national panel, and no clearly documented standard licensable
feed, that reports property- or market-level adoption of pricing software in a
form HHP-Lab could treat as a stable annual covariate.

## Public Sources Screened

### Rental Housing Finance Survey

The Census/HUD Rental Housing Finance Survey is useful for descriptive national
ownership structure questions, but not for this repository's MSA-year panel
needs.

- It measures ownership and management characteristics of rental properties.
- Public-use files exist for 2012, 2015, 2018, 2021, and 2024, so the public
  release cadence is triennial rather than annual.
- The 2024 codebook shows the geography needed for a metropolitan panel is
  internal-use only: state, city, region, and tract identifiers are all marked
  `IUF Only`, as are owner city and owner state fields.

Inference: RHFS public files can support national or heavily coarsened
tabulations, but not a reproducible canonical MSA panel for HHP-Lab.

### NMHC Top Owners / Top Managers

The National Multifamily Housing Council publishes annual top-owner and
top-manager rankings, but they are not a market-universe panel.

- The lists cover only the largest firms, not all landlords in a market.
- The methodology is survey-based.
- The ownership definition is multifamily-only and excludes the SFR side of the
  institutional ownership question that is especially relevant here.

These lists are useful context, but not a covariate source.

### ZTRAX

ZTRAX is the strongest non-commercial research alternative.

- Zillow states that ZTRAX is available to qualified academic, nonprofit, and
  government researchers through ICPSR.
- Zillow also states that it includes deed transfers, mortgages, foreclosures,
  tax delinquencies, and parcel/property attributes across thousands of
  counties, with updates approximately twice per year.

This makes ZTRAX a plausible research input for a custom ownership panel, but
it is still not an off-the-shelf MSA-year institutional ownership series. It
would require the same owner-entity resolution and annual stock construction
work as a commercial deed/tax feed, while remaining access-restricted rather
than public in the usual HHP-Lab sense.

## Credible Licensable Paths

### General Deed / Assessor / Parcel Vendors

Several current commercial products appear capable of supporting a custom
ownership-concentration panel:

- Cotality advertises nationwide property records spanning 50 years with 99.9%
  market coverage, plus products with current ownership, mailing data, and
  owner-transfer history.
- ATTOM advertises assessor data with current and past ownership across 3,000+
  counties and recorder/deed data across 2,690+ counties, both collatable to
  multiple geography levels.
- LightBox advertises nationwide parcel coverage with ownership names,
  addresses, and transaction histories.
- First American DataTree advertises nationwide assessor/tax and ownership
  products.

These products are the right class of input if HHP-Lab later decides to pay for
this channel.

### Multifamily-Focused Vendor Data

Yardi Matrix appears useful for the multifamily subset of the question.

- It advertises national property-level coverage with full ownership and
  management information plus rent, occupancy, and sales history.

That makes Yardi Matrix potentially attractive for multifamily landlord
concentration. It is less clearly suitable for institutional SFR ownership,
small-landlord exposure, or a unified rental-stock concentration measure.

## Why A Custom Build Is Still Hard

Even with a strong commercial source, HHP-Lab would still need substantial
custom work before this could become a credible covariate:

- owner-name normalization across LLC aliases, SPEs, mergers, and spelling
  variation;
- beneficial-owner rollups where multiple legal entities represent the same
  operator or fund;
- separation of stock from flows, because deeds record transfers while the
  analysis needs annual ownership snapshots;
- rental-stock filtering, especially for distinguishing owner-occupied property,
  SFR rentals, build-to-rent, small multifamily, and large multifamily;
- annual numerator/denominator construction before MSA aggregation;
- defensible concentration definitions such as top-10 share, HHI, corporate
  ownership share, and out-of-state ownership share.

This is feasible, but it is not a low-friction covariate ingest.

## Algorithmic Rent-Setting Exposure

Public evidence confirms that algorithmic pricing is a real multifamily market
phenomenon, but not that there is a usable exposure panel.

- DOJ sued RealPage on 2024-08-23 and amended the case on 2025-01-07 to add
  major landlords.
- DOJ announced additional proposed settlements on 2026-07-06, which confirms
  the enforcement track is current rather than historical.
- RealPage markets AI Revenue Management and Market Analytics as property- and
  market-level multifamily tools, but does not publish a stable client-by-market
  adoption panel.

Inference: algorithmic exposure is currently observable only through partial,
event-driven fragments such as complaints, settlements, press releases, or
individual case studies. That is not enough for a clean annual top-150 MSA
covariate.

The best plausible proxy would be a narrow multifamily-only construct built by
matching a proprietary ownership/management database to named RealPage clients
from litigation or public disclosures. That would be selective, incomplete, and
fragile to legal-event timing, so it should not be treated as a first-choice
channel for this roadmap.

## Recommended HHP-Lab Action

1. Do not add a new public covariate source to `hhplab/covariates/catalog.py`
   for this topic now.
2. Treat this channel as data-access constrained rather than conceptually
   rejected.
3. If HHP-Lab later secures deed/tax or parcel-level vendor access, prefer a
   county- or property-native ingest that constructs annual ownership snapshots
   before canonical MSA rollup.
4. If the project ever pays this fixed cost, prioritize these derived measures:
   `corporate_owner_share`, `top10_owner_share`, `landlord_hhi`,
   `out_of_state_owner_share`, and SFR-specific versions of the same measures.
5. Treat algorithmic rent-setting exposure as currently unmeasurable at the
   required scope unless a vendor can document property-level software adoption
   or litigation releases a systematic property roster.

## References

- Census RHFS overview: <https://www.census.gov/programs-surveys/rhfs/about.html>
- Census RHFS 2024 public-use files:
  <https://www.census.gov/programs-surveys/rhfs/data/puf/2024/microdata.html>
- Census RHFS 2024 codebook PDF:
  <https://www2.census.gov/programs-surveys/rhfs/data/public-use-files/2024/Codebook-Version-1.pdf>
- Zillow ZTRAX overview: <https://www.zillow.com/research/ztrax/>
- Cotality property data overview: <https://www.cotality.com/our-data>
- Cotality property characteristics:
  <https://www.cotality.com/products/property-characteristics>
- Cotality owner transfer:
  <https://www.cotality.com/products/owner-transfer>
- ATTOM ownership data:
  <https://www.attomdata.com/data/property-ownership-data/>
- ATTOM assessor data:
  <https://www.attomdata.com/data/property-data/assessor-data/>
- ATTOM recorder/deed data:
  <https://www.attomdata.com/data/transactions-mortgage-data/recorder-deeds/>
- LightBox parcel data:
  <https://www.lightboxre.com/data/lightbox-parcel-data/>
- First American DataTree tax/assessor data:
  <https://www.datatree.com/taxsource>
- Yardi Matrix overview: <https://www.yardimatrix.com/>
- NMHC 2026 Top Owners: <https://www.nmhc.org/research-insight/the-nmhc-50/top-50-lists/2026-top-owners/>
- NMHC 50 methodology:
  <https://www.nmhc.org/research-insight/the-nmhc-50/nmhc-50-methodology/>
- DOJ RealPage complaint press release:
  <https://www.justice.gov/archives/opa/pr/justice-department-sues-realpage-algorithmic-pricing-scheme-harms-millions-american-renters>
- DOJ 2026 settlement announcement:
  <https://www.justice.gov/opa/pr/justice-department-requires-realpage-end-sharing-competitively-sensitive-information-and>
- RealPage AI Revenue Management:
  <https://www.realpage.com/insights-analytics/revenue-management/>
- RealPage Market Analytics:
  <https://www.realpage.com/insights-analytics/market-analytics/>
