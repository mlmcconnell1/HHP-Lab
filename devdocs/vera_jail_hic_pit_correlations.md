# Vera Jail Population vs HIC Beds and PIT Counts: Contemporaneous Correlations

Date: 2026-07-07

## Question

Using Vera Institute county jail population (bead
`coclab-vera-incarceration-catalog-registration-tqwe8`), screen same-year
cross-sectional correlations against (a) HIC bed capacity by category and
(b) PIT homelessness counts, across the pooled top-150 MSA cohort.

Same design caveat as the earlier overdose/HIC correlation work: these are
simple same-year Pearson correlations across MSAs -- **no fixed effects, no
lag, no rent or population control except where noted** -- so they answer
"do MSAs with more jail population per capita also have more/less of X per
capita, in the same year," not a causal or timing claim.

## Data and Construction

- PIT/HIC: same pooled top-50 + rank-51-150 base (150 MSAs, no overlap) and
  HIC-by-category rollup approach as
  `devdocs/overdose_homelessness_lag_screen.md`, extended back to 2015-2019
  (boundary vintage 2020, county vintage 2023) alongside the existing
  2020/2022/2023 rollups (2024-2025 excluded here, see below).
- Vera: `data/curated/vera/vera_incarceration_county__Y1970-2026@C2020.parquet`,
  `total_jail_pop`, rolled up to MSA by hand (not yet a registered covariate
  source) via `hhplab.msa.read_msa_county_membership`, simple sum (additive
  stock measure), population-weighted coverage tracked via
  `vera_county_count`/`vera_county_expected`. Unlike CDC overdose's
  county-count-weighted ratio (a known gap, bead
  `coclab-cdc-overdose-coverage-population-weight-wrwfx`), this rollup is
  population-aware from the start via the panel's own PEP population.
- Window: **2015-2020 + 2022-2023** (8 years, 2021 excluded for the usual
  COVID-PIT reason). Restricted to Vera's actually-reliable jail-coverage
  window (1999-2023, verified same day in
  `coclab-vera-incarceration-catalog-registration-tqwe8`); 2024-2025 are
  excluded here because Vera's own coverage tapers sharply in those years,
  not because PIT/HIC lack data.
- **Connecticut is entirely absent from Vera's county file** (CT abolished
  county government in 1960 and runs jails at the state level, not a
  project bug) -- Bridgeport-Stamford-Danbury, Hartford-West Hartford-East
  Hartford, New Haven, and Waterbury-Shelton, CT all have zero Vera coverage
  in this panel.
- Build scripts: `scripts/build_vera_hic_pit_panel.py`,
  `scripts/vera_hic_pit_correlations.py` (tracked, reproducible). Panel:
  1,200 rows (150 MSAs x 8 years); 142-144 of 150 MSAs have non-null jail
  data per year (CT MSAs plus a handful of others with partial county gaps).

## Results: Jail vs HIC Bed Categories

| Category | Pooled log-log r | p | Partial r (\|log_pop) | p | n |
| --- | ---: | ---: | ---: | ---: | ---: |
| Permanent Supportive Housing | **-0.166** | <0.0001 | -0.099 | 0.0008 | 1149 |
| All HIC beds | -0.149 | <0.0001 | -0.092 | 0.0018 | 1149 |
| Other Permanent Housing | -0.145 | <0.0001 | -0.114 | 0.0009 | 837 |
| Rapid Re-Housing | -0.150 | <0.0001 | -0.101 | 0.0007 | 1139 |
| Emergency Shelter | -0.144 | <0.0001 | -0.101 | 0.0006 | 1149 |
| Transitional Housing | +0.009 | 0.752 | +0.049 | 0.095 | 1147 |
| Safe Haven | -0.026 | 0.531 | +0.010 | 0.800 | 595 |

By year (log-log r; `*` = p<0.05):

| Category | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2022 | 2023 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Permanent Supportive Housing | -0.11 | -0.10 | -0.07 | -0.08 | -0.17* | -0.27* | -0.27* | -0.25* |
| All HIC beds | -0.07 | -0.06 | -0.05 | -0.06 | -0.15 | -0.26* | -0.24* | -0.24* |
| Emergency Shelter | -0.04 | -0.02 | -0.02 | -0.04 | -0.14 | -0.28* | -0.24* | -0.26* |
| Other Permanent Housing | -0.13 | -0.14 | -0.06 | -0.00 | -0.04 | -0.11 | -0.20* | -0.19* |
| Rapid Re-Housing | -0.04 | -0.12 | -0.13 | -0.11 | -0.16 | -0.13 | -0.09 | -0.14 |
| Transitional Housing | +0.01 | +0.04 | +0.08 | +0.05 | -0.05 | -0.12 | -0.15 | -0.10 |
| Safe Haven | -0.07 | -0.06 | +0.02 | -0.01 | -0.04 | +0.03 | +0.18 | +0.20 |

**Nearly every HIC category is negatively correlated with jail population,
and the relationship strengthens sharply starting 2019-2020**: near-zero
and non-significant 2015-2018, then significant and roughly doubling in
magnitude by 2020-2023 for PSH, emergency shelter, and total beds. Metros
with more jail population per capita have systematically *less* housing/
shelter infrastructure per capita, and that gap widened through the COVID
era. Note the sign flip from the earlier overdose screen: PSH beds
correlated *positively* with overdose deaths there, but *negatively* with
jail population here -- these are two different underlying axes, not the
same story twice. Transitional Housing and Safe Haven show no relationship
in either direction.

## Results: Jail vs PIT Homelessness Margins

| Margin | Pooled log-log r | p | Partial r (\|log_pop) | p | n |
| --- | ---: | ---: | ---: | ---: | ---: |
| Unsheltered PIT | +0.113 | 0.0001 | +0.142 | <0.0001 | 1143 |
| Total PIT | -0.133 | <0.0001 | -0.100 | 0.0007 | 1144 |
| Sheltered PIT | **-0.243** | <0.0001 | -0.211 | <0.0001 | 1144 |

By year:

| Margin | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2022 | 2023 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Unsheltered PIT | +0.16 | +0.17* | +0.14 | +0.07 | +0.13 | +0.22* | +0.16 | +0.17* |
| Total PIT | -0.11 | -0.08 | -0.10 | -0.16* | -0.19* | -0.10 | -0.16 | -0.18* |
| Sheltered PIT | -0.27* | -0.23* | -0.22* | -0.21* | -0.30* | -0.30* | -0.31* | -0.34* |

**The unsheltered/sheltered split is opposite-signed and this lines up
cleanly with the project's established finding that sheltered PIT counts
measure shelter-capacity policy, not exposure** (log beds ~ log sheltered
PIT, R^2=0.95, see the main longitudinal record). Sheltered homelessness is
consistently and increasingly negatively correlated with jail population
(-0.21 to -0.34, significant in every single year) -- metros that build
less shelter/housing capacity incarcerate more people, mirroring the HIC-beds
result above (sheltered counts ARE a beds-capacity proxy). Unsheltered
homelessness runs the other way, positive in every year and significant in
3 of 8 -- metros with more people literally on the street per capita also
have more jail population per capita, consistent with either a shared
poverty/instability driver or a criminalization-of-homelessness cycle
(unsheltered people cycling through jail) -- this design can't distinguish
those. Total PIT, being sheltered-dominated in most metros, inherits the
negative sign.

## Interpretation

Two distinct, opposite-signed relationships, both consistent with a
"punitive vs. service-investment" policy-orientation story, though this
screen cannot establish that as causal:

1. **Housing/shelter infrastructure (HIC beds, sheltered PIT) is negatively
   related to jail population**, and that gap opened up specifically around
   2019-2020 -- worth checking against the existing sanctuary-status finding
   (sanctuary metros added ~5%/yr more beds after 2015, per the main
   longitudinal record) as a candidate explanation for *why* this axis
   opened up when it did, not done here.
2. **Unsheltered street homelessness is positively related to jail
   population** -- the opposite direction from every housing-capacity
   measure -- consistent with the project's standing rule to never pool
   sheltered and unsheltered as a single "homelessness" measure; here they
   don't just differ in magnitude, they differ in *sign* against a third
   variable.

As with every correlation in this doc and the overdose screen before it:
no fixed effects, no lag, no causal identification. The natural next step,
if useful, is the entity+year-FE lag design already built for overdose --
does *growth* in jail population predict *subsequent* change in HIC beds or
PIT margins within the same metro -- not done here.

## Caveats

- Connecticut structurally absent from Vera (see above) -- not a data
  quality issue to chase, a genuine source limitation.
- Vera's own reliable jail-coverage window is 1999-2023; this panel starts
  at 2015 only because rank-51-150's PIT base panel does, not because Vera
  lacks earlier years. Extending PIT/HIC back to 1999-2014 (top-50 only,
  since rank-51-150 doesn't reach that far) would give an 11-year top-50-only
  panel if useful later.
- Vera is not yet a registered covariate source (see
  `coclab-vera-incarceration-catalog-registration-tqwe8`); this rollup is
  bespoke to this script, not the shared `aggregate covariate` path.
- `total_jail_pop` is a stock/population count, not an admissions or
  turnover rate; it says nothing about how many *distinct* people cycle
  through jail in a year, which may matter more for a
  criminalization-of-homelessness mechanism than the standing population
  count used here.

## Artifacts

`outputs/vera_hic_pit/` (gitignored): `vera_hic_pit_levels.parquet`,
`vera_hic_correlations_{pooled,by_year}.csv`,
`vera_pit_correlations_{pooled,by_year}.csv`. Reuses
`outputs/overdose_lag/hic_by_category/` rollups (extended to 2015-2019 by
this work). Build scripts: `scripts/build_vera_hic_pit_panel.py`,
`scripts/vera_hic_pit_correlations.py`.
