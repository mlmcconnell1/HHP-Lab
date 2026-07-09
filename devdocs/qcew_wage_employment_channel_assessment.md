# BLS QCEW Wage And Employment Channel Assessment

This note resolves bead `coclab-mzpm7.9`: whether BLS QCEW can support a
credible MSA panel for local wage and employment growth as a rent-growth
channel.

## Decision

Yes, QCEW is a viable source for this channel, but HHP-Lab should not treat the
published MSA series as the system of record.

The recommended design is:

1. ingest QCEW as a county-native annual covariate source;
2. aggregate county totals into the project's canonical MSA definitions;
3. derive MSA wage-per-worker measures after aggregation from summed totals.

This keeps the source aligned with HHP-Lab's existing county-to-MSA panel
machinery and avoids the major stability problems in the direct QCEW MSA
product.

## Why The County Path Is Better

Direct QCEW MSA data are good enough for a quick descriptive check, but they
are the wrong long-run contract for this repository.

- QCEW now publishes MSA data only at total covered employment. That is enough
  for the all-industry wage/employment channel, but it eliminates any future
  option to add industry detail from the MSA product.
- BLS explicitly warns that QCEW MSA definitions are not a time-series
  product. Definition switches introduce breaks when QCEW adopts newer CBSA
  delineations.
- BLS prioritizes county disclosure over MSA disclosure, so missing county
  cells can propagate into missing MSA values.
- HHP-Lab already has mature county-native aggregation paths for MSA panels,
  including Connecticut legacy-county to planning-region handling.

The county path avoids all four problems. County annual averages are the stable
local geography in QCEW and fit the repo's established aggregation model.

## Recommended Raw Product

Use QCEW annual county data, restricted to all-industry total-covered rows.

The simplest source layout for this repo is the county high-level annual file,
because it already exposes the county totals needed for screening:

- `Area Type = County`
- `Own = 0` (`Total Covered`)
- `NAICS = 10` (`Total, all industries`)

Required measures from the annual file:

- `Annual Average Employment`
- `Annual Total Wages`
- `Annual Average Establishment Count`

Useful derived measures:

- `annual_avg_weekly_wage = total_annual_wages / (52 * annual_avg_employment)`
- `avg_annual_pay = total_annual_wages / annual_avg_employment`

## Recommended Curated Contract

County-native curated artifact:

```text
data/curated/covariates/covariate__qcew__Y<first>-<last>.parquet
```

Minimal county columns:

- `geo_type`
- `geo_id`
- `county_fips`
- `year`
- `annual_avg_emplvl`
- `total_annual_wages`
- `annual_avg_estabs`

Optional county convenience columns:

- `annual_avg_weekly_wage`
- `avg_annual_pay`

The MSA aggregation should treat only the totals as primitive measures:

- `annual_avg_emplvl`: `extensive_sum`
- `total_annual_wages`: `extensive_sum`
- `annual_avg_estabs`: `extensive_sum`

Then derive MSA wage metrics after the rollup:

- `annual_avg_weekly_wage = total_annual_wages / (52 * annual_avg_emplvl)`
- `avg_annual_pay = total_annual_wages / annual_avg_emplvl`

This is important because the generic county-to-MSA covariate rollup currently
supports sums, rates, and population-weighted means only. QCEW wage measures
should not be population-weighted.

## HHP-Lab Implementation Plan

### Ingest

Add a new county-native covariate source:

- source contract module in `hhplab/covariates/`
- catalog entry in `hhplab/covariates/catalog.py`
- raw-path parser in `hhplab/covariates/ingest.py`
- tests covering:
  - staged annual county files
  - `Own=0` / `NAICS=10` filtering
  - year-token derivation from staged files
  - provenance metadata

The contract should be annual, calendar-year aligned, and county-native.

### Aggregate

Use the existing county-to-MSA covariate path with the canonical MSA
definition version, summing employment, wages, and establishments. Derive wage
metrics only after the MSA totals are materialized.

If it is cleaner, add a small post-aggregation hook for source-specific derived
columns rather than extending the aggregation enum beyond the current
`extensive_sum`, `rate`, and `intensive_pop_weighted_mean`.

### Screen

Add a pooled top-150 MSA workflow parallel to the existing ACS1 labor-market
screen.

Core regressors to test against `d_log_zori`:

- `d_log_qcew_annual_avg_emplvl`
- `d_log_qcew_total_annual_wages_real`
- `d_log_qcew_annual_avg_weekly_wage_real`

Recommended baseline specification:

```text
d_log_zori ~ d_log_pop + qcew_term + year FE
```

Recommended follow-up specifications:

- horse race versus ACS1 employment/labor-force measures;
- real-wage growth versus nominal-wage growth, using the existing CPI-U
  ingest path;
- one-at-a-time screens first, then a combined wage-plus-employment model.

## Alignment

For the rent-growth question, QCEW annual averages should be treated as
calendar-year `Y` exposures and joined to the same analysis year `Y`.

For later homelessness outcome models, a lagged specification is more natural:
use QCEW year `Y` as a candidate predictor for PIT year `Y+1`.

## Caveats

- QCEW covers wage-and-salary employment reported through UI systems, not the
  full resident labor market. Self-employed workers and some other groups are
  outside coverage.
- QCEW is place-of-work, not place-of-residence. That is acceptable for a
  local labor-demand / local wage-bidding channel, but it is not an individual
  income measure.
- Direct QCEW MSA series should be avoided as the canonical panel source
  because CBSA definition changes create time-series breaks.

## Recommendation

QCEW is worth pursuing as a Tier 3 follow-up, but the right implementation is a
county-native covariate plus MSA rollup, not a direct MSA ingest.
