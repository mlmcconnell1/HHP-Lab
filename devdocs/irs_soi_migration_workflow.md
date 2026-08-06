# IRS SOI Migration Workflow

HHP-Lab supports IRS Statistics of Income (SOI) county-to-county migration
flows as a county-native covariate source with an MSA aggregation path that
uses county-pair flows.

IRS publishes annual migration data from year-to-year address changes reported
on individual income tax returns. HHP-Lab supports the post-2011 SOI series
because IRS marks 2011-2012 as the start of an improved methodology.

## Source Files

Stage county-to-county CSV files under:

```text
data/raw/irs_soi/
```

The ingest expects matched file-year pairs:

```text
data/raw/irs_soi/countyinflow2122.csv
data/raw/irs_soi/countyoutflow2122.csv
```

The four-digit suffix is the IRS flow pair. For example,
`countyinflow2122.csv` is the 2021-to-2022 county inflow file. HHP-Lab labels
that flow as panel year `2022`, the later filing year.

Required raw columns are:

| Column | Meaning |
|--------|---------|
| `y2_statefips`, `y2_countyfips` | Destination state/county in the later filing year. |
| `y1_statefips`, `y1_countyfips` | Origin state/county in the earlier filing year. |
| `n1` | Returns, approximately households. |
| `n2` | Exemptions, approximately people. |
| `agi` | Aggregate adjusted gross income, in thousands of dollars. |

IRS summary and special rows are not pair flows. HHP-Lab excludes state-total
rows, pseudo-state summary rows, and same-county non-migrant rows from the
pair table. It records skipped-row counts and summary-row details in parquet
provenance. The `57`, `58`, and `59` pseudo-state rows feed the
`other_flows_*` suppressed-remainder county columns.

## Curated Contracts

County-year curated output. The year token is derived from the staged file
years actually ingested; for the current staged post-2011 series:

```text
data/curated/covariates/covariate__irs_soi_migration__Y2012-2023.parquet
```

| Column group | Columns |
|--------------|---------|
| Keys | `geo_type`, `geo_id`, `county_fips`, `year` |
| Gross inflow | `inflow_returns`, `inflow_exemptions`, `inflow_agi_thousands` |
| Gross outflow | `outflow_returns`, `outflow_exemptions`, `outflow_agi_thousands` |
| Suppressed inflow remainder | `other_flows_inflow_returns`, `other_flows_inflow_exemptions`, `other_flows_inflow_agi_thousands` |
| Suppressed outflow remainder | `other_flows_outflow_returns`, `other_flows_outflow_exemptions`, `other_flows_outflow_agi_thousands` |

Pair-level companion output:

```text
data/curated/covariates/covariate_pairs__irs_soi_migration__Y2012-2023.parquet
```

| Column | Meaning |
|--------|---------|
| `year` | Later filing year. |
| `origin_county_fips` | Origin county in year 1. |
| `destination_county_fips` | Destination county in year 2. |
| `migration_returns` | Migrating returns. |
| `migration_exemptions` | Migrating exemptions. |
| `migration_agi_thousands` | Aggregate AGI of movers, in thousands. |

The county-year output is useful for county-native descriptive work. The
pair-level output is required for MSA aggregation because intra-MSA county
moves must not be counted as MSA in-migration or out-migration.

MSA panel output:

```text
data/curated/covariates/covariate_panel__irs_soi_migration__Y2012-2023.parquet
```

| Column group | Columns |
|--------------|---------|
| Keys | `geo_type`, `geo_id`, `msa_id`, `year`, `definition_version` |
| External inflow | `inflow_returns`, `inflow_exemptions`, `inflow_agi_thousands` |
| External outflow | `outflow_returns`, `outflow_exemptions`, `outflow_agi_thousands` |
| Net flow | `net_returns`, `net_exemptions`, `net_agi_thousands` |
| Internal churn | `intra_msa_returns`, `intra_msa_exemptions`, `intra_msa_agi_thousands` |
| Suppressed unallocated flow | `suppressed_unallocated_inflow_*`, `suppressed_unallocated_outflow_*`, `suppressed_unallocated_returns` |
| Diagnostics | `membership_county_count`, `coverage_ratio` |

## Commands

List supported covariate sources:

```bash
hhplab list covariates --json
```

Ingest all staged IRS SOI county migration CSV pairs:

```bash
hhplab ingest covariate \
  --source irs_soi_migration \
  --raw-path data/raw/irs_soi \
  --json
```

The JSON payload reports the county output path, pair output path, row counts,
and skipped special-row counts. Use `--force` to rebuild existing curated
artifacts.

Aggregate to Census MSA panel-ready rows:

```bash
hhplab aggregate covariate \
  --source irs_soi_migration \
  --target-geo msa \
  --years 2022-2023 \
  --msa-definition-version census_msa_2023 \
  --json
```

MSA aggregation uses the pair-level artifact recorded in the county parquet
provenance. If that companion parquet is missing, the aggregator looks for a
pair artifact with the same data-derived year token as the county artifact. If
neither exists, re-run ingest with `--force`.

The MSA semantics are:

- MSA inflow: destination county is in the MSA and origin county is outside
  that MSA.
- MSA outflow: origin county is in the MSA and destination county is outside
  that MSA.
- Intra-MSA churn: both counties are in the same MSA. These moves are emitted
  as `intra_msa_*` and excluded from external inflow/outflow.
- Suppressed remainders: county-level `other_flows_*` buckets are summed to
  `suppressed_unallocated_*` columns and reduce `coverage_ratio`.

By default, low `coverage_ratio` MSA rows are kept and reported as JSON
warnings. Use `--min-coverage-ratio` to set the warning threshold and
`--drop-below-min-coverage` to filter rows below that threshold.

Join the MSA migration covariates to a panel:

```bash
hhplab panel enrich \
  --panel outputs/top50_msa_longitudinal_2010_2025.parquet \
  --source data/curated/covariates/covariate_panel__irs_soi_migration__Y2012-2023.parquet \
  --columns inflow_returns,outflow_returns,net_returns,intra_msa_returns,coverage_ratio \
  --panel-geo-column msa_id \
  --source-geo-column msa_id \
  --include-year \
  --output outputs/top50_msa_with_irs_soi_migration.parquet \
  --json
```

Run a screening correlation:

```bash
hhplab analyze correlate \
  --panel outputs/top50_msa_with_irs_soi_migration.parquet \
  --columns log_zori,inflow_returns,outflow_returns,intra_msa_returns,pit_unsheltered \
  --output outputs/top50_msa_irs_soi_migration__analysis_correlate.parquet \
  --json
```

Run a screening regression:

```bash
hhplab analyze regress \
  --panel outputs/top50_msa_with_irs_soi_migration.parquet \
  --outcome pit_unsheltered \
  --predictors log_zori,outflow_returns,intra_msa_returns \
  --output outputs/top50_msa_irs_soi_migration__analysis_regress.parquet \
  --json
```

## Year Alignment

Curated IRS SOI migration rows use the later filing year. A 2021-to-2022 flow is
stored as `year = 2022`.

For descriptive migration panels, join IRS SOI rows on the same `year`. For
PIT-aligned homelessness outcome models, treat an IRS flow labeled year `Y` as
a preceding exposure for PIT year `Y+1`. For example, use 2021-to-2022 IRS SOI
flows (`year = 2022`) as a candidate predictor for the January 2023 PIT count.
This mirrors the project convention of using lagged context measures for PIT
outcomes when the source is released or observed after the relevant exposure
period.

## Caveats

Non-filers are invisible. The lowest-income households, including many people
at the highest risk of homelessness, are undercounted. IRS SOI flows are lower
bounds on vulnerable-population churn, not full mobility counts.

IRS suppresses county pairs below roughly 20 returns into `other flows`
summary buckets. Always inspect MSA `coverage_ratio`, JSON warnings, and
`suppressed_unallocated_*` columns before interpreting cross-MSA differences.
When an MSA-year has no known external flow and no suppressed external-flow
denominator, `coverage_ratio` is missing rather than 1.0 because the aggregate
cannot distinguish true zero migration from an absent flow record.

Do not mix pre-2011 and post-2011 IRS SOI migration data in change analyses.
HHP-Lab starts this source at 2011 because IRS identifies 2011-2012 as a new
series with methodology improvements.

Address changes approximate household moves, but timing can slip. Late filers
and filing-season behavior mean a flow-year pair is not a precise calendar-year
move interval.

Intra-MSA county moves are internal churn. They are excluded from external MSA
inflow/outflow and emitted separately as `intra_msa_*`. This is intentional:
counting them as external migration would overstate gross MSA movement in
multi-county metros.

Connecticut changed from legacy county FIPS to planning-region county
equivalents in the IRS SOI window. For pre-2022 CT rows, HHP-Lab crosswalks
legacy counties to planning regions before MSA aggregation using the
area-share bridge in `hhplab/geographies/coc/ct_planning_regions.py`. This preserves
external MSA inflow/outflow coverage for CT-inclusive MSAs, but the
`intra_msa_*` series has an era break at 2022 because old counties and larger
planning regions observe different within-metro moves. Do not use CT
intra-MSA churn in change designs that cross the 2021-to-2022 transition.

AGI is aggregate thousands of dollars. AGI per return can be derived, but it is
composition-sensitive and should not be read as a stable wage or income measure
for all movers.

Exploratory correlations and regressions involving IRS SOI migration should be
treated as screening evidence. They are not causal estimates and inherit the
usual small-sample, measurement-error, and confounding limits.
