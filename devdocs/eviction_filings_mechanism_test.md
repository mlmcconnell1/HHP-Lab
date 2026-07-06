# Eviction Filings Mechanism Test

Date: 2026-07-06

## Source And Scope

This rerun uses the Eviction Lab Eviction Tracking System all-states monthly
download:

```text
https://evictionlab.org/uploads/allstates_monthly_2020_2021.csv
```

The current ETS download is not a national county-year file. It is a monthly
tract/ZIP file for tracked jurisdictions. The analysis uses only `Census Tract`
rows because county FIPS can be derived deterministically from the first five
digits of tract GEOIDs. ZIP rows were excluded because this repo does not carry
a ZIP-to-county allocation contract.

The top-50 first-difference panel has annual rows for 2016-2020 and 2023-2025.
The overlap with ETS is therefore 2023-2025.

## Coverage Audit

| Item | Value |
| --- | ---: |
| ETS tract rows used | 656,136 |
| Top-50 MSA-years in 2023-2025 overlap | 150 |
| MSA-years with any tract-derived ETS coverage | 30 |
| MSA-years with full county coverage | 9 |
| Top-50 MSAs with any coverage | 10 |
| Top-50 MSAs with full coverage | 3 |

Full-coverage top-50 MSAs:

| MSA | Full years |
| --- | ---: |
| Indianapolis-Carmel-Greenwood, IN | 3 |
| Milwaukee-Waukesha, WI | 3 |
| Minneapolis-St. Paul-Bloomington, MN-WI | 3 |

Partially covered top-50 MSAs:

| MSA | Mean county coverage |
| --- | ---: |
| Chicago-Naperville-Elgin, IL-IN | 0.308 |
| Cincinnati, OH-KY-IN | 0.200 |
| Kansas City, MO-KS | 0.643 |
| Louisville/Jefferson County, KY-IN | 0.333 |
| Philadelphia-Camden-Wilmington, PA-NJ-DE-MD | 0.091 |
| Providence-Warwick, RI-MA | 0.833 |
| St. Louis, MO-IL | 0.467 |

## Method

Tract-month filings were summed to county-year and then to Census 2023 MSA-year
using `data/curated/msa/msa_county_membership__census_msa_2023.parquet`.
Eviction changes use `d_log_eviction_filings = diff(log1p(eviction_filings))`.

Main specs require full MSA county coverage and use MSA-clustered standard
errors:

```text
d_log_eviction_filings ~ d_log_zori + d_log_pop
d_log_eviction_filings_lead1 ~ d_log_zori + d_log_pop
d_log_unshelt_rate ~ d_log_zori + d_log_eviction_filings + d_log_pop
d_log_unshelt_rate_lead1 ~ d_log_zori + d_log_eviction_filings_lead1 + d_log_pop
d_log_eviction_filings ~ d_log_zori + d_log_zori_x_vacancy + d_log_pop
```

Sensitivity specs keep any covered MSA-year and add
`eviction_coverage_ratio` as a control.

Generated ignored artifacts:

| Artifact | Purpose |
| --- | --- |
| `outputs/eviction_mechanism/top50_msa_eviction_mechanism_panel.parquet` | analysis panel |
| `outputs/eviction_mechanism/coverage_by_msa.csv` | coverage audit |
| `outputs/eviction_mechanism/coverage_by_year.csv` | annual coverage audit |
| `outputs/eviction_mechanism/eviction_mechanism_regressions.csv` | model output |

## Results

The strict full-coverage panel is too small for a credible mechanism test:
`n=9` for same-year specs, and only `n=6` for the next-year filings-to-
unsheltered spec. Treat the strict estimates as a coverage diagnostic, not
substantive evidence.

Strict full-coverage estimates:

| Spec | Term | Estimate | SE | p-value | n |
| --- | --- | ---: | ---: | ---: | ---: |
| rent to filings, same year | `d_log_zori` | -0.878 | 2.683 | 0.744 | 9 |
| rent to filings, next year | `d_log_zori` | 3.843 | 4.689 | 0.412 | 9 |
| filings to unsheltered, same year | `d_log_eviction_filings` | -0.243 | 0.376 | 0.519 | 9 |
| churn buffer | `d_log_zori_x_vacancy` | -1.281 | 3.542 | 0.718 | 9 |

Partial-coverage sensitivity estimates:

| Spec | Term | Estimate | SE | p-value | n |
| --- | --- | ---: | ---: | ---: | ---: |
| rent to filings, same year | `d_log_zori` | 1.736 | 1.376 | 0.207 | 30 |
| rent to filings, next year | `d_log_zori` | 8.986 | 3.520 | 0.011 | 30 |
| filings to unsheltered, same year | `d_log_eviction_filings` | -0.813 | 0.171 | 0.000002 | 30 |
| filings to unsheltered, next year | `d_log_eviction_filings_lead1` | -0.276 | 0.329 | 0.401 | 20 |
| churn buffer | `d_log_zori_x_vacancy` | -0.427 | 0.839 | 0.611 | 30 |

## Interpretation

This is not supportive evidence for the rent-to-homelessness chain operating
through observed eviction filings in the current top-50 panel. The strongest
positive rent-to-filings estimate appears only in the partial-coverage
sensitivity, where the identifying variation is dominated by a small,
non-national subset of tracked jurisdictions. The same sensitivity does not show
a positive filings-to-unsheltered link; if anything, the same-year coefficient
is negative. The vacancy interaction also does not provide evidence that the
churn buffer operates through fewer filings per rent shock.

The main finding is therefore a data-coverage result: Eviction Lab ETS coverage
is too sparse for this top-50 MSA 2015-2025 mechanism test unless the project
adds a national historical county-year Eviction Lab file or a vetted ZIP/tract
allocation workflow for the tracked post-2020 data.
