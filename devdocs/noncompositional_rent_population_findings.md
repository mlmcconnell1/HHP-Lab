# Non-Compositional Rent/Population Mechanism Screens

Generated with:

```bash
uv run python scripts/build_noncompositional_rent_population_panel.py
```

This screen follows the pooled top-50 plus rank-51-150 MSA first-difference
design used in `devdocs/composition_rent_population_findings.md`, excluding
2021. It tests non-compositional mechanisms raised after the household
composition screens failed to explain the population/rent orthogonality.

Generated, ignored artifacts:

- `outputs/noncompositional_rent_population/noncompositional_rent_population_levels.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd_regressions.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd_regressions.csv`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_summary.json`

## Measure Discovery

- Housing supply constraints are supported by already curated
  `census_bps` and Saiz covariates. The direct screen below uses
  pre-sample permit scarcity from the Census Building Permits Survey.
- A true short-term-rental source is not registered. The plausible free ACS
  fallback, B25004 vacancy status with the seasonal/recreational/occasional
  category, now has registry support from this change, but the current
  curated ACS1/ACS5 MSA artifacts used by this run predate that support and
  must be reingested before claiming a direct STR/vacation-home test.
- ACS work-from-home commute table B08301 now has registry support from this
  change, but the current curated ACS1 artifacts used by this run predate
  that support. The available fallback here is ACS1 B25068 bedroom mix among
  gross-rent units, used as a weak proxy for demand for more rental space
  rather than a direct remote-work measure.

## Coverage

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS1 vintages used for bedroom-mix proxy: 2009-2019 and 2021-2024
- Complete supply-constraint FD rows: 1,096
- Complete space-demand-proxy FD rows: 1,074

Median level bedroom mix among ACS1 gross-rent units:

| Measure | Median |
| --- | ---: |
| 2+ bedroom share | 0.717 |
| 3+ bedroom share | 0.325 |

## Housing Supply Constraints

The supply screen asks whether constrained metros have higher rent growth net
of population growth, and whether population changes translate differently
into rent changes under tighter supply. Models use MSA-clustered standard
errors and year fixed effects:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `supply_constraint_bps` | 0.0055 | 0.0009 | <0.001 | 1,096 |
| `d_log_pop x supply_constraint_bps` | 0.1145 | 0.0873 | 0.189 | 1,096 |
| `supply_constraint_bps_long` | 0.0034 | 0.0012 | 0.003 | 1,096 |
| `d_log_pop x supply_constraint_bps_long` | 0.1027 | 0.0832 | 0.217 | 1,096 |

Interpretation: constrained metros do show higher rent growth net of
population growth and common year shocks. The interaction term is positive
but imprecise, so this run does not show that population growth itself has a
statistically different rent slope in constrained metros. The evidence is
therefore more consistent with a supply-constraint level channel -- persistent
rent pressure in constrained places independent of headcount changes -- than
with a sharply different population/rent elasticity by constraint level.

## Space-Demand Proxy

ACS work-from-home commute table B08301 is now registered, but existing
curated ACS1 artifacts need reingest before it can be tested directly. This
screen therefore uses ACS1 B25068 bedroom mix among gross-rent units as the
currently available fallback proxy for rental space demand. Models use
MSA-clustered standard errors and year fixed effects:

| Model | Proxy term | Estimate | SE | p-value | N |
| --- | --- | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + proxy + year FE` | `d_gross_rent_2plus_bedroom_share` | -0.0320 | 0.0231 | 0.165 | 1,080 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + proxy + year FE` | `d_gross_rent_2plus_bedroom_share` | 1.1950 | 0.5471 | 0.029 | 1,074 |
| `d_log_zori ~ d_log_pop + proxy + year FE` | `d_gross_rent_3plus_bedroom_share` | -0.0639 | 0.0199 | 0.001 | 1,080 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + proxy + year FE` | `d_gross_rent_3plus_bedroom_share` | -0.0726 | 0.5730 | 0.899 | 1,074 |

Interpretation: this fallback does not support a positive rental-space-demand
explanation for rent rising independent of population. The rent coefficients
are negative, and the 3+ bedroom share coefficient is statistically
significant in the opposite direction. The positive 2+ bedroom-share
coefficient in the unsheltered-rate model is exploratory and does not pair
with higher rent growth, so it is not evidence for the proposed rent channel.

## Interpretation

Among the non-compositional candidates that can be tested with current
curated data, supply constraints are the only one that materially improves
the story: constrained MSAs have higher rent growth even after controlling
for population growth. This directly weakens the idea that population changes
alone should explain rent changes, but it does not prove that constrained and
unconstrained metros have statistically different population/rent slopes.

The current artifacts cannot complete a real short-term-rental/vacation-home
test yet: commercial STR data are not registered, and the curated ACS1/ACS5
artifacts need reingest to include newly supported ACS B25004
seasonal/recreational vacancy status. The current artifacts also cannot test
remote-work share directly until ACS1 is reingested with newly supported
B08301. The B25068 bedroom-mix fallback is negative/null for rent growth, so
it should not be treated as confirming a space-demand channel.
