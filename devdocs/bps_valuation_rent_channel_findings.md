# BPS Mix-Adjusted Permit Valuation as a Rent-Growth Channel

This note closes the actual channel test that `coclab-mzpm7.13` ("Assess
construction cost inflation as a rent-growth channel") was scoped for.
`coclab-mzpm7.17` built `bps_mix_adjusted_permit_value_per_unit_thousands`
and `devdocs/bps_valuation_benchmark.md` validated it against BLS PPI and
checked distinctness from the existing permits-scarcity exposure, but
neither step regressed the covariate against rent growth. `mzpm7.13` was
closed on 2026-07-09 with that step still undone; it was reopened to run it.

## Reproduction

```bash
uv run python -m hhplab.results.workflows.build_bps_valuation_rent_channel
```

or:

```bash
uv run hhplab build result bps-valuation-rent-channel --json
```

Uses this project's standard pooled top-150 base panel (top-50 2010-2025 +
rank51-150 2015-2025, 2021 excluded) merged with the BPS MSA panel, and this
project's standard 3-tier FE robustness ladder (year, primary-state x year,
census-region x year) with MSA-clustered SEs.

## Specifications

- **contemporaneous**: `d_log_zori(t) ~ d_log_pop(t) + d_log_bps_valuation(t)`
- **lead**: `d_log_zori(t) ~ d_log_pop(t) + d_log_bps_valuation_lag1(t)` --
  does last year's construction-cost growth predict this year's rent growth
- **reverse placebo**: `d_log_bps_valuation(t) ~ d_log_pop(t) + d_log_zori_lag1(t)`
  -- does last year's rent growth predict this year's cost growth (checks
  the lead-lag direction isn't actually reversed, e.g. permit valuations
  chasing a hot market)

## Results

| Direction | FE tier | estimate | p-value | n | clusters |
| --- | --- | ---: | ---: | ---: | ---: |
| contemporaneous | year | -0.0020 | 0.838 | 720 | 131 |
| contemporaneous | state x year | -0.0056 | 0.543 | 720 | 131 |
| contemporaneous | region x year | -0.0027 | 0.743 | 720 | 131 |
| lead (BPS -> rent) | year | 0.0045 | 0.494 | 660 | 131 |
| lead (BPS -> rent) | state x year | 0.0128 | 0.115 | 660 | 131 |
| lead (BPS -> rent) | region x year | 0.0024 | 0.743 | 660 | 131 |
| reverse placebo (rent -> BPS) | year | 0.0403 | 0.807 | 515 | 128 |
| reverse placebo (rent -> BPS) | state x year | -0.1050 | 0.772 | 515 | 128 |
| reverse placebo (rent -> BPS) | region x year | 0.1271 | 0.543 | 515 | 128 |

Full output: `outputs/bps_valuation_rent_channel/bps_valuation_rent_channel_regressions.csv`.

**No specification is statistically significant at conventional levels.**
Contemporaneous correlation is essentially zero across all three FE tiers.
The lead direction (last year's cost growth predicting this year's rent
growth) is the closest to a signal -- positive in all three tiers, and the
state x year FE spec reaches p=0.115 -- but doesn't cross p<0.05 in any
tier. The reverse-direction placebo is unstable in sign across FE tiers and
never close to significant, which at least argues against the observed lead
coefficient being an artifact of rents mechanically pulling permit
valuations forward.

**Bottom line: no robust evidence that BPS mix-adjusted permit valuation
growth leads (or is contemporaneous with) MSA rent growth in this sample.**
The state x year FE lead spec is suggestive (right sign, p=0.115) but not
strong enough to call a finding.

## Coverage caveat

This screen inherits the covariate's known non-random missingness
(`devdocs/bps_valuation_benchmark.md`): the complete-case sample is 720
rows / 131 MSAs (contemporaneous) and 660 rows / 131 MSAs (lead) out of the
1,450 year-over-year rows in the full pooled top-150 FD panel -- roughly
half the panel, skewed toward larger and more consistently-permitting
MSAs. A null result here specifically means "no detectable lead-lag
relationship among larger, consistently-permitting MSAs," not a fully
general null across the whole top-150 cohort.

## Recommendation

Treat construction-cost inflation (as measured by this proxy) as ruled out
as a channel explaining MSA rent-growth variance, with the caveat above.
If this channel is revisited, the state x year FE lead spec's near-miss
(p=0.115) plus the small-MSA/recession-year coverage gap means a cleaner
test would need either a wider-coverage cost proxy (e.g. one of the
alternatives suggested in `bps_valuation_benchmark.md`: rolling-base mix,
coarser class aggregation, or PPI x local-mix interaction) or an explicit
power analysis before concluding the null is real rather than a
underpowered/coverage artifact.
