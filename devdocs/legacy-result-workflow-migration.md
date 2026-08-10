# Legacy result-script migration

This note resolves bead `coclab-c8spc`. The 29 files under `scripts/` are
thin compatibility entrypoints that delegate to
`hhplab.results.workflows`; they do not contain an independent computation.

## Migration policy

The package CLI is the canonical interface for documented result workflows:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab build result <workflow> --json
```

The legacy scripts remain available through 2026-12-31 (or until the next
breaking release, whichever is later) so historical commands remain
reproducible. New documentation and new analyses must use the package CLI.
Before removal, re-check downstream use and retain this mapping in the release
notes. The wrappers should not gain new analysis logic.

The repository search on 2026-08-10 found no CI, package entrypoint, or tracked
documentation dependency on the `scripts/` paths. Existing tests intentionally
cover the delegation contract.

## Complete command mapping

| Legacy script | Canonical workflow command |
| --- | --- |
| `scripts/analyze_composition_rent_population_robustness.py` | `composition-rent-population-robustness` |
| `scripts/analyze_core_rent_shock_state_year_fe.py` | `core-rent-shock-state-year-fe` |
| `scripts/analyze_noncompositional_rent_population_robustness.py` | `noncompositional-rent-population` |
| `scripts/analyze_overdose_psh_state_year_robustness.py` | `overdose-psh-state-year-robustness` |
| `scripts/analyze_rent_growth_r2_decomposition.py` | `rent-growth-r2-decomposition` |
| `scripts/analyze_sanctuary_longdiff_robustness.py` | `sanctuary-longdiff-robustness` |
| `scripts/build_bps_valuation_benchmark.py` | `bps-valuation-benchmark` |
| `scripts/build_bps_valuation_rent_channel.py` | `bps-valuation-rent-channel` |
| `scripts/build_employment_labor_force_composition_panel.py` | `employment-labor-force-composition` |
| `scripts/build_eviction_rate_timing_panel.py` | `eviction-rate-timing` |
| `scripts/build_household_size_composition_panel.py` | `household-size-composition` |
| `scripts/build_housing_cost_burden_composition_panel.py` | `housing-cost-burden-composition` |
| `scripts/build_income_inequality_composition_panel.py` | `income-inequality-composition` |
| `scripts/build_irs_migration_pooled_panel.py` | `irs-migration-pooled` |
| `scripts/build_local_income_composition_panel.py` | `local-income-composition` |
| `scripts/build_noncompositional_rent_population_panel.py` | `noncompositional-rent-population` |
| `scripts/build_overdose_lag_panel.py` | `overdose-lag` |
| `scripts/build_poverty_longitudinal_panel.py` | `poverty-longitudinal` |
| `scripts/build_qcew_labor_market_panel.py` | `qcew-labor-market` |
| `scripts/build_recent_mover_income_composition_panel.py` | `recent-mover-income-composition` |
| `scripts/build_renter_household_share_composition_panel.py` | `renter-household-share-composition` |
| `scripts/build_subsidized_housing_stock_panel.py` | `subsidized-housing-stock` |
| `scripts/build_supply_iv_panel.py` | `supply-iv` |
| `scripts/build_vera_hic_pit_longitudinal.py` | `vera-hic-pit-longitudinal` |
| `scripts/build_vera_hic_pit_longitudinal_pooled.py` | `vera-hic-pit-longitudinal-pooled` |
| `scripts/build_vera_hic_pit_panel.py` | `vera-hic-pit-panel` |
| `scripts/generate_top50_msa_coc_pit_contract_rent_2010_2020.py` | `top50-msa-coc-pit-contract-rent-2010-2020` |
| `scripts/overdose_hic_category_correlations.py` | `overdose-hic-category-correlations` |
| `scripts/vera_hic_pit_correlations.py` | `vera-hic-pit-correlations` |

The grouped workflows `vera-hic-pit`, `overdose-hic`,
`composition-rent-population`, `noncompositional-rent-population`, and
`all-documented-results` are also available when the full documented result
family is required.

## Compatibility details

The CLI adds structured JSON output, consistent error envelopes, and finding
sidecars. The wrapper tests verify that each legacy entrypoint still delegates
to its package workflow. `build_supply_iv_panel.py` is the only wrapper with
an explicit argument-forwarding contract; use the package module directly for
custom `--msa-count` runs because the result CLI intentionally uses registered
workflow defaults:

```bash
uv run python -m hhplab.results.workflows.build_supply_iv_panel --msa-count 50
```

Generated `__pycache__` directories are not reproducibility artifacts and may
be removed whenever the wrappers are retired.
