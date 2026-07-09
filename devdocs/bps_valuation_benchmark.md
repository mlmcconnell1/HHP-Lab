# Census BPS Mix-Adjusted Permit Valuation Benchmark

This note resolves beads `coclab-u3xij` and `coclab-dje6b` for the
`bps_mix_adjusted_permit_value_per_unit_thousands` covariate.

## Reproduction

Run:

```bash
uv run python -m hhplab.results.workflows.build_bps_valuation_benchmark
```

or:

```bash
uv run hhplab build result bps-valuation-benchmark --json
```

The workflow writes:

- `outputs/bps_valuation_benchmark/bps_valuation_ppi_benchmark.parquet`
- `outputs/bps_valuation_benchmark/bps_valuation_ppi_benchmark.csv`
- `outputs/bps_valuation_benchmark/bps_valuation_distinctness.parquet`
- `outputs/bps_valuation_benchmark/bps_valuation_distinctness.csv`
- `outputs/bps_valuation_benchmark/bps_valuation_benchmark_summary.json`

By default it uses embedded annual-average BLS PPI `WPUSI012011` observations
fetched from the public BLS API on 2026-07-09. Set
`HHPLAB_BPS_FETCH_LIVE_PPI=1` to refresh the PPI series from BLS.

## Current Results

Using the local 2000-2024 Census BPS county and MSA artifacts:

| Check | Result |
| --- | ---: |
| National fixed-mix BPS index, 2024 (2000=100) | 235.721 |
| BLS PPI `WPUSI012011`, 2024 (2000=100) | 227.982 |
| Annual log-change correlation, 2001-2024 | 0.521 |
| Distinctness correlation versus `supply_constraint_bps`, 2010-2014 | 0.155 |
| Distinctness MSA count | 316 |

The national benchmark remains consistent with the shipped validation claim:
the fixed-mix BPS valuation index tracks construction-input PPI in direction
and long-run magnitude, but the annual correlation is moderate rather than
high. The distinctness check also remains consistent with the original claim:
the valuation measure is not just a restatement of the existing BPS
permits-scarcity exposure.

The current PPI 2024 index differs slightly from the earlier reported 227.916
because the BLS series now returns 328.522 for the 2024 annual average, which
indexes to 227.982 against the 2000 value of 144.1.

## Coverage Constraint

The real-data MSA panel has severe non-random missingness in the fixed-mix
valuation column:

| Coverage diagnostic | Result |
| --- | ---: |
| MSA-year rows | 9,675 |
| Non-null mix-adjusted valuation rows | 6,642 |
| Non-null share | 68.7% |
| Lowest annual non-null share | 55.0% in 2011 |
| Corr(MSA NaN rate, log average permitted units) | -0.567 |

The root cause is the fixed-base mix contract. For each MSA, the derivation
uses the first year with positive permitted units to set structure-class
weights. If a positive-weighted class has no permitted units in a later year,
the workflow emits `NaN` rather than imputing or reweighting across the
remaining classes. That row-level behavior is preferable to inventing a
valuation for a missing required class, but the aggregate consequence is a
size-driven complete-case sample: small and low-permit MSAs drop out much more
often, especially around the Great Recession.

## Recommendation

Do not use this covariate in a pooled FE channel test without explicitly
reporting the complete-case sample and the coverage diagnostics above. A null
construction-cost result using this column could reflect non-random coverage
rather than absence of a construction-cost channel.

For `coclab-mzpm7.13`, treat the covariate as usable only for a documented
sensitivity check, not as a clean full-sample mechanism measure. If the channel
becomes central, the next modeling step should compare this fixed-mix measure
against alternatives that trade off purity and coverage explicitly, such as
national PPI interacted with local structure mix, a rolling-base mix, or a
coarser class aggregation that reduces zero-unit class failures.
