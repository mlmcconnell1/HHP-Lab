# Eviction-rate timing and rent growth

Run the tracked top-150 workflow with:

```bash
uv run hhplab build result eviction-rate-timing --json
```

The workflow tests same-year eviction-rate growth, a lagged channel, a lead
placebo, and reverse causality. Each timing model is estimated with year,
region-by-year, and state-by-year fixed effects and MSA-clustered errors.
Eviction Lab national coverage ends in 2018.

| Timing specification | Year FE p | Region x year p | State x year p |
| --- | ---: | ---: | ---: |
| Same year | 0.028 | 0.108 | 0.910 |
| Lag 1 channel | 0.097 | 0.229 | 0.869 |
| Lead 1 placebo | 0.299 | 0.373 | 0.588 |
| Reverse causality | 0.254 | 0.348 | 0.618 |

The only conventional year-FE signal is the descriptive same-year model. It
weakens monotonically and becomes null under state-by-year fixed effects.
Neither the lag channel nor its timing checks support an eviction-rate channel
to rent growth independent of shared state-level conditions.

The complete ladder is stored in
`outputs/eviction_rate_timing/eviction_rate_timing_regressions.parquet` with
embedded provenance metadata.
