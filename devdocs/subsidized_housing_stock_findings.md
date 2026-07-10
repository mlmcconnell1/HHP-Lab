# Subsidized housing stock and rent growth

Run the tracked top-150 workflow with:

```bash
uv run hhplab build result subsidized-housing-stock --json
```

The first-difference models regress MSA rent growth on population growth and
growth in HUD subsidized-household or housing-choice-voucher stock per 1,000
residents. Standard errors are clustered by MSA. Each model is now estimated
with year, region-by-year, and state-by-year fixed effects.

| Focal term | Year FE p | Region x year p | State x year p |
| --- | ---: | ---: | ---: |
| Subsidized households | 0.079 | 0.035 | 0.158 |
| Housing choice vouchers | 0.065 | 0.089 | 0.209 |

Both negative baseline associations are marginal and neither survives the
strict state-by-year specification. The available evidence therefore does not
support treating subsidized-stock growth as an MSA-specific rent-growth
channel independent of shared state-level conditions.

The workflow writes the full regression ladder to
`outputs/subsidized_housing_stock/subsidized_housing_stock_fd_regressions.parquet`
with embedded provenance metadata.
