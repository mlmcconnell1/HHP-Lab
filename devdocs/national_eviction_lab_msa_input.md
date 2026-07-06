# National Eviction Lab MSA input

Bead `coclab-x4pgw` requested a national or allocatable eviction-filings input
for MSA mechanism tests. The ETS all-states monthly extract used in the first
mechanism rerun covers only tracked jurisdictions, leaving too few full-coverage
top-50 MSA-years.

## Added input path

The covariate catalog now includes `eviction_lab_national` for the Eviction Lab
national map county-year data:

- source: `https://evictionlab.org/map/`
- native geography: county
- years: 2000-2018
- required columns: `county_fips`, `year`
- measures: `eviction_filings`, `eviction_rate`
- MSA aggregation: supported through `hhplab aggregate covariate --target-geo msa`

The existing `eviction_lab` source remains available for compatibility with
previous local extracts.

## Agent workflow

Stage a county-year CSV from the Eviction Lab national map export, then ingest:

```bash
hhplab ingest covariate \
  --source eviction_lab_national \
  --raw-path <county-year-eviction-lab.csv> \
  --force \
  --json
```

Aggregate to Census MSA panels with a county population denominator:

```bash
hhplab aggregate covariate \
  --source eviction_lab_national \
  --target-geo msa \
  --msa-definition-version census_msa_2023 \
  --county-population-path <pep-county-year.parquet> \
  --years 2015-2018 \
  --force \
  --json
```

The output includes `coverage_ratio`, `county_count`, and
`membership_county_count`, so mechanism tests can require full MSA coverage or
report partial-coverage sensitivity explicitly.
