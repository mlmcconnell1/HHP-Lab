# MPI Unauthorized Immigrant Estimates Workflow

HHP-Lab supports Migration Policy Institute (MPI) unauthorized immigrant topline
estimates as a county-native covariate source.

The supported workbook is the MPI mid-2023 state/county topline XLSX:

```text
data/raw/mpi/MPI-2023_Unauthorized_Profiles-State-County-Topline_Estimates-FINAL.xlsx
```

The ingest contract accepts files matching:

```text
MPI-2023_Unauthorized_Profiles-State-County-Topline_Estimates*.xlsx
```

If the workbook is staged outside the repo, pass its path with `--raw-path`. The
source contract expects sheets `U.S. and States` and `U.S. and Counties`, with
headers on row 3.

## Source Contract

MPI publishes mid-2023 estimates, not annual flow counts. HHP-Lab exposes these
canonical measures:

| Column | Definition |
|--------|------------|
| `unauthorized_immigrant_population` | MPI estimated unauthorized immigrant population. |
| `unauthorized_immigrant_share_of_us_total` | Row share of MPI's U.S. unauthorized immigrant total. |

County-native curated rows include `county_fips`, `geo_id`, `state_fips`,
`state_name`, `county_label`, `year`, `estimate_period`, source/provenance
fields, and the measure columns above.

The parser writes only rows that resolve to a unique county or county-equivalent
FIPS. It skips the U.S. total and county labels that cannot be resolved to a
county-equivalent row. Resolved multi-county rows and native MPI MSA rows are
retained in parquet provenance so MSA aggregation can recover source rows whose
member counties all belong to one MSA or whose MSA name uniquely matches the
selected MSA definition. Skipped row counts and reasons are written to parquet
provenance and JSON CLI output. State totals remain source context only and are
not emitted as county-native panel rows.

## Commands

List the source contract:

```bash
hhplab list covariates --json
```

Ingest the staged workbook:

```bash
hhplab ingest covariate \
  --source mpi_unauthorized_immigrants \
  --raw-path data/raw/mpi/MPI-2023_Unauthorized_Profiles-State-County-Topline_Estimates-FINAL.xlsx \
  --json
```

By default, MPI ingest resolves county names through local Census PEP county raw
files under `data/raw/pep`. To use a different county reference, provide a CSV
or Parquet with either `STNAME`, `CTYNAME`, `STATE`, `COUNTY` or
`state_name`, `county_name`, `county_fips`:

```bash
hhplab ingest covariate \
  --source mpi_unauthorized_immigrants \
  --raw-path data/raw/mpi/MPI-2023_Unauthorized_Profiles-State-County-Topline_Estimates-FINAL.xlsx \
  --county-reference-path data/raw/pep/pep_county__v2020__2026-03-27.csv \
  --json
```

The curated output path is:

```text
data/curated/covariates/covariate__mpi_unauthorized_immigrants__Y2023-2023.parquet
```

Aggregate to Census MSA panel-ready rows for a PIT-aligned year:

```bash
hhplab aggregate covariate \
  --source mpi_unauthorized_immigrants \
  --target-geo msa \
  --years 2024 \
  --msa-definition-version census_msa_2023 \
  --county-population-path data/curated/pep/pep_county__v2024.parquet \
  --json
```

MPI is a static mid-2023 estimate. When `--years` is supplied, the aggregator
carries the 2023 estimate to the requested panel year(s), records
`source_estimate_year`, and writes a `static_year_policy` provenance block.
For MSA output, the aggregate command also reports `coverage_policy`,
`coverage_diagnostics`, and `warnings` in JSON. By default, MSA rows below full
county-membership coverage are kept but warned on. Use
`--min-coverage-ratio` to set the warning threshold and
`--drop-below-min-coverage` to filter rows below that threshold.

The panel-ready output path is:

```text
data/curated/covariates/covariate_panel__mpi_unauthorized_immigrants__Y2023-2023.parquet
```

## Correlation Workflow

Join the panel-ready MPI rows to another MSA-year panel on `geo_id` and `year`
using a recipe join step or another explicit panel assembly step. A joined panel
can then feed the standard analyzer:

```bash
hhplab analyze correlate \
  --panel outputs/msa_pit_mpi_2024.parquet \
  --columns pit_total,unauthorized_immigrant_population \
  --output outputs/msa_pit_mpi_2024__analysis_correlate.parquet \
  --json
```

Missing MPI inputs fail at the stage where they are missing:

- `hhplab ingest covariate` reports missing workbook, bad sheet/header layout,
  or unmapped county labels.
- `hhplab aggregate covariate` reports missing curated MPI parquet, unsupported
  target geography, or missing PEP county population weights.
- `hhplab analyze correlate` reports missing panel columns and lists available
  columns.

## Caveats

MPI estimates are model-based estimates built from ACS, SIPP, DHS administrative
benchmarks, and MPI legal-status assignment methodology. Treat them as estimated
stock measures for mid-2023, not observed counts or migration flows.

County-to-MSA aggregation sums MPI count/share measures across covered counties
and recoverable same-MSA multi-county rows. When MPI publishes a native MSA row
whose name uniquely matches the selected MSA definition, that native MSA estimate
is used for the MSA output row and marked with `mpi_msa_source_row_count`.
Coverage diagnostics in CLI JSON and provenance report partial MSA coverage and
source counties that do not belong to the selected MSA definition.

Coverage must be inspected before interpreting cross-MSA comparisons. MPI does
not publish every county as a standalone row, and many MSA rows can be
substantially below full county-membership coverage. Treat low-coverage rows as
biased partial estimates unless they are filtered out or the missing geography
is explicitly resolved.

Exploratory correlations involving MPI estimates are descriptive screening
results. They are not causal evidence and should be interpreted with the usual
small-sample, measurement-error, and confounding cautions.
