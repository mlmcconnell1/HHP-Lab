# PRISM County-Mode Temperature Workflow

HHP-Lab supports PRISM Climate Group temperature as a county-native covariate source.
The workflow is intentionally county mode:

1. Download one PRISM monthly 4km raster ZIP.
2. Materialize county-level means with Census county geometry.
3. Use existing county-to-CoC or county-to-MSA recipe transforms with
   `aggregation: weighted_mean`.

Direct recipe-time raster zonal statistics to CoC/MSA geometry are out of scope. Keeping the
raster step at county geometry makes the expensive raster operation explicit, writes a reusable
curated parquet with provenance, and lets existing recipe crosswalk machinery handle CoC/MSA
aggregation.

## Source URL Pattern

The current HTTPS data-directory pattern is:

```text
https://data.prism.oregonstate.edu/time_series/us/an/4km/{variable}/monthly/{year}/prism_{variable}_us_25m_{YYYYMM}.zip
```

For January 2024 minimum temperature:

```text
https://data.prism.oregonstate.edu/time_series/us/an/4km/tmin/monthly/2024/prism_tmin_us_25m_202401.zip
```

The ingester currently supports monthly `tmin`, `tmean`, and `tmax`. PRISM monthly time-series
directories begin at 1895 and continue into current releases. PRISM revises recent monthly grids
for roughly six months; treat recent months as provisional and prefer stable months for panel
builds.

## Commands

Download and register a raw ZIP:

```bash
hhplab ingest prism --variable tmin --year 2024 --month 1 --json
```

This writes:

```text
data/raw/prism/tmin/monthly/2024/prism_tmin_us_25m_202401.zip
```

Materialize county means:

```bash
hhplab build prism-county --variable tmin --year 2024 --month 1 --county-vintage 2023 --json
```

This writes:

```text
data/curated/prism/prism_county_monthly__tmin__Y2024M01@C2023.parquet
```

The curated artifact includes `geo_id`, `county_fips`, `year`, `month`, `date`, the temperature
column such as `tmin_c`, source/provenance fields, and raster coverage diagnostics:
`raster_total_cell_count`, `raster_valid_cell_count`, `raster_nodata_cell_count`, and
`raster_coverage_ratio`.

## Recipe Dataset

Declare the materialized county artifact as `provider: prism`, `product: temperature`:

```yaml
prism_tmin_county:
  provider: prism
  product: temperature
  version: 1
  native_geometry: { type: county, vintage: 2023 }
  years: "2024-2024"
  year_column: year
  geo_column: county_fips
  params:
    variable: tmin
    month: 1
    align: point_in_time_jan
  path: data/curated/prism/prism_county_monthly__tmin__Y2024M01@C2023.parquet
```

Aggregate to CoCs:

```yaml
- resample:
    dataset: prism_tmin_county
    to_geometry: { type: coc, vintage: 2025, source: hud_exchange }
    method: aggregate
    via: county_to_coc_population
    measures:
      tmin_c: { aggregation: weighted_mean }
```

Aggregate to Census MSAs:

```yaml
- resample:
    dataset: prism_tmin_county
    to_geometry: { type: msa, source: census_msa_2023 }
    method: aggregate
    via: county_to_msa_population
    measures:
      tmin_c: { aggregation: weighted_mean }
```

A complete example with both targets is available at
`recipes/examples/coc-msa-prism-tmin-january-2024.yaml`.
