# HUD HIC Backfill Runbook

HUD Housing Inventory Count files may return an interactive WAF challenge to
non-browser download clients. When `hhplab ingest hic --year <YEAR>` reports a
WAF challenge, use the manual placement path below.

## Manual Download

1. Download the HUD HIC file for the target year in a browser.
   - Per-year file pattern:
     `https://www.huduser.gov/portal/sites/default/files/xls/<YEAR>-HIC-Counts-by-State.csv`
   - Aggregate workbook examples:
     `2007-2024-HIC-Counts-by-CoC.xlsx`
     `2007-2024-HIC-Counts-by-State.xlsx`
2. Place the downloaded file under:
   `data/raw/hic/<YEAR>/`
3. Parse the local file into the canonical curated artifact:
   ```bash
   hhplab ingest hic --year <YEAR> --parse-only --json
   ```

The parser accepts year-specific CSV/XLS/XLSX/XLSB files and aggregate HUD HIC
workbooks with year-specific bed/unit columns. Canonical outputs are written as:

```text
data/curated/hic/hic__H<YEAR>.parquet
```

## Expanded HIC Measures

Curated HIC artifacts preserve stable `hic_*` panel measure names alongside the
legacy `total_beds` and `total_units` compatibility aliases.

Project-type year-round bed columns:

- `hic_es_year_round_beds`: emergency shelter (ES) year-round beds.
- `hic_th_year_round_beds`: transitional housing (TH) year-round beds.
- `hic_sh_year_round_beds`: safe haven (SH) year-round beds.
- `hic_rrh_year_round_beds`: rapid rehousing (RRH) year-round beds.
- `hic_psh_year_round_beds`: permanent supportive housing (PSH) year-round beds.
- `hic_oph_year_round_beds`: other permanent housing (OPH) year-round beds.

Derived bed totals:

- `hic_shelter_year_round_beds`: shelter inventory, defined as ES + TH + SH.
- `hic_total_beds`: all-program inventory, defined as
  ES + TH + SH + RRH + PSH + OPH. This is the preferred total-bed column for
  recipes and panels. `total_beds` is retained as a compatibility alias for the
  same all-program value.

Unit columns use the same project-type split where HUD publishes units for
households with children: `hic_es_family_units`, `hic_th_family_units`,
`hic_sh_family_units`, `hic_rrh_family_units`, `hic_psh_family_units`, and
`hic_oph_family_units`. `hic_shelter_family_units` is ES + TH + SH, and
`hic_total_units` is the all-program unit total. `total_units` is retained as a
compatibility alias.

Historical availability:

- 2010-2012 HUD sheets publish shelter aggregate columns and PSH columns, but
  not distinct ES/TH/SH/RRH/OPH project columns. Distinct unavailable project
  columns are filled as 0; shelter and all-program totals are still populated.
- 2013 sheets publish an ES/TH/RRH/SH aggregate plus RRH and PSH columns.
  Shelter-only totals subtract RRH from that aggregate when RRH is present.
- 2014 and later sheets publish the full ES/TH/SH/RRH/PSH/OPH project-type
  split, so all-program totals can be checked directly against component sums.

## Backfill Loop

Run this for each PIT-aligned panel year that needs HIC coverage:

```bash
for year in 2020 2021 2022 2023 2024; do
  hhplab ingest hic --year "$year" --parse-only --json
done
```

Use the actual supported panel years for the analysis, not this example list.
If an aggregate workbook covers multiple years, the same file may be placed in
each target `data/raw/hic/<YEAR>/` directory, or copied/symlinked there before
running the parse-only command.

For the 2010-2024 PIT-backed standard panel range, the current local backfill
uses the 2025 aggregate CoC workbook for early years that do not have
year-specific CoC workbooks:

```bash
for year in 2010 2011 2012 2013 2014; do
  mkdir -p "data/raw/hic/$year"
  ln -sf ../2025/2007-2025-HIC-Counts-by-CoC.xlsx \
    "data/raw/hic/$year/2007-2025-HIC-Counts-by-CoC.xlsx"
done

for year in $(seq 2010 2024); do
  hhplab ingest hic --year "$year" --parse-only --json
done
```

## Coverage QA

After parsing, compare HIC coverage to canonical PIT coverage:

```bash
hhplab diagnostics hic-coverage --years 2020-2024 --json
```

The report includes:

- `missing_hic_coc_year`: CoC-year exists in PIT but not HIC.
- `unexpected_hic_coc_year`: CoC-year exists in HIC but not PIT.
- `duplicate_hic_coc_year`: duplicate HIC rows for a CoC-year.
- `large_hic_bed_yoy_swing`: large year-over-year HIC total-bed changes.

The backfill blocker is cleared when `hhplab status --json` lists the required
HIC years, `hhplab diagnostics hic-coverage --years <RANGE> --json` reports only
known/accepted warnings, and at least one standard CoC panel is regenerated with
`hic_total_beds`, `hic_total_units`, and the expanded project-type HIC columns.

## Current 2010-2024 Backfill Result

As of the 2010-2024 backfill, `hhplab status --json` reports HIC artifacts for
2010 through 2025. The PIT-backed standard range remains 2010-2024 because the
local canonical PIT vintage is `pit_vintage__P2024.parquet`.

The coverage QA command:

```bash
hhplab diagnostics hic-coverage --years 2010-2024 --json
```

uses `data/curated/pit/pit_vintage__P2024.parquet` and the 15 HIC artifacts
from `hic__H2010.parquet` through `hic__H2024.parquet`. It passes with warnings
only:

- `missing_hic_coc_year`: 19, limited to historical CoC IDs `LA-509`,
  `MD-514`, `CA-530`, and `AR-505`.
- `unexpected_hic_coc_year`: 356, concentrated in older years where HIC retains
  historical CoC IDs that are not present in the canonical PIT vintage. The
  count declines from 67 in 2010 to 0 in 2024.
- `large_hic_bed_yoy_swing`: 100 warnings for manual review; these are not
  blocking errors.

The reproducible recipe:

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe \
  --recipe recipes/examples/coc-pit-hic-2010-2024.yaml --json
```

writes:

```text
../HHP-Data/coc_pit_hic_2010_2024/panel__Y2010-2024@B2025.parquet
../HHP-Data/coc_pit_hic_2010_2024/panel__Y2010-2024@B2025.manifest.json
```

The generated panel has 6,095 PIT-anchored rows and includes the expanded HIC
project-type bed columns, `hic_shelter_year_round_beds`, `hic_total_beds`, and
`hic_total_units`; 6,076 rows have non-null HIC values.
