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
`hic_total_beds` and `hic_total_units`.
