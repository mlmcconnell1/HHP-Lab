# CoC-Lab CLI Evaluation Test Plan (Agent Runbook)

**Purpose:** Exercise the CoC-Lab CLI end-to-end with a focus on (a) crosswalk integrity, (b) reproducibility/idempotency, and (c) the new **direct ACS tract-population ingest** and **sanity checks vs. CoC embedded population denominators**.

This document is written for an evaluation agent. Follow steps in order. Capture console output (full logs) and record pass/fail for each test.

---

## Conventions

- Commands assume the `coclab` CLI is on `PATH`.
- Run from the repo root (or ensure relative paths match your environment).
- When a step mentions an output file, verify it exists and is non-empty.
- If a command supports `--force`, test both with and without.
- Record:
  - **Command**
  - **Exit code**
  - **Key output lines**
  - **Artifacts created/updated**
  - **Observed anomalies**

---

## 0) Environment and CLI Smoke Tests

### 0.1 CLI help renders without exceptions
```bash
coclab --help
```
**Pass criteria**
- Help text renders (no stack trace).
- Subcommands listed.

### 0.2 Minimal smoke tests (if present)
```bash
pytest tests/test_smoke.py -v
```
**Pass criteria**
- Test suite completes.
- No import-time crashes.
- Tests that require network or credentials skip gracefully (if applicable).

---

## 1) Boundary Ingest and Vintage Registry

### 1.1 Ingest a boundary vintage from HUD Exchange
```bash
coclab ingest-boundaries --source hud_exchange --vintage 2025
```
**Pass criteria**
- Command completes successfully.
- Output indicates download + parsing + writing artifacts.
- Artifacts appear under the expected `data/` directories.

**Notes / anomalies to flag**
- Non-deterministic results between repeated runs.
- “Downloaded” but no outputs written.

### 1.2 List boundary vintages
```bash
coclab list-vintages
```
**Pass criteria**
- Vintage `2025` appears.
- Formatting stable (table output).
- Timestamps/metadata are plausible.

### 1.3 Idempotency check for boundary ingest
Run the same ingest again:
```bash
coclab ingest-boundaries --source hud_exchange --vintage 2025
```
**Pass criteria**
- Either:
  - reports “already ingested / no-op” (preferred), or
  - re-ingests cleanly without changing results unexpectedly.

**Flag**
- Repeated ingestion produces materially different outputs without explanation.

---

## 2) TIGER/Line Tract Ingest (Geometry Baseline)

### 2.1 Ingest census tracts for a tract vintage (example: 2023)
```bash
coclab ingest-census --year 2023
```
**Pass criteria**
- Successful completion.
- Tract artifact(s) written.
- Re-run is fast and does not redownload unless forced.

### 2.2 Force re-ingest tracts
```bash
coclab ingest-census --year 2023 --force
```
**Pass criteria**
- Explicitly re-downloads/rebuilds.
- Output clearly states forcing behavior.

---

## 3) Crosswalk Build + Diagnostics (CoC ↔ Tracts)

### 3.1 Build crosswalks for boundary + tracts
```bash
coclab build-xwalks --boundary 2025 --tracts 2023
```
**Pass criteria**
- Crosswalk artifact(s) written, e.g. a parquet in `data/curated/xwalks/`.
- No major warnings about missing geometries.

### 3.2 Run crosswalk diagnostics and show problems
```bash
coclab xwalk-diagnostics --crosswalk data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet --show-problems
```
**Pass criteria**
- Diagnostics summary prints.
- Coverage ratios are near 1.0 for most CoCs.
- Problems (if any) are clearly listed and attributable.

**Flag**
- Many CoCs below coverage threshold (suggests geometry mismatch, tract vintage mismatch, or broken intersection logic).
- Max contribution anomalies (e.g., one tract dominating a large CoC unexpectedly).

### 3.3 Diagnostics threshold stress test
```bash
coclab xwalk-diagnostics --crosswalk data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet   --coverage-threshold 0.98 --max-contribution 0.20 --show-problems
```
**Pass criteria**
- Output still coherent and deterministic.
- “Problem” classification changes appropriately with tighter thresholds.

---

## 4) Build CoC Measures (Baseline with Embedded Denominators)

### 4.1 Build measures for boundary vintage and ACS vintage
```bash
coclab build-measures --boundary 2025 --acs 2022
```
**Pass criteria**
- Measures file written, e.g. `data/curated/measures/coc_measures__2025__2022.parquet`.
- Output includes summary stats.
- Measures contain expected key fields: at least `coc_id` and `total_population` (used later in crosscheck).

**Flag**
- Missing `total_population` column in measures output.
- Non-unique `coc_id` keys.

### 4.2 Idempotency: rebuild measures
```bash
coclab build-measures --boundary 2025 --acs 2022
```
**Pass criteria**
- Re-run does not change results unexpectedly.
- If it overwrites, it should do so deterministically.

---

## 5) NEW: Direct ACS Tract-Population Ingest + Rollup + Crosscheck

These tests validate:
1) tract-level ACS population ingest,
2) rollup to CoC populations via crosswalk,
3) crosscheck vs. `coc_measures.total_population`,
4) exit-code semantics for automation safety.

### 5.1 Ingest tract-level ACS population (B01003)
```bash
coclab ingest-acs-population --acs 2019-2023 --tracts 2023
```
**Pass criteria**
- Output file exists: `data/curated/acs/tract_population__2019-2023__2023.parquet` (or equivalent naming).
- Schema includes (minimum):
  - `tract_geoid` (string, 11 digits with leading zeros preserved)
  - `total_population` (non-negative numeric)
  - provenance fields such as `acs_vintage`, `tract_vintage`, `data_source`, `source_ref`, `ingested_at` (as implemented)

**Flag**
- GEOIDs appear numeric (leading zeros lost).
- Many null or negative populations.

### 5.2 Roll up tract population to CoC population (area-weighted)
```bash
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
```
**Pass criteria**
- Output file exists:
  - `data/curated/acs/coc_population_rollup__2025__2019-2023__2023__area.parquet` (or equivalent).
- Output includes (minimum):
  - `coc_id`
  - `coc_population`
  - `coverage_ratio`
  - `max_tract_contribution`
  - `tract_count`

### 5.3 Roll up tract population to CoC population (population-mass-weighted)
```bash
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting population_mass
```
**Pass criteria**
- Same as 5.2, different weighting.
- Values should differ meaningfully from area-weighted for at least some CoCs (otherwise weighting selection may not be applied).

### 5.4 Crosscheck rollup vs measures total_population (area-weighted)
```bash
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
echo $?
```
**Pass criteria**
- Command prints a report including worst deltas.
- Output report parquet written, e.g.:
  - `data/curated/acs/acs_population_crosscheck__2025__2019-2023__2023__area.parquet`
- **Exit code contract (must hold):**
  - `0` if no *errors* (warnings allowed)
  - `2` if *errors* found (threshold exceeded)

**Flag**
- Prints “ERROR” but exits `0`.
- Warnings incorrectly trigger exit `2`.

### 5.5 Crosscheck threshold stress test (tight thresholds)
```bash
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area   --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
echo $?
```
**Pass criteria**
- Exit code reflects thresholds:
  - `2` if any CoCs breach error thresholds or minimum coverage policy.
- The report should explicitly list which rows triggered warnings vs errors.

### 5.6 Crosscheck (population_mass) compares sensibly
```bash
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting population_mass   --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
echo $?
```
**Pass criteria**
- Similar overall behavior to area-weighted, but deltas may differ.
- Any systematic improvement/degradation should be explainable (coverage and weighting).

### 5.7 One-shot verify (ingest → rollup → crosscheck)
```bash
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area   --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
echo $?
```
**Pass criteria**
- Produces the same artifacts as running steps 5.1–5.5 manually.
- Preserves the same exit code discipline (0 vs 2).
- Does not silently skip steps without reporting.

**Flag**
- Manual pipeline fails but verify passes (or vice versa) without clear explanation.
- Verify uses defaults inconsistent with individual subcommands.

### 5.8 Force behavior (fresh rebuild)
Run each of the above with `--force` (where supported), and confirm outputs are regenerated and timestamps/provenance update.

Examples:
```bash
coclab ingest-acs-population --acs 2019-2023 --tracts 2023 --force
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area --force
```
**Pass criteria**
- Outputs are re-materialized.
- The console indicates forced rebuild explicitly.

---

## 6) Boundary Drift and Panel Integrity (Optional but Recommended)

### 6.1 Compare vintages for boundary drift
```bash
coclab show vintage-diffs -v1 2024 -v2 2025
```
**Pass criteria**
- Output categorizes CoCs into added/removed/changed/unchanged.
- Counts sum correctly.

**Flag**
- Non-deterministic diff results between repeated runs.

### 6.2 Build panel and run panel diagnostics (if implemented)
```bash
coclab build-panel --start 2018 --end 2024
coclab panel-diagnostics --panel data/curated/panels/coc_panel__2018_2024.parquet --format text
```
**Pass criteria**
- Primary keys (CoC/year) are unique and present.
- Missingness summaries are coherent.
- Boundary-change flags align with compare-vintages.

---

## 7) Common Failure Patterns to Record Precisely

When something fails, categorize it:

1. **Vintage mismatch** (boundary vs tracts vs ACS)
   - Symptoms: low coverage, huge deltas, many missing joins.

2. **ID normalization** (GEOID leading zeros, CoC id formatting)
   - Symptoms: join failures, zero-pop rollups.

3. **Caching / force semantics inconsistent**
   - Symptoms: `--force` doesn’t rebuild or only partially rebuilds.

4. **Exit code discipline broken**
   - Symptoms: errors do not flip exit code to 2, or warnings flip it incorrectly.

5. **Definition mismatch between measures and direct ACS**
   - Symptoms: systematic deltas across many CoCs even when coverage is high.
   - Potential causes: different weighting method, different ACS concept/vintage, or measures not truly using B01003.

---

## 8) Deliverables for the Evaluation Report

Provide:

- A checklist-style pass/fail table for each test section.
- The full console log for the following commands:
  - `build-xwalks`, `diagnostics`, `build-measures`,
  - `ingest-acs-population`, `rollup-acs-population` (both weightings),
  - `crosscheck-acs-population` (both weightings),
  - `verify-acs-population`.
- For each key parquet output, include:
  - file path
  - row count
  - sample 5 rows (head)
  - schema (columns + dtypes)

Key parquets:
- tract population: `data/curated/acs/tract_population__*.parquet`
- CoC rollups: `data/curated/acs/coc_population_rollup__*.parquet`
- crosscheck report: `data/curated/acs/acs_population_crosscheck__*.parquet`
- measures: `data/curated/measures/coc_measures__*.parquet`

---

## Appendix: Suggested “Default Verification Run” (Copy/Paste)

```bash
# Baseline
coclab --help
pytest tests/test_smoke.py -v

# Foundations
coclab ingest-boundaries --source hud_exchange --vintage 2025
coclab ingest-census --year 2023
coclab build-xwalks --boundary 2025 --tracts 2023
coclab xwalk-diagnostics --crosswalk data/curated/xwalks/coc_tract_xwalk__2025__2023.parquet --show-problems

# Measures baseline
coclab build-measures --boundary 2025 --acs 2022

# New ACS population validation pipeline
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area   --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
echo $?
```
