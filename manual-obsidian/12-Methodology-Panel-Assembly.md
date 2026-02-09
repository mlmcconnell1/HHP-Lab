# Methodology: Panel Assembly (Phase 3)

This section documents how CoC × year analysis panels are constructed by joining PIT counts with ACS demographic measures.

## Panel Assembly Algorithm

Panel assembly follows these steps for each year in the requested range:

1. **Load PIT counts** from canonical Parquet files
2. **Apply alignment policy** to determine boundary and ACS vintages
3. **Load ACS measures** for the aligned vintage
4. **Join** PIT and ACS data by CoC ID
5. **Detect boundary changes** from prior year
6. **Compute coverage ratio** from crosswalk weights

## Alignment Policies

Alignment policies are **pure functions** that map PIT years to data vintages. Using [[08-Temporal-Terminology|shorthand notation]]:

| Policy | Rule | Notation |
|--------|------|----------|
| Boundary vintage | `f(pit_year) = pit_year` | P{Y}@B{Y} — **period-faithful** alignment |
| ACS vintage | `f(pit_year) = pit_year - 1` | A{Y-1} joined via **vintage gap** |

**Example:** PIT year 2024 (P2024) uses:
- Boundary vintage B2024 — period-faithful
- ACS vintage A2023 (covering 2019-2023) — 1-year vintage gap

Policies are recorded in panel provenance metadata for reproducibility.

## Alignment Type Field

Panel outputs include `alignment_type` to make boundary alignment explicit:

| Value | Meaning |
|-------|---------|
| `period_faithful` | Boundary vintage matches PIT year (P{Y}@B{Y}) |
| `retrospective` | Boundary vintage is newer than PIT year (e.g., P2018@B2025) |
| `custom` | Boundary vintage is older than PIT year or uses non-standard labels |

Mixed policies can yield multiple `alignment_type` values within the same panel.

## Boundary Change Detection

The `boundary_changed` flag indicates a **boundary break**—whether a CoC's boundary differs from the prior year:

```
boundary_changed[coc, year] =
    (boundary_vintage[year] ≠ boundary_vintage[year-1]) OR
    (geom_hash[coc, year] ≠ geom_hash[coc, year-1])
```

First year in panel always has `boundary_changed = False`.

Consecutive years where `boundary_changed = False` represent a **stable span** suitable for trend analysis without discontinuities.

## Coverage Ratio Interpretation

The `coverage_ratio` field reflects crosswalk completeness:

| Value | Interpretation |
|-------|----------------|
| `1.0` | Perfect coverage—all CoC area mapped to tracts |
| `0.95-0.99` | Minor boundary/tract misalignment |
| `< 0.90` | Significant gaps—investigate crosswalk |
| `> 1.0` | Overlapping tract assignments (rare) |

## Panel Diagnostics

The `diagnostics panel` command provides:

1. **Coverage summary** - Min/max/mean coverage by year
2. **Boundary change summary** - CoCs with changes and affected years
3. **Missingness report** - Missing values per column per year
4. **Weighting sensitivity** - Compare area vs population weighting effects

## Known Limitations

### 1. Vintage Gap

The **vintage gap** (ACS vintage Y-1) means demographic data is 1-2 years old relative to PIT counts. Rapidly changing areas may show measurement lag.

### 2. Boundary Change Granularity

Boundary changes are detected at annual resolution. Mid-year changes are assigned to the later vintage.

### 3. Missing Data Handling

CoCs missing from PIT or ACS for a given year are excluded from the panel for that year. Use `missingness_report()` to identify gaps.

---

**Previous:** [[11-Methodology-ZORI-Aggregation]] | **Next:** [[13-Bundle-Layout]]
