# Temporal Terminology

This chapter defines shorthand notation and vocabulary for describing the various dates, vintages, and temporal relationships in CoC Lab datasets.

## Core Temporal Concepts

| Concept | Definition |
|---------|------------|
| **Vintage** | A version of a dataset as released/published, not necessarily when the underlying data was collected |
| **Reference Year** | The year a measure purports to describe (e.g., PIT 2023 describes homelessness in January 2023) |
| **Collection Window** | The actual time span of data collection (ACS 2022 vintage = 2018–2022 collection window) |

## Shorthand Notation

Single-letter prefixes identify dataset types:

| Concept | Shorthand | Example | Notes |
|---------|-----------|---------|-------|
| CoC boundary version | **B**{year} | B2025 | The geographic shape definition |
| Census tract geometry | **T**{year} | T2023 | TIGER tract shapes |
| Census county geometry | **C**{year} | C2023 | TIGER county shapes |
| ACS vintage (end year) | **A**{year} | A2022 | Implies 5-year window ending that year |
| PIT count year | **P**{year} | P2024 | The January count year |
| ZORI yearly series | **Z**{year} | Z2024 | Yearly-collapsed ZORI (default: January alignment) |
| Panel year | **Y**{year} | Y2023 | The "as-of" year for analysis |

### ACS Collection Windows

ACS 5-year estimates have an implicit collection window. The vintage year is the *end* of that window:

| Notation | Collection Window | Release Year (typical) |
|----------|-------------------|------------------------|
| A2022 | 2018–2022 | Late 2023 |
| A2023 | 2019–2023 | Late 2024 |
| A2024 | 2020–2024 | Late 2025 |

## ZORI Temporal Conventions

ZORI is published as a **monthly** series. In CoC Lab notation:

- **Z{year}** refers to the **yearly-collapsed** ZORI value for that year.
- The default collapse method is **January alignment** (`pit_january`) to match PIT timing.
- If the collapse method matters, annotate it explicitly (e.g., `Z2024[pit_january]`, `Z2024[calendar_mean]`, `Z2024[calendar_median]`).

## Compound Notation

When describing which vintages were combined in a derived dataset, use `@` for "analyzed using" and `×` for crosswalk joins:

| Notation | Meaning |
|----------|---------|
| **P2024@B2025** | 2024 PIT counts analyzed using 2025 boundaries |
| **A2022@B2025** | ACS 2022 aggregated to 2025 CoC boundaries |
| **A2022@B2025×T2023** | ACS 2022 aggregated to 2025 CoC boundaries via 2023 tract crosswalk |
| **P2020@B2025×T2023** | 2020 PIT re-aligned to 2025 boundaries using 2023 tracts |
| **Z2024@B2025×C2023** | 2024 ZORI aggregated to 2025 CoC boundaries via 2023 county crosswalk |

### Reading Compound Notation

- The first element is the **source data** being analyzed
- `@B{year}` specifies the **target CoC boundaries**
- `×T{year}` or `×C{year}` specifies the **intermediary geometry** used for spatial joins
- ZORI aggregation uses ACS-based weights; note the weight vintage in prose when needed (e.g., "weights A2023")

## Temporal Mismatch Terminology

These terms describe common scenarios where vintages don't align:

| Term | Definition | Example |
|------|------------|---------|
| **Retrospective alignment** | Applying newer boundaries to older data | Analyzing P2018 using B2025 |
| **Period-faithful** | Using boundaries that were in effect when data was collected | Analyzing P2018 using B2018 |
| **Vintage gap** | Normal lag between data collection and availability | A2022 is latest available for B2025 analysis |
| **Geometry mismatch** | Tract/county geometry differs from boundary vintage | T2020 tracts crossed with B2025 boundaries |

### When Mismatches Matter

- **Retrospective alignment** is necessary for consistent time-series analysis but may misattribute counts if CoC boundaries changed significantly
- **Period-faithful** analysis preserves original reporting relationships but complicates cross-year comparisons
- **Vintage gaps** are unavoidable; ACS data typically lags 2+ years behind boundary releases
- **Geometry mismatches** introduce small interpolation errors at tract boundaries that changed between vintages

## Temporal Flags and Spans

Terms for describing temporal characteristics of panel data:

| Term | Definition | Use Case |
|------|------------|----------|
| **Boundary break** | A CoC's geography changed between consecutive years | Flagging discontinuities in time series |
| **Stable span** | Consecutive years with identical boundaries | Identifying periods safe for trend analysis |
| **Backfilled** | Data re-associated with boundaries published after the original report | Documenting retrospective alignment |

### In Schema

These concepts map to schema fields:

```
COC_PANEL.boundary_changed = True  →  Boundary break from prior year
COC_PANEL.boundary_vintage_used   →  Documents which B was applied
COC_PANEL.acs_vintage_used        →  Documents which A was applied
```

## Usage Examples

### In Documentation

> "The 2018–2024 panel uses retrospective alignment (P{year}@B2025×T2023) to enable consistent time-series analysis. Years with boundary breaks are flagged."

### In Filenames

The shorthand maps to existing filename conventions:

| Shorthand | Filename Pattern |
|-----------|------------------|
| B2025 | `coc_boundaries__2025.parquet` |
| A2022@B2025×T2023 | `coc_measures__2025__2022.parquet` (boundary__acs) |
| P2024 | `pit_counts__2024.parquet` |

### In Provenance Metadata

```json
{
  "boundary_vintage": "2025",
  "tract_vintage": "2023",
  "acs_vintage": "2022",
  "notation": "A2022@B2025×T2023"
}
```

---

**Previous:** [[06-Data-Model]] | **Next:** [[08-Workflows]]
