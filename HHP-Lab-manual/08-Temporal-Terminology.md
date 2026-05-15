# Temporal Terminology

This chapter defines shorthand notation and vocabulary for describing the various dates, vintages, and temporal relationships in HHP-Lab datasets.

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
| ZORI ingest coverage tag | **Z**{year} | Z2026 | Maximum year present in an ingested monthly ZORI file |
| Panel year | **Y**{year} | Y2023 | The "as-of" year for analysis |
| Definition version | **D**{version} | Dglynnfoxv1 | Synthetic geography definition (see below) |

### ACS Collection Windows

ACS 5-year estimates have an implicit collection window. The vintage year is the *end* of that window:

| Notation | Collection Window | Release Year (typical) |
|----------|-------------------|------------------------|
| A2022 | 2018–2022 | Late 2023 |
| A2023 | 2019–2023 | Late 2024 |
| A2024 | 2020–2024 | Late 2025 |

## ZORI Temporal Conventions

ZORI is published as a **monthly** series. In HHP-Lab notation:

- `zori__county__Z2026.parquet` means the ingested monthly file contains observations through 2026.
- Yearly-collapsed CoC and metro outputs do **not** use `Z{year}` in filenames; they are keyed by ACS/crosswalk vintages and include `__m{method}`.
- The default yearly collapse method is **January alignment** (`pit_january`) to match PIT timing.
- If the collapse method matters in prose, annotate it explicitly (for example `pit_january`, `calendar_mean`, `calendar_median`).

### Definition Version Notation

The `D{version}` prefix identifies synthetic analysis geographies that are defined by researcher membership rules rather than by a single boundary vintage. The version string is normalized for filenames by removing underscores (e.g., `glynn_fox_v1` → `glynnfoxv1`).

| Notation | Definition Version | Source |
|----------|--------------------|--------|
| Dglynnfoxv1 | `glynn_fox_v1` | Glynn & Fox (2019), 25 metro analysis units |

Metro-derived filenames embed `__metro__` as a geography segment and use `D` instead of `B`:

| Shorthand | Filename |
|-----------|----------|
| metro PIT P2024 | `pit__metro__P2024@Dglynnfoxv1.parquet` |
| metro panel Y2020-2024 | `panel__metro__Y2020-2024@Dglynnfoxv1.parquet` |
| metro measures A2023 | `measures__metro__A2023@Dglynnfoxv1xT2020.parquet` |

## Compound Notation

When describing which vintages were combined in a derived dataset, use `@` for "analyzed using" and `×` for crosswalk joins:

| Notation | Meaning |
|----------|---------|
| **P2024@B2025** | 2024 PIT counts analyzed using 2025 boundaries |
| **A2022@B2025** | ACS 2022 aggregated to 2025 CoC boundaries |
| **A2022@B2025×T2020** | ACS 2022 aggregated to 2025 CoC boundaries via 2020-era tracts |
| **P2020@B2025×T2020** | 2020 PIT re-aligned to 2025 boundaries using 2020-era tract geometry where an intermediary tract join is involved |
| **Z2024@B2025×C2023** | 2024 ZORI aggregated to 2025 CoC boundaries via 2023 county crosswalk |

### Metro Compound Notation

When the target analysis geography is metro, `@D{version}` replaces `@B{year}`:

| Notation | Meaning |
|----------|---------|
| **P2024@Dglynnfoxv1** | 2024 PIT counts aggregated to Glynn/Fox metro definitions |
| **A2023@Dglynnfoxv1×T2020** | ACS 2023 aggregated to metros via 2020-era tracts |
| **Z2024@Dglynnfoxv1×C2023** | 2024 ZORI aggregated to metros via 2023 county membership |

### Reading Compound Notation

- The first element is the **source data** being analyzed
- `@B{year}` specifies the **target CoC boundaries**; `@D{version}` specifies a **target metro definition**
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

> "The 2018–2024 panel uses retrospective alignment (A{year-1}@B2025×T2020 for recent ACS vintages) to enable consistent time-series analysis. Years with boundary breaks are flagged."

### In Filenames

The shorthand maps directly to filenames (use `x` instead of `×` for ASCII compatibility):

| Shorthand | Filename |
|-----------|----------|
| B2025 | `coc__B2025.parquet` |
| A2022@B2025×T2020 | `measures__A2022@B2025xT2020.parquet` |
| P2024 | `pit__P2024.parquet` |

### In Provenance Metadata

```json
{
  "boundary_vintage": "2025",
  "tract_vintage": "2020",
  "acs_vintage": "2022",
  "notation": "A2022@B2025×T2020"
}
```

---

**Previous:** [[07-Data-Model]] | **Next:** [[09-Workflows]]
