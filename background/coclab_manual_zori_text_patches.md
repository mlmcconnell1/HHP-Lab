# Text Patches for CoC Lab Manual (ZORI Rent-to-Income Integration)

This document contains **ready-to-paste Markdown text patches** for the CoC Lab manual, addressing minor clarity gaps identified after the ZORI rent-to-income panel integration.

Each patch includes:
- **Recommended insertion location**
- **Exact Markdown text**
- **Purpose / rationale**

These patches are non-breaking and purely documentary.

---

## Patch 1: Explicit ZORI → Year Alignment Rule

### Location
**Methodology section**, immediately after the first description of yearly ZORI aggregation or the first mention of `pit_january`.

### Insert the following text

```markdown
### Yearly Alignment of ZORI to PIT Counts

Unless otherwise specified, ZORI is collapsed to yearly values using the **January observation** of each calendar year. This choice aligns the rent measure with HUD Point-in-Time (PIT) counts, which are conducted in January.

Alternative yearly collapse methods (e.g., calendar-year mean or median) may be used for sensitivity analysis, but January-aligned ZORI is the default for all standard CoC Lab panels and examples.
```

### Rationale
- Makes the temporal alignment decision explicit and normative
- Prevents silent confusion between January vs annual averages
- Matches HUD PIT timing assumptions

---

## Patch 2: Panel Output Naming When ZORI Is Included

### Location
**Panel Construction or Output Files section**, near the description of `build-panel` outputs.

### Insert the following text

```markdown
### Panel Naming with ZORI-Enhanced Outputs

When the `--include-zori` option is enabled in `coclab build-panel`, the resulting panel file includes a `__zori` suffix in its filename. This convention distinguishes panels that include rent-based affordability measures from panels built without rent data.

Example:

```
data/curated/panels/coc_panel__2018_2024__zori.parquet
```

This naming convention supports side-by-side comparison of panels built under different analytic assumptions.
```

### Rationale
- Clarifies expectations as panel variants proliferate
- Encourages reproducible comparison across panels
- Avoids accidental mixing of rent and non-rent panels

---

## Patch 3: Explicit Analytic Universe Restriction for `rent_to_income`

### Location
**Panel Variables or Derived Measures section**, immediately after the definition of `rent_to_income`.

### Insert the following text

```markdown
### Analytic Universe for Rent-to-Income Measures

Analyses that use the `rent_to_income` variable **must be restricted** to CoC-year observations where `zori_is_eligible == True`.

CoC-years that fail ZORI eligibility criteria (e.g., insufficient coverage of underlying counties) have `rent_to_income` set to null and should not be included in rent-affordability inference. No imputation is performed for ineligible CoCs.
```

### Rationale
- Makes the eligibility rule operational, not just descriptive
- Protects users from inadvertently including invalid observations
- Aligns documentation with actual panel semantics

---

## Patch 4 (Optional but Recommended): Interpretation Notes for ZORI-Based Affordability

### Location
**Interpretation, Caveats, or Notes section** near the end of the manual, or immediately after the rent-to-income discussion.

### Insert the following text

```markdown
### Interpretation Notes: ZORI-Based Rent Affordability

- `rent_to_income` reflects **market asking rents** derived from Zillow listings, not observed lease rents.
- ZORI coverage is systematically lower in rural, tribal, and Puerto Rico CoCs due to limited rental listings; this is a data availability constraint, not a modeling choice.
- As a result, analyses using `rent_to_income` primarily reflect housing market dynamics in urban and suburban CoCs, which account for the majority of the national homeless population.
```

### Rationale
- Anticipates reviewer and policymaker questions
- Frames exclusions as data limitations, not analytic bias
- Improves interpretability without weakening conclusions

---

## Summary

These patches:
- Do **not** change any code or behavior
- Improve clarity, reproducibility, and reviewer trust
- Make implicit assumptions explicit

They can be applied independently or together, but applying all four is recommended for publication-grade documentation.
