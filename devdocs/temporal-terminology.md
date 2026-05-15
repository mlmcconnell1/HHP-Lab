# Temporal Terminology (Background Reference)

> Note: the repository/runtime is now HHP-Lab/`hhplab`. This file is kept in
> its historical form, so older CoC-Lab references may remain intentionally.

This is a compact reference aligned to `HHP-Lab-manual/08-Temporal-Terminology.md`.
Use the manual chapter as the canonical version.

## Core Concepts

| Concept | Definition |
|---------|------------|
| Vintage | A dataset release version, not necessarily collection date |
| Reference Year | Year a measure is intended to represent |
| Collection Window | Actual measurement period (for example, ACS 5-year window) |

## Shorthand

| Concept | Notation | Example |
|---------|----------|---------|
| CoC boundary vintage | `B{year}` | `B2025` |
| Tract geometry vintage | `T{year}` | `T2023` |
| County geometry vintage | `C{year}` | `C2023` |
| ACS vintage end year | `A{year}` | `A2023` |
| PIT count year | `P{year}` | `P2024` |
| ZORI yearly series | `Z{year}` | `Z2024` |
| Analysis/panel year | `Y{year}` | `Y2024` |

## Compound Notation

Use `@` for "analyzed on" and `x` for crosswalk geometry:

- `P2024@B2025`
- `A2023@B2025xT2023`
- `Z2024@B2025xC2023`

## Timing Caveats

- PIT is point-in-time in January.
- PEP is as-of July 1.
- ACS is a 5-year rolling estimate with lagged publication.
- Alignment choices must be explicit in aggregation metadata.

For full examples and provenance conventions, see `HHP-Lab-manual/08-Temporal-Terminology.md`.
