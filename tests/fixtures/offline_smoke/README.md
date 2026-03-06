# Offline Smoke Fixture

Small synthetic fixture for fast, deterministic offline pipeline tests.

Contents:
- `boundaries.csv`: two synthetic CoC boundary polygons (WKT)
- `tracts.csv`: three synthetic census tracts (WKT)
- `counties.csv`: two synthetic counties (WKT)
- `acs_2020_2024_tracts_2020.csv`: tract-level ACS-like inputs for aggregation
- `pit_2024.csv`: curated PIT-like inputs for panel assembly

Design goals:
- no network dependency
- runs quickly in CI/local runs
- stable geometry overlaps for repeatable crosswalk/aggregation behavior
