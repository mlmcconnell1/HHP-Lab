# MEDSL County Presidential Source

Status: staged for ingest
Version checked: 2026-02-25

## Source

Use MIT Election Data and Science Lab's county presidential returns dataset for
county-native political leaning measures.

| Field | Value |
|-------|-------|
| Dataset title | County Presidential Election Returns 2000-2024 |
| DOI | `https://doi.org/10.7910/DVN/VOQCHQ` |
| Dataverse datafile id | `13573089` |
| Download URL | `https://dataverse.harvard.edu/api/access/datafile/13573089` |
| Version | `20` |
| Release/update date | `2026-02-25` |
| License | CC0 1.0 |
| Local raw path | `data/raw/medsl/countypres_2000-2024.tab` |
| Raw SHA256 | `aac3d9acf9c5b1bd152a3e9b331f4f18dedfac5907cd617886d4f43308d927d1` |
| Raw file size | `9845506` bytes |

The local file has 94,151 rows and 12 columns:
`state`, `county_name`, `year`, `state_po`, `county_fips`, `office`,
`candidate`, `party`, `candidatevotes`, `totalvotes`, `version`, and `mode`.
Election years are 2000, 2004, 2008, 2012, 2016, 2020, and 2024.

The pre-existing `data/raw/medsl/1976-2024-president.csv` is state-level only.
It has `state_fips` but no `county_fips` or county-equivalent identifier, so it
is not suitable for county/MSA analysis.

## Geography Handling

Use `county_fips` as the join key for county-native aggregation. Normalize it as
a zero-padded five-character string before joining to Census county membership
or crosswalk artifacts.

DC is represented as county-equivalent `11001` and can participate in normal
county joins.

Alaska rows are represented with election-district style `county_name` values
and nonstandard `county_fips` values. Do not silently join Alaska rows to
Census county-equivalent geography until an ingest rule explicitly validates the
mapping. The first ingest should surface Alaska coverage diagnostics separately.

Rows with missing `county_fips` are special statewide/federal/UOCAVA/write-in
records for Connecticut, Maine, and Rhode Island. In the staged file there are
52 such rows. Some 2012-2020 rows have positive vote totals, but they still do
not identify a county-equivalent geography. The ingest should exclude those
known non-county rows from county-year measures and report the dropped row count
by state/year.

Missouri `KANSAS CITY` rows use `county_fips == 2938000`, which is not a
five-character county FIPS. Treat these as known non-county place rows and
exclude them from county-native outputs unless a later mapping task explicitly
defines an allocation rule.

The file includes `TOTAL` rows and voting-mode-specific rows. County-year
measure materialization should use `mode == "TOTAL"` after validating that the
expected county/year coverage exists; it should not sum all modes together.

## Source Registry Entry

Record the staged raw artifact in `data/curated/source_registry.parquet` with:

- `source_type`: `medsl`
- `source_name`: `MEDSL County Presidential Election Returns 2000-2024`
- `source_url`: `https://dataverse.harvard.edu/api/access/datafile/13573089`
- `raw_sha256`: `aac3d9acf9c5b1bd152a3e9b331f4f18dedfac5907cd617886d4f43308d927d1`
- `file_size`: `9845506`
- `local_path`: `data/raw/medsl/countypres_2000-2024.tab`

Metadata should include the DOI, Dataverse datafile id, release/update date,
version, license, citation note, year coverage, and the geography decisions
above. The default registry path is ignored with `data/`, so this document is
the committed source contract for recreating the local registry entry.

## Derived County-Year Measures

The canonical county-year political leaning artifact is:

`data/curated/medsl/medsl_president_county__Y2000-2024@C2020.parquet`

It contains one row per `county_fips`/presidential election year. Vote shares
use `totalvotes` as the denominator. Democratic/Republican ratios are null when
the opposite-party denominator is zero or missing. `democratic_margin` is
`democratic_vote_share - republican_vote_share`, and `major_party_vote_share` is
`two_party_votes / totalvotes`.

## Recipe Aggregation Pattern

Recipes should aggregate MEDSL vote counts before computing target-level
political shares or ratios. Treat `democratic_votes`, `republican_votes`,
`two_party_votes`, and `totalvotes` as additive count measures with
`aggregation: sum`. Then derive target-level measures from the summed counts,
for example `democratic_votes / totalvotes`, `republican_votes / totalvotes`,
`democratic_votes / republican_votes`, and `(democratic_votes -
republican_votes) / totalvotes`.

County-level share or ratio columns such as `democratic_vote_share` can be
carried as descriptive covariates with `weighted_mean` only when the weighting
choice is explicit and defensible. They should not be the default way to build
target-level election shares, because averaging county ratios can diverge from
the ratio of aggregated vote counts.
