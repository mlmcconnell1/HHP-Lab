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

## Coverage and Limitations

The committed MEDSL workflow is county-native and quadrennial. The supported
election years are 2000, 2004, 2008, 2012, 2016, 2020, and 2024. Rows outside
those years are rejected by the ingest/materialization path unless a caller
explicitly changes the expected-year contract.

County-equivalent geographies should be treated as follows:

- DC uses county-equivalent `11001` and can join normally to Census county or
  MSA membership artifacts.
- Alaska election-district rows are excluded from county-native outputs until a
  validated Census county-equivalent allocation rule exists.
- Known non-county rows with missing `county_fips` for Connecticut, Maine, and
  Rhode Island are dropped and counted in provenance.
- Missouri `KANSAS CITY` place rows with `county_fips == 2938000` are dropped
  and counted in provenance.

These exclusions mean a national aggregate from the curated county artifact is
not a certified national popular-vote total. It is designed for geography-join
panel features where rows must identify a county or county-equivalent key.

## Derived County-Year Measures

The canonical county-year political leaning artifact is:

`data/curated/medsl/medsl_president_county__Y2000-2024@C2020.parquet`

It contains one row per `county_fips`/presidential election year. Vote shares
use `totalvotes` as the denominator. Democratic/Republican ratios are null when
the opposite-party denominator is zero or missing. `democratic_margin` is
`democratic_vote_share - republican_vote_share`, and `major_party_vote_share` is
`two_party_votes / totalvotes`.

Measure definitions:

| Column | Definition |
|--------|------------|
| `democratic_votes` | Sum of candidate votes where `party_simplified == DEMOCRAT` within the county/year. |
| `republican_votes` | Sum of candidate votes where `party_simplified == REPUBLICAN` within the county/year. |
| `two_party_votes` | `democratic_votes + republican_votes`. |
| `totalvotes` | MEDSL county/year total votes from the `TOTAL` mode rows. |
| `democratic_vote_share` | `democratic_votes / totalvotes`; null when `totalvotes` is zero or missing. |
| `republican_vote_share` | `republican_votes / totalvotes`; null when `totalvotes` is zero or missing. |
| `democratic_republican_vote_ratio` | `democratic_votes / republican_votes`; null when `republican_votes` is zero or missing. |
| `republican_democratic_vote_ratio` | `republican_votes / democratic_votes`; null when `democratic_votes` is zero or missing. |
| `democratic_margin` | `democratic_vote_share - republican_vote_share`; null when either share is null. |
| `major_party_vote_share` | `two_party_votes / totalvotes`; null when `totalvotes` is zero or missing. |

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

For target geographies such as MSAs and CoCs, recommended recipe measures are:

- `democratic_votes`, `republican_votes`, `two_party_votes`, and `totalvotes`
  with `aggregation: sum`.
- `democratic_vote_share` derived from `democratic_votes / totalvotes`.
- `republican_vote_share` derived from `republican_votes / totalvotes`.
- `democratic_republican_vote_ratio` derived from
  `democratic_votes / republican_votes`.

The committed examples show this pattern in:

- `recipes/examples/county-medsl-pep-2024.yaml`
- `recipes/examples/msa-census-pit-pep-medsl-2024.yaml`

## Temporal Alignment

MEDSL presidential returns are observed only in presidential election years.
Recipes must make the temporal interpretation explicit:

- Election-year-only panels should restrict `universe` to MEDSL years such as
  2024 and join directly.
- Multi-year annual panels can leave MEDSL missing for non-election years if the
  regression design treats political leaning as observed only during elections.
- Carry-forward, carry-backward, or interpolation strategies should be applied
  in a separate, clearly named derived artifact or recipe step when needed.
  Do not silently broadcast county ratios across non-election years in the
  source artifact.
- January-aligned homelessness panels should document whether the election
  measure is intended as a lagged political context, a same-calendar-year
  outcome, or a downstream post-election covariate.

## Citation

Use the MIT Election Data and Science Lab / Harvard Dataverse source citation
for any panel that includes these measures:

`MIT Election Data and Science Lab, County Presidential Election Returns 2000-2024, Harvard Dataverse, https://doi.org/10.7910/DVN/VOQCHQ`

The local source contract was checked against Dataverse version `20`, released
or updated on `2026-02-25`, with CC0 1.0 licensing. Downstream publications or
reproducibility bundles should include the raw SHA256 and curated artifact
provenance metadata from the parquet file.
