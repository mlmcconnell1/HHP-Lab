# 2010 block population and geometry inputs

These paired Census inputs support a block-level historical comparison using 2010 boundaries and 2010 decennial population. The population numerator/denominator source is the 2010 PL 94-171 redistricting file; block geometry is 2010 TIGER/Line tabulation blocks.

## Canonical artifacts

| Artifact | Canonical path | Rows | File size | Byte SHA-256 |
|---|---|---:|---:|---|
| PL 94-171 block population | `data/curated/census/pl_blocks__N2010xK2010.parquet` | 11,155,486 | 69,859,266 bytes | `499bae3cb56e60819827dbad22d78e7395a71f8d3908c13df504ed9435695642` |
| TIGER block geometry | `data/curated/tiger/blocks__K2010.parquet` | 11,155,486 | 9,911,349,845 bytes | `62b260e7afd207262242a5a02d2ed923f74e294f75494b112f2e75009c1e6d26` |

The byte hashes above were independently computed for the canonical files and their external originals under `/home/matt/HHP-Output/historical-site-selection/population-2010/`; each pair matched. Canonical and external row counts, schemas, and embedded `hhplab_provenance` metadata also match.

Both inputs cover the same 52 FIPS: the 50 states, DC, and Puerto Rico. They contain 11,155,486 full 2010 block GEOIDs with exact per-state ID-set parity. PL `POP100` sums to **312,471,327**. The audit found **4,897,539 true zero-population blocks**, with no missing or negative population values.

## Sources and provenance

The PL ingest uses the public Census bulk PL 94-171 state ZIPs, so it does not require a Census API key. Its retained raw files and per-state source manifest are under:

`/home/matt/HHP-Output/historical-site-selection/population-2010/raw/census_pl/2010/`

The manifest is `manifest.json`. It records each state's source URL, raw path, hash, byte size, block count, population sum, and county-total count. PL Parquet provenance records the same manifest path, all 52 per-state source hashes, state population totals, the state FIPS inventory, and 3,221 county totals. Its source URL template is:

`https://www2.census.gov/programs-surveys/decennial/2010/data/01-Redistricting_File--PL_94-171/<State>/<abbr>2010.pl.zip`

TIGER raw state ZIPs are retained under `/home/matt/HHP-Output/historical-site-selection/population-2010/raw/tiger/2010/blocks/`; normalized state parts are under `/home/matt/HHP-Output/historical-site-selection/population-2010/curated/tiger/blocks__K2010_parts/`. The final geometry provenance contains all 52 raw paths and all 52 part paths, an empty missing-state list, the aggregate source byte size (6,989,560,451), and the ingest's combined source-content hash `a4c0953e5ffd553e225a9cd72942821bb9fa41fa7e70da7a1e80d17cec876588`. Its URL template is:

`https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_<state>_tabblock10.zip`

The external directory is the retained raw and resumable build record; the canonical Parquet files live in the repository paths listed above.

## Reproducing the ingests

From the repository root, these native CLI commands write the canonical artifacts and use the retained raw root for resumable source storage:

```bash
uv run hhplab ingest pl-blocks \
  --decennial 2010 \
  --blocks 2010 \
  --output-dir data/curated/census \
  --raw-root /home/matt/HHP-Output/historical-site-selection/population-2010/raw \
  --json

uv run hhplab ingest block-geometry \
  --year 2010 \
  --output-dir data/curated/tiger \
  --raw-root /home/matt/HHP-Output/historical-site-selection/population-2010/raw \
  --json
```

The PL command downloads the official public bulk files when a state snapshot is absent. TIGER downloads are state-sharded and can resume from retained raw ZIPs and normalized parts. The commands above leave 2020 inputs on their existing paths.

## Validation

The PL parser validated nonmissing, nonnegative `POP100`, rejected duplicate block GEOIDs and duplicate state/county totals, and checked state and county population sums against the PL summary rows. The completed audit verified 52/52 FIPS, 11,155,486 rows in each artifact, exact per-state GEOID set parity, and the 312,471,327 national population sum.

Focused validation passed with `uv run pytest tests/test_pl_block_population.py tests/test_tiger_tracts.py -q` (44 passed). The combined repository run also passed 521 tests.
