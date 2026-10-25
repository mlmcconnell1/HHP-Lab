# Audit of the cached 2015–2026 MSA ZORI artifact

## Finding

The cached 2015–2026 balanced MSA ZORI file predates two fixes that affect its inputs and failure handling. Its output records zero total population in 2025 and 2026, and zero contributing counties in every year. The 2025 year-suffixed PEP file existed before this output, but the PEP loader did not discover that filename until a later fix; the current curated PEP inputs have no 2026 population year. Together, these facts explain why no county met the balanced requirement of usable ZORI and population in every requested year. The cache provenance records null explicit input paths, so it does not independently prove which exact files the original invocation resolved.

The current PEP discovery includes year-suffixed vintages, and the current MSA aggregator raises an error when any requested population year is absent. Running a 2015–2026 aggregation now therefore stops at the missing 2026 weights before it writes an artifact. The usable cached 2015–2025 counterpart already exists. Do not overwrite either file or run a 2015–2026 rebuild until the curated county population inputs include 2026.

## Cached artifact comparison

Both files were inspected read-only in `/home/matt/Data/hhplab/curated/zori/`.

| Artifact | Rows | Measure schema | Nonmissing rent values | Population/coverage evidence | Provenance `created_at` (UTC) | SHA-256 |
|---|---:|---|---:|---|---|---|
| `zori__msa__Y2015-2026@Mcensusmsa2023xC2023__wpopulation__mpit_january__balanced.parquet` | 4,716 (393 MSAs × 12 years) | Legacy `zori_coc` column | 0 of 4,716 | `county_count` is 0 on every row. Coverage is 0 across 2015–2024; `total_population` is 0 and coverage is null in 2025 and 2026. | `2026-07-05T12:57:09.298418+00:00` | `cdb8615144cff64ac75d08c077a7e7d523eeb68a0e9778bc817845eeff9e9197` |
| `zori__msa__Y2015-2025@Mcensusmsa2023xC2023__wpopulation__mpit_january__balanced.parquet` | 4,323 (393 MSAs × 11 years) | Current `zori` column | 2,178; 198 MSAs have a rent value in each year | PEP totals and county contributions are present. The result does not require every MSA to have ZORI; the balanced county set is per MSA. | `2026-07-05T15:51:31.756329+00:00` | `2406fb83fe058d20b8fdd824ce763d777d40b49cd9bed34f3e6bcd351ea4db2b` |

The two artifacts' `county_population_path` and `zori_path` provenance fields are null. In this CLI those fields record only explicit path options; null means the caller used default discovery, not that the aggregation ran without those inputs. The fields do not record which files discovery resolved, so the historical input snapshot cannot be recovered from those fields alone. The later 2015–2025 artifact has the expected `zori` measure schema and nonmissing values; its provenance still lacks resolved input paths.

The county ZORI input currently discovered by default is `zori__county__Z2026.parquet`, which contains January 2015 through January 2026. Default PEP loading currently combines the county PEP vintages and yields years 2010–2025. Thus the current inputs support the ZORI side for 2026, but do not provide a 2026 population weight.

## Code and creation timeline

| UTC time | Evidence | Consequence |
|---|---|---|
| 2026-05-09 21:11 | `pep_county__v2025__y2020-2025.parquet` provenance | The year-suffixed PEP source file existed before the bad ZORI artifact. |
| 2026-07-05 12:57 | Empty 2015–2026 ZORI artifact provenance | Artifact was created before the PEP discovery and population-year checks below. Its output lacks the current `zori` schema column. |
| 2026-07-05 13:03:48 | Commit `3b0549120a8b703ac47da5ba4464704f32aa97c8`, “Fix PEP vintage discovery to include year-suffixed files” | Changed the loader glob from `pep_county__v[0-9][0-9][0-9][0-9].parquet` to `pep_county__v[0-9][0-9][0-9][0-9]*.parquet`, allowing the available 2025 file to be discovered. This fix postdates the empty artifact by about seven minutes. |
| 2026-07-05 15:15:43 | Commit `f8a55bf5ce9f877263ccf4f7b4c01259f4e1cfa3`, “fix historical boundaries and curated validation” | Added a check in `aggregate_yearly_zori_to_msa` that raises when requested years are missing from county population; added conversion to the current yearly artifact schema (`zori_coc` → `zori`). Both changes postdate the empty artifact. |
| 2026-07-05 15:51 | 2015–2025 artifact provenance | The 2015–2025 counterpart was written after both fixes; its schema and nonmissing counts differ from the older 2015–2026 file. |

Current aggregation flow:

1. `load_zori("county")` uses `discover_zori_ingest`, which selects the county ingest with the largest `Z` vintage when no explicit ZORI path is supplied.
2. `load_pep_county(None)` discovers all `pep_county__v*.parquet` files, sorts latest vintages first, and keeps the latest record per county-year. The current output has population years through 2025.
3. `aggregate_yearly_zori_to_msa(..., years=...)` now verifies that every requested year occurs in county population data. It raises with 2026 as missing before the CLI reaches `write_parquet_with_provenance`.

The key aggregation detail is the balanced set calculation: a county is included only if it has nonmissing ZORI and positive population in every requested year. The previous code had no missing-population-year rejection. Missing support in 2025 and 2026 made the complete county-key set empty, so the later per-year loop filtered every MSA's counties out for all requested years. That explains the zero values across 2015–2026, rather than only a null 2026 observation.

## Downstream path and validation behavior

- `discover_zori_ingest` discovers county input files only; it does not choose among MSA yearly outputs.
- The generated `zori_msa` recipe dataset uses an explicit path based on its requested year range. A recipe requesting 2015–2026 points at the unusable 2015–2026 filename; there is no automatic fallback to the 2015–2025 artifact.
- The recipe declares measures `zori` and `coverage_ratio`. The old artifact has `zori_coc` instead of `zori`, so recipe preflight's measure probe reports `zori` missing; the executor also validates requested measure columns before applying the resample.
- `hhplab validate curated-layout` checks the MSA ZORI filename pattern, not the parquet schema or data values. That check can accept the unusable filename. The recipe measure check catches the legacy-column mismatch, but an all-null `zori` artifact with the right schema would require a separate value/support check to detect.

## Follow-up and disposition

No production rebuild is warranted from the currently available population inputs. The valid 2015–2025 counterpart already exists; the 2015–2026 request is not supported until the curated PEP county input includes 2026. No output path for a replacement 2015–2026 artifact is proposed because creating a correctly populated file is not currently possible.

No code change is needed to prevent another full-year population gap from producing this artifact: the current aggregation guard rejects that case. Two hardening improvements remain useful but are not required to use the 2015–2025 counterpart:

1. Record the resolved county ZORI file and all resolved PEP input paths/vintages in aggregate provenance, including when paths were auto-discovered.
2. Reject or prominently report a balanced aggregation with zero complete county support for every MSA, even when each requested year exists somewhere in the population input. This covers geography-level gaps that the current whole-year presence check cannot detect.

The related longitudinal diagnostics derive MSA-year ZORI from county data with explicit population and membership inputs. They do not consume either cached MSA artifact, and this audit did not modify their data or outputs.
