# Historical 2010 site-selection comparison

The available 2010 CoC polygons, MSA definitions in effect in 2010, and 2010 Census population select more sites than the frozen modern geography: **80 versus 72 at 95% overlap**, and **71 versus 64 at 99%**. These historical counts are provisional because the CoC archive has unresolved coverage differences and a quarantined Massachusetts shared-jurisdiction polygon.

## Counts

| Overlap | Holdout policy | 2010 sites | Modern sites | Difference | 2010 distinct CoCs | Modern distinct CoCs |
|---|---|---:|---:|---:|---:|---:|
| 95% | No holdouts | 80 | 72 | +8 | 129 | 107 |
| 99% | No holdouts | 71 | 64 | +7 | 97 | 84 |
| 95% | Existing four holdout IDs | 78 | 68 | +10 | 124 | 100 |
| 99% | Existing four holdout IDs | 69 | 60 | +9 | 92 | 77 |

The holdouts are Bridgeport–Stamford–Norwalk (`14860`), Sacramento (`40900`), Salem (`41420`), and York–Hanover (`49620`). Bridgeport and Salem pass both historical thresholds and are removed by this policy. Sacramento and York already fail both historical thresholds. All four pass the modern geographic rules before the holdouts are applied. Matching these holdouts by MSA code does not establish identical historical footprints.

The composition changes substantially beyond the net increase. After holdouts, the 95% sets share 55 MSA codes, with 23 selected only historically and 13 only in the modern set. At 99%, they share 49, with 20 historical-only and 11 modern-only. A surviving MSA code can still have changed counties. This comparison changes CoC boundaries, MSA membership, and population together; it does not isolate the contribution of each change.

## Inputs and selection rules

| Input | Historical run | Frozen modern comparison |
|---|---|---|
| CoC boundaries | B2010, 441 available individual polygons | B2024 |
| MSA definitions | December 2009 OMB update, effective during 2010 | July 2023 definitions |
| MSA candidates | 374, including 8 Puerto Rico MSAs | 393, including 8 Puerto Rico MSAs |
| County geometry | 2010 | 2023 |
| Census block geometry and population | 2010 | 2020 |

The historical recipe uses the existing native whole-CoC rules. At threshold T, a CoC is selected for an MSA when at least T of its population falls inside that MSA. Those selected CoCs must together cover at least T of the MSA population. The existing checks also reject missing population and excessive overlapping population coverage. Population measures geographic fit; no PIT or HIC counts are allocated or tested by this recipe.

The Census inputs each contain 11,155,486 blocks across the 50 states, DC, and Puerto Rico. Their GEOID sets match exactly by state; total population is 312,471,327, with no missing or negative values. All 52 state crosswalks were checked against their input/output hashes before the native CLI assembled 1,416 CoC–MSA pairs covering all 374 MSAs and 432 CoCs. A CoC without an MSA intersection need not appear in this crosswalk.

See [the CoC acquisition and reconciliation](historical-coc-2010-acquisition.md), [historical MSA inputs](historical-msa-2010-inputs.md), and [block population and geometry audit](historical-population-2010-inputs.md) for source evidence, hashes, and reproduction details.

## Unresolved source coverage

B2010 is explicitly marked `partial_unresolved`. The native registration listing and archive are different source rosters: twelve listed IDs lack archive polygons and three archive IDs are absent from the listing. The actual native HIC roster also differs from the listing. These discrepancies cannot be resolved by substituting modern geometry or treating the registration roster as a count roster.

The extra Massachusetts shared-jurisdiction feature cannot defensibly be assigned to the four individual CoCs named in its ID. It remains quarantined. Screening against later MSA geometry flags Manchester–Nashua (`31700`) and Worcester (`49340`) among selected sites at both thresholds. These are potential effects, not proven historical misassignments. Named-county leads for other missing polygons are flagged in the comparison CSV using 2010 MSA county membership. No complete county list was recovered for NE-503. An unflagged site is not certified unaffected.

The counts above are conditional on the available polygons. They are geographic exploration results and do not establish a longitudinal PIT/HIC sample with complete count histories.

## Artifacts and reproduction

The committed recipe is [msa-coc-historical-sites-2010.yaml](../recipes/msa-coc-historical-sites-2010.yaml). From the repository root, after materializing the documented native inputs:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab generate msa-xwalk \
  --allocation-basis block_population --boundary 2010 \
  --definition-version census_msa_2010 --counties 2010 \
  --blocks 2010 --decennial 2010 --state-shards --json

HHPLAB_NON_INTERACTIVE=1 uv run hhplab build recipe-preflight \
  --recipe recipes/msa-coc-historical-sites-2010.yaml --json

HHPLAB_NON_INTERACTIVE=1 uv run hhplab build recipe \
  --recipe recipes/msa-coc-historical-sites-2010.yaml --json
```

The completed run used `--reuse-shards` only after checking the exact 52-state inventory, all source/part hashes, metadata, and missing-population checks. The native command trusts cached shards, so that verification is required before reuse. Its intermediate state directory was moved intact outside curated after assembly to satisfy curated-layout policy; no state files were deleted.

Native site and CoC-membership Parquet/CSV files and their summary are under `/home/matt/HHP-Output/msa_coc_historical_sites_2010/`. The frozen comparison is under `/home/matt/HHP-Output/msa_coc_overlap_sites_2024/`.

The retained comparison work is under `/home/matt/HHP-Output/historical-site-selection/`:

- `site_count_comparison.json`: counts, code entry/exit lists, source caveats, and input hashes.
- `site_selection_comparison.csv`: both thresholds, every candidate MSA, eligibility/reasons, membership counts, and source-warning leads.
- `compare_site_counts.py`: reproducible comparison of the native outputs; run with `uv run python` from this repository.
- `crosswalk_state_inputs.json` and `crosswalk_state_validation.json`: input/shard hashes, all-state validation, and retained shard paths.
- `crosswalk-state-shards-2010/`: all 52 preserved intermediate state results.

Validation passed: 521 combined tests, then 13 population/cache regression tests after the CLI cache fix; Ruff and diff checks; recipe preflight/build; exact Census ID/total checks; and final curated-layout validation.
