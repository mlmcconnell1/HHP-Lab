# Public historical CoC boundary archive availability, 2010–2024

## CA-524 county reconstruction, 2026-10-27

**Owner decision:** use **Sutter and Yuba counties for CA-524 starting in 2010**, treating its pre-2015 GIS gaps as errors. The [county override rule](../recipes/site-inputs/coc-boundary-county-overrides.csv) covers 2010–2014 using the union of Census 2010 counties `06101` and `06115`. The official 2010–2012 California ZIPs have no CA-524 layer or attribute record; the 2013–2014 polygons cover Sutter County alone. The missing Yuba territory was essentially unassigned to any CoC in those earlier GIS files, unlike the documented MA-501 territorial transfer. The 2013 name already refers to Yuba and Sutter, and [Yuba City's February 2014 adopted housing element, printed page 34](https://www.hcd.ca.gov/housing-elements/docs/yuba_city_5th_adopted022814.pdf) identifies the Sutter-Yuba Homeless Consortium, its participation from both counties, and its PIT work before the 2015 polygon change.

**Footnote:** CA-524 is represented by Sutter and Yuba counties starting in 2010. The official 2010–2012 California GIS archives contain no CA-524 polygon, and the 2013–2014 polygons omit Yuba County. These are treated as GIS errors: the 2010 Census county union reconstructs the 2010–2014 footprint by study-owner decision. Original source files are retained.

This is a reviewed exception to the general single-year restoration rule. It reconstructs the three missing years and replaces the two Sutter-only shapes in the qualified `HUDAnnual_<year>` curated series. The original archives, the first curation run and before-images of revised curated files remain audit evidence. The 2015 onward annual shapes retain their existing two-county footprints; the frozen B2024 study reference and source PIT/HIC values are preserved. This geometry decision does not introduce a count allocation or a new automatic count-compatibility ruling.

The [retained correction run](</home/matt/HHP-Output/historical-site-selection/yuba-sutter-county-correction-20261027>) contains the five-year override ledger, native geometry annotations, registry before-image, revised comparison/review tables and validation. Its publication manifest supersedes the first annual publication's hashes for 2010–2014 only. The earlier full-period report below describes its original historical run.

The revised series has **6,041 annual features**, with all 77 selected CoCs present in every year from 2010 through 2024. CA-524's reconstructed 2010–2014 geometries are identical; its 2014→2015 difference falls to approximately **0.863 km² (0.0266% of union)**, retaining the later source's minor perimeter variation. The large apparent expansion therefore leaves the priority queue. Updated tables contain 5,573 comparisons and 107 ID events. The [current selected review queue](</home/matt/HHP-Output/historical-site-selection/yuba-sutter-county-correction-20261027/selected-unresolved-review.csv>) has 106 comparisons, accounting for this reconstruction and the earlier MA-504 owner-directed count hold; the original 108-row queue remains historical evidence.

Independent validation confirms exact county-union geometry, valid/unique features, registry hashes and feature counts, immutable prior annotation definitions, exact geometry/comparison footnote applicability, complete selected coverage, and preservation of all other CoC rows and curated files. The revised series has 1,654 corrected geometry/year cells. Sixteen additional intersections of at least 1 m² with neighboring source polygons are retained in the overlap audit; none reaches 1 km² (largest approximately 0.488 km² with CA-515). They are not silently clipped. The native compatibility/panel/registry tests pass (99 tests), as do Ruff lint and curated-layout validation. See the retained run's `validation.json` and `published-series.json`.

## Full-period curation, 2026-10-27

The owner's one-year-dropout rule has been applied to the **2010–2024** annual series. The [retained full-period run](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027>) preserves source-only and corrected series, acquisition receipts, executable analysis scripts and their source snapshots, correction ledgers, native annotations, and separate comparison outputs. The earlier 2010–2017 results below remain the source-screen audit trail; their unresolved single-year gaps are superseded where corrected here.

### Source completion

Official national ZIPs supply 2019–2024. The tested national archive URLs for 2010–2018 returned 404, so the retained official state ZIPs supply 2010–2017 and a newly acquired state/DC/territory collection supplies 2018. These results concern the tested endpoints, not a claim that no other national copy exists. The [new acquisition run](</home/matt/Data/hhplab/raw/hud_exchange/acquisition_runs/20261027T003514960501Z>) records URLs, status, ZIP hashes, extracted member inventories and fallback receipts. Raw source bytes are preserved.

The 2018 CA-520 state ZIP contains both a canonical CoC layer and a CDBG auxiliary layer. Applying the package's existing auxiliary-layer exclusion recovered the canonical CA-520 geometry without imputation. For the missing 2022 AL-505 and AR-508 national features, the corresponding state ZIPs were downloaded and checked; neither supplied the missing ID. Other isolated gaps were also absent from the acquired state sources. Massachusetts shared-jurisdiction layers remain quarantined rather than assigning their common area to every named CoC.

The completed source-only screen contains **6,031 annual features, 5,556 same-ID adjacent-year comparisons, and 122 ID appearance/disappearance events**, covering all fourteen transitions from 2010→2011 through 2023→2024. Source diagnostics and receipts are under `source/`; they remain distinct from the owner-assumption corrections.

### One-year correction rule and footnotes

For an internal year Y, restore `(source[Y−1] ∩ source[Y+1]) − source[Y]`. A wholly missing ID uses the common area of its bracketing source years. Decisions use original source neighbors simultaneously: a correction cannot supply evidence for another correction. If the bracketing shapes differ, only their common area is restored. Multi-year omissions, continuing changes, endpoint changes, and temporary additions are retained for review. In particular, a loss lasting several years is not converted into several single-year repairs.

Overlays use EPSG:6933 on a 1 cm grid. Components smaller than 1 m², or so thin that a 1 cm inward buffer removes their entire interior, are treated as numerical noise. Fixed-grid rounding can produce tiny excursions beyond an exact unsnapped bracket; these are measured in the ledger and checked against a 2 cm bracketing buffer. This numerical treatment does not establish reporting-jurisdiction compatibility.

There are **1,650 corrected CoC/year cells**, mostly small boundary-edge harmonizations, including **26 restorations of at least 1 km²**. These are applications of the owner's assumption, not 1,650 independently verified source errors. Seven missing ID-years were reconstructed:

| CoC | Year | Common bracketing area restored, km² |
| --- | ---: | ---: |
| FL-602 | 2013 | 1,852.890 |
| NC-509 | 2015 | 2,950.942 |
| NJ-512 | 2015 | 901.829 |
| NY-501 | 2015 | 9,924.233 |
| FL-500 | 2018 | 3,477.132 |
| AL-505 | 2022 | 6,577.381 |
| AR-508 | 2022 | 11,028.957 |

The corrected series contains **6,038 annual features**. In the active 77-CoC study membership, every ID is present for 2013–2024; **CA-524 remains absent in 2010–2012**, a multi-year gap outside this rule. Material selected restorations include NC-506 in 2012, WA-504 in 2016, and FL-606 in 2019, as well as FL-602 and FL-500 above. The multi-year Fresno/Madera and Battle Creek losses and continuing Hampden and Yuba/Sutter expansions remain review cases.

Each corrected geometry has an immutable native annotation identifying its CoC, original source year, restored area, bracketing evidence and owner assumption. The annotation explicitly does not claim independently verified PIT/HIC scope. The [correction ledger](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/curation-ledger.csv>) includes readable footnotes and hashes of all three source inputs. Native contracts bind notes to exact geometry/year cells and to affected metrics in derived comparisons; a correction for Y can affect comparison intervals ending in Y and Y+1 without changing the note's original source-year meaning. Review tables preserve those contracts. Existing frozen-boundary recipe outputs are not retroactively relabeled. Carrying these geometry notes through generated crosswalks into recipe measures requires explicit lineage/rebinding support; that separate integration has not been performed by this comparison run.

### Curated storage and interpretation

The annual series uses `curated/coc_boundaries/coc__BHUDAnnual_<year>.parquet`, registered as `HUDAnnual_<year>` with source `hud_exchange_annual_curated`. This qualified namespace makes annual inputs selectable without replacing the existing numeric boundary vintages or the frozen B2024 study reference. Publication canonicalizes nullable string columns across all fifteen files. The [publication manifest](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/published-series.json>) records paths, hashes, a registry before-image and verification that prior curated files and the default latest vintage were preserved. Curated-layout validation explicitly recognizes these annual filenames; crosswalk-to-recipe annotation integration is tracked separately in `coclab-y0rf8`.

Remaining differences and overlaps introduced by restorations are review signals, not grounds for automatically clipping another CoC, changing counts or approving a longitudinal jurisdiction. Whole-MSA union checks remain necessary where a change transfers area between two members of one site. The corrected series does not allocate PIT/HIC by population, change merger arithmetic, alter the selected cohort, or certify disjoint national reporting jurisdictions. Continuing changes require individual review under `coclab-t6ywr.13`; shared-jurisdiction allocation remains separate work.

The corrected screen contains **5,570 same-ID comparisons and 108 ID events**, with 907 differences of at least 1 km². Of these, 112 comparisons involve 57 selected CoCs, including the four already reviewed San Diego, Ventura, Polk and Providence merger transitions. The [priority review table](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/persistent-review.csv>) distinguishes changes continuing into the next curated annual geometry, unbracketed 2024 changes and residual cases. The [complete residual screen](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/all-remaining-changes.csv>) retains all 4,952 differences of at least 1 m²; the priority threshold does not approve smaller differences. These are transition counts, not counts of distinct substantive jurisdiction changes.

The persistence check identifies 866 national priority comparisons continuing into the following year; 34 are unbracketed 2024 changes and seven lack the following ID. The [selected unresolved queue](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/selected-unresolved-review.csv>) contains **108 selected comparisons after removing the four reviewed merger transitions**. Native annotation bindings on persistence areas and review classifications include all three geometry dependencies, including the following year; definitions retain their original source-year scope. Filtered study outputs preserve these exact bindings.

**Subsequent owner decision, 2026-10-27:** MA-504 and MA-507 PIT/HIC are flagged incompatible for 2010–2012 inclusive. The 2013 starting year for the new boundaries and counts is provisional and was not established by primary sources; revisit if either CoC enters a downstream study. See the [decision and footnote](site-historical-reconciliation-inventory.md#western-massachusetts-provisional-count-incompatibility-2026-10-27). This updates interpretation of the retained review queue without rewriting its original diagnostics or certifying 2013 onward.

The [introduced-overlap audit](</home/matt/HHP-Output/historical-site-selection/full-period-boundary-curation-20261027/corrected/introduced-overlaps.csv>) records 2,294 CoC-pair/year intersections increasing by at least 1 m² relative to the source-only shapes, mostly small boundary effects. Five increases exceed 1 km²: AL-507/GA-505 in 2018 (1,676.218 km²), WA-500/WA-504 in 2016 (16.673 km²), IL-500/IL-502 in 2020 (8.295 km²), IL-500/IL-517 in 2016 (7.850 km²), and MI-500/MI-513 in 2019 (1.776 km²). The Washington pair involves selected members of the same Seattle MSA; its whole-site union should be checked before interpreting a count implication. The large Alabama/Georgia case is outside the selected study. Restoring a one-year loss does not resolve neighboring claims to that area; these intersections stay explicit rather than being silently clipped.

### Verification

The actual staged data passed checks of geometry validity, unique IDs, all 6,038 feature rows, all 1,650 exact annotation/year cells, preservation of unchanged geometry bytes, original-source bracketing support, all 26 material restored fragments, and selected-cohort coverage. Thirteen policy tests cover isolated holes and missing IDs, common-only restoration, multi-year losses, persistent changes, additions, endpoints and numerical noise. Another 174 package tests cover curated-layout policy, normalization and the boundary registry; Ruff lint and the actual curated-layout check pass. The optional formatter check reports pre-existing formatting differences in both touched package/test files, also present in their unmodified HEAD versions; no broad formatting rewrite was applied.

Exact code snapshots preserve the staged endpoints' and other years' respective generation sources. Comparison outputs retain their precision model, measured area-closure error and tolerance; fixed-grid overlay closure is checked with a perimeter-scaled numerical budget. Published schemas match across all fifteen files, geometry bytes and annotation contracts match staging, and prior files, registry rows and the default latest vintage are preserved. See `corrected/curation-validation.json`, `publication-validation.json` and the comparison/review manifests in the retained run.

## Full official acquisition, 2026-10-26

The owner authorized acquisition of the complete official state/DC/territory archive series. **All 51 state/DC archives were acquired for every year 2011–2017.** Previously audited 2010 ZIPs were also staged into the same asset-store layout. This supersedes the earlier sample-only acquisition status below. It establishes complete state/DC endpoint acquisition, not completeness or correctness of every year's CoC polygons or reporting jurisdictions.

The run attempted all 56 jurisdiction endpoints in each of eight years (448 attempts). It acquired **433 ZIPs**: 408 state/DC files plus 25 territory files. GU, PR, and VI succeeded in every year; MP succeeded in 2017 and returned 404 for 2010–2016; AS returned 404 in every year. These are results for the exact tested URLs, not evidence that no other historical source exists. There were no request failures or incomplete layer inventories in the finished run. Existing 2010 and survey ZIPs were reused only after checking their earlier manifested URL and SHA-256; the remaining sources were fetched directly from HUD. Existing source and extracted files were never overwritten.

| Year | State/DC ZIPs | Territory ZIPs | Source feature records | Unique IDs in native attributes | Invalid non-null features | Compound-ID features |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2010 | 51 | 3 | 442 | 441 | 11 | 1 |
| 2011 | 51 | 3 | 431 | 430 | 13 | 1 |
| 2012 | 51 | 3 | 422 | 421 | 15 | 1 |
| 2013 | 51 | 3 | 411 | 410 | 19 | 0 |
| 2014 | 51 | 3 | 413 | 412 | 19 | 0 |
| 2015 | 51 | 3 | 399 | 398 | 14 | 1 |
| 2016 | 51 | 3 | 398 | 397 | 16 | 1 |
| 2017 | 51 | 4 | 397 | 396 | 15 | 1 |

Counts include territories. There are no null or empty geometries. Compound-ID features are separate source records whose attributes name several CoCs; their component IDs may also have individual source records. Unique IDs therefore are not a count of approved, disjoint national CoCs. The existing 2010 roster discrepancies remain unresolved. The raw Massachusetts compound layers occur in 2010, 2011, 2012, 2015, 2016, and 2017 and require explicit treatment before normalization. Geometry validity is assessed in each layer's native CRS, with reasons and WKB hashes retained; no repair or curated ingest was performed.

The compressed originals total **464,199,066 bytes (464.2 MB)**, including the previously acquired 2010 set. Extracted file contents total **883,592,263 bytes (883.6 MB)**, for about 1.35 GB combined before manifests and filesystem overhead. The 2011–2017 ZIPs alone total 397,117,870 bytes.

Canonical raw storage uses the configured asset root, currently `/home/matt/Data/hhplab` (the repository's `data` symlink):

```text
raw/hud_exchange/<year>/
  per_state/<ST>/
    CoC_GIS_State_Shapefile_<ST>_<year>.zip
    CoC_GIS_State_Shapefile_<ST>_<year>_<sha256-prefix>/
      <original archive member paths>
  manifest__<run-id>.json
raw/hud_exchange/acquisition_runs/<run-id>/
  scope.json
  <year>_<ST>.json
  summary.json
  verified-inventory.json
```

The [retained run](</home/matt/Data/hhplab/raw/hud_exchange/acquisition_runs/20261026T191510281426Z>) includes per-source requested/final URLs and HTTP metadata for new downloads, or explicit lineage to an earlier verified download; source SHA-256/size and ZIP CRC checks; every extracted member's hash/size; layer CRS, raw IDs and filename IDs; and feature geometry diagnostics. Immutable year manifests link to these receipts. The [acquisition script](</home/matt/HHP-Output/historical-site-selection/archive-survey/acquire_state_archive_series.py>) uses the package's configured raw-root helper, six workers, and bounded transient-request retries. Run from the repository with `PYTHONPATH=. .venv/bin/python <script> --json`. Every invocation creates a new receipt run and verifies existing extracted bytes before reuse. Source files are preserved; it does not silently repair shapes, assign compound polygons, or replace curated boundaries.

Independent verification checks every ZIP hash and CRC, compares every extracted file byte hash with both its ZIP member and receipt, verifies the 448 unique jurisdiction/year attempts and all state/DC successes, and checks the script hash and inventory counts. The resulting `verified-inventory.json` additionally lists adjacent-year ID additions/removals, repeated ID claims, and compound/invalid feature review queues. These are source-inventory differences, not evidence by themselves of mergers or count-jurisdiction changes.

Annual derived cleanup, shape comparisons, and selected-study population sensitivity are now completed below. Remaining work is interpretation of populated source differences and reporting scope, including shared-jurisdiction allocations. The Stanford and FY17 mirror investigations remain useful provenance comparisons, but using the directly acquired official state files does not depend on resolving those mirrors.

## Annual shape comparisons, 2026-10-26

The owner requested adjacent-year comparisons of the acquired series. The [retained comparison run](</home/matt/HHP-Output/historical-site-selection/annual-shape-comparison-20261026/run-02>) covers **2010→2011 through 2016→2017**, with **2,844 same-ID comparisons** and **85 ID appearance/disappearance events**. It is a national source-geometry diagnostic, not a complete 2010–2024 panel or a finding that PIT/HIC reporting jurisdictions changed. IDs are compared literally; predecessor unions are not silently substituted into this raw same-ID screen.

### Conservative derived cleanup

Eight derived GeoParquet files are retained under `normalized/coc__B<year>.parquet`, in EPSG:4326 with native provenance. These are external diagnostic artifacts; no curated file or registry was replaced. All **3,305 ordinary feature records** were retained. One Massachusetts shared-jurisdiction source layer per year was withheld, including 2013/2014 layers whose filenames say shared jurisdiction while their DBF attribute names only MA-510. The individually supplied CoC polygons remain present; the extra shared area is not assigned to each named ID. No other repeated ordinary IDs remained after this quarantine.

The package's `fix_geometry` and `ensure_polygon_type` helpers repaired **122 invalid source feature records** on derived geometries. The largest absolute relative area change in native coordinates was **1.99×10⁻¹¹**; no additional repairs were needed after WGS84 transformation or projection to the measurement CRS. Original source bytes, raw geometry hashes and validity reasons remain in the acquisition receipts. Repair/type/area details and quarantined source paths are in `normalization-<year>.json` and `normalization-summary.json`. No simplification or coordinate snapping was applied to the normalized annual artifacts.

### Comparison method and national totals

Measurements use EPSG:6933 and retain old/new area, removed/added area, symmetric difference divided by union, exact topological equality, source-repair flags, and part counts. Added/removed IDs are separate records rather than zero-area comparisons. A **1% of union plus 1 km²** flag prioritizes large source changes; it is not a compatibility or exclusion rule. Most years' shapes are technically unequal, including many very small differences, so exact inequality alone is not interpreted as a substantive change.

| Adjacent years | Common IDs compared | Differences ≥1 km² | Differences ≥1% and ≥1 km² | Same flag within the selected 77 CoCs |
| --- | ---: | ---: | ---: | ---: |
| 2010→2011 | 427 | 16 | 15 | 1 |
| 2011→2012 | 420 | 107 | 26 | 6 |
| 2012→2013 | 407 | 86 | 33 | 7 |
| 2013→2014 | 410 | 38 | 1 | 0 |
| 2014→2015 | 394 | 88 | 38 | 9 |
| 2015→2016 | 393 | 71 | 12 | 0 |
| 2016→2017 | 393 | 32 | 6 | 0 |

Seven nearly coincident 2010→2011 comparisons failed the initial floating-point area-balance check. Their measurement overlays were retried on an explicit fixed precision grid: six at 1 mm and WV-501 at 1 cm. Raw measurements and `overlay_grid_m` are retained alongside the resulting metrics. All seven are outside the selected 77-CoC cohort. The annual source artifacts remain unsnapped. All final rows pass area-balance checks; these numerical treatments do not confer reporting compatibility.

### Selected-study population sensitivity and source questions

The selected-cohort annotations use the existing **60-site/77-CoC** membership, not a new site selection. The accepted source polygons contain 76 of those IDs in 2010–2013 and all 77 in 2014–2017. The missing IDs are **CA-524 in 2010–2012** and **FL-602 in 2013**. These gaps remain explicit; source counts with the same identifier do not supply a missing polygon.

The fixed-2020 Census block method was applied to **all 51 selected-CoC same-ID changes of at least 1 km²**, regardless of their percentage of total area. This covers 28 distinct CoCs and therefore also checks populated fragments whose relative area is small. All intersecting US counties are included, all population joins are one-to-one with no missing counts, and block geometry/population are held fixed across boundary years. Uniform area allocation includes water; geometric-centroid assignment is retained as an alternative. Percentages in the population output use the later source footprint's allocated population, not the full MSA or frozen B2024 denominator. Gross estimates sum removed and added population and are not observed resident or homeless-person movements.

Of the 51 comparisons, **14 have gross allocated population below 1,000**, seven are between 1,000 and 10,000, and 30 are at least 10,000. Three of the large comparisons are the already reviewed San Diego, Ventura, and Polk merger transitions; the existing count-reconciliation rules remain unchanged. The other 27 large comparison rows involve 14 CoCs and warrant source/count-scope review. These thresholds organize review rather than approve or exclude observations.

| Selected source change requiring further interpretation | Source polygon finding | Fixed-2020 gross population allocated to changed fragments |
| --- | --- | ---: |
| MA-504, 2011→2012 | Springfield city-sized source polygon expands to the named Hampden County scope; 94.78% symmetric difference relative to union | 309,964 |
| CA-524, 2014→2015 | Yuba/Sutter source footprint expands; 51.42% symmetric difference relative to union | 81,575 |
| CA-511, 2012→2013 | Stockton/San Joaquin source gains a populated fragment despite a relatively small area share | 66,349 |
| CA-514, 2011→2012 | Fresno/Madera source loses a populated fragment; a similar population estimate is regained in 2014→2015 | 112,365 |
| MI-514, 2011→2012 | Battle Creek/Calhoun source loses a populated interior area; a similar estimate is regained in 2014→2015 | 52,703 |
| NC-506, 2011→2012 | Wilmington-area source loses a populated fragment; a similar estimate is regained in 2012→2013 | 113,994 |

Additional large or repeated populated changes occur in FL-504, FL-505, FL-513, FL-601, FL-605, FL-606, WA-500, and WA-504. The full joined `review-queue.csv`/Parquet records names, MSA membership, area and population metrics, and the three reviewed merger transitions. `reversal-checks.json` tests whether opposite-signed population changes also concern the same spatial fragments: nine pairs match at more than 99.8% intersection-over-union. Repeated removals and restorations motivate a source-consistency check; they do not prove that the actual count jurisdiction contracted and expanded. CoC-level transfers may also cancel within a multi-CoC MSA, so whole-site unions must be checked before changing site outcomes or eligibility. Follow-up source/count-scope reconciliation is tracked in `coclab-t6ywr.13`; the quantitative comparison work is `coclab-t6ywr.12`.

These populated differences are substantially larger than the four small-perimeter cases and the Massachusetts predecessor-union/Fall River residuals previously reviewed. The small-fragment assumption is not extended automatically to them. No PIT/HIC values, source-specific merger operations, cohort membership, or historical compatibility records were changed.

### Artifacts, visualization and validation

The [four-panel map](</home/matt/HHP-Output/historical-site-selection/annual-shape-comparison-20261026/run-02/selected-shape-changes.png>) shows Springfield/Hampden expansion, Yuba/Sutter expansion, and populated interior removals in Fresno/Madera and Battle Creek/Calhoun. Panel scales are independent; the map shows source polygons, not established PIT/HIC coverage.

The retained run contains `comparisons.csv`/provenance Parquet, `id-events.csv`/provenance Parquet, `summary.json`, normalized annual GeoParquets, per-year repair/quarantine audits, `review-queue.csv`/provenance Parquet, and `population-sensitivity/` with all 51 estimates, block-level details, embedded provenance and validation. Scripts are retained in the parent directory. The exact normalization-stage source is preserved in `normalization-script-snapshot.json`; current comparison-stage source hashes and input hashes are embedded in output provenance. Use the comparison script's `--output-dir <fresh-directory>` to reproduce without overwriting retained outputs. The population and plotting scripts explicitly target the retained `run-02` directory and refuse to overwrite completed outputs.

Independent validation read back all annual files and checked unique IDs, geometry validity, CRS, output hashes, quarantine/repair accounting and embedded provenance. It reconciled every comparison/event against annual ID sets, verified area sums, fractions and closure for all 2,844 rows, and recomputed the five largest selected differences with a separate symmetric-difference overlay. The 51 population results were independently reconstructed from block rows, including added/removed/gross/net allocations, centroid totals, loose touched-block ceilings and population closure. The validation JSONs retain these results. No production package code was changed; validation concerns the actual data artifacts rather than an unrelated package test suite.

## Earlier bounded survey (2026-10-22)

The following records the initial sample-only survey and its follow-up national-copy audits. It is preserved as the acquisition trail and is superseded by the full official acquisition above where it describes current collection coverage. The 2019–2024 national copies are archived separately.

## Findings

HUD's published report host serves the state-level archive pattern
`https://files.hudexchange.info/reports/published/CoC_GIS_State_Shapefile_{ST}_{YYYY}.zip`.
All 19 tested state/year URLs returned HTTP 200 and downloaded as ZIP files:

| Probe group | Years | State samples | Result |
|---|---:|---|---|
| Annual continuity | 2011–2017 | CA | 7/7 ZIPs, 39–42 CoC `.shp` members per year |
| Geographic/size contrasts | 2011, 2014, 2017 | AK, DC, NY, TX | 12/12 ZIPs; from one DC CoC shapefile to 32 NY shapefiles |

The files contain one folder per CoC and its shapefile sidecars. This is useful evidence that the state/year route is live and its layout changed little across the sampled period. It does **not** establish nationwide completeness for every year: other state, territory, and year combinations were not enumerated or downloaded. The exact probes, HTTP metadata, full SHA-256 hashes, ZIP member lists, and local paths are in the machine-readable matrix under `/home/matt/HHP-Output/historical-site-selection/archive-survey/`.

The historical code in the [UCSF CoC Data project](https://github.com/ucsf-bhhi/coc-data/blob/main/R/process_coc_shapefiles.R) documents the distinction: prior to 2013, HUD distributed individual CoC shapefiles grouped by state; its code uses the same `files.hudexchange.info/reports/published/CoC_GIS_State_Shapefile_{ST}_{YYYY}.zip` naming pattern. The project's [reference documentation](https://ucsf-bhhi.github.io/coc-data/reference/get_shapefiles.html) describes that pre-2013 handling.

Two national copies were verified separately:

| Vintage | Copy and evidence | Survey result | Interpretation |
|---:|---|---|---|
| 2014 | [Stanford PURL `wm843fh7398`](https://purl.stanford.edu/wm843fh7398) and its [Cocina JSON metadata](https://purl.stanford.edu/wm843fh7398.json) | Public metadata identifies “CoC Grantee Areas, 2014,” HUD as creator/publisher, and a cutoff of Nov. 14, 2014. See the vintage audit below for the original shapefile and derivative counts. The public [FlatGeobuf file](https://stacks.stanford.edu/file/druid:wm843fh7398/CoC_Grantee_Areas.fgb) is a Stanford derivative. | Source metadata identifies the layer as a 2014 snapshot, but its all-2015 `YEAR` attribute is an unresolved field-level conflict; do not treat the vintage as independently verified. The derivative also drops one null-geometry row, so the original shapefile bundle is the better preservation source. |
| 2017 | [HUD CoC Geography Crosswalk repository](https://github.com/tomhbyrne/HUD-CoC-Geography-Crosswalk) README calls the file 2017 HUD CoC boundaries and says it was obtained from the linked HUD site. The [public repository ZIP](https://raw.githubusercontent.com/tomhbyrne/HUD-CoC-Geography-Crosswalk/master/data/CoC_GIS_NatlTerrDC_Shapefile_2017.zip) is third-party hosted (23,034,500 bytes; SHA-256 `c40b73d77cabeb3807536c7da2ad0a9bd45c569a8402a8f6696f52f5ae9f2b3b`). | ZIP integrity test passed; it contains `FY17_CoC_National_Bnd.gdb`. The layer has 399 MultiPolygon records, EPSG:4269, 55 `ST` codes, 399 distinct non-null `COCNUM` values, and bounds `[-178.228, 13.234, 146.065, 71.390]`. | Classify it as a third-party FY17 candidate, not a verified canonical national release: it has no `YEAR` field, and a direct HUD 2017 California state archive differs by one CoC code (details below). |

The original Stanford shapefile has one null geometry and 71 invalid non-null geometries; the Stanford FlatGeobuf derivative omits the null-geometry `WI-500` record and retains 71 invalid geometries. The 2017 GDB has no null or empty geometries, but 15 invalid polygons. A focused follow-up is recorded below.

## Vintage and geometry audit (2026-10-25)

The downloaded evidence is retained at
[`open-bead-audit-20261025/archive-vintage`](</home/matt/HHP-Output/open-bead-audit-20261025/archive-vintage>).
The six source hashes listed below were independently verified during integration.

This audit used fresh downloads under `/tmp/hhplab-archive-vintage-20261025/`; it did not modify curated data. It inspected Stanford's source metadata and original shapefile bundle as well as the derivative FlatGeobuf, the third-party FY17 GDB, and direct HUD state/year archives. The HUD catalog describes its downloadable GIS boundary datasets as organized “by Year” ([Data.gov record](https://catalog.data.gov/dataset/continuum-of-care-coc-gis-tools)); that general statement does not by itself resolve an individual file's exact release date.

| Candidate | Source-level vintage evidence | Coverage/field checks | Audit classification |
|---|---|---|---|
| Stanford original shapefile | The preserved `.shp.xml` and `.shp_FGDC.xml` metadata identify the item as HUD's “CoC Grantee Areas, 2014”; state its data cutoff as 2014-11-14 / FY2014; and cite the HUD GIS Maps and Data collection. The item record has 2014 publication metadata. | Original SHP has 410 records and 410 unique non-null `COCNUM`s across 52 `STUSAB` values (50 states, DC, PR). All records have `YEAR=2015`; this conflicts with the source metadata, and neither source metadata explains the field. One row, `WI-500` / Wisconsin Balance of State CoC, has null geometry. Stanford's metadata declares 410 features. | **Source metadata identifies FY2014; the `YEAR=2015` conflict remains unresolved.** Do not use this attribute to relabel the vintage. Preserve the original SHP bundle and its source metadata. |
| Stanford FlatGeobuf derivative | Stanford Cocina JSON lists the FGB as a Stanford-generated derivative (`sdrGeneratedText: true`); it is not the original HUD shapefile component. | It has 409 records, omits exactly `WI-500`, and retains 409 unique IDs and all 52 jurisdiction codes. It has 71 invalid geometries. The omitted row is the original SHP's null-geometry row, not evidence that HUD omitted that CoC. | **Do not use as the complete 2014 source.** It is a convenient derivative only if row loss is explicitly handled. |
| HUD Wisconsin state archive for 2014 | Direct HUD URL `https://files.hudexchange.info/reports/published/CoC_GIS_State_Shapefile_WI_2014.zip`; its embedded FGDC metadata says current for 2014, “all of the 2014 claimed CoC areas,” and `CoC FY2014`. | The archive contains a one-feature `WI-500` polygon with the same CoC identifier/name as the null Stanford row. Its polygon is nonempty but invalid due to a ring self-intersection. `make_valid` produced a valid MultiPolygon and changed planar area in EPSG:4326 by relative `-8.91e-16`. This is an explicit same-ID official source recovery candidate, not an inferred geography. | **Candidate recovery source, pending same-vintage verification**, with source ZIP hash `e4230f826280640394f46e1bb8d580099116d5cae1f3ee22127d9047594e4592`. Any downstream fill must record that the geometry came from the HUD Wisconsin 2014 archive and retain the invalid-source/repair audit. |
| Third-party FY17 national GDB | The ZIP member is named `FY17_CoC_National_Bnd.gdb`; the repository README calls it 2017 HUD CoC boundaries and says it was obtained from HUD. The exposed layer schema has no `YEAR` attribute. | It has 399 unique IDs in 55 state/territory/DC codes. The direct HUD 2017 California state archive has 42 unique CoCs, all 42 identifiers are present in the national GDB. Shapely 2.1.2 reports all 42 pairs topologically equal (`equals`); each pair's symmetric difference is empty with planar area and length both `0.0` in EPSG:4269. Only 12 pairs pass `equals_exact(..., 0)` (also 12 after `normalize`), so this is not evidence of coordinate-sequence or byte identity. The GDB has one additional California code, `CA-529`, absent from the direct state archive. | **FY17 candidate, provenance/version unresolved.** The README supports the claimed HUD origin, but the mirror is not a direct HUD copy and the California mismatch prevents treating it as an exact national equivalent of the current direct state/year files. Do not silently merge the extra code or call the whole file canonical. |

### Geometry repair diagnostics and safe ingestion policy

Diagnostics used the repository `uv` environment (GeoPandas 1.1.2, pyogrio 0.12.1, Shapely 2.1.2) and Shapely `make_valid` in memory only. No source geometry or curated artifact was changed. The 2014 original has 71 invalid non-null polygons (68 ring self-intersections and 3 self-intersections), plus the null `WI-500` geometry. For the 71 invalid polygons, `make_valid` returned valid results (69 MultiPolygon, 2 Polygon); only 3 had an absolute planar area delta over `1e-12` degrees squared, the maximum relative delta was about `1.94e-9`, and none changed by more than 1%. The official HUD Wisconsin `WI-500` recovery polygon also became valid with negligible computed area delta. The FY17 national GDB's 15 invalid polygons (all ring self-intersections) became valid (12 MultiPolygon, 3 Polygon); none changed by more than `1e-12` degrees squared and the maximum relative delta was about `1.15e-14`. For MultiPolygon-only ingest, the two 2014 and three FY17 repaired Polygon results must each be explicitly wrapped and the operation recorded. Small computed area deltas do not establish that the repaired boundaries are cartographically correct.

Recommended policy:

1. For 2014, use the preserved original HUD-described Stanford shapefile bundle, not its FGB derivative, as the primary national row inventory. Keep `YEAR=2015` as an unresolved source attribute; metadata identifies a 2014-11-14/FY2014 snapshot, but do not treat the actual boundary vintage as independently verified while the conflict remains. Treat the direct HUD Wisconsin 2014 archive as a candidate `WI-500` geometry source only after same-vintage verification; if used, key by exact matching `COCNUM` and preserve both the null original row and recovery lineage in provenance. Apply `make_valid` on a derived working geometry, report every changed geometry/type and validate resulting IDs, CRS, geometry validity, and national jurisdiction coverage before curated ingestion.
2. For 2017, acquire and manifest the official HUD 2017 state/territory/DC files that the HUD archive publishes before creating the canonical national layer; record unavailable expected jurisdictions distinctly. Keep the third-party national GDB as a separately hashed comparison/reference copy until every jurisdiction's direct source is checked, including resolution of `CA-529`. If the national mirror is ever used as a provisional dataset, label its third-party provenance and retain the one-code mismatch explicitly; do not infer a correction.
3. In either vintage, make no implicit row drops, repairs, interpolations, code substitutions, or completeness claims from extent alone. Keep source bytes and metadata immutable; perform repairs only in a new, provenance-bearing derived artifact after the ID/jurisdiction checks pass.

Fresh evidence hashes: Stanford Cocina JSON `4cd9dda1993aa2eaa2985bc0578cdfdc48b6046e0324dc0ce2cd9bb3b3fe7d3d`; original Stanford SHP `c9e8687a8ef5f8ef2757e620eb2548687d44ef62fc532173d83af1e87a885c8b`; Stanford FGB `5e63fc94bf48c901ea91251effda2c5c2bb808c3b02fbb64aa89b1ad8897f64f`; 2017 national mirror ZIP `c40b73d77cabeb3807536c7da2ad0a9bd45c569a8402a8f6696f52f5ae9f2b3b`; direct HUD California 2017 ZIP `6a7f5ce9534445774bedf581531985521c730e6d3e2225d3d9998c49b409277f`; direct HUD Wisconsin 2014 ZIP `e4230f826280640394f46e1bb8d580099116d5cae1f3ee22127d9047594e4592`.

The four guessed legacy paths probed in the script returned HTTP 404. Those guesses were under `/resource/coc/gis/`, the `www.hudexchange.info` host, and `/resources/documents/`. This only says those exact URLs did not resolve on survey day; it does not rule out other HUD paths, web archives, or copies held by organizations.

HUD's [CoC GIS Tools catalog record](https://catalog.data.gov/dataset/continuum-of-care-coc-gis-tools) describes year-based boundary datasets and links to the HUD Exchange GIS tools page, while now-current HUD GIS service/catalog records describe active grantee areas. These current services do not by themselves prove that their geometries are historical vintages, so they are not treated here as substitutes for the archived boundary files.

## Reproduction and files

Run:

```bash
python3 /home/matt/HHP-Output/historical-site-selection/archive-survey/survey_archive_sources.py
```

The download script performs the 19 representative HUD state/year downloads, downloads the two national copies, and runs four HEAD-only legacy-path probes. It writes `source_matrix.csv` and `source_matrix.json`; downloaded samples and national files are kept under `downloads/`. The matrix includes UTC probe time, response URL/status, advertised and observed sizes, SHA-256, ZIP entry and `.shp` counts, and each ZIP member name. ZIP CRC checks passed for all 20 downloaded ZIPs. The separate `inspect_historical_geometry.py` script reads the Stanford FlatGeobuf and the GDB layer directly from its ZIP using repository `uv` dependencies (`geopandas` 1.1.2, `pyogrio` 0.12.1, Shapely 2.1.2); it writes `geometry_inspection.json` and makes no repairs. Re-running the download script fetches the same fixed sample; existing files are reused only when their bytes match the current response, and the script refuses to overwrite a differing existing download.

## Earlier acquisition recommendation and remaining work

The state/DC manifest, full official state-file acquisition, annual diagnostic normalization and quantitative comparisons were completed in the 2026-10-26 runs. The list is retained as the original survey recommendation. Remaining work is populated source-change/count-scope review (`coclab-t6ywr.13`), the 2010 jurisdiction/compound-feature questions (`coclab-t6ywr.8`), and optional mirror vintage/provenance reconciliation (`coclab-t6ywr.9`). Missing exact territory endpoints remain explicit; their absence is not silently filled. Completed acquisition and comparison do not certify the native CoC roster or PIT/HIC jurisdiction in any year.

1. Build a complete annual manifest from HUD's state/territory roster for 2011–2017, then probe each expected endpoint with HEAD before downloading. Record missing, renamed, and zero-size responses distinctly. The present state sample cannot serve as that manifest.
2. Acquire the full per-state set in bounded annual batches, initially 2011–2012 (legacy individual-CoC layout), then 2013–2017. Preserve original ZIPs, HTTP final URLs, retrieval timestamps, and SHA-256 hashes; inspect extracted shapefile fields and coordinate reference systems before ingestion.
3. Keep Stanford 2014 as an independent national preservation copy and keep the 2017 repository source labeled as third-party. Compare national copies with reconstructed state collections after all expected states and territories are accounted for.
4. Search web archives and institutional holdings for national 2011–2013, 2015–2016 copies and any files absent from the HUD endpoint. The current survey found no national-source evidence for those vintages.
