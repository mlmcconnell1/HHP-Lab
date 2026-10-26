# Providence CoC merger reconciliation audit

Audit date: 2026-10-21. Scope is the selected Providence-Warwick MSA's frozen 2024 MA-505 membership and its predecessor CoCs, MA-505 and MA-519. The machine-readable calculations and source hashes are in [`~/HHP-Output/site-boundary-history-audit/providence/ma505-ma519-reconciliation.json`](</home/matt/HHP-Output/site-boundary-history-audit/providence/ma505-ma519-reconciliation.json>). This is an evidence audit, not an approval for historical compatibility or a longitudinal analysis.

## Merger and count provenance

The HUD 2007–2024 PIT workbook has a `CoC Mergers` sheet. Its row 38 lists `MA-519` in column A (`CoC Number Pre-Merger`), `MA-505` in column B (`CoC Number Post Merger`), and `2024` in column C (`Merger Year`). The workbook's SHA-256 is `ae88fbb58dadfbfccc254619b714d37e0720bd9854a31a16ad0e381fa14959ed`. The Bristol County CoC history says the two memberships voted to merge in May 2024 and the newly merged CoC first met in June 2024. Its [statement of intent](https://www.bristolcountycoc.com/bristolcountycoc/wp-content/uploads/2024/07/STATEMENT-OF-INTENT-TO-MERGE-COCS.pdf), signed December 21, 2023, identifies the two predecessor areas and says HUD reporting data may need to be prepared or merged. The statement does not specify a PIT/HIC count-year crosswalk.

In the raw cumulative PIT P2024 workbook, the 2020, 2022 and 2023 sheets have one MA-505 row and no MA-519 row. For all three years, the MA-505 `Unsheltered Homeless` value equals the sum of the two native MA-505 and MA-519 rows in the P2025 workbook. The `Overall Homeless` total also matches the sum in each year. This supports treating the P2024 values as already aggregated for the merger; adding MA-519 to P2024 would double-count. The workbook does not describe the detailed method used to create those historical totals.

Annual HIC workbooks retain distinct MA-505 and MA-519 rows in 2020, 2022 and 2023. Their `Total Year-Round Beds (ES, TH, SH)` values sum to 678, 708 and 641, respectively. HIC 2024 has a single MA-505 row with 1,170 shelter beds and no MA-519 row. Thus, a candidate historical count treatment is panel-specific: use P2024 MA-505 once for PIT, and sum the two distinct annual HIC rows before 2024; use the single merged MA-505 row for 2024 HIC. The raw HIC sheets contain no inspected merger note explaining this treatment.

## Boundary comparison

I compared only the two predecessor polygons and frozen B2024 MA-505, projected to EPSG:5070. For 2020 I used curated HUD B2020, for 2022 and 2023 the corresponding HUD national-boundary archives. The predecessor polygons are disjoint at area precision in each source. Their unions compare as follows:

| Source geometry | Predecessor union area | B2024 MA-505 area | Symmetric difference | Assessment |
| --- | ---: | ---: | ---: | --- |
| B2020 | 1,403.647487 km² | 1,393.079716 km² | 13.499083 km² | Not equivalent; residual cause unclassified. |
| HUD FY22 archive | 1,403.626579 km² | 1,393.079716 km² | 13.463419 km² | Not equivalent; residual cause unclassified. |
| HUD FY23 archive | 1,393.079716 km² | 1,393.079716 km² | 11.2 m² | Very close in area, but not exactly topologically equal. |

For FY23, symmetric difference is about `8.04e-9` of the combined union area (about `8.04e-7%`), with each geometry covering more than 99.9999995% of the other. This is strong geometric evidence that the FY23 predecessor union and the locally captured FY2024 MA-505 boundary represent the same footprint at practical area precision. Exact equality still fails, and no tolerance rule was set in advance, so this audit does not label the record `compatible` or approved. The 2020 and FY22 unions have about 0.96% symmetric difference relative to combined union area; they remain unresolved. I did not attribute those residuals to coastlines, islands, digitization, reprojection, or a change in administrative coverage.

The local B2024 file is a 2026 capture of HUD's CoC boundary layer, which HUD's [catalog entry](https://catalog.data.gov/dataset/continuum-of-care-coc-grantee-areas) describes as FY2024 coverage. It is not a preserved January 2024 snapshot. The source archives and local files are hash-pinned in the JSON artifact.

## Administrative geography cross-check

HUD's FY2023 [Geographic Codes with PPRN report](https://www.hud.gov/sites/dfiles/CPD/documents/CoC/FY-2023-GeoCodes-PPRN-Report-Revised.pdf) assigns geocode 251614 (New Bedford) to MA-505, and geocodes 250126 (Attleboro), 259005 (Bristol County), and 252418 (Taunton) to MA-519. It lists Fall River separately as geocode 250744 and MA-515. The official Bristol County CoC [site](https://www.bristolcountycoc.com/) describes the merged MA-505 as all Bristol County cities and towns except Fall River. HUD's FY2024 [HIC report](https://files.hudexchange.info/reports/published/CoC_HIC_State_MA_2024.pdf) uses one MA-505 row named “New Bedford, Attleboro, Taunton/Bristol County CoC.” These records support the administrative successor relation and the predecessor area names.

An independent jurisdiction check uses U.S. Census TIGER/Line county-subdivision files for Massachusetts. I selected Bristol County's 19 named municipal subdivisions other than Fall River and overlaid that set against each HUD predecessor union and frozen B2024. Each CoC union has 99.982%–99.985% of its area inside the same 19-municipality union across 2020, FY2022, FY2023 and B2024. Only about 0.21–0.25 km² falls outside that union. The named municipal set is stable in the inspected 2020 and 2023 Census files. This supports the conclusion that the same cities and towns remained in scope and that the 2020/2022 difference from B2024 is not evidence of an entire municipality moving into or out of the CoC.

The Census municipal polygons include water: their reported AWATER is about 192.7 km², while B2024 covers about 90.5% of the total municipal-union polygon area. Therefore the intersection percentages do not measure land-only completeness. The jurisdiction overlay distinguishes the named administrative scope, but it does not assign the remaining 13.5 km² polygon difference to shoreline, islands, water treatment, cartographic generalization, or boundary editing. The HUD FY2020 geocode report lists New Bedford, Attleboro, Bristol County, Taunton and Fall River codes, but its inspected table does not associate the codes with a parent CoC. The FY2023 report is the directly verified HUD source-to-CoC geocode grouping; 2022 geocode parentage was not independently recovered.

This gives an evidence path for an explicitly named *administrative predecessor-union* outcome: use the two distinct HIC predecessor counts once each in 2020/2022/2023, and use the P2024 MA-505 PIT row once because HUD's merger sheet and source-vintage row sums show it already contains both predecessors. Attribute the series to the merged administrative jurisdictions while preserving the 2020/2022 geometry residual as a documented limitation. It does not establish exact geometric identity to the frozen B2024 polygon. The supplementary source list, archive hashes, Census query, and containment calculations are in [`jurisdiction-crosscheck.json`](</home/matt/HHP-Output/site-boundary-history-audit/providence/jurisdiction-crosscheck.json>).

## Reconciliation recommendation and count operations

The evidence supports a source-specific count reconciliation to the merged administrative jurisdiction for 2020, 2022, and 2023: the official merger mapping identifies MA-519 → MA-505 in 2024; the historical P2024 PIT row is already the two-predecessor sum; HIC retains the two predecessor rows; official jurisdiction descriptions name the corresponding cities/towns; and the Census overlay supports the same municipality set across the compared vintages. For 2023 the predecessor-polygon union is also nearly identical to frozen B2024. For 2020 and 2022, retain the 13.5 km² geometry difference as unresolved perimeter evidence; do not claim exact polygon equality or characterize the residual as coastline without further source evidence.

The official evidence supports treating the predecessor counts as the merged administrative area for all three requested historical years, with the source-specific rules above. The FY23 geometry must not be used as retroactive proof of earlier geometries; the 2020 and 2022 polygons remain distinct from B2024 even though the independent municipality evidence supports stable jurisdictional scope.

`coc_boundary_compatibility/1` can represent a direct ID or one-to-one mapped ID, but cannot state that a frozen CoC's count comes from multiple source IDs or that a single source row already represents several predecessor areas. Version 2 therefore records explicit `source_coc_ids` and a panel/year `count_operation`: `preaggregated` for the P2024 PIT MA-505 row, whose evidence identifies the original MA-505 + MA-519 scope; `sum` for the distinct 2020/2022/2023 HIC MA-505 and MA-519 rows; and `single` for the merged 2024 MA-505 HIC row. It separates the count reconciliation from polygon compatibility and retains source-count evidence, a review reference, the administrative basis, and geometry residual evidence. Under that contract, neither the P2024 row nor the historical HIC predecessor rows are joined or added based on ID coincidence alone, and a missing predecessor prevents a partial HIC sum. The 2020/2022 geometry comparison remains unresolved against frozen B2024, while 2023 is near-equal but not exactly equal. This audit documents the evidence basis and intended operation; it does not assert that the predecessor polygons are identical or approve unreviewed geographic equivalence.

A focused package aggregation harness against the curated rows produced the following component counts:

| Year | P2024 PIT unsheltered, MA-505 row consumed once | Annual HIC shelter beds, MA-505 + MA-519 summed |
| ---: | ---: | ---: |
| 2020 | 76 | 678 |
| 2022 | 109 | 708 |
| 2023 | 96 | 641 |

The [verified harness artifact](</home/matt/HHP-Output/site-boundary-history-audit/providence/predecessor-reconciliation-verified-harness.json>) records source-file hashes, the v2 evidence payload, operation lineage, geometry assessments, and package output. It uses the actual Providence-Warwick MSA ID and a canonical identity for a one-member `MA-505` diagnostic subset at threshold 0.99; it is not a producer artifact or a complete Providence MSA result. The curated P2024 PIT row has no `boundary_vintage` field, so the harness explicitly declares publication vintage 2024 and keeps the annual predecessor geometry comparison separate. Its review reference points to the orchestrator's count-rule review, while the 2020/2022 residuals remain unresolved and the full site universe is not approved.

The detailed machine-readable artifact contains the exact raw workbook and archive hashes, PIT/HIC row values, projected area calculations, source URLs, a reproducible geometry method, and limitations. No national overlay was run.

## Integration review: administrative count reconciliation

On 2026-10-21 the orchestrator reviewed the official merger mapping, source-specific row arithmetic, jurisdiction cross-check, implementation, and regression coverage under `coclab-7c0nt`. The accepted count rule is to consume the historical P2024 MA-505 PIT row once with documented MA-505/MA-519 predecessor scope, and to sum the two distinct annual HIC rows for 2020, 2022, and 2023. This section is the review reference for that administrative count rule. It does not certify exact equality of the historical polygons to B2024, resolve the 2020/2022 perimeter residuals, or approve the full frozen Providence MSA or 64-site longitudinal panel.

The implementation supports `coc_boundary_compatibility/2` alongside v1. It requires explicit source IDs and `single`, `preaggregated`, or `sum` operations, separate count and geometry evidence, and a review reference for reconciled records. It rejects overlapping predecessor claims and leaves an outcome null if any required predecessor count is missing or invalid. The locked integration test also verifies that PIT consumes its preaggregated value once while HIC sums both source rows during offline replay. Historical geometry clearance remains part of `coclab-k0hwb`; Connecticut reporting-footprint clearance remains in `coclab-k34yp`.

## Extension to the full 2010–2024 study window

This section extends the Massachusetts evidence review to the selected 2010–2024 panel, excluding 2021. The full source list, hashes, per-year operations, and geometry measurements are in [`ma-massachusetts-historical-scope.json`](</home/matt/HHP-Output/site-boundary-history-audit/providence/ma-massachusetts-historical-scope.json>). It supersedes the earlier characterization of MA-505's 2020/2022 perimeter residual as unresolved for administrative compatibility. Those residuals remain unclassified polygon differences; the additional municipal and HUD evidence supports treating them as the same named predecessor/successor administrative area. This is a component-level geographic decision, not scientific approval of the panel or the full site universe.

### MA-505: administrative scope and boundary residuals

HUD's FY2024 merger mapping names MA-519 as the predecessor and MA-505 as the successor. The CoC's statement of intent identifies the old MA-505 area as New Bedford and the old MA-519 area through its included municipalities; HUD's FY2023 geocode report independently assigns New Bedford to MA-505, Attleboro, Bristol County, and Taunton to MA-519, and Fall River to separate MA-515. The official Bristol County CoC describes the merged service area as all Bristol County cities and towns except Fall River. HUD's 2010 Annual Homeless Assessment Report also describes MA-505 as City of New Bedford, MA-519 as Attleboro/Taunton/Bristol County, and MA-515 as City of Fall River. Together these sources support a stable named administrative scope at the two-predecessor level before the 2024 merger.

I obtained the official HUD Massachusetts state boundary archives for 2010 and 2014 and national archive for 2019. I compared the MA-505 + MA-519 union with frozen B2024 MA-505 in EPSG:5070. The symmetric difference is 15.809 km² (1.124% of the union) in 2010, 14.314 km² (1.018%) in 2014, 13.499 km² (0.961%) in 2019, 13.499 km² (0.961%) in 2020, and 13.463 km² (0.958%) in the FY2022 archive. The FY2023 archive differs by about 11.2 m² (8.04×10⁻⁹ of the union). The near-coincident FY2023 union is a recent endpoint and does not establish that older polygons were identical.

As an independent administrative cross-check, I overlaid the older unions with the 19 named Bristol County municipalities in the merger statement, excluding Fall River, using Census TIGER/Line 2020 subdivisions. The union area inside that municipal set is 99.884% in 2010, 99.949% in 2014, and 99.983% in 2019; the prior audit records corresponding coverage from 99.983% to 99.985% for B2020, FY2022, FY2023, and frozen B2024. This supports the same municipality-level scope across the compared vintages and gives no evidence that a whole named municipality moved into or out of the CoC. The remaining perimeter area has not been assigned to water, coastline, cartographic generalization, or administrative change.

For historical PIT, P2024 has one MA-505 row and the raw-vintage audit verified that for every 2010–2023 year its unsheltered and overall counts equal the native MA-505 + MA-519 sum in P2025. Use the P2024 MA-505 value once; adding MA-519 would double count. Annual HIC retains two distinct rows through 2023, so sum MA-505 and MA-519 once each before 2024. The 2024 PIT and HIC each use the single merged MA-505 row. This reconciles the counts to the merged administrative scope and does not claim exact equality of the historical polygons to B2024.

### MA-515: stable Fall River administrative scope

HUD's 2010 AHAR identifies MA-515 as City of Fall River. HUD's FY2021 names-and-numbers list and FY2023 geocode report continue to identify MA-515 as Fall River; the 2023 report associates it with Fall River city geocode 250744. HUD's 2024 HIC report continues to label it Fall River CoC. No MA-515 merger or transfer was found in the inspected HUD merger record or official scope history. The annual source row should therefore be matched one-to-one and consumed once in both PIT and HIC for every study year.

Sampled official HUD geometries for 2010, 2014, 2019, 2022, 2023, and B2024 align with the Fall River municipal geography. Compared with the 2020 Census Fall River city polygon, 99.937% of the 2010 MA-515 polygon and 99.940% of the 2014 polygon falls inside the city; the 2019, 2022, 2023, and B2024 percentages are 99.99995% or greater. The older source polygons differ from B2024 by about 2.37% of union area in 2010/2014, 2.29% in 2019, and 2.27% in FY2022. By FY2023, the difference is about 1.15 m². Municipality-scale evidence supports continued Fall River administrative scope, while the older outline residuals remain unexplained and are not described as exact polygon compatibility.

### Proposed annual Massachusetts reconciliation

| Year | P2024 PIT for frozen MA-505 | Annual HIC for frozen MA-505 | PIT and HIC for frozen MA-515 |
| ---: | --- | --- | --- |
| 2010 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2011 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2012 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2013 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2014 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2015 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2016 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2017 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2018 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2019 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2020 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2021 | excluded by study design | excluded by study design | excluded by study design |
| 2022 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2023 | use P2024 MA-505 once (preaggregated successor scope) | sum distinct MA-505 + MA-519 | use MA-515 once |
| 2024 | use single merged MA-505 row once | use single merged MA-505 row once | use MA-515 once |

For a v2 compatibility record, MA-505 PIT uses `preaggregated` with source ID MA-505 for 2010–2023; MA-505 HIC uses `sum` with source IDs MA-505 and MA-519 for 2010–2023; 2024 MA-505 uses `single`; and MA-515 uses `single` with source ID MA-515 for each included year. Keep the count operation and geometry evidence separate, cite the measured residual, and require both MA-505 HIC predecessor rows before emitting a sum.

**Evidence limits:** The inspected sources support administrative reconciliation for these Massachusetts components. They do not prove count methodology beyond the recorded P2024 row-sum evidence, classify every polygon residual, grant scientific approval, or approve the other CoCs in the 61-site panel. B2024 remains a 2026 capture of HUD's layer attributed to FY2024 coverage, not a January 2024 snapshot.

### Integration review of the extended scope

The orchestrator reviewed the extended source evidence and count operations on 2026-10-21 (local date). The administrative reconciliation above is accepted for the Massachusetts components of the 61-site design: historical MA-505 PIT is consumed once, HIC predecessors are summed before 2024, and MA-515 is consumed once. Historical polygon residuals must remain attached to the evidence rather than being relabeled as exact compatibility. This review can be cited when binding source-specific v2 records to the current membership identity; those full-panel records and the longitudinal analysis have not yet been produced.

## Fixed-2020 block-population sensitivity, 2026-10-26

The owner requested the same population sensitivity check used for El Paso, Imperial County, Honolulu, and San Francisco. This analysis compares the sampled 2010, 2014, 2019, 2020, FY2022, and FY2023 outlines with the study's **frozen live B2024** polygons. Before the merger, MA-505 is always the spatial union of MA-505 and MA-519; Fall River is MA-515 alone. This examines the two Massachusetts components of the Providence-Warwick site, not the full MSA. It does not fill the intervening years with verified annual geometry.

Population and block geometry are held fixed at 2020 using the native curated Census PL population and TIGER block datasets. Intersections use EPSG:6933, with the same uniform-within-block allocation, including water, and geometric-centroid alternative as the earlier sensitivity analyses. Every US county intersecting any sampled footprint is included, including Rhode Island neighbors, and all intersecting blocks have matched population. Percentage denominators are the **area-allocated fixed-2020 populations within each frozen B2024 CoC**: 485,067.994 for merged MA-505 and 93,902.849 for MA-515. These are allocation-based reference populations, not official published CoC populations or the full Providence MSA population. The corresponding centroid-assigned totals are 484,858 and 94,153.

Gross sensitivity sums population allocated to both the removed and added fragments, so it does not cancel transfers in opposite directions. Fractional residents are estimates, not observed residential locations.

| Earlier outline versus frozen B2024 | MA-505/MA-519 union: gross residents | % of MA-505 reference population | Fall River: gross residents | % of MA-515 reference population |
| --- | ---: | ---: | ---: | ---: |
| 2010 | 604.409 | 0.124603% | 116.814 | 0.124399% |
| 2014 | 372.350 | 0.076762% | 124.087 | 0.132144% |
| 2019 | 45.975 | 0.009478% | 97.183 | 0.103493% |
| 2020 | 45.965 | 0.009476% | 97.183 | 0.103493% |
| FY2022 | 42.285 | 0.008717% | 97.151 | 0.103459% |
| FY2023 | 0.001270 | <0.000001% | 0.000392 | <0.000001% |

The largest signed net area-allocation change, measured as frozen B2024 minus the earlier outline, is −230.669 residents for MA-505 (2010) and −105.641 for MA-515 (2010). Centroid assignment gives MA-505 net changes of −55, −48, +7, +7, +7, and zero in the table's year order; MA-515 gives −17 in every sampled earlier outline through FY2022 and zero for FY2023. Thus the recent differences are effectively zero under both allocation methods, and the older area-allocated differences remain below 0.14% of each component's reference population.

The entire-population ceilings for every block with any positive-area fragment intersection remain deliberately loose: 47,296–52,375 residents for MA-505 and 9,427–9,513 for MA-515 across sampled years. These are not estimated changes; they count every resident of a touched block, including numerical slivers. No populated whole block is contained in a changed footprint, giving a zero lower bound. At FY2023, no individual block intersection with the changed footprint reaches 1 m² in either component. Full threshold screens, removed/added allocations, and raw ceilings are retained in the machine-readable output. The block data alone cannot establish where within a partially intersected block residents, shelters, or encampments were located.

**Research recommendation:** These results support applying the same practical exploratory assumption to the reviewed Massachusetts residuals: treat the changed fragments as containing neither a shelter nor an encampment, retain the already reconciled whole-CoC PIT/HIC series, and revisit noticeable count changes or direct contrary evidence. The named municipal-scope evidence and population sensitivity support this treatment; exact annual polygon identity is not required by that assumption. Keep the numerical sensitivity and assumption attached to the analysis. The established count rules still apply: consume historical P2024 MA-505 PIT once, sum the distinct MA-505 and MA-519 HIC predecessors before 2024, and consume MA-515 once. No count adjustment or new cohort exclusion was made, and broader historical coverage work remains open.

The [retained analysis directory](</home/matt/HHP-Output/site-boundary-history-audit/massachusetts-block-population-sensitivity-20261026>) contains `analyze.py`, `summary.json`, `summary.csv`, provenance-bearing `summary.parquet`, `block-details.csv`, and the independent geometry cross-check. The manifest records input and script SHA-256 hashes. Run the script from this repository with `PYTHONPATH=. .venv/bin/python <analysis-directory>/analyze.py`; copy it to a fresh sibling directory before reproducing, as it refuses to overwrite retained outputs. Independent checks reconstructed gross allocations and ceilings from block rows, verified unique joins, centroid sums, net closure, source hashes against the prior audit, and embedded provenance. The earlier EPSG:5070 geometry metrics were independently recomputed for comparison with the prior audit. A maximum 0.022 m² numerical excess in coincident-edge block intersections was retained and measured; affected blocks had zero population, so this roundoff contributes zero to the population estimates.
