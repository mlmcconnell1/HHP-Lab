# Historical CoC reconciliation inventory (2010–2024)

**Scope:** the current frozen design has 61 sites and 81 CoCs, uses 2010–2024 except 2021, and excludes CT-503, OR-504, and PA-512. The 854 possible site-years have source rows in the current PIT/HIC coverage check. This inventory separates row coverage from historical geography compatibility.

The detailed machine-readable membership, source-hash, archive, annual-coverage, and merger evidence is in `~/HHP-Output/site-boundary-history-audit/inventory/historical-reconciliation-inventory.json` (external artifact). The current membership and count-coverage inputs are under `~/HHP-Output/msa_coc_longitudinal_2010_2024_sites/`.

## Boundary archive coverage

Actual HUD national boundary archives were inspected for FY2019, FY2020, FY2021, FY2022, FY2023, and FY2024. All 81 current selected IDs occur in every archive. The FY2019, FY2020, FY2021, and FY2024 ZIPs were downloaded on 2026-10-22 into `~/HHP-Output/site-boundary-history-audit/archives/`; full SHA-256, source URL, size, server metadata, and retrieval time are recorded in the JSON. FY2022 and FY2023 archives were already present and are recorded there as well.

The comparison uses the actual archived polygons, reprojected to EPSG:6933, and computes same-ID symmetric difference divided by union against the FY2024 archive. This is a geometric screen only. Excluding the separately assigned MA-505 reconciliation, the number of selected IDs with a ratio above 0.00001 was 64 in 2019, 59 in 2020, 57 in 2021, 49 in 2022, 3 in 2023, and 0 in 2024. The 2023 residuals are TX-603, CA-613, and CA-612. Those counts are summary-only; the detailed 2023 overlay already present in the audit directory uses the curated/live B2024 geometry as its reference, so it is a different comparison. The FY2024 national archive has not been directly matched to the curated/live B2024 geometry. Polygon differences do not establish a reporting-area change or a count mismatch.

There is no complete, verified national archive sequence for 2010–2018. Earlier evidence is fragmented: authentic HUD MA state archives for 2010 and 2014 are in `~/HHP-Output/site-boundary-history-audit/providence/`; a local all-state HUD Exchange 2018 shapefile exists; and authentic per-state 2020 ZIPs are in `data/raw/hud_exchange/2020/states/`. A direct read of the 2018 shapefile's `COCNUM` field found no FL-500 or OR-504 feature. OR-504 is excluded from the current design; FL-500 remains selected and has no verified geometry in this 2018 source. The Stanford 2014 national copy remains a research lead and is not treated here as inspected evidence. Do not extend 2019–2024 geometry evidence backward over 2010–2018.

## Additional merger scope risks

The case-specific findings below are superseded where resolved by the integration review at the end of this note. The original screening table is retained to show why native-source verification was necessary.

The PIT workbook `CoC Mergers` sheet lists four selected successor IDs beyond the separately handled MA-505 case:

| Predecessor → selected successor | Listed merger year | Current evidence and scope implication |
| --- | ---: | --- |
| CA-527a → CA-515 | 2010 | CA-527a has no row in the curated 2010 HIC; no pre-2010 year is in the study. Confirm the 2010 PIT/HIC source footprint before treating this as cleared. |
| CA-610 → CA-601 | 2011 | Both IDs have separate 2010 HIC rows: 1,584 and 2,366 year-round shelter beds. The selected successor-only PIT value is not a native-coverage proof. Determine whether CA-610's 2010 territory falls within the frozen 2024 site before summing. |
| CA-605 → CA-611 | 2013 | Separate predecessor HIC rows exist in 2010–2013. For example, 2010 has 227 predecessor and 355 successor shelter beds; 2011 has 292 + 234; 2012 has 279 + 140; 2013 has 247 + 206. Reconcile predecessor geography to the frozen footprint year by year. |
| FL-516 → FL-503 | 2015 | Separate predecessor HIC rows exist in 2010–2014. Shelter-bed pairs for 2010–2014 are 78+552, 106+434, 71+416, 106+369, and 75+433. Confirm those jurisdictions against the frozen selected footprint before summing or excluding. |

The PIT merger rows are present in both the 2024 and 2025 cumulative workbooks. For these four cases, the historical rows in the inspected P2024 and P2025 curated vintages use the selected successor ID, with no separate predecessor row; P2024/P2025 also agree on the selected historical count. The 2007–2020 PIT workbook has the same successor-only row pattern. These workbooks do not provide an independent annual-native geography check, so they cannot establish whether the survivor value is backcast, combined, or otherwise assigned. The P2024/P2025 data rows alone are not proof of a frozen-footprint count.

Annual HIC tables retain predecessor and successor rows separately in the years shown. Their selected successor counts remain complete, but completeness does not make the selected rows geographically compatible. A county or project allocation may be needed if a predecessor jurisdiction only partly overlaps the frozen 2024 site. The full predecessor/successor values by year, HIC source-table hashes, PIT vintage values, and all 81 selected memberships are in the machine-readable artifact.

## Coverage interpretation and next checks

The source coverage artifact reports 81 PIT rows and 82 HIC rows per pre-2024 year (the extra HIC row is MA-519, handled by the assigned Massachusetts reconciliation) and 81 HIC rows for 2024. It reports no missing, duplicate, or invalid required rows. That establishes availability and numeric validity for the requested rows; it does not establish that each year's CoC reporting area equals the frozen 2024 membership.

Before constructing the historical panel, resolve the three open in-window merger cases by checking source-year PIT reports and the predecessor jurisdictions against the frozen site footprints. Use predecessor values only when their geography lies in the current selected footprint; otherwise obtain a sub-CoC split where possible. Separately confirm CA-527a/CA-515 for 2010, check FL-500's 2018 archive coverage, and retain the existing MA-505 predecessor reconciliation. Keep any unresolved year-site outcome flagged rather than filling it from whole-CoC totals.

## Native-source integration review for coclab-fjcho

The orchestrator reviewed the Luna High investigations against the selected P2024 PIT vintage and annual HIC tables. The following are administrative count reconciliations, not assertions that historical polygons exactly equal the frozen boundary. They apply to membership identity `4861b55cdb14c1e6dc14f270dc2d99a9952da4cd2aae844ee65163ce83771144`, with 2021 excluded.

### San Diego: reviewed count operations

HUD's native 2010 PIT reports give CA-601 (city) 4,526 persons: 2,477 sheltered and 2,049 unsheltered; CA-610 (county remainder) has 3,857: 1,441 sheltered and 2,416 unsheltered. Their sums, 8,383 total / 3,918 sheltered / 4,465 unsheltered, exactly reproduce P2024's historical CA-601 row. Consume that row **once**, with `preaggregated` predecessor scope CA-601 + CA-610. Annual 2010 HIC requires `sum`: 2,366 + 1,584 = 3,950 ES+TH+SH year-round beds. HUD's 2011 supplemental AHAR records the merger, and 2011 HIC has the countywide successor only (3,982 shelter beds). From 2011 onward, use CA-601 once in each panel.

The city-plus-county merger evidence and current CoC jurisdiction description support the countywide administrative unit. The frozen HUD polygon differs from Census's county polygon by 6.394% of county area; that comparison is not an historical predecessor overlay and its cause is not established. Retain that limitation instead of claiming exact geometry. Raw PDFs, URLs, hashes, and arithmetic are in [the San Diego evidence](</home/matt/HHP-Output/site-boundary-history-audit/mergers/san-diego/san-diego-reconciliation.json>).

### Ventura: panel-specific transition years

Native HUD PIT reports prove that P2024's CA-611 rows already combine CA-605 and CA-611 in 2010–2012:

| Year | Native total-person sum | Sheltered sum | Unsheltered sum |
| ---: | ---: | ---: | ---: |
| 2010 | 1,295 + 520 = 1,815 | 380 + 144 = 524 | 915 + 376 = 1,291 |
| 2011 | 1,172 + 638 = 1,810 | 292 + 538 = 830 | 880 + 100 = 980 |
| 2012 | 1,404 + 522 = 1,926 | 543 + 140 = 683 | 861 + 382 = 1,243 |

Use `preaggregated` for those three P2024 rows, consuming CA-611 once with predecessor scope CA-605 + CA-611. HUD's 2013 report already gives a single merged CA-611 count (1,674 total, 600 sheltered, 1,074 unsheltered), so PIT uses `single` from 2013 onward. The report asset has `2012` in part of its filename but explicitly identifies its PIT date as January 29, 2013; the document content establishes the year.

HIC retains separate CA-605 and CA-611 rows through **2013**, so use `sum` in 2010–2013: 582, 526, 419, and 453 shelter beds respectively. HIC uses the single CA-611 row from 2014 onward. The PIT merger year cannot be applied mechanically to HIC.

County reports contain some totals different from HUD's published submissions, so the reconciled series retains the pinned HUD source values. Countywide administrative scope is supported by the merger and jurisdiction records; Census/HUD outline differences remain distinct from that count decision.

Source PDFs, hashes, and full arithmetic are recorded in [the Ventura evidence](</home/matt/HHP-Output/site-boundary-history-audit/mergers/ventura/ventura_ca605_ca611_reconciliation.json>). In particular, [HUD's 2013 report](https://files.hudexchange.info/reports/published/CoC_PopSub_CoC_CA-611-2012_CA_2013.pdf) establishes the consolidated PIT value, whereas annual HIC still requires two source rows that year.

### Placer: a merger followed by a split

The original CA-527 represented Nevada County, not the unrelated later CA-527 for Tehama County. Its merger into CA-515 did not make all historical CA-515 counts comparable to today's Placer-only footprint. HUD's [2020 unsheltered-homelessness study](https://www.huduser.gov/portal/sites/default/files/pdf/Implementing-Aprroaches-to-Address-Unsheltered-Homlessness-2020.pdf), printed page 12, footnote 26, explicitly says CA-515's 2015 and 2017 service area included Nevada County, which had its own CA-531 in 2019. The study combined those two CoCs to maintain its own geography; that solution would expand this study's frozen site and cannot be silently adopted here.

Official 2010 and 2014 HUD California archives independently show CA-515 covering both Placer and Nevada counties, despite the 2010 label naming only Placer. The frozen CA-515 polygon covers Placer without Nevada. CA-531 first appears in both the selected cumulative PIT vintage and annual HIC in 2019. These findings support rejecting the unadjusted 2010–2018 CA-515 rows for the frozen Placer footprint, rather than accepting them based on the surviving identifier or county label.

CA-515 belongs to multi-CoC Sacramento MSA 40900. A missing Placer component makes the whole Sacramento site-year incomplete; dropping that component while retaining the other CoCs would create a partial site. The universe is unchanged. County-specific PIT and HIC recovery, or an owner-selected whole-site holdout, is needed before a balanced full-period panel can use this site. Available 2017 county reports disagree with the pinned cumulative HUD totals, so their figures cannot safely be subtracted without reconciling publication revisions and definitions.

The [Placer geometry evidence](</home/matt/HHP-Output/site-boundary-history-audit/mergers/san-diego/polk-geometry-assist/ca515-geometry.json>) pins both official state archives. The [joint Polk/Placer review](</home/matt/HHP-Output/site-boundary-history-audit/mergers/polk-placer/reconciliation.json>) records the split and source-count limits. CA-515 is treated as Placer-only from 2019 onward, when Nevada has separate CA-531 PIT and HIC records.

### Polk: reviewed predecessor union and counts

The official 2010 and 2014 Florida boundary archives show FL-503 + FL-516 covering over 99.997% of frozen FL-503. The two predecessor polygons are disjoint apart from 5.7 m² in 2010 and exactly disjoint in 2014. Their union has only negligible overlap with Highlands County; historical labels alone had overstated that concern. The [geometry audit](</home/matt/HHP-Output/site-boundary-history-audit/mergers/san-diego/polk-geometry-assist/polk-geometry-reconciliation.json>) preserves the nonzero outline residuals.

Native HUD PIT reports reproduce all three P2024 measures for every pre-merger study year. Use FL-503 once with `preaggregated` scope FL-503 + FL-516 for 2010–2014. HIC uses `sum` over those two annual rows during the same years. Both panels use the single successor from 2015 onward.

| Year | Native unsheltered sum | Native sheltered sum | HIC shelter-bed sum |
| ---: | ---: | ---: | ---: |
| 2010 | 309 + 68 = 377 | 366 + 77 = 443 | 552 + 78 = 630 |
| 2011 | 121 + 481 = 602 | 403 + 95 = 498 | 434 + 106 = 540 |
| 2012 | 217 + 367 = 584 | 253 + 44 = 297 | 416 + 71 = 487 |
| 2013 | 140 + 55 = 195 | 278 + 47 = 325 | 369 + 106 = 475 |
| 2014 | 164 + 124 = 288 | 212 + 36 = 248 | 433 + 75 = 508 |

The [2011–2014 native PDF audit](</home/matt/HHP-Output/site-boundary-history-audit/mergers/ventura/polk-pit-assist/polk_fl503_fl516_pit_reconciliation.json>) records exact URLs and hashes. The orchestrator independently downloaded the [2010 FL-503 report](https://files.hudexchange.info/reports/published/CoC_PopSub_CoC_FL-503-2010_FL_2010.pdf) and [2010 FL-516 report](https://files.hudexchange.info/reports/published/CoC_PopSub_CoC_FL-516-2010_FL_2010.pdf), retained under `mergers/root-review/`, and verified their person-count rows. Their SHA-256 values are `0e4e52634eb14550603030159a2bd17a318d42eb41ac4b99686a8f6b8f27d335` and `d10c62a6b0b1808ab4645742fd03205aa4ddc6e6f1d57b086710f0ccf6265efa`.

### Membership-bound evidence and package verification

[The v2 evidence](../recipes/site-inputs/coc-boundary-compatibility-longitudinal-mergers.json) contains 112 explicit records: four CoCs × 14 years × two source panels. San Diego, Ventura and Polk use the reviewed operations above. CA-515 has 18 incompatible records (PIT and HIC for 2010–2018), and uses a single row from 2019 onward. Other CoCs are omitted and resolve to `unknown`; this file is a partial evidence set, not clearance of the entire universe. `pit_p2024` declares publication vintage 2024 explicitly; that label alone does not establish historical geography, which is assessed separately in each record.

The package validator and site-year assembler accepted the evidence against the full 61-site membership. A diagnostic produced 42 complete site-years per measure for the three independently complete sites and left Sacramento incomplete. It checked the predecessor sums, including Ventura's 2013 HIC transition, without emitting partial-site counts. The diagnostic CSV and validation/provenance JSON are under `~/HHP-Output/site-boundary-history-audit/mergers/root-review/`. This is not a completed longitudinal results recipe. Placer reconstruction or a whole-site design decision remains outstanding under `coclab-fjcho`; broader boundary work remains under `coclab-k0hwb`.
