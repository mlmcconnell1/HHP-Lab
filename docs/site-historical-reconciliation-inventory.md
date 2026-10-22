# Historical CoC reconciliation inventory (2010–2024)

**Scope:** the current frozen design has 61 sites and 81 CoCs, uses 2010–2024 except 2021, and excludes CT-503, OR-504, and PA-512. The 854 possible site-years have source rows in the current PIT/HIC coverage check. This inventory separates row coverage from historical geography compatibility.

The detailed machine-readable membership, source-hash, archive, annual-coverage, and merger evidence is in `~/HHP-Output/site-boundary-history-audit/inventory/historical-reconciliation-inventory.json` (external artifact). The current membership and count-coverage inputs are under `~/HHP-Output/msa_coc_longitudinal_2010_2024_sites/`.

## Boundary archive coverage

Actual HUD national boundary archives were inspected for FY2019, FY2020, FY2021, FY2022, FY2023, and FY2024. All 81 current selected IDs occur in every archive. The FY2019, FY2020, FY2021, and FY2024 ZIPs were downloaded on 2026-10-22 into `~/HHP-Output/site-boundary-history-audit/archives/`; full SHA-256, source URL, size, server metadata, and retrieval time are recorded in the JSON. FY2022 and FY2023 archives were already present and are recorded there as well.

The comparison uses the actual archived polygons, reprojected to EPSG:6933, and computes same-ID symmetric difference divided by union against the FY2024 archive. This is a geometric screen only. Excluding the separately assigned MA-505 reconciliation, the number of selected IDs with a ratio above 0.00001 was 64 in 2019, 59 in 2020, 57 in 2021, 49 in 2022, 3 in 2023, and 0 in 2024. The 2023 residuals are TX-603, CA-613, and CA-612. Those counts are summary-only; the detailed 2023 overlay already present in the audit directory uses the curated/live B2024 geometry as its reference, so it is a different comparison. The FY2024 national archive has not been directly matched to the curated/live B2024 geometry. Polygon differences do not establish a reporting-area change or a count mismatch.

There is no complete, verified national archive sequence for 2010–2018. Earlier evidence is fragmented: authentic HUD MA state archives for 2010 and 2014 are in `~/HHP-Output/site-boundary-history-audit/providence/`; a local all-state HUD Exchange 2018 shapefile exists; and authentic per-state 2020 ZIPs are in `data/raw/hud_exchange/2020/states/`. A direct read of the 2018 shapefile's `COCNUM` field found no FL-500 or OR-504 feature. OR-504 is excluded from the current design; FL-500 remains selected and has no verified geometry in this 2018 source. The Stanford 2014 national copy remains a research lead and is not treated here as inspected evidence. Do not extend 2019–2024 geometry evidence backward over 2010–2018.

## Additional merger scope risks

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
