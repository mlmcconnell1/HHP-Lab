# CT-503 historical geography reconciliation

Audit date: 2026-10-21. This memo addresses CT-503 in the frozen 99% MSA–CoC site universe. It separates the HUD boundary vintage from HUD's reporting code crosswalk and from the geography actually used for each January PIT and HIC count.

**Study-owner decision, 2026-10-21:** Hold CT-503 out of the prospective longitudinal universe because the missing reporting-footprint evidence is likely difficult to obtain. The dependent Bridgeport–Stamford–Danbury MSA (14860), whose sole selected constituent is CT-503, is excluded in `recipes/msa-coc-longitudinal-sites.yaml`. The revised 99% universe contains 63 MSAs and 83 CoCs. This resolves CT's blocker through an explicit exclusion, not a finding of compatible boundaries. The original three compatibility records remain `unknown`, and old result bundles remain historical records. Reintroducing CT-503 would require resolving the evidence gap below.

## Finding

The frozen B2024 GIS polygon has a material footprint difference from the 2020, 2022, and 2023 HUD polygons. The older polygons follow Fairfield County's municipal territory; B2024 follows the 2023 Census/Connecticut planning-region units Greater Bridgeport and Western Connecticut. The town-level overlay isolates **Shelton moving out** and **Bridgewater and New Milford moving in**. This is a real GIS-vintage change, not a shoreline or projection artifact. It does **not** establish that CT-503 changed its service or PIT/HIC reporting jurisdiction in 2024.

The historical PIT and HIC records are one row per CoC. They do not carry town or project-location counts that would let this audit reallocate the historical totals to the changed footprint. The 2024 total also remains unverified against a January 2024 boundary: HUD's FY2024 registration crosswalk and B2024 geometry do not, by themselves, prove that the January PIT/HIC count used the later FY2024 footprint. Do not mark CT-503 historically compatible through an ID-only join; do not silently treat 2024 as the resolved baseline.

## Boundary evidence

The comparison uses the selected CT-503 polygon from the official national HUD boundary ZIPs for 2022 and 2023, curated B2020, and the study's frozen `coc__B2024.parquet` row. Measurements use EPSG:5070 and exact Shapely topology operations, with no tolerance-based approval. B2024 is a 2026 capture of the live HUD service attributed to FY2024, not a January 2024 archived snapshot; see [the source audit](site-pit-hic-2024-input-audit.md#evidence-for-the-fy2024-coc-geography).

| Boundary comparison | Historical area (km²) | B2024 area (km²) | Symmetric difference (km²) | Symmetric difference / union | Exact topological equality |
| --- | ---: | ---: | ---: | ---: | --- |
| B2020 vs B2024 | 1,670.259 | 1,798.757 | 295.606 | 15.704% | No |
| B2022 vs B2024 | 1,670.259 | 1,798.757 | 295.606 | 15.704% | No |
| B2023 vs B2024 | 1,671.482 | 1,798.757 | 292.531 | 15.549% | No |
| B2020 vs B2022 | — | — | 0.000065 | 0.0000039% | No |
| B2022 vs B2023 | — | — | 3.076 | 0.184% | No |

The small B2020–B2022 and B2022–B2023 differences are GIS-vintage differences; they do not overturn the town-membership finding. The 2023-to-B2024 difference decomposes geographically as follows:

| Difference from B2023 to B2024 | Area (km²) | Municipality overlay |
| --- | ---: | --- |
| Removed from CT-503 | 82.628 | Shelton |
| Added to CT-503 | 209.903 | New Milford 165.064; Bridgewater 44.839 |

For a cross-check independent of the CoC polygons, 2020 Census TIGER county subdivisions contain 23 Fairfield County towns. The 2023 Census TIGER/Connecticut OPM planning-region list gives 6 Greater Bridgeport towns and 18 Western Connecticut towns. Their combined membership retains 22 old Fairfield County towns, drops Shelton, and adds Bridgewater and New Milford. The historical HUD CoC polygons fall within the union of the old Fairfield County town polygons; B2024 falls within the union of the new region town polygons. In each comparison the excluded area is about 194–195 km², consistent with coastal/water geometry omitted from the HUD CoC footprint (the remainder was not separately classified). The town-level differences between the CoC polygons themselves match the three changed towns exactly.

This is strong evidence that the FY2024-attributed GIS boundary reflects the county-equivalent transition. It does not date that GIS change to the January 2024 count or prove that the CoC used the same footprint for reporting.

## CT HMIS reporting and coordinated-access geography

The official [Connecticut HMIS 2024 PIT report](https://cthmis.com/wp-content/uploads/2024/07/PIT-2024-Nutmeg-Final-Report_2024.07.30.pdf) dates the statewide count to **January 23, 2024** and explains that, since 2021, unsheltered PIT inputs came from HMIS outreach records plus a state PIT database for non-HMIS providers. It publishes results by statewide, Balance of State (BOS), and Opening Doors Fairfield County (ODFC) categories, including ODFC annual trends from 2016 through 2024. It does not publish municipality-level PIT totals or identify an exact municipality list for the ODFC record. The [2023 official report](https://cthmis.com/wp-content/uploads/2024/07/HIC%20PIT%202023.pdf) likewise reports ODFC as an aggregate. Public historical CT HMIS reports and HUD PIT/HIC workbooks reviewed here therefore do not expose town-level values that could reallocate the 2020, 2022, 2023, or 2024 CT-503 totals.

There is positive evidence that operational CAN coverage did not simply follow the old Fairfield County line. The CT Coordinated Access Network policy hosted by the CT Balance of State site is marked **Updated 3/27/23**, before the January 2024 count. Its [Fairfield County CAN list](https://housing-collective.files.svdcdn.com/production/downloads/CT-CAN-Policies-HUB.pdf?dm=1725508867) includes Bridgewater and New Milford and places Shelton in Greater New Haven CAN. The [Connecticut DMHAS CAN brochure](https://portal.ct.gov/-/media/dmhas/publications/can-brochures.pdf) gives the same split. The policy's Fairfield County CAN list also includes Roxbury, which lies outside the Western/Greater Bridgeport planning-region combination represented by B2024. CAN service geography is not itself proof of the formal HUD CoC boundary or the exact PIT/HIC reporting rule, but this pre-count evidence means the HUD GIS town changes cannot be treated as evidence that ODFC's service area changed in 2024.

The 2024 PIT report and the HUD FY2024 GIS catalog establish year and CoC identity, not a town crosswalk between the count and the live GIS layer. The current ODFC public pages are not used to infer the January 2024 boundary. No public town-level PIT allocation or HIC project-location extract was found. Resolving exact equivalence requires a dated CT-503 PIT/HIC submission or reporting crosswalk showing the included towns/projects, or permissioned HMIS/PIT source records with town-of-count and project-location assignments plus the CoC's applicable boundary rule.

Accordingly, the evidence supports neither calling HUD's GIS polygon incorrect nor treating it as an authoritative description of January 2024 reporting. It is a valid FY2024-attributed GIS product whose equivalence to the actual CT-503 service/reporting footprint remains unverified. The previous conclusion that the GIS town transition demonstrated an administrative service-area change is superseded.

## Official code records and temporal limits

HUD's [FY2024 Geographic Codes Report](https://www.hud.gov/sites/dfiles/CPD/documents/CoC/FY2024-GeoCodes-Report.pdf) lists the new Connecticut codes `099120` Greater Bridgeport Planning Region and `099190` Western Connecticut Planning Region, and lists `099001` Fairfield County among the removed codes. It also adds `090996` Shelton. The [Connecticut OPM municipality-to-planning-region list](https://portal.ct.gov/opm/-/media/opm/igpp/municipal-directories/municipalities-planning-region-cog.pdf?hash=5FA6591861C9AFC393A28D510CFEA8A3&rev=e22636b55de446c4b6bb5e7d37b81810) puts Shelton in Naugatuck Valley and Bridgewater/New Milford in Western Connecticut. The [Census/Connecticut county-equivalency notice](https://public-inspection.federalregister.gov/2022-12063.pdf) explains that planning regions replace counties as county-equivalent units in Census products; it does not state that HUD PIT/HIC counts were reallocated.

The FY2023 HUD CT-503 profile map labels seven municipality geocodes (Bridgeport, Danbury, Fairfield, Greenwich, Norwalk, Stamford, Stratford) plus Fairfield County. The official HUD search-index result for the **FY2023 Geocodes/PPRN report** lists `099001 Fairfield County` for CT-503; HUD's report URL returned 404 on retrieval, so its index excerpt is retained at `~/HHP-Output/site-boundary-history-audit/connecticut/hud_fy2023_ct503_geocodes_index_excerpt.json` rather than treated as a preserved PDF. An official HUD search-index result for the **2024 CoC and ESG Recipient Crosswalk** lists nine CT-503 geographies: those same seven city codes plus `099120 Greater Bridgeport Planning Region` and `099190 Western Connecticut Planning Region`. That index result is retained as a qualified excerpt at `~/HHP-Output/site-boundary-history-audit/connecticut/hud_fy2024_ct503_crosswalk_index_excerpt.json`. HUD's generic crosswalk URL has since been updated to FY2025; no immutable FY2024 crosswalk PDF was recoverable in this audit. Accordingly, indexed crosswalk records are corroboration, not preserved originals. The FY2024 geocode catalog, OPM town list, Census town geometries, and direct B2024 overlay independently support the town transition. Current FY2025 crosswalk content was not used as proof of the FY2024 assignment.

HUD's FY2023 dashboard notes that information is submitted by CoCs and is not independently verified by HUD; methodology changes may affect count comparisons. The [FY2023 CT-503 dashboard](https://files.hudexchange.info/reports/published/CoC_Dash_CoC_CT-503-2023_CT_2023.pdf) is an annual report keyed to CT-503, not a town-level count file.

## Count rows and what can be reconstructed

The study's cumulative PIT vintage P2024 has the same CT-503 unsheltered value as PIT vintage P2025 in each audited year below. HIC source files are annual. These equalities check vintage revision only; they do not resolve geography.

| Count year | PIT unsheltered (P2024) | PIT total | HIC shelter year-round beds | HIC total beds |
| --- | ---: | ---: | ---: | ---: |
| 2020 | 57 | 697 | 747 | 2,957 |
| 2022 | 52 | 603 | 574 | 2,975 |
| 2023 | 67 | 597 | 550 | 3,061 |
| 2024 | 109 | 713 | 532 | 3,163 |

The [HUD PIT workbook](https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb) and HIC by-CoC/by-state products retain one aggregate record for CT-503, not town allocations. The official CT HMIS reports also show only ODFC/BOS/statewide results. A [2024 Bridgeport consolidated-plan extract](https://www.bridgeportct.gov/sites/default/files/2025-05/Website%20-%20City%20of%20Bridgeport%202025-2029%20Con%20Plan%20Draft-4-30-Original.pdf) labels the 713 PIT total as Opening Doors Fairfield County, without municipality-specific PIT values. No public municipal PIT table or historical project-location HIC extract was found that can recompute the 2020/2022/2023 totals on B2024 or another exact 2024 scope.

Therefore CT-503 observations remain unresolved for a panel frozen to B2024, including the 2024 source row. No approval, remapping, or town allocation is made here. The previous 2024 `compatible` record is superseded with `unknown` in each overlap-threshold compatibility artifact; this leaves all other records and site memberships intact while preventing CT-503's value from being treated as geography-validated. Existing recipe locks and result bundles are retained as historical artifacts; their locks predate this evidence correction and are intentionally stale against the current compatibility artifacts.

## Retained evidence

Source downloads, hashes, selected CT-503 geometries, year-by-year geometry metrics, the TIGER town crosswalk, count rows, and newly retrieved CT HMIS/CAN documents are retained under `~/HHP-Output/site-boundary-history-audit/connecticut/`. The principal files are `source_manifest.json`, `ct503_selected_vintage_boundaries.geojson`, `ct503_geometry_comparison.json`, `ct503_municipality_transition.csv`, `ct503_municipality_transition.json`, `ct503_aggregate_count_rows.json`, `ct503_reporting_geography_sources.json`, and `hud_fy2024_ct503_crosswalk_index_excerpt.json`.
