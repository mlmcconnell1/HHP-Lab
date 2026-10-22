# HUD 2010 CoC state boundary acquisition

Acquired HUD Exchange's 2010 state shapefile archives for all 50 states and DC. The reproducible downloader and complete machine-readable record are stored outside the repository:

- Script: `/home/matt/HHP-Output/historical-site-selection/coc-2010/acquire_audit.py`
- Manifest: `/home/matt/HHP-Output/historical-site-selection/coc-2010/manifest.json`
- Raw ZIPs and native listing PDF: `/home/matt/HHP-Output/historical-site-selection/coc-2010/archives/` and `2010ListingCoCs.pdf`

Rerun with `python3 /home/matt/HHP-Output/historical-site-selection/coc-2010/acquire_audit.py`. It uses four concurrent workers, a 30-second request timeout, no retries, and does not overwrite existing raw archives. It records archive URLs, retrieval timestamps, byte sizes, SHA-256 hashes, ZIP member CRC results, shapefile paths, layer and feature counts, DBF fields, CoC IDs, and `.prj` WKT. The native listing PDF is downloaded and parsed with `pdftotext` when that executable is available.

## Acquisition results

All **51 of 51** state/DC URLs returned ZIP archives that passed ZIP format and member CRC checks. Their SHA-256 hashes and retrieval dates are in the manifest. The source URL pattern was `https://files.hudexchange.info/reports/published/CoC_GIS_State_Shapefile_{ST}_2010.zip`; this includes DC using `DC` in place of a state abbreviation. The script retrieved the previously confirmed OR and PA archives and CA, FL, and MA along with the remaining jurisdictions.

The state/DC archives contain **438 shapefile layers and 438 geometry records**, with **441 layer-to-CoC ID assignments and 437 distinct CoC IDs**. Each layer has one geometry record. All state/DC `.prj` files use the same projected North America Albers Equal Area Conic definition on NAD83, in meters; the WKT does not include an EPSG authority identifier. DBF attributes include `CoCNum`, `CoCName`, and geometry measures. One Massachusetts shared-jurisdiction layer lists MA-510, MA-513, MA-516, and MA-518; each of those IDs also appears in its own layer. The shared layer adds four repeated assignments, accounting for 441 assignments versus 437 unique IDs; it has three more assignments than a single-ID layer. The manifest records the overlap without deduplicating geometries.

## Native listing comparison

The comparison source is HUD's [2010 Listing of CoCs](https://files.hudexchange.info/resources/documents/2010ListingCoCs.pdf), revised April 2010. Its introduction says it includes active CoCs registered in the 2009 CoC Registration process. It lists **446 state/DC IDs** and **four territory IDs**. The state/DC archives have **437 unique IDs**, of which **434 are also in the native listing**. Twelve listed IDs are absent from the archives, and three archive IDs are absent from the listing:

| State | Listed in native 2010 source but absent from state archive |
|---|---|
| AR | AR-502, AR-506, AR-511 |
| CA | CA-525, CA-527 |
| CT | CT-504, CT-507 |
| MI | MI-522 |
| NE | NE-503 |
| NY | NY-506 |
| SC | SC-504 |
| TX | TX-613 |

The three archive IDs absent from the listing are **FL-515, FL-516, and KS-507**. Treat these differences as possible registration, reporting, or source-vintage changes; they do not indicate a failed download. The native list describes a registration process, while the boundary archive is a separate source, and this audit does not resolve the timing or jurisdictional explanation for each difference.

The same source pattern was tried separately for the five U.S. territories. GU-500, PR-502, PR-503, and VI-500 are present in both the native listing and valid territory archives. AS and MP have no IDs in the native listing and their archive URLs returned HTTP 404. Territory results are recorded separately in the manifest and are not counted against the 51 state/DC acquisition.

## Reconciliation and normalized artifact

The archived state/DC IDs are not interchangeable with the April 2010 registration list or with counts. The curated 2010 HIC contains 446 distinct IDs, including four territories. Compared as sets:

- The native listing has 446 state/DC IDs and four territory IDs (450 total).
- The state/DC archives contain 437 individual CoC IDs; 434 also occur in the native listing. Twelve listed state/DC IDs have no archive polygon, while FL-515, FL-516, and KS-507 have archive polygons but are absent from the listing.
- The native HIC has 446 IDs: all 437 state/DC archive IDs and all four available territory IDs occur in HIC. Five HIC IDs have no archive polygon: AR-512, CA-524, CA-525, NE-503, and NY-506. CA-525, NE-503, and NY-506 are in the listing; AR-512 and CA-524 are absent from both the listing and archive. HIC omits nine state/DC IDs that do appear in the listing. These set differences show why the listing cannot stand in for counts: HIC corroborates available count records, but does not establish missing geometry or resolve boundary-vintage differences.
- The retrospective PIT file `pit_vintage__P2020.parquet`, filtered to 2010, contains 389 CoC IDs. It is a cumulative 2007–2020 PIT vintage, not the native 2010 reporting roster, so it is only a secondary ID/count crosscheck. Five PIT IDs have no matching archive geometry: AR-512, CA-524, CA-525, LA-509, and NY-506. CA-525 and NY-506 also appear in the native listing; AR-512, CA-524, and LA-509 do not. The PIT-derived MSA aggregation uses B2018 shapes and is not evidence that those IDs have B2010 boundaries.

The geospatial ingest uses `hhplab ingest boundaries --source hud_exchange --vintage 2010 --archive-dir ... --archive-manifest ...`. It reads the audited state/DC ZIPs plus the available GU, PR, and VI ZIPs, verifies each archive's SHA-256, normalizes valid single-CoC layers with the production geometry pipeline, and writes the canonical `data/curated/coc_boundaries/coc__B2010.parquet` with embedded provenance and registry entries. The artifact has 441 unique CoC features (437 state/DC plus four territories). Its provenance says `coverage_status=partial_unresolved`, lists the twelve native-listing gaps and three archive-only IDs, and records the unavailable AS/MP archive attempts. It is not a complete 2010 national boundary set.

The Massachusetts archive has an additional feature whose ID is the compound value `MA-510_MA-513_MA-516_MA-518 Shared Jurisdiction`. The archive also supplies separate polygons for all four listed CoCs. In the source Albers coordinates, the shared polygon intersects the union of the four individual polygons by only about 2.8 square metres while covering about 1.93 billion square metres itself. That is no defensible basis to assign it to all four IDs or subtract it from their shapes. The compound layer is quarantined from B2010. Its overlap with 2023 MSA polygons flags Boston-Cambridge-Newton (14460), Worcester (49340), and Manchester-Nashua (31700); the separate 2018 MSA-CoC crosswalk associates nearly all shared-layer area with Boston. These overlaps identify where the source discrepancy may affect a site count, but do not resolve the feature's meaning or support adding its area to an individual CoC.

The twelve missing native-listing state/DC IDs are AR-502, AR-506, AR-511, CA-525, CA-527, CT-504, CT-507, MI-522, NE-503, NY-506, SC-504, and TX-613. Only CA-525 and NY-506 have a row in the retrospective PIT file; CA-525, NE-503, and NY-506 have HIC rows. No missing ID has a source polygon. Names and the current Census county/MSA membership give candidate affected sites for some, but do not recover historical spatial boundaries:

| Missing 2010 CoC | Native 2010 name | Candidate 2023 MSA ID from named locality/county | Evidence limit |
|---|---|---:|---|
| AR-502 | Conway/Arkansas River Valley | 30780 | Faulkner County is in Little Rock-North Little Rock-Conway; full historic CoC extent is unavailable. |
| AR-506 | Johnson, Pope, Yell Counties | — | Named counties do not appear in the 2023 MSA county membership. |
| AR-511 | Jonesboro/Northeast Arkansas | 27860 | Craighead County is in Jonesboro; full historic CoC extent is unavailable. |
| CA-525 | El Dorado County | 40900 | El Dorado County is in Sacramento-Roseville-Folsom; HIC and retrospective PIT rows exist. |
| CA-527 | Nevada County | — | Nevada County does not appear in the 2023 MSA county membership. |
| CT-504 | Middletown/Middlesex County | 25540 | Current planning-region membership places Lower Connecticut River Valley in Hartford. |
| CT-507 | Norwich/New London City and County | 35980 | Current Southeastern Connecticut planning region is in Norwich-New London. |
| MI-522 | Alpena, Iosco, Presque Isle/NE Michigan | — | Named counties do not appear in the 2023 MSA county membership. |
| NE-503 | Southwest Nebraska | — | No complete county list is supplied by the native CoC name; HIC row exists. |
| NY-506 | Fulton, Montgomery, Schoharie Counties | 10580 | Schoharie County is in Albany-Schenectady-Troy; the full historic CoC extent is unavailable. |
| SC-504 | Florence City & County/Pee Dee | 22500 | Florence County is in Florence; neither the 2010 HIC table nor the retrospective P2020 PIT file has a row. |
| TX-613 | Longview/Marshall Area | 30980 | Gregg and Harrison counties are in Longview; neither the 2010 HIC table nor the retrospective P2020 PIT file has a row. The PIT result describes that file's coverage, not native 2010 reporting. |

These MSA IDs are screening leads based on names/current county membership, not certified assignments for the missing 2010 polygons. The shared Massachusetts feature spatially overlaps Boston-Cambridge-Newton (14460), Worcester (49340), and Manchester-Nashua (31700); its B2018 CoC-to-MSA allocation is nearly all Boston. Any 2010 site count must therefore be described as conditional on available archive geometry and should flag candidate sites in the listed areas. The available shapes cannot certify a complete 2010 national count or rule out a false site-screen pass where omitted polygons would change coverage.

The machine-readable acquisition evidence remains in `manifest.json`; normalized geometry, discrepancy lists, hashes, repair metadata, and coverage status are embedded in B2010 provenance.
