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

## Scope and limitations

This work verifies acquisition, ZIP integrity, archive structure, projection text, record counts, and CoC identifiers. It does not normalize or reproject geometries, assess polygon topology, or determine whether differences in IDs indicate a boundary change or a different reporting jurisdiction. The per-state and per-territory evidence, including every URL, hash, field list, layer ID, and discrepancy, is in `manifest.json` for follow-on validation.
