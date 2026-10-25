# Public historical CoC boundary archive availability, 2011–2017

Survey run: 2026-10-22. This is a bounded availability inventory, not a census of every possible archive or every state/year file. It covers years 2011–2017; the 2010 check is tracked separately, and 2019–2024 copies are already archived elsewhere in this project.

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

## Acquisition recommendation and open work

1. Build a complete annual manifest from HUD's state/territory roster for 2011–2017, then probe each expected endpoint with HEAD before downloading. Record missing, renamed, and zero-size responses distinctly. The present state sample cannot serve as that manifest.
2. Acquire the full per-state set in bounded annual batches, initially 2011–2012 (legacy individual-CoC layout), then 2013–2017. Preserve original ZIPs, HTTP final URLs, retrieval timestamps, and SHA-256 hashes; inspect extracted shapefile fields and coordinate reference systems before ingestion.
3. Keep Stanford 2014 as an independent national preservation copy and keep the 2017 repository source labeled as third-party. Compare national copies with reconstructed state collections after all expected states and territories are accounted for.
4. Search web archives and institutional holdings for national 2011–2013, 2015–2016 copies and any files absent from the HUD endpoint. The current survey found no national-source evidence for those vintages.
