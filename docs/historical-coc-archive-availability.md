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
| 2014 | [Stanford PURL `wm843fh7398`](https://purl.stanford.edu/wm843fh7398) and its [Cocina JSON metadata](https://purl.stanford.edu/wm843fh7398.json) | Public-download metadata identifies “CoC Grantee Areas, 2014,” a HUD-created shapefile current as of Nov. 14, 2014. Its public [FlatGeobuf file](https://stacks.stanford.edu/file/druid:wm843fh7398/CoC_Grantee_Areas.fgb) downloaded (45,646,240 bytes; SHA-256 `5e63fc94bf48c901ea91251effda2c5c2bb808c3b02fbb64aa89b1ad8897f64f`). Opened with repository `uv` environment: 409 MultiPolygon features, EPSG:4326, 52 `STUSAB` codes, 409 distinct non-null `COCNUM` values, and bounds `[-178.228, 17.881, -65.244, 71.390]`. | A national preservation copy exists for 2014 by catalog description and broad spatial extent. However, every feature's `YEAR` value is `2015`, which does not match the record title/date. This could be an attribute convention or a vintage mismatch and needs source-level clarification before using the record as an exact 2014 boundary vintage. |
| 2017 | [HUD CoC Geography Crosswalk repository](https://github.com/tomhbyrne/HUD-CoC-Geography-Crosswalk) documents a HUD national/territory/DC geodatabase; its README identifies the file as obtained from HUD. The [public repository ZIP](https://raw.githubusercontent.com/tomhbyrne/HUD-CoC-Geography-Crosswalk/master/data/CoC_GIS_NatlTerrDC_Shapefile_2017.zip) downloaded (23,034,500 bytes; SHA-256 `c40b73d77cabeb3807536c7da2ad0a9bd45c569a8402a8f6696f52f5ae9f2b3b`). | ZIP integrity test passed; the archive contains `FY17_CoC_National_Bnd.gdb` with 46 geodatabase members. Opened directly from the ZIP: 399 MultiPolygon features, EPSG:4269, 55 distinct `ST` codes, 399 distinct non-null `COCNUM` values, and bounds `[-178.228, 13.234, 146.065, 71.390]`. | This is a third-party hosted copy attributed to HUD, not a currently verified direct HUD national download. It is useful as a recovery lead and should retain that provenance label. |

The read-only geometry checks found no null or empty geometries, and no duplicate or missing `COCNUM` values in either layer. Shapely 2.1.2 reports 71 of 409 Stanford geometries invalid (68 ring self-intersections, 3 self-intersections) and 15 of 399 2017 GDB geometries invalid (all ring self-intersections). These flags are preserved as observed; the survey did not repair or assess whether they reflect intentional boundary topology. The 2017 extent reaches Guam, while the 2014 feature code list contains Puerto Rico but not the Pacific territories in the 2017 list. Treat these feature counts and extents as layer inspection evidence, not a proof that every expected CoC or territory is present.

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
