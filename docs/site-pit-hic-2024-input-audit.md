# 2024 PIT and HIC CoC input audit

**Prospective longitudinal universe:** On 2026-10-21 the study owner directed holding out CT-503. The rebuilt 99% universe excludes its dependent MSA 14860 (Bridgeport–Stamford–Danbury), leaving **63 sites and 83 CoCs**. All other selected membership pairs are unchanged. The original cross-sectional runs below retain their original samples and limitations.

**Updated finding:** CT-503's January 2024 PIT/HIC reporting footprint has not been verified against the frozen boundary. The original same-year compatibility conclusion is superseded for that CoC; existing results remain reproducible but are not fully geography-validated. See [the CT-503 reconciliation](ct503-historical-reconciliation.md) and follow-up bead `coclab-k34yp`.

Audit date: 2026-10-21. This note supports a cross-sectional comparison of 2024 PIT and HIC measures over the frozen MSA–CoC memberships in `HHP-Output/msa_coc_overlap_sites_2024`. It documents the source-year geography match used for the evidence records. It does not claim that the local boundary file is an archived snapshot of the exact geometry in place on a particular day in 2024.

## Evidence for the FY2024 CoC geography

HUD's [Data.gov catalog entry for Continuum of Care (CoC) Grantee Areas](https://catalog.data.gov/dataset/continuum-of-care-coc-grantee-areas) identifies the dataset's **Date of Coverage as FY 2024** and lists the same ArcGIS FeatureServer endpoint queried for the local snapshot: `https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services/Continuum_of_Care_Grantee_Areas/FeatureServer/0`. The catalog identifies ArcGIS item `c930d736b1764c259371fc7111e02740` and says the dataset is updated annually. HUD's separate [CoC_Geo_Type layer](https://services.arcgis.com/VTyQ9soqVukalItT/ArcGIS/rest/services/CoC_Geo_Type/FeatureServer/0) explicitly describes its geography as current for FY2024 and says it includes FY2024 CoC boundaries and selected HIC and PIT variables. These HUD descriptions support using the CoC geography for FY2024 data.

The local [B2024 CoC GeoParquet](../data/curated/coc_boundaries/coc__B2024.parquet) has 387 records, `boundary_vintage=2024`, `source=hud_exchange`, and source reference `https://hudgis-hud.opendata.arcgis.com/datasets/HUD::continuum-of-care-coc-grantee-areas`. Its raw snapshot request calls the FeatureServer endpoint above; its manifest records 387 features retrieved on 2026-07-04 and SHA-256 `7e08bdb38e111dd542bdec6a2a9cdf12a6fd34e89dfcbc81e810e1f617d3dbab` for the captured NDJSON. Thus the local geometry is an immutable 2026 capture of the HUD layer whose official catalog coverage is FY2024; it is not evidence of a historical, point-in-time January 2024 geometry snapshot.

The B2025 local file was retrieved from the same live service on 2026-08-09. All 387 shared CoC IDs have identical local geometry hashes between B2024 and B2025. This is consistent with both files reflecting the same current service content and is a known vintage-label concern (`coclab-zspyc`). The FY2024 coverage attribution above supports this analysis's same-year source-to-geography match, but the local files alone do not establish when those geometries first took effect.

## PIT and HIC source coverage

The raw PIT workbook `data/raw/pit/2024/2007-2024-PIT-Counts-by-CoC.xlsb` has a `2024` sheet with a `CoC Number` column. Its local metadata names the HUD source URL `https://www.huduser.gov/portal/sites/default/files/xls/2007-2024-PIT-Counts-by-CoC.xlsb`. The curated [PIT 2024 CoC table](../data/curated/pit/pit__P2024@B2024.parquet) has 385 unique `(pit_year, coc_id)` rows for 2024 and includes `pit_sheltered`.

The raw HIC workbook `data/raw/hic/2024/2007-2024-HIC-Counts-by-CoC.xlsx` has a `2024` sheet whose `CoC Number` column identifies the CoC rows. The local HIC provenance block records this workbook path, its SHA-256, and `hic_year=2024`. The curated [HIC 2024 CoC table](../data/curated/hic/hic__H2024.parquet) has 385 unique `(hic_year, coc_id)` rows. Its `source_ref` column points to a HUD User by-state CSV, while the provenance block identifies the by-CoC workbook consumed by this local ingest; the workbook path and hash are used here to pin the consumed input.

In the curated 2024 tables, PIT and HIC have the same 385 CoC IDs, with no duplicate year–CoC keys. Each ID occurs in B2024; the two additional B2024 IDs are `AS-500` and `MP-500`. Every selected site membership exists once in both source tables, and selected values for `pit_sheltered` and `hic_total_beds` are non-null:

| Two-way overlap threshold | Eligible MSAs | Selected CoCs | PIT rows available | HIC rows available |
| --- | ---: | ---: | ---: | ---: |
| 0.90 | 87 | 144 | 144 | 144 |
| 0.95 | 72 | 107 | 107 | 107 |
| 0.99 | 64 | 84 | 84 | 84 |

Raw input SHA-256 values are PIT `ae88fbb58dadfbfccc254619b714d37e0720bd9854a31a16ad0e381fa14959ed` and HIC `4f881539918d89cfe83e7eedf736522fc8ff847fc54c32cea907d1b8560c3a24`. The local CoC boundary GeoParquet SHA-256 is `93eec38a508d5faf9589edded506613851a14dd0112025314e5359e468f9d6ac`. The site and membership parquet SHA-256 values are `4b7467d828f503751234bce24120f4e6460070540add24736c149831410a1e80` and `8d86dbde6b27715f57cf644336b441401089559c08bb2d29733abf4dffafc0e8`, respectively.

## Interpretation of HIC beds

The registered `hic_total_beds` measure is an all-program year-round bed total: emergency shelter (ES), transitional housing (TH), safe haven (SH), rapid rehousing (RRH), permanent supportive housing (PSH), and other permanent housing (OPH). It includes permanent housing inventory and is broader than shelter capacity. The registered `hic_shelter_year_round_beds` measure is the explicit ES + TH + SH shelter-only alternative. Matt selected the shelter-only measure for this run on 2026-10-21.

## Completed 2024 runs

The three `recipes/site-pit-hic-2024-overlap{90,95,99}.yaml` recipes correlate whole-CoC sums of `pit_sheltered` with `hic_shelter_year_round_beds` across eligible MSA sites. Every selected constituent has both measures; no eligible site was excluded for missing outcomes. Independent calculation from the curated source tables reproduced the recipe results.

| Threshold | Sites (n) | CoCs | Pearson r | Run ID |
| --- | ---: | ---: | ---: | --- |
| 90% | 87 | 144 | 0.994879022900 | `6d962da1ec254d6d84784c4f9dc57871` |
| 95% | 72 | 107 | 0.996489524103 | `fc0347f4b8b6416bacc39e121f8d49ce` |
| 99% | 64 | 84 | 0.997820441878 | `308271a7a164427e83971c291c006648` |

Bundles reside under `~/HHP-Output/results/<recipe-id>/<run-id>/`. All three runs matched their recipe locks. Full input panels and site artifacts were retained with `--private` because the saved site artifact's redistribution terms are unclassified. An initial unlocked 90% run (`cdabc7790c3b4d5abc3d64a370af6022`) completed but withheld dependent outputs under the default distribution policy; it remains in the ledger and is superseded for this comparison by the locked private run above.

All three bundles passed `hhplab results verify`: integrity and offline reproducibility passed, with no differences, errors, partial reasons, or unavailable inputs.

These are correlations of counts across differently sized sites. They do not estimate a causal effect of beds on homelessness or establish equal shelter utilization. The three thresholds also select different site sets. As an independent diagnostic, removing New York from the 90% sample lowers r to 0.972616; this diagnostic did not change any recipe's sample.

## Unsheltered PIT comparison

Matt requested the same comparison with `pit_unsheltered` on 2026-10-21. The three `recipes/site-unsheltered-pit-hic-2024-overlap{90,95,99}.yaml` recipes retain the same year, source files, shelter-bed definition, site memberships, and boundary evidence. All selected constituent values are present and non-null, so the samples have the same site counts as the sheltered runs. Independent whole-CoC aggregation reproduces the recipe results.

| Threshold | Sites (n) | CoCs | Pearson r | Run ID |
| --- | ---: | ---: | ---: | --- |
| 90% | 87 | 144 | 0.322499189951 | `b289ff4a261e48378bd3cec8e5f49606` |
| 95% | 72 | 107 | 0.324617337337 | `b1578791d63e4dc1ab6c18724dbea753` |
| 99% | 64 | 84 | 0.981890760290 | `8d1f740060e446a39416dc33c612a317` |

All runs matched their recipe locks and retain full local inputs in private bundles under `~/HHP-Output/results/<recipe-id>/<run-id>/`, using the same distribution policy as the sheltered runs. All three bundles passed integrity and full offline reproducibility verification, with no differences, errors, partial reasons, or unavailable inputs.

The threshold comparison is sensitive to site composition. New York–Newark–Jersey City (MSA 35620) is included at 90% and 95%, but excluded at 99%. Its selected CoCs sum to 5,739 unsheltered people and 123,784 shelter beds. An independent leave-one-site-out diagnostic raises r to 0.784839 at 90% and 0.980923 at 95% when New York is removed. At 99%, removing Los Angeles (MSA 31080) lowers r to 0.933459. These diagnostics describe influence only; no site was removed from the recipe analyses.

## Historical audit of the frozen 99% universe

On 2026-10-21 Matt requested verification of earlier years with both unsheltered PIT and ES + TH + SH beds, explicitly excluding 2021. The audit retains all 64 selected MSAs and their 84 frozen CoC memberships; it does not drop sites or silently map predecessor CoCs. It uses the same cumulative PIT vintage 2024 as the cross-sectional runs, plus the annual HIC files.

| Earlier years | Sites with complete outcomes | Outcome-coverage conclusion |
| --- | ---: | --- |
| 2010 | 63 of 64 | PA-512 is missing in both sources; York-Hanover is incomplete. |
| 2011–2019 | 63 of 64 | OR-504 is missing in both sources; Salem is incomplete. |
| 2020, 2022, 2023 | 64 of 64 | All 84 CoCs have one non-null, finite, non-negative integer count in each source. |
| 2021 | Not evaluated | Excluded by the study owner. |

Data coverage is not boundary compatibility. The full frozen universe is **not cleared for an ID-only historical join** in any of the three complete-data years. MA-505 is a documented counterexample: the [Bristol County CoC's merger history](https://www.bristolcountycoc.com/about-us/) records the merger of New Bedford (MA-505) with Attleboro/Taunton/Bristol County (MA-519) in May–June 2024. Historical native MA-505 therefore covers less geography than the frozen 2024 MA-505. The official [2022](https://files.hudexchange.info/resources/documents/CoC_GIS_National_Boundary_2022.zip) and [2023](https://files.hudexchange.info/resources/documents/CoC_GIS_National_Boundary_2023.zip) national boundary archives independently show that change: historical MA-505 is about 52.5 km² versus 1,393.1 km² in the reference, with roughly 96.23% symmetric difference relative to their union in EPSG:5070.

The source tables also treat this merger differently. For these years, cumulative PIT vintage 2024 has no MA-519 row and its MA-505 unsheltered value equals the sum of the two predecessor rows in PIT vintage 2025. Annual HIC files retain separate predecessor rows:

| Year | PIT P2024 MA-505 | PIT P2025 MA-505 + MA-519 | HIC MA-505 beds | HIC MA-519 beds | Sum of predecessor beds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2020 | 76 | 48 + 28 | 395 | 283 | 678 |
| 2022 | 109 | 61 + 48 | 407 | 301 | 708 |
| 2023 | 96 | 67 + 29 | 367 | 274 | 641 |

These equalities establish a source-vintage difference that is consistent with retrospective PIT aggregation; they do not authorize adding MA-519 again to P2024 PIT or automatically treating a predecessor union as validated geometry. A historical panel needs source-specific reconciliation. Using only the historical HIC MA-505 row would omit the second predecessor's beds. The existing compatibility contract permits only one-to-one ID mappings, so a many-to-one predecessor rule needs an explicit extension and evidence review (`coclab-7c0nt`). No historical compatibility approvals or longitudinal model were produced by this audit.

The local B2019 file is unsuitable as independent historical evidence: its raw live-service response has the same SHA-256 (`7e08bdb38e111dd542bdec6a2a9cdf12a6fd34e89dfcbc81e810e1f617d3dbab`) and byte length as the B2024 capture. That provenance issue is recorded in `coclab-zspyc`. Genuine geometric differences also remain to be classified separately from coastline, island, digitization, and reprojection differences; a small area difference is not automatically an approval, and a nonzero difference is not automatically an administrative change.

The 2020 comparison has no topologically identical member polygons and a median symmetric-difference/union ratio of about 0.043%. Its largest difference is MA-505 (about 96.23%); CT-503 is also substantial (about 15.70%). The 2023 archive is much closer overall to the reference: its median absolute area difference is about 0.000000053% of reference area, but MA-505 and CT-503 remain prominent exceptions. CT-503's 2023 polygon is about 7.08% smaller in area than the reference; its administrative explanation remains unresolved. Area/bounds screening is not an exhaustive topological-equivalence test for 2022/2023. The detailed archive overlay was measured for MA-505; other differing members remain unapproved pending review (`coclab-k0hwb`).

The machine-readable `coverage.json`, `local-boundaries.json`, `archives-boundaries.json`, and downloaded archives are retained under `~/HHP-Output/site-boundary-history-audit/`. Supplement `archives-boundaries-epsg6933.json` uses a global equal-area projection for the full universe, including Hawaii; EPSG:5070 is appropriate for the detailed Massachusetts comparison. `archives-boundaries-2023-precision-check.json` records a strict 1 cm snapped-geometry screen with no identical pairs among the 84 selected CoCs; failure of this numerical equality screen alone does not imply a material boundary change. The official ZIP SHA-256 values are `e96f8ec07750d623a75ccd20f92e072bfab6fba7f524054e09c31caeb5bc72f4` (2022) and `502c5d1887c56ac2b5f4911acc7c4c1db8aa78aaee34fde3d5f663ea15befc2a` (2023); source paths, reference hashes, comparison methods, and per-CoC measurements are recorded in the JSON reports. This audit does not establish a usable longitudinal window. Even after geography reconciliation, 2020, 2022, 2023, and the existing 2024 baseline provide a short, gapped series; 2020→2022 must not be treated as a one-year lag.

## Boundary evidence artifact

**Review update, 2026-10-21:** The blanket same-year compatibility guidance below is superseded for CT-503 by the [Connecticut reconciliation audit](ct503-historical-reconciliation.md). B2024's live 2026 capture, attributed by HUD to FY2024, removes Shelton and adds Bridgewater and New Milford relative to the older polygons. Pre-count CT CAN material already assigned Bridgewater and New Milford to Fairfield County CAN and Shelton to Greater New Haven CAN, so this GIS change does not establish a 2024 operational service-area change. The official 2024 CT HMIS report confirms the January 23 count and ODFC aggregation, but it does not state the exact municipality list or publish town-level PIT/HIC allocations. CT-503's three 2024 compatibility records are now `unknown` pending a dated CoC/HMIS scope crosswalk; all other records and memberships remain as previously defined. The saved runs remain reproducible calculations, but their locks predate this evidence correction and are intentionally stale against the updated compatibility artifacts. Do not reuse the CT-503 rows as resolved geographic evidence. No historical panel or longitudinal window is approved.

Use one `coc_boundary_compatibility/1` JSON artifact for each threshold, since its header binds one membership identity. The required header fields are `schema_version`, `evidence_id`, `artifact_id`, `threshold`, `membership_identity`, `frozen_boundary_vintage`, and `records`. Each record identifies `source_panel`, `frozen_coc_id`, `year`, `frozen_boundary_vintage`, `source_boundary_vintage`, `status`, and `reason`; `evidence_ref` is required for `compatible`. The records may use `status: compatible` for the 2024 same-year source join when they cite this audit and the HUD FY2024 catalog/service metadata, and state that the source CoC number directly matches the frozen B2024 CoC ID. Do not add `approval_ref` for a compatible record; that field is for an explicitly approved ID mapping. The evidence establishes the declared FY2024 source-to-geography match and direct ID resolution, not historical boundary stability.

For `artifact_id: overlap_samples` and `site_key: msa_id`, the selected membership identities are:

| Threshold | Selected membership rows | `membership_identity` |
| --- | ---: | --- |
| 0.90 | 144 | `sha256:24f3ba261df115e529664f95e903a1f4d96e674a3eea1973a6620c404c83419d` |
| 0.95 | 107 | `sha256:482174ce2976a17eedad0190e5038db5f24102c719f77364917a2034086c06eb` |
| 0.99 | 84 | `sha256:a6f8f3e35c335d9c1877b2fff8fd6727d362fe89321f11dbfe2f80ef98f23338` |

Each hash is SHA-256 over canonical JSON containing schema `msa_coc_sites/1`, artifact ID, numeric threshold, site key `msa_id`, and the sorted selected `(msa_id, coc_id)` pairs. For a shared CoC outcome panel containing both 2024 measures, supply one evidence record per selected CoC and the exact panel key used by the recipe. Use `frozen_boundary_vintage: "2024"` and `source_boundary_vintage: "2024"` only when the recipe's declared source-panel provenance is also pinned to the HUD FY2024 CoC layer. For CT-503 specifically, the record remains `unknown` with an evidence reference to the Connecticut reconciliation; other records retain their existing treatment. The HUD references and workbook rows establish CoC identity, while the separate CT HMIS/CAN sources do not prove exact correspondence to B2024. No historical remapping or boundary approval is asserted.

## Follow-up reconciliation and ingestion safeguards

The [Providence audit](providence-historical-reconciliation.md) identifies the official PIT workbook's merger mapping and documents why historical PIT and HIC need different aggregation rules. The new `coc_boundary_compatibility/2` contract implements explicit predecessor reconciliation and retains v1 replay, superseding the earlier implementation-gap statement. Its reviewed administrative count rule does not clear the older polygon residuals or the full frozen site universe. The [Connecticut audit](ct503-historical-reconciliation.md) explains the substantial CT-503 polygon difference through town membership rather than cartographic precision. This establishes a GIS footprint change, not the timing or existence of a change in actual PIT/HIC reporting jurisdiction: older coordinated-access documents already place Bridgewater and New Milford with Fairfield, omit Shelton, and also include Roxbury outside the frozen polygon. The count footprint remains unresolved. CT-503 records in the three current compatibility artifacts are superseded as `unknown`; the original bundles and locks remain historical records and must not be presented as current geographic approval.

Both live HUD ingestion entry points now require an actual UTC retrieval-date identity. Numeric boundary vintages use year-specific archives, and an unavailable archive does not authorize falling back to live geometry under that year. Explicit archive URLs and local archive ingestion remain available. Existing artifacts were preserved, not relabeled or regenerated.

The affected-artifact review found seven local B2019-named parquets (boundary, one MSA–CoC population crosswalk, and five MSA PIT panels), six B2025-named parquets (boundary and five crosswalks), and eight B2024-named parquets (boundary, two MSA crosswalks, and five PIT products). These are an inventory of potentially affected dependencies, not a claim that every value is wrong. B2019 cannot substantiate historical geometry; B2025 cannot establish its claimed year from the shared live response; B2024 has official FY2024 catalog attribution but still requires source-count footprint reconciliation, particularly for CT-503. The existing saved site universe and result bundles retain their original inputs for auditability.

Archive provenance now records the actual successful national, legacy, or per-state ZIP URLs and raw hashes in both the source registry and embedded GeoParquet metadata (`coclab-fvu4y`). Reused extraction directories cannot substitute older outer-directory files for the downloaded archive, and local-only ingestion records local input identity instead of guessing a national URL. Existing historical artifacts have not been regenerated.

The corrected CT-503 evidence passes static/readiness checks with an explicit unknown-boundary warning; `ready` is not a scientific compatibility approval. Current aggregation leaves that member's site outcome incomplete, and the evidence fingerprint differs from the original locks. No new result runs or automatic site exclusions were made after this correction.

The complete 2023 pairwise polygon overlay is retained in `~/HHP-Output/site-boundary-history-audit/archives-boundaries-2023-full-overlay.json`. It covers all 84 selected CoCs in EPSG:6933, with source hashes and exact symmetric-difference measurements. Outside MA-505 and CT-503, the largest symmetric-difference/union ratios are TX-603 (0.016159%), HI-501 (0.012320%), and CA-613 (0.001620%). These improve on net-area screening, which can hide offsetting additions and removals; they are not automatic approvals of geography or count scope.

## Owner-directed CT-503 holdout

The prospective recipe is `recipes/msa-coc-longitudinal-sites.yaml`, with artifact ID `longitudinal_sites` and threshold 0.99. It uses the unchanged crosswalk, CoC names, vintages, and overlap rules, with MSA 14860 removed from the candidate MSA input before site production. This removes its entire site rather than computing a partial count or redefining its CoC coverage. No outcomes informed the exclusion.

The filtered MSA input and its embedded exclusion provenance are at `~/HHP-Output/msa_coc_longitudinal_sites_inputs/metro_universe__census_msa_2023__exclude_CT503.parquet`; `exclusion-policy.json` in that directory records the source, hashes, and reason. The source universe has 393 rows and SHA-256 `5e72e881627d19f447c73766808b3d0631d84b7faeb606e9e9ea7211f38932ff`. The filtered input retains every row whose `metro_id` is not `14860`, producing 392 candidates and SHA-256 `8ca4dacbd9cbf2832de324549279d4b34af9354718621b1a5e1ac764e2fcccc8`. Both IDs and the exclusion rationale are independent of PIT/HIC values.

Package preflight and build completed with no findings. The validated paired site/membership artifacts are under `~/HHP-Output/msa_coc_longitudinal_sites/`. Membership identity is `sha256:0ed7d40f8d75d761862ab3b8c1b8bc9f43853eebbecf3ce666c77942152f5a54`. `ct503-holdout-verification.json` records resolver output and verifies that the only removed selected pair is `(14860, CT-503)`, leaving 63 eligible MSAs and 83 CoCs. Curated-layout checks passed before and after the build.

Future longitudinal recipes must use this new artifact and membership identity, with newly bound boundary evidence and locks. Existing 90/95/99 exploratory universes and six cross-sectional bundles are preserved. The holdout removes CT-503 as a study blocker; remaining historical geography checks still apply, and 2021 remains excluded from longitudinal outcomes.
