# 2024 PIT and HIC CoC input audit

**Observed-count correction and reviewed reruns, 2026-10-22:** The earlier checks below used reported numeric PIT values without HUD's Count Types declarations. **129 of 840 site-years lack a complete observed unsheltered count**, leaving **508 of the previous 720 annual transitions** with observed unsheltered counts at both endpoints. The reviewed observed-only reruns at the end of this note supersede those earlier longitudinal findings. The earlier negative two-year PSH-leading association no longer has an interval excluding zero; positive same-year shelter-bed/sheltered-PIT associations remain. Earlier artifacts are preserved as historical calculations. Beads `coclab-r0jjj` and `coclab-xpk4w` cover the reruns and derived-artifact correction.

**Current longitudinal design:** On 2026-10-22 the study owner added a whole-Sacramento holdout for an exploratory check, retaining the CT-503, Salem and York holdouts. The current 99% universe has **60 sites and 77 frozen CoCs**, with **840 site-years for 2010–2024 excluding 2021**. Known merger operations are applied; remaining historical administrative continuity is an explicit exploratory assumption, not full geography clearance. The earlier 61-site design and cross-sectional runs below remain historical records. Results of the new check are at the end of this note.

**Historical reconciliation update:** The Luna High review supports administrative count reconciliation for MA-505/MA-519 and stable Fall River scope, with older polygon residuals retained explicitly. The county review recommends stable administrative units for TX-603, HI-501, CA-613, and CA-501 with moderate confidence. Native HUD reports verify source-specific predecessor treatment for CA-601, CA-611, and FL-503; the saved partial v2 evidence remains bound to the earlier 61-site membership. CA-515's 2010–2018 source scope includes Nevada County, outside the frozen Placer footprint; the owner-selected whole-Sacramento holdout now removes that problem from the exploratory sample. The earlier coverage check establishes successor-row availability, not geographic completeness. See [the historical inventory and integration review](site-historical-reconciliation-inventory.md), [Massachusetts review](providence-historical-reconciliation.md), and [county review](site-boundary-remaining-geometries.md).

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

This section records the earlier 63-site intermediate universe. The current design is recorded in the following section.

The prospective recipe is `recipes/msa-coc-longitudinal-sites.yaml`, with artifact ID `longitudinal_sites` and threshold 0.99. It uses the unchanged crosswalk, CoC names, vintages, and overlap rules, with MSA 14860 removed from the candidate MSA input before site production. This removes its entire site rather than computing a partial count or redefining its CoC coverage. No outcomes informed the exclusion.

The filtered MSA input and its embedded exclusion provenance are at `~/HHP-Output/msa_coc_longitudinal_sites_inputs/metro_universe__census_msa_2023__exclude_CT503.parquet`; `exclusion-policy.json` in that directory records the source, hashes, and reason. The source universe has 393 rows and SHA-256 `5e72e881627d19f447c73766808b3d0631d84b7faeb606e9e9ea7211f38932ff`. The filtered input retains every row whose `metro_id` is not `14860`, producing 392 candidates and SHA-256 `8ca4dacbd9cbf2832de324549279d4b34af9354718621b1a5e1ac764e2fcccc8`. Both IDs and the exclusion rationale are independent of PIT/HIC values.

Package preflight and build completed with no findings. The validated paired site/membership artifacts are under `~/HHP-Output/msa_coc_longitudinal_sites/`. Membership identity is `sha256:0ed7d40f8d75d761862ab3b8c1b8bc9f43853eebbecf3ce666c77942152f5a54`. `ct503-holdout-verification.json` records resolver output and verifies that the only removed selected pair is `(14860, CT-503)`, leaving 63 eligible MSAs and 83 CoCs. Curated-layout checks passed before and after the build.

Future longitudinal recipes must use this new artifact and membership identity, with newly bound boundary evidence and locks. Existing 90/95/99 exploratory universes and six cross-sectional bundles are preserved. The holdout removes CT-503 as a study blocker; remaining historical geography checks still apply, and 2021 remains excluded from longitudinal outcomes.

## Selected 2010–2024 longitudinal design

The study owner subsequently selected a 2010 start and consistent exclusions of Salem and York as well as CT-503. The excluded pairs are `(14860, CT-503)`, `(41420, OR-504)`, and `(49620, PA-512)`. Each excluded CoC is the sole selected constituent of its MSA. All other original 99% membership pairs are retained unchanged. The window is 2010–2024 excluding 2021: 14 annual observations per site, potentially 854 site-years. There are 12 consecutive one-year transitions per site; 2020–2022 is not a one-year lag.

`recipes/msa-coc-longitudinal-sites.yaml` now produces `msa_coc_longitudinal_2010_2024_sites`, artifact ID `longitudinal_sites`, under `~/HHP-Output/msa_coc_longitudinal_2010_2024_sites/`. The recipe's production year remains 2024 because it produces frozen geography, not the longitudinal outcome panel. Its filtered MSA input is `~/HHP-Output/msa_coc_longitudinal_sites_inputs/metro_universe__census_msa_2023__exclude_CT503_OR504_PA512.parquet`, SHA-256 `f0b5691ee554aa135d933f106399a0e4b041393cb533c636f38f1d9d14e2d678`. It retains the original 393-candidate input except MSA IDs 14860, 41420, and 49620, leaving 390 candidates. The input embeds exclusion provenance; `exclusion-policy-2010-2024.json` beside it records the exact study years and source hash.

The resulting 61-site/81-CoC membership identity is `sha256:4861b55cdb14c1e6dc14f270dc2d99a9952da4cd2aae844ee65163ce83771144`. Package preflight and build passed without findings, and the resolver verified that only the three named pairs were removed. The previous CT-only artifacts and all original results remain preserved in their existing directories.

`membership-and-outcome-coverage.json` in the new output directory records the validated membership, source hashes, and annual count checks. All 81 required P2024 PIT rows have valid unsheltered counts in each selected year. Annual HIC has valid ES+TH+SH year-round bed counts for all 81 frozen IDs plus MA-519 in each pre-2024 year (82 source IDs); 2024 needs only the 81 frozen IDs. No required rows are missing, duplicated, null, negative, fractional, or nonfinite. This is source availability verification, not boundary validation or a completed historical panel. Future analysis must bind its evidence and locks to the new membership identity and apply the explicit source-specific MA-505 reconciliation. The historical checks in `coclab-k0hwb` now apply to this design.

## Exploratory longitudinal check without Sacramento — 2026-10-22

The owner requested a whole-Sacramento holdout and an exploratory check of the remaining sites, deferring careful site selection. The revised producer removes MSA 40900 and all four of its members (CA-503, CA-515, CA-521, CA-525) across every year. No other selected pair changes. The previous 61-site files remain intact. The active recipe produces `msa_coc_longitudinal_2010_2024_exclude_sacramento_sites`, containing 60 sites and 77 CoCs, with membership identity `d9cb7f511c9b8d4d212a90bff4b854cf5c021dd0c50e956c6219b1cbe1e660ec`. Its filtered candidate input and exclusion-policy JSON are under `~/HHP-Output/msa_coc_longitudinal_sites_inputs/`.

The check uses P2024 unsheltered PIT counts and ES+TH+SH year-round HIC beds. P2024 merger rows are consumed once; HIC adds MA-519 to MA-505 before 2024, CA-610 to CA-601 in 2010, CA-605 to CA-611 through 2013, and FL-516 to FL-503 through 2014. All required inputs pass uniqueness and nonnegative integer checks; all 840 site-years are numerically complete. Remaining same-ID geographic continuity is assumed for this exploratory calculation and is explicitly labeled provisional. This does not change any unreviewed compatibility record to approved. `coclab-fjcho` is closed for the active study because the three merger cases are resolved and the unresolved Placer case is now excluded; broader review remains in `coclab-k0hwb`.

### Findings

There is no clear annual-change lead/lag signal in this check. Correlations of annual changes in `log(1 + count)`, after removing site and year means, are:

| Timing | Pairs | Pearson r | 95% site-bootstrap interval |
| --- | ---: | ---: | ---: |
| Bed changes precede unsheltered changes by one year | 600 | 0.083 | −0.032 to 0.179 |
| Same-year changes | 720 | 0.014 | −0.070 to 0.102 |
| Unsheltered changes precede bed changes by one year | 600 | −0.010 | −0.110 to 0.095 |

Both two-year directions also have intervals spanning zero. In a pre-2020 sensitivity check, the one-year bed-leading correlation is 0.074 and the reverse is −0.034. Leaving out each site in turn keeps the full-period bed-leading point estimate between 0.050 and 0.102; the reverse ranges from −0.027 to 0.011. These checks do not establish a causal effect or rule out smaller, heterogeneous, or longer-run relationships.

Log-count levels show modest positive associations after site/year adjustment: same-year r=0.207, beds one year earlier r=0.179, and unsheltered counts one year earlier r=0.214. Those associations can reflect persistent trends; the larger raw-count correlation (same-year r=0.925) also reflects differences in site size. Neither establishes temporal direction. The changing pair sets across lags and exploratory multiple comparisons preclude selecting the largest coefficient as evidence of which variable leads.

### Method, validation, and saved artifacts

Exact calendar-year joins define all lags. There are 720 one-year level pairs and 600 one-year pairs of annual changes; 2020→2022 is never a one-year lag. Annual changes require both adjacent years, so no 2022 change is constructed. Positive lag in saved results means beds precede PIT. Balanced panels allow exact removal of site and paired-year means by two-way demeaning. Percentile intervals use 1,999 whole-site bootstrap draws (seed 20261022), preserving each sampled site's time series and refitting the means. Intervals are exploratory, not adjusted for multiple comparisons, and do not address all possible cross-site dependence or count-method changes.

The package site-universe resolver verified the new identity and exact four-pair removal. Recipe preflight/build had no findings; curated-layout checks passed before and after. Independent checks reproduced all lineage sums, selected merger totals, the calendar-pair count, and the raw one-year correlation. The native `lagged_associations_panel` helper uses row shifts without checking calendar distance; bug `coclab-lcyua` records this incidental finding. This check uses explicit year joins instead. The native package correlation function independently writes the pooled raw-count result with provenance.

All check artifacts are retained under `/home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/`:

- [Reproducible analysis script](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/run_check.py>) — run from the repository with `uv run python` and the script path.
- [Summary, source hashes, and all coefficients](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/summary.json>).
- [Lead/lag chart](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/lead_lag_check.png>).
- [840-row provisional panel](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/exploratory_site_year_panel.parquet>) with embedded input, membership, policy, and script provenance; CSV and constituent lineage accompany it.

This is a saved exploratory diagnostic, not a locked results recipe or a fully geographically validated panel. No outcome-driven site selection was performed after examining these results.

### Separate bed-increase and bed-decline checks — 2026-10-22

At the owner's request, a second diagnostic keeps only shelter beds (ES+TH+SH, excluding PSH) and separates annual bed increases from annual declines. The same 60-site panel contains 341 increases, 357 declines, and 22 flat transitions. Flat transitions are excluded from both groups. Selection uses the observed bed-change sign before adjustment; unsheltered PIT changes can have either sign. Declines remain signed negative changes, not absolute magnitudes.

Neither group shows a clear lead/lag association. Results below correlate annual changes in `log(1 + count)` after separately fitting site and calendar-year fixed effects within each group and lag. All samples include 60 sites, although their available years differ.

| Bed-change group | Timing | Pairs | Pearson r | 95% site-bootstrap interval |
| --- | --- | ---: | ---: | ---: |
| Increases | Beds precede PIT by one year | 267 | 0.085 | −0.044 to 0.194 |
| Increases | Same year | 341 | 0.084 | −0.003 to 0.189 |
| Increases | PIT precedes beds by one year | 265 | −0.051 | −0.176 to 0.101 |
| Declines | Beds precede PIT by one year | 317 | 0.003 | −0.139 to 0.139 |
| Declines | Same year | 357 | −0.029 | −0.118 to 0.069 |
| Declines | PIT precedes beds by one year | 313 | 0.006 | −0.134 to 0.146 |

Both two-year directions also have intervals spanning zero in each group. Before 2020, the one-year bed-leading estimates are 0.096 for increases and 0.014 for declines. Leaving out each site in turn gives corresponding full-period ranges of 0.060–0.116 and −0.033–0.058. These conditional, range-restricted correlations do not establish causality, rule out effects, or constitute a formal test that increases and declines behave differently. The slightly positive increases-only same-year estimate is also uncertain; all ten exploratory intervals include zero and are not adjusted for multiple comparisons.

Exact calendar joins preserve the earlier exclusion of 2021: no annual difference bridges the missing year, and no 2022 annual change is constructed. A two-year lag can span the omitted period when both annual changes are observed. Because sign selection makes the panels unbalanced, fixed effects use converged alternating projections rather than the balanced-panel demeaning shortcut. The 1,999 whole-site bootstrap draws (seed 20261022) refit fixed effects using site multiplicity weights. Every subgroup/lag projection was independently checked against explicit dummy-variable least squares, and weighted bootstrap correlations were checked against explicitly duplicated site samples. Sign counts and calendar matching pass assertions.

Reproducible artifacts are in [split-bed-changes](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/split-bed-changes/>): `run_split_check.py`, `summary.json` (input/script hashes, methods, validation, all results), `correlations.csv`, and `paired_changes.csv`. Run the script from this repository with `uv run python`. The original panel, analysis, and geographic limitations remain unchanged.

### Total homelessness follow-up — 2026-10-22

The owner requested the same split using total homelessness (sheltered plus unsheltered). This changes only the PIT outcome to `pit_total`: shelter HIC remains ES+TH+SH, the same 60 sites and 2010–2024 window excluding 2021 apply, and bed-change selection remains 341 increases, 357 declines, and 22 excluded flat transitions. P2024 totals are summed once per frozen member using the existing lineage. Every selected source row and every one of the 840 site-year totals satisfies `pit_total = pit_sheltered + pit_unsheltered`; the newly aggregated unsheltered counts exactly reproduce the earlier panel.

| Bed-change group | Timing | Pairs | Pearson r | 95% site-bootstrap interval |
| --- | --- | ---: | ---: | ---: |
| Increases | Beds precede PIT by one year | 267 | 0.023 | −0.137 to 0.194 |
| Increases | Same year | 341 | 0.259 | 0.134 to 0.385 |
| Increases | PIT precedes beds by one year | 265 | −0.013 | −0.169 to 0.164 |
| Declines | Beds precede PIT by one year | 317 | −0.023 | −0.195 to 0.133 |
| Declines | Same year | 357 | 0.152 | 0.022 to 0.276 |
| Declines | PIT precedes beds by one year | 313 | −0.015 | −0.169 to 0.156 |

Total homelessness shows a positive same-year association in both groups, with nominal 95% intervals excluding zero. All one- and two-year lead/lag intervals include zero. Increases-only and declines-only same-year coefficients before 2020 are 0.236 and 0.165; leaving out each site in turn gives full-period ranges 0.232–0.285 and 0.127–0.191. Within the declines subset, bed changes remain signed: the positive coefficient means more negative bed changes tend to accompany more negative (or less positive) total-PIT changes after adjustment, not that every bed decline coincides with declining homelessness.

These are conditional correlations of annual log1p changes, not causal effects or formal tests of differences between outcomes or bed-change groups. Including sheltered people can create a closer same-year association with shelter capacity and occupancy; this check cannot distinguish that explanation from other mechanisms. Intervals are not adjusted for the multiple exploratory comparisons, and historical geography remains provisional.

The follow-up imports the unchanged split-check estimator and uses identical calendar joins, within-subset fixed effects, 1,999 site-bootstrap draws, and seed. All ten fixed-effect projections again match dummy-variable least squares, and bootstrap weights match explicitly duplicated site samples. Artifacts in [total-pit-bed-changes](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/total-pit-bed-changes/>) include `run_total_check.py`, the provenance-bearing total-PIT panel, `total_pit_provenance.json` with source and script hashes, `summary.json`, `correlations.csv`, and `paired_changes.csv`. Earlier outputs are preserved.

### Sheltered homelessness follow-up — 2026-10-22

The same analysis was repeated with `pit_sheltered` as the outcome. Shelter HIC remains ES+TH+SH, with the same 60 sites, 2010–2024 excluding 2021, 341 bed increases, 357 declines, and 22 excluded flat transitions. The validated panel from the total-PIT follow-up already contains sheltered counts; input and source hashes were rechecked, and all 840 sheltered site-year counts are nonnegative integers equal to total minus unsheltered PIT. Every predictor value and calendar pair exactly matches the total-PIT analysis.

| Bed-change group | Timing | Pairs | Pearson r | 95% site-bootstrap interval |
| --- | --- | ---: | ---: | ---: |
| Increases | Beds precede PIT by one year | 267 | −0.103 | −0.256 to 0.072 |
| Increases | Same year | 341 | 0.409 | 0.280 to 0.528 |
| Increases | PIT precedes beds by one year | 265 | 0.010 | −0.138 to 0.155 |
| Declines | Beds precede PIT by one year | 317 | −0.023 | −0.166 to 0.103 |
| Declines | Same year | 357 | 0.323 | 0.183 to 0.480 |
| Declines | PIT precedes beds by one year | 313 | −0.051 | −0.220 to 0.127 |

Same-year associations are positive in both groups, with nominal 95% intervals excluding zero. They are numerically larger than the total-PIT and unsheltered-PIT estimates, but differences between outcomes have not been formally tested. All one- and two-year lead/lag intervals include zero. Before 2020, same-year correlations are 0.400 for increases and 0.310 for declines. Leaving out each site in turn yields full-period same-year ranges of 0.375–0.438 and 0.292–0.363. Declines remain signed; a positive coefficient associates more negative bed changes with more negative or less positive sheltered-count changes after adjustment.

The stronger same-year relationship is consistent with shelter capacity and sheltered occupancy moving together; this diagnostic does not distinguish mechanisms or establish causality. Geography remains provisional, selection on bed-change sign restricts the range, and the exploratory intervals are not corrected for multiple comparisons.

The unchanged split estimator again uses annual log1p changes, exact calendar joins, within-subset site/year fixed effects, and 1,999 whole-site bootstrap replicates with seed 20261022. The driver converts nullable sheltered-derived values to dense floats for numerical linear algebra without changing values. All ten projections match dummy-variable least squares; bootstrap frequency weights match explicitly duplicated site samples. [Sheltered-check artifacts](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/sheltered-pit-bed-changes/>) include `run_sheltered_check.py`, `sheltered_pit_provenance.json`, `summary.json`, `correlations.csv`, and `paired_changes.csv`; earlier outputs and the input panel are preserved.

### PSH beds versus unsheltered homelessness — 2026-10-22

The owner requested a PSH lead/lag check on the same site universe. This substitutes HIC `hic_psh_year_round_beds` for shelter beds and retains unsheltered P2024 PIT counts, the 60 sites/77 frozen CoCs, membership identity `d9cb7f511c9b8d4d212a90bff4b854cf5c021dd0c50e956c6219b1cbe1e660ec`, and 2010–2024 excluding 2021. All 840 PSH site-year totals are finite nonnegative integers. Their unsheltered counts exactly match the earlier shelter diagnostic. HIC predecessor IDs and source-specific merger rules are identical; an independent exploded-source aggregation reproduces every PSH site-year total.

The primary check correlates annual changes in `log(1 + count)` after removing site and paired-year fixed effects. Positive lag means the PSH change precedes the PIT change. All pooled samples include 60 sites.

| Timing | Pairs | Pearson r | 95% site-bootstrap interval |
| --- | ---: | ---: | ---: |
| PSH changes precede unsheltered changes by two years | 480 | −0.094 | −0.192 to −0.014 |
| PSH changes precede unsheltered changes by one year | 600 | 0.064 | −0.031 to 0.155 |
| Same-year changes | 720 | 0.015 | −0.058 to 0.101 |
| Unsheltered changes precede PSH changes by one year | 600 | 0.019 | −0.065 to 0.097 |
| Unsheltered changes precede PSH changes by two years | 480 | −0.069 | −0.198 to 0.069 |

The pooled two-year PSH-leading association is small and negative, with a nominal interval excluding zero. Its pre-2020 point estimate is −0.116; leaving out each site in turn yields −0.118 to −0.067. Same-year and one-year intervals include zero. These are correlations between changes, not estimates of people housed per additional PSH bed.

There are 437 PSH increases, 235 declines, and 48 flat annual transitions. As in the earlier diagnostics, the separate sign checks select on the observed bed-change sign before adjustment and keep declines signed. Their fixed effects are refitted within each subgroup and lag; flat transitions are excluded from both sign groups.

| PSH-change group | Timing | Pairs | Contributing sites | Pearson r | 95% site-bootstrap interval |
| --- | --- | ---: | ---: | ---: | ---: |
| Increases | PSH precedes PIT by two years | 301 | 60 | −0.129 | −0.274 to −0.011 |
| Increases | PSH precedes PIT by one year | 377 | 60 | 0.049 | −0.035 to 0.155 |
| Increases | Same year | 437 | 60 | −0.062 | −0.211 to 0.058 |
| Increases | PIT precedes PSH by one year | 356 | 60 | 0.078 | −0.041 to 0.191 |
| Increases | PIT precedes PSH by two years | 286 | 60 | −0.102 | −0.208 to 0.001 |
| Declines | PSH precedes PIT by two years | 145 | 54 | −0.119 | −0.297 to 0.159 |
| Declines | PSH precedes PIT by one year | 183 | 57 | 0.077 | −0.291 to 0.329 |
| Declines | Same year | 235 | 59 | 0.143 | −0.008 to 0.307 |
| Declines | PIT precedes PSH by one year | 202 | 57 | 0.208 | 0.010 to 0.327 |
| Declines | PIT precedes PSH by two years | 159 | 56 | −0.296 | −0.534 to 0.027 |

The increases-only two-year PSH-leading point estimate is −0.169 before 2020, and its leave-one-site-out range is −0.179 to −0.098. The declines-only two-year interval is wide and includes zero. Declines also show a positive nominal association when unsheltered changes precede PSH changes by one year, so the exploratory results do not establish a single direction of response. These sign-restricted estimates are not a formal test of asymmetry or differences between groups.

The two-year negative association is suggestive rather than causal evidence. Intervals are not corrected for the multiple lags, transforms, and subgroups examined. Housing-market changes, capacity responding to need, changes in program classification, and PIT measurement can influence these associations; the diagnostic does not separately identify those mechanisms. HIC measures PSH bed capacity, not placements or utilization. Historical geography retains the earlier provisional status.

The driver imports the original pooled and split estimators without changing those files. It uses the same 1,999 whole-site bootstrap replicates and seed 20261022. Exact calendar joins exclude 2021 and prevent any annual difference from bridging the omitted year; no 2022 annual change is constructed. Two-year lags can span the gap when both annual changes are observed. All ten split projections match explicit dummy-variable least squares, and frequency-weighted bootstrap correlations match explicitly duplicated site histories. Source hashes and prior estimator hashes are checked before execution.

Artifacts are under [psh-unsheltered](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/psh-unsheltered/>): `run_psh_check.py`, the provenance-bearing `exploratory_site_year_panel.parquet`, source lineage, `summary.json` and `lead_lag_correlations.csv` for pooled checks, `split_summary.json`, `split_correlations.csv`, `split_paired_changes.csv`, `psh_provenance.json`, and [the lead/lag chart](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/psh-unsheltered/psh_lead_lag_changes.png>). The initial attempt is preserved under `retained-attempts/`; its numerical calculation completed but its chart step failed, and the corrected driver was rerun. Existing shelter, total-PIT, and sheltered-PIT outputs remain intact.

### Count Types: source occurrence and carried-forward estimates — 2026-10-22

The parser previously discarded HUD's Count Types column. Numeric availability therefore overstated current-year unsheltered observations. The [2024 AHAR Part 1](https://www.huduser.gov/portal/sites/default/files/pdf/2024-AHAR-Part-1.pdf), printed page xiii, describes carrying forward the previous year's unsheltered estimate where no count occurred, including 22 sheltered-only CoCs in both 2023 and 2024. Direct inspection of the cached P2024 workbook confirms 22 sheltered-only and 363 full-count rows in 2024. Source declarations, rather than zeroes, missing values or equality with last year's count, now determine occurrence.

Curated CoC PIT rows retain all reported numbers and add these nullable fields:

| Field | Meaning |
| --- | --- |
| `count_types` | Original HUD declaration, unchanged. Missing or unrecognized declarations remain unknown. |
| `unsheltered_count_observed` | True for full or partial unsheltered counts; false for sheltered-only or `No YYYY PIT Count`; unknown without usable declarations. |
| `unsheltered_count_carried_forward` | True when a declared non-count year contains a published unsheltered value under AHAR's carry-forward policy. False for observed counts or absent estimates. Unknown for sheltered-only numeric values in 2021 and for unknown declarations. This records the reporting policy, not an inference from equal values. |
| `unsheltered_count_partial` | True for `Sheltered and Partial Unsheltered Count`. Partial observations are distinct from complete counts and from carry-forwards. |

The exceptional 2021 workbook contains both partial counts and sheltered-only zeroes; those zeroes do not independently establish a reused estimate. HUD's [2021 count release](https://archives.hud.gov/news/2022/pr22-022.cfm) also distinguishes communities that actually conducted unsheltered counts. The study's existing 2021 exclusion remains in force. The P2024 workbook's 2007 sheet lacks Count Types; those rows have unknown flags rather than assumed observations.

`hhplab.sources.hud.pit.observation.observed_pit_mask` selects complete observed unsheltered or total-PIT values using all three flags and value availability. For CoC sheltered outcomes, it accepts full, partial-unsheltered and sheltered-only declarations, but excludes declared no-PIT-count years and unknown declarations. Genuine observed zeroes remain eligible. Each annual PIT difference must have eligible observations at both exact calendar endpoints; missing years cannot be bridged. Lead/lag checks must likewise validate the endpoints of the PIT change used in each pair.

CoC panels and identity recipe resampling retain the raw declarations and flags. Native whole-CoC metro and fractional MSA PIT aggregates preserve reported sums, require all contributing members for observed status, and mark carry-forward or partial status if any constituent has it. Their `count_types` is a JSON map from CoC ID to the original declaration. The whole-CoC site-year builder marks unsheltered/total outcomes `source_limited` when any required source count is unobserved, partial or unknown; its analysis aggregate is null and its diagnostics identify the affected frozen CoCs. Source-level reported numbers remain intact. Boundary compatibility checks remain separate.

The three existing curated CoC vintages P2020, P2024 and P2025, plus the annual P2024@B2024 CoC table, were refreshed from cached workbooks. Every `(pit_year, coc_id)` key and all three reported count measures match the original files exactly, across 5,457, 6,884, 7,431 and 385 rows respectively. Parser provenance records the Count Types policy version, reference and flag inventories. The refresh manifest additionally pins workbook and output hashes. All original PIT files were copied before refresh, and the four replaced originals were also retained separately; no original was deleted.

At the initial correction, the existing fractional MSA outputs and earlier analysis panels were left intact because their provenance did not identify the cumulative PIT workbook vintage used. The reviewed rebuild below subsequently replaced the 29 curated MSA outputs, retaining every original separately. Earlier analysis panels remain historical artifacts; the new observed-only panel is saved separately. General additive recipe aggregation now propagates Count Types metadata; unsupported non-additive or allocation paths reject PIT observation metadata explicitly.

For the current frozen 60-site/77-CoC universe, the P2024 declaration audit uses the previously reconciled `pit_source_id` in every lineage row, consuming retrospective preaggregates once. Source-declared complete unsheltered site-years are:

| Year | Complete observed sites, out of 60 |
| --- | ---: |
| 2010 | 31 |
| 2011 | 60 |
| 2012 | 31 |
| 2013 | 60 |
| 2014 | 37 |
| 2015 | 60 |
| 2016 | 46 |
| 2017 | 60 |
| 2018 | 49 |
| 2019 | 60 |
| 2020 | 50 |
| 2022 | 59 |
| 2023 | 54 |
| 2024 | 54 |

This leaves 711 complete observed site-years and 508 exact annual PIT changes. Membership remains unchanged; observation availability varies by year. These are workbook-declared observations, not independent validation of constituent count quality inside retrospective preaggregates. No correlations were rerun in this correction. Reruns must accommodate the resulting unbalanced sample, refit site/year effects within each lag and sign subset, and validate their estimators against explicit dummy-variable least squares and duplicated-site bootstrap samples. Balanced-panel demeaning cannot simply be reused after masking.

[Refresh and impact artifacts](</home/matt/HHP-Output/pit-count-types-audit/20261022/>) include `manifest.json`, `frozen-site-impact.json`, `frozen-site-coc-count-types.csv`, `frozen-site-year-observation-status.csv`, staged refreshed files and the preserved originals. Bead `coclab-z98aw` implements the correction; `coclab-r0jjj` tracks observed-only longitudinal reruns.

### Reviewed derived PIT rebuild — 2026-10-22

All 29 existing curated fractional MSA PIT files were rebuilt, independently checked and installed with Count Types lineage. Every numeric PIT value matches its baseline. The explicit source choice is **P2025 for measure years 2010–2023 and 2025, and P2024 for 2024**: 27 P2025 outputs and two P2024 outputs. This is the declared source of the new rebuild; numerical agreement does not recover the unrecorded vintage of the old files.

The historical source choice matters. P2024 retrospectively combines some historical counts into successor CoCs. Pairing those counts with older native-CoC crosswalks changed 1,165 cells in an initial staging attempt, including a large Boston difference from MA-516 and MA-505. Review rejected that source choice for these derived artifacts. P2025 retains the historical breakdown needed to reproduce the existing rollups. Both staging attempts remain available. The separately reconciled whole-CoC longitudinal study continues to use P2024 and its explicit predecessor rules; these fractional MSA artifacts are a different aggregation and should not be substituted into that study.

Derived metadata now requires the expected contributing members, preserves original declarations in JSON maps, and propagates all-observed/any-carried/any-partial status. A nullable aggregate-only `pit_sheltered_count_observed` additionally prevents a partial sheltered sum from appearing complete when a member or its sheltered value is missing. Contributor selection matches the numeric rollup, including its tiny-share cutoff and exclusion of zero-weight contributions. Invalid weights and attempts to sum observation flags are rejected. Source CoC reported counts remain unchanged.

`hhplab aggregate pit --pit-vintage <release>` selects that cumulative input explicitly, bypassing competing annual files and default vintage discovery. CoC and MSA outputs retain selected input paths, hashes, known workbook vintage and inherited provenance; MSA outputs also pin the crosswalk hash and aggregation policy. An annual file without verified release metadata remains unknown rather than being assigned its measure year as a workbook vintage.

The independent review reproduced all three count measures in every output with direct crosswalk-weighted sums, checked eligible observation masks against raw declarations and expected membership, and verified input, crosswalk and output hashes. Historical source-to-boundary compatibility remains a separate, provisional issue; Count Types establishes count occurrence, not geographic equivalence.

[Reviewed rebuild artifacts](</home/matt/HHP-Output/pit-count-types-derived-rebuild/20261022/reviewed-vintages/>) include the driver, source/output manifest, staged artifacts, retained `originals/` and `promotion.json`. [Independent review evidence](</home/matt/HHP-Output/pit-count-types-review/20261022/>) includes `reviewed_vintages_artifact_review.json` and the direct weighted-sum review driver.

### Reviewed observed-only longitudinal checks — 2026-10-22

The same 60-site/77-CoC membership and provisional geography are retained. The panel preserves all 840 reported site-year values and supplies separate eligibility flags: **711 observed unsheltered/total site-years and 818 sheltered site-years**. Annual PIT differences require eligible values at both exact calendar endpoints, leaving **508 unsheltered/total changes and 698 sheltered changes**. Only 55 sites contribute same-year unsheltered annual changes. HIC predictors are formed independently of PIT observation availability: the unchanged 720 HIC transitions contain 341 shelter-bed increases, 357 declines and 22 flats, or 437 PSH increases, 235 declines and 48 flats. Sign selection precedes matching and fixed-effect adjustment.

The rerun covers the previous 70 comparisons: 10 pooled raw-level correlations, 10 pooled log-level correlations with site/year effects, and 50 annual log-change comparisons across pooled and sign-restricted samples. Each family uses exact calendar lags −2 through +2; positive lag means beds precede PIT. It retains pre-2020 point estimates and leave-one-site-out ranges. Missing PIT observations do not suppress otherwise usable HIC predictor years, and no annual difference bridges the omitted 2021 year.

Fixed effects are refitted within each unbalanced lag/sign sample using exact weighted least squares: site effects are absorbed and a small rank-tolerant year-effect system is solved. This replaces balanced-panel double demeaning. Each interval uses 1,999 whole-site bootstrap replicates, seed 20261022, resampling the active contributing sites in that sample and refitting effects; both the frozen universe and active count are reported. All 70 results have 1,999 defined replicates with no degeneracy or solver failures. Numerical zero-variance outcomes are explicitly treated as undefined.

Selected annual-change findings, with nominal 95% site-bootstrap intervals:

| Predictor / PIT outcome | Bed-change sample | Timing | Pairs | r | 95% interval |
| --- | --- | --- | ---: | ---: | --- |
| Shelter beds / unsheltered | All | Same year | 508 | −0.005 | −0.099 to 0.107 |
| Shelter beds / unsheltered | Increases | Same year | 243 | 0.117 | 0.007 to 0.232 |
| Shelter beds / total | Increases | Same year | 243 | 0.383 | 0.256 to 0.501 |
| Shelter beds / total | Declines | Same year | 246 | 0.133 | −0.047 to 0.311 |
| Shelter beds / sheltered | Increases | Same year | 330 | 0.476 | 0.371 to 0.595 |
| Shelter beds / sheltered | Declines | Same year | 346 | 0.319 | 0.171 to 0.485 |
| PSH beds / unsheltered | All | PSH precedes PIT by two years | 345 | −0.025 | −0.120 to 0.045 |
| PSH beds / unsheltered | Declines | PIT precedes PSH by one year | 152 | 0.253 | 0.040 to 0.364 |

Every pooled annual shelter-bed/unsheltered and PSH/unsheltered interval includes zero across all five timings. The earlier pooled two-year PSH-leading result, r = −0.094 with interval −0.192 to −0.014, weakens to −0.025 with interval −0.120 to 0.045. The increases-only two-year PSH result also no longer excludes zero. Same-year shelter-bed/sheltered-PIT associations remain positive for both increases and declines. The total-PIT increases association remains positive, while its declines interval now includes zero. Small nominal associations remain for same-year shelter-bed increases versus unsheltered PIT and for unsheltered PIT preceding PSH declines; these do not establish an intervention effect or a single direction of response.

These are preliminary conditional correlations, with unadjusted intervals across many timings, outcomes and subgroups. The reported and observed-only samples differ in composition as well as observation quality; changes in coefficients are not an isolated estimate of carry-forward bias. Sign restriction limits the range, HIC measures capacity rather than placements, and historical geography remains provisional. No causal or formal asymmetry claim is warranted.

The integrator and an independent Luna High reviewer reproduced all 70 estimates from separate reference calculations; the reviewer additionally fit explicit dummy-variable least squares to all persisted annual pairs and level samples. Maximum differences were below 6 × 10⁻¹⁶. Weighted-bootstrap calculations matched explicitly duplicated site samples; disconnected observation graphs and fully absorbed outcomes passed separate checks. Input hashes, the 6,884-row PIT refresh bridge, HIC predecessor consumption and script snapshot hashes were verified.

The committed [analysis driver](../scripts/analyze_observed_site_pit_hic_longitudinal.py) supports `--json`, `--non-interactive` and `--output-dir`, and refuses an already populated output directory. [Final reviewed artifacts](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/>) include the provenance-bearing panel, all 70 results, annual pairs and counts, observation inventories, site/year availability, the manifest and exact script snapshot. The interrupted attempts and their snapshots are retained separately. [Independent review evidence](</home/matt/HHP-Output/pit-count-types-review/20261022/longitudinal_independent_review.json>) pins the reviewed artifacts and comparison results. A [whitespace attestation](</home/matt/HHP-Output/pit-count-types-review/20261022/script_whitespace_attestation.json>) records a subsequent cleanup of one blank line; the executed snapshot is retained and its Python syntax tree is identical to the committed driver.

The graphs have also been regenerated from these reviewed observed-count results. They show confidence intervals and lag-specific paired-observation/contributing-site counts, with positive lag explicitly meaning beds precede PIT. The earlier graphs remain historical artifacts.

- [PSH annual changes: pooled, increases and declines](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/psh_lead_lag_changes.png>) ([SVG](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/psh_lead_lag_changes.svg>)).
- [Pooled shelter/PSH versus unsheltered PIT: adjusted log levels and annual changes](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/lead_lag_check.png>) ([SVG](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/lead_lag_check.svg>)).
- [Shelter-bed increases and declines versus unsheltered, total and sheltered PIT](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/shelter_lead_lag_changes.png>) ([SVG](</home/matt/HHP-Output/site-boundary-history-audit/exploratory-without-sacramento/observed-counts/final-attempt-02/graphs/reviewed/shelter_lead_lag_changes.svg>)).

The [plotting driver](../scripts/plot_observed_site_pit_hic_longitudinal.py) validates the saved estimates against the reviewed analysis manifest and renders the 60 adjusted comparisons. Raw-level comparisons remain in the numerical results table. Use `--results-dir` and a fresh `--output-dir` with `--json` to regenerate figures. The graph directory contains a script snapshot, `plotted_estimates.csv` and `plot_manifest.json` recording source and output hashes. No statistical estimates were recalculated.

### ACS1 gross rent versus observed unsheltered PIT — 2026-10-24

The requested rent-only diagnostic retains the frozen **60-site/77-CoC universe**, 2010–2024 excluding 2021, and the reviewed source-observation flags. It uses **ACS 1-year median gross rent, B25064_001E**, including utilities, from the existing native-CBSA curated files. No PSH adjustment is performed in this check.

ACS vintage **Y−1** is assigned to January PIT year **Y**. Thus the 2010 panel baseline uses ACS2009, and the first annual comparison pairs ACS2009→2010 rent growth with January2010→January2011 unsheltered growth. Annual changes begin in 2011 because 2010 is the PIT baseline. Positive lag k means the aligned rent observation/change precedes PIT by k additional calendar years. For example, lag +2 pairs ACS2009→2010 rent growth with PIT2012→2013 growth. Reverse lags are diagnostic, not prospective rent controls.

The primary comparison is **Δlog(gross rent) versus Δlog(1+unsheltered PIT), lag 0**, after site and outcome-year effects. Secondary checks cover lags −2 through +2, log rent versus log(1+PIT) levels with the same effects, and a pre-2020 restriction requiring both aligned rent and PIT endpoints before 2020. The analysis plan was saved before estimation. No raw cross-sectional correlation or sign-restricted grid was added.

All 840 reported PIT site-years and their flags remain unchanged. Rent is available for **814 site-years**, with **691** also having an eligible unsheltered observation. There are 54 matched rent sites at the 2010 baseline and 60 by 2024; 54 sites have rent matches in every included year. The primary annual comparison contains **492 pairs across 54 represented sites**, versus 508 PIT-only annual changes before matching rent coverage. A site represented by only one usable observation contributes no independent within-site variation after its fixed effect.

Missing rent matches remain explicitly unavailable; no aliases are guessed, no median is apportioned or summed, and no source value is interpolated. Existing omissions affect Crestview, Kiryas Joel–Poughkeepsie–Newburgh, Los Angeles, North Port, Santa Maria–Santa Barbara, and Urban Honolulu in some early vintages. Kiryas Joel has a match only for ACS2023. Bead **coclab-p37ps** tracks the source-geography and historical-ID audit. A curated file's target definition label does not establish that its historical CBSA footprint has been harmonized. Native metro rent remains a proxy for the whole-CoC site, and geography retains its provisional status.

Annual-change correlations, with nominal 95% whole-site bootstrap intervals:

| Lag | Full-period pairs / sites | Full-period r [95% interval] | Pre-2020 pairs / sites | Pre-2020 r [95% interval] |
| --- | ---: | --- | ---: | --- |
| −2 | 298 / 52 | −0.102 [−0.235, 0.031] | 250 / 50 | −0.072 [−0.232, 0.088] |
| −1 | 397 / 54 | −0.056 [−0.191, 0.074] | 297 / 52 | −0.130 [−0.264, 0.020] |
| 0 — primary | 492 / 54 | **0.067 [−0.047, 0.161]** | 343 / 52 | 0.080 [−0.049, 0.180] |
| +1 | 410 / 53 | −0.027 [−0.139, 0.111] | 313 / 51 | −0.045 [−0.193, 0.111] |
| +2 | 333 / 52 | 0.079 [−0.113, 0.258] | 283 / 51 | 0.023 [−0.148, 0.180] |

Adjusted log-level correlations, a secondary specification:

| Lag | Full-period pairs / sites | Full-period r [95% interval] | Pre-2020 pairs / sites | Pre-2020 r [95% interval] |
| --- | ---: | --- | ---: | --- |
| −2 | 531 / 60 | 0.052 [−0.089, 0.191] | 374 / 59 | 0.047 [−0.090, 0.181] |
| −1 | 591 / 60 | 0.118 [−0.023, 0.262] | 420 / 59 | 0.124 [−0.022, 0.269] |
| 0 | 691 / 60 | 0.130 [−0.007, 0.260] | 476 / 59 | 0.156 [0.036, 0.286] |
| +1 | 600 / 59 | 0.089 [−0.035, 0.220] | 444 / 59 | 0.160 [0.037, 0.283] |
| +2 | 548 / 59 | 0.145 [0.006, 0.272] | 387 / 59 | 0.149 [0.032, 0.263] |

**Interpretation:** annual rent increases show a small positive primary correlation with changes in unsheltered PIT, but its interval includes zero. Every annual-change interval includes zero, including the pre-2020 checks. Some adjusted level comparisons show a modest positive association, especially before 2020; the full-period +2 interval only narrowly excludes zero. This is suggestive of a relationship in how levels evolve, rather than a clear year-to-year pattern. Level models can retain differing regional trends that annual differences remove, and the matched samples also differ. These correlations are not elasticities or causal effects, and the secondary intervals are unadjusted across the 20 comparisons.

ACS gross rent measures incumbent households' housing costs and can respond differently from asking rents. Its annual medians contain sampling and composition changes. The whole-site bootstrap captures variation across sampled regional histories, not a separate propagation of ACS survey margins of error. Year effects absorb common national nominal growth; they do not resolve region-specific confounding. The result does not establish that rent pressure is irrelevant or that adding rent controls cannot change a PSH estimate.

The [driver](../scripts/analyze_observed_site_rent_longitudinal.py) reuses the reviewed unbalanced fixed-effect estimator and refits effects in each of **1,999 whole-site bootstrap draws**, seed 20261024. All 20 results have 1,999 defined draws with no solver or degeneracy failures. Direct dummy-variable least squares and explicitly duplicated-site bootstrap checks agree with the estimator. A separate review reconstructs every paired endpoint directly from the original ACS and PIT inputs, verifies all point estimates and input/output hashes, and confirms the original PIT values and flags are unchanged. Focused timing, nullable coverage, genuine-zero, invalid-rent, and output-preservation tests pass alongside the existing estimator tests: **36 tests passed**, with Ruff clean.

The reviewed artifacts are in [run-02](</home/matt/HHP-Output/site-rent-unsheltered-longitudinal/20261024/run-02/>): `analysis_plan.json`, `site_year_panel.parquet` with embedded provenance, `longitudinal_results.csv`, `paired_observations.csv`, `pair_counts.csv`, coverage and ACS-source inventories, exact driver/estimator snapshots, `manifest.json`, and `independent_review.json`. The preliminary `run-01` remains intact; a nullable missing-rent flag in its coverage diagnostics was corrected to explicit false in run-02, with all numerical estimates and intervals unchanged.

The [lead/lag graph](</home/matt/HHP-Output/site-rent-unsheltered-longitudinal/20261024/run-02/rent_unsheltered_lead_lag.png>) ([SVG](</home/matt/HHP-Output/site-rent-unsheltered-longitudinal/20261024/run-02/rent_unsheltered_lead_lag.svg>)) shows annual changes and adjusted levels, their intervals, and full-period sample counts. It was rendered directly from the saved results and visually reviewed. Existing shelter and PSH artifacts remain intact.

### ZORI versus observed unsheltered PIT, with matched ACS comparisons — 2026-10-24

The ZORI diagnostic uses **Zillow Economic Research's county All Homes smoothed monthly asking-rent series**, from the cached March 4, 2026 release. The original CSV's SHA-256 matches both the curated county rows and their source provenance. The analysis does not use the cached MSA ZORI artifacts: the retained 2015–2026 balanced artifact contains no usable values and lacks complete input references; bead **coclab-w1hvx** tracks its audit.

For timing comparable to ACS, a calendar-year ZORI mean in **Y−1** is aligned with January PIT in **Y**. The overlap therefore covers **2015–2023 rent data and 2016–2024 PIT**, excluding 2021. Annual changes begin in 2017. At lag 0, ZORI2015→2016 pairs with PIT2016→2017; at lag +1, that same rent growth pairs with PIT2017→2018. Positive lag is an additional delay beyond the preceding-calendar-year alignment. This is not a comparison of January ZORI with January PIT.

Each county/calendar year needs **12 distinct finite, positive monthly values**. A fixed county support is selected for each MSA: member counties having at least one complete year within the requested period. Every usable regional year must contain **all** counties in that support. Incomplete region-years remain missing, so county entry or exit cannot masquerade as a rent change. Relative weights are fixed to **2020 county population from PEP vintage 2020**, using 2023 MSA county membership. The core package's annual ZORI aggregator supplies the population-weighted mean and coverage diagnostics; an additional complete-support guard enforces constant composition. At least 90% of full-MSA population must be covered, following the package's default ZORI coverage threshold. Actual coverage of usable study site-years is at least **99.42%**.

This produces a regional proxy from county ZORI, rather than Zillow's independently estimated native metro index. It is also a different concept from ACS gross rent including utilities. The frozen **60-site/77-CoC membership** is retained; 57 sites have at least one usable ZORI year. Bend, El Centro, and Lynchburg have no qualifying regional years under these measurement rules. They remain in the universe and coverage inventory. The prior Bridgeport, Salem, York, and Sacramento exclusions remain in force. Of 480 study site-years, **379** have usable ZORI and **338** also have an eligible observed unsheltered count. The primary annual comparison contains **218 pairs across 48 represented sites**.

The analysis plan records a primary **lag-0 Δlog(ZORI) versus Δlog(1+unsheltered PIT)** correlation with site and outcome-year effects. Secondary checks cover lags −2 through +2, adjusted log levels, and pre-2020 restrictions. Rent predictors do not require observed PIT at their own aligned year. Exact adjacent endpoints are required for every annual change, with no gap bridging or imputation.

ZORI annual-change results on all available qualifying observations, with nominal 95% whole-site bootstrap intervals:

| Lag | Full-period pairs / sites | Full-period r [95% interval] | Pre-2020 r [95% interval] |
| --- | ---: | --- | --- |
| −2 | 70 / 38 | −0.064 [−0.446, 0.297] | Undefined after fixed effects |
| −1 | 149 / 48 | 0.152 [−0.098, 0.384] | −0.113 [−0.378, 0.131] |
| 0 — primary | 218 / 48 | **0.100 [−0.122, 0.332]** | −0.039 [−0.306, 0.236] |
| +1 | 137 / 42 | **0.189 [0.046, 0.316]** | 0.212 [−0.064, 0.425] |
| +2 | 63 / 34 | −0.021 [−0.311, 0.275] | Undefined after fixed effects |

The one-year-leading point estimate remains positive when any single represented site is omitted: **0.153 to 0.210**. Its pre-2020 point estimate is similar, but the interval is wider and includes zero; that sensitivity has only 61 pairs across 31 represented sites. The primary pre-2020 comparison has 96 pairs across 38 sites. For the extreme pre-2020 annual lags, every site has at most one matched observation, so site effects absorb all variation. Those correlations and intervals are **undefined**, not zero. The graph omits their sensitivity markers.

Adjusted log-level results for ZORI are also inconclusive: all full-period and pre-2020 level intervals include zero. At lag 0, full-period r is **0.122 [−0.040, 0.279]**, and at lag +1 it is **0.136 [−0.054, 0.314]**. The full grid and lag-specific sample counts are saved in the numerical artifacts.

To separate source changes from sample changes, ZORI and ACS1 gross rent are additionally estimated on **identical site/year pairs at each lag**, requiring both measures at every predictor endpoint before fitting effects:

| Timing | Common pairs / sites | ZORI r [95% interval] | ACS1 gross rent r [95% interval] |
| --- | ---: | --- | --- |
| Lag 0 annual changes | 213 / 47 | 0.101 [−0.135, 0.331] | 0.100 [−0.057, 0.274] |
| Lag +1 annual changes | 134 / 41 | **0.194 [0.050, 0.326]** | 0.015 [−0.219, 0.260] |

The one-year signal is therefore present in ZORI on the common sample and is much weaker in ACS at that timing. This does **not** establish that the two correlations differ statistically; their difference was not formally tested. Nor is the common-sample result an independent replication of the available-sample result: the observations largely overlap.

**Interpretation:** there is a modest positive, one-year-leading association worth following up: increases in market asking rents precede increases in unsheltered PIT in this specification. Same-year and two-year annual comparisons remain inconclusive, and the level specifications do not provide clear corroboration. The one-year result is a secondary, unadjusted comparison within a 60-row exploratory grid, with 54 estimable rows. It does not establish a causal rent effect or a reliably identified delay. The shorter period, missing monthly ZORI coverage, observation eligibility, and smaller samples limit precision. These results support retaining rent pressure as a candidate control when revisiting PSH; no PSH-adjusted estimate has been run here.

Each estimable result uses **1,999 whole-site bootstrap draws**, seed 20261024, refitting effects each time. All 54 estimable rows have 1,999 defined draws and no solver, degeneracy, or bootstrap-validation failures. The six fully absorbed sensitivity rows have no defined interval. Direct dummy-variable least squares and explicitly duplicated-site bootstrap checks agree with the estimator.

A separate review reproduced **7,946 county-month values directly from the raw CSV**, checked all 540 regional calendar-year rows against direct fixed-population weighted sums, and reconstructed every paired endpoint independently of the driver. It reproduced the 54 point estimates within **2.5 × 10⁻¹⁴**, verified the six undefined cases, confirmed that common-sample keys match exactly, checked all original input/output hashes, and confirmed PIT values and observation flags match the reviewed parent panel. Timing, coverage, fixed-composition, invalid-weight, and short-window degeneracy checks pass with the existing estimator and metro-ZORI tests: **84 tests passed**, with Ruff clean.

The [existing rent driver](../scripts/analyze_observed_site_rent_longitudinal.py) now accepts `--rent-source zori`; use a fresh `--output-dir`, `--json`, and `--non-interactive`. [Reviewed run-02 artifacts](</home/matt/HHP-Output/site-zori-unsheltered-longitudinal/20261024/run-02/>) include the plan, source/coverage inventories, provenance-bearing site and regional panels, all results and pairs, driver/estimator/rollup snapshots, manifest, and independent review. The interrupted first execution is retained separately; it exposed the need to report fully absorbed short-window comparisons explicitly.

The [reviewed graph](</home/matt/HHP-Output/site-zori-unsheltered-longitudinal/20261024/run-02/graphs/reviewed/zori_unsheltered_lead_lag.png>) ([SVG](</home/matt/HHP-Output/site-zori-unsheltered-longitudinal/20261024/run-02/graphs/reviewed/zori_unsheltered_lead_lag.svg>)) was rendered from the saved estimates. Its plot manifest pins the numerical result, analysis manifest, current plot-script snapshot, and image hashes. A heading-only variable-shadowing fix was applied after estimation; the independent review confirms the statistical code's syntax tree is unchanged. Earlier figures and all prior ACS, shelter, and PSH results are retained.

### ZORI versus total homelessness — 2026-10-24

The total-PIT rerun changes the outcome to **sheltered plus unsheltered homelessness**, retaining the ZORI construction, original site universe, 2016–2024 PIT period excluding 2021, timing alignment, site/year effects, lags, and bootstrap method above. Boston (MSA 14460 / MA-500) and New York City (MSA 35620 / NY-600) remain absent; they were already outside the selected 99% universe, so their explicit exclusion removes no observations.

Total PIT is eligible only when its total, sheltered, and unsheltered observation flags are all true, and the reported total equals the sum of the components. Annual differences require eligibility at both exact calendar endpoints. All 480 study rows satisfy the reported count identity, and total and unsheltered eligibility agree here. The panels, ZORI rollups, rent values, site/year pair keys, and sample sizes therefore match the previous unsheltered analysis exactly. Only the outcome changes; **log(1+total)** is calculated from summed headcounts, not from a sum of component logs.

Annual-change results on all qualifying ZORI observations, with nominal 95% whole-site bootstrap intervals:

| Lag | Pairs / sites | Total-PIT r [95% interval] | Previous unsheltered r [95% interval] |
| --- | ---: | --- | --- |
| −2 | 70 / 38 | 0.194 [−0.212, 0.559] | −0.064 [−0.446, 0.297] |
| −1 | 149 / 48 | 0.230 [0.032, 0.416] | 0.152 [−0.098, 0.384] |
| 0 — primary | 218 / 48 | **0.213 [−0.020, 0.410]** | 0.100 [−0.122, 0.332] |
| +1 | 137 / 42 | **0.042 [−0.154, 0.200]** | 0.189 [0.046, 0.316] |
| +2 | 63 / 34 | −0.225 [−0.593, 0.199] | −0.021 [−0.311, 0.275] |

Positive lag retains the preceding-year rent alignment: lag 0 pairs ZORI2015→2016 with PIT2016→2017, and lag +1 pairs that rent change with PIT2017→2018. The primary total-PIT point estimate is more positive than its unsheltered counterpart, but its interval includes zero. The earlier one-year-leading unsheltered association does not appear in the total-PIT estimate. This is a descriptive comparison of point estimates and intervals, not a formal test that the outcome correlations differ. Log changes also prevent interpreting the difference as a headcount flow between sheltered and unsheltered states.

The positive annual reverse-timing estimate at lag −1 means the results do not identify a unique direction of response. The two-year-forward annual comparison contains only two outcome years, with a wide interval. Pre-2020 annual estimates are also uncertain: lag 0 is **0.070 [−0.205, 0.304]** on 96 pairs across 38 sites, and lag +1 is **0.415 [−0.061, 0.703]** on 61 pairs across 31 sites. Extreme pre-2020 annual lags remain fully absorbed by fixed effects and are undefined rather than zero.

Adjusted log-level models show modest positive associations at some forward timings:

| Lag | Pairs / sites | Total-PIT r [95% interval] |
| --- | ---: | --- |
| 0 | 338 / 55 | 0.182 [−0.014, 0.355] |
| +1 | 246 / 53 | **0.229 [0.021, 0.424]** |
| +2 | 202 / 49 | **0.199 [0.007, 0.368]** |

Pre-2020 levels at lag 0 and +1 also have nominal intervals above zero; lag +2 does not. Level associations may retain regional trends that differencing removes. These secondary comparisons do not establish a causal effect or a stable delay, and their intervals are unadjusted across the expanded exploratory series.

The matched ACS comparison remains inconclusive. On identical primary observations (213 pairs / 47 sites), total-PIT r is **0.211 [−0.030, 0.406]** for ZORI and **−0.028 [−0.176, 0.135]** for ACS1 gross rent. At lag +1 (134 pairs / 41 sites), the estimates are **0.040 [−0.147, 0.208]** and **−0.009 [−0.249, 0.191]**, respectively. Differences between the rent measures were not formally tested.

**Interpretation:** total homelessness shows some positive association with ZORI in level models and a positive but uncertain primary annual estimate. It does not reproduce the one-year-forward annual signal found for unsheltered homelessness. The mix of forward, reverse, and specification-dependent results warrants caution about assigning a particular response time or mechanism. No PSH adjustment is included.

The [rent driver](../scripts/analyze_observed_site_rent_longitudinal.py) now supports `--pit-outcome total`, including an explicit component-observation and count-identity guard. Reproduce with `--rent-source zori --pit-outcome total --json --non-interactive --output-dir <fresh-directory>`. It uses the same 1,999 whole-site bootstrap replicates and seed 20261024. All 54 estimable rows have 1,999 defined draws and no solver, degeneracy, or bootstrap-validation failures; six short-window sensitivity rows remain undefined. The relevant estimator, rent, and metro-ZORI tests pass: **93 tests passed**, with Ruff clean and curated-layout validation passing.

[Reviewed artifacts](</home/matt/HHP-Output/site-zori-total-pit-longitudinal/20261024/run-01/>) include the plan, panel with provenance, all results and pairs, source/coverage inventories, script snapshots, manifest, `total_vs_unsheltered_comparison.csv`, and `independent_review.json`. The independent review reconstructs outcomes directly from the parent sheltered and unsheltered components, checks every transformed endpoint and fixed-effect point estimate, verifies identical pairing and rent values against the prior analysis, and pins input/output hashes. The [graph](</home/matt/HHP-Output/site-zori-total-pit-longitudinal/20261024/run-01/zori_total_lead_lag.png>) ([SVG](</home/matt/HHP-Output/site-zori-total-pit-longitudinal/20261024/run-01/zori_total_lead_lag.svg>)) shows annual changes, adjusted levels, intervals, and full-period sample counts; it was visually reviewed.
