# 2024 PIT and HIC CoC input audit

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

## Boundary evidence artifact

Use one `coc_boundary_compatibility/1` JSON artifact for each threshold, since its header binds one membership identity. The required header fields are `schema_version`, `evidence_id`, `artifact_id`, `threshold`, `membership_identity`, `frozen_boundary_vintage`, and `records`. Each record identifies `source_panel`, `frozen_coc_id`, `year`, `frozen_boundary_vintage`, `source_boundary_vintage`, `status`, and `reason`; `evidence_ref` is required for `compatible`. The records may use `status: compatible` for the 2024 same-year source join when they cite this audit and the HUD FY2024 catalog/service metadata, and state that the source CoC number directly matches the frozen B2024 CoC ID. Do not add `approval_ref` for a compatible record; that field is for an explicitly approved ID mapping. The evidence establishes the declared FY2024 source-to-geography match and direct ID resolution, not historical boundary stability.

For `artifact_id: overlap_samples` and `site_key: msa_id`, the selected membership identities are:

| Threshold | Selected membership rows | `membership_identity` |
| --- | ---: | --- |
| 0.90 | 144 | `sha256:24f3ba261df115e529664f95e903a1f4d96e674a3eea1973a6620c404c83419d` |
| 0.95 | 107 | `sha256:482174ce2976a17eedad0190e5038db5f24102c719f77364917a2034086c06eb` |
| 0.99 | 84 | `sha256:a6f8f3e35c335d9c1877b2fff8fd6727d362fe89321f11dbfe2f80ef98f23338` |

Each hash is SHA-256 over canonical JSON containing schema `msa_coc_sites/1`, artifact ID, numeric threshold, site key `msa_id`, and the sorted selected `(msa_id, coc_id)` pairs. For a shared CoC outcome panel containing both 2024 measures, supply one evidence record per selected CoC and the exact panel key used by the recipe. Use `frozen_boundary_vintage: "2024"` and `source_boundary_vintage: "2024"` only when the recipe's declared source-panel provenance is also pinned to the HUD FY2024 CoC layer. The official references above, workbook references, source ID, and this note provide the evidence reference; no historical remapping or boundary approval is asserted.
