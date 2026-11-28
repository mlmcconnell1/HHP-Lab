# Shelter-capacity sample curation

This is the November 28, 2026 working review for `subst_fd`, the primary first-difference analysis. It records **533 accepted, 13 excluded by study decision, and 17 open** adjacent-year pairs across 563 candidates. The 2024–25 transition has **46 accepted, one excluded, and one open**. The stricter boundary sensitivity subset contains **101** accepted pairs. The production example currently ends in 2024, and this ledger documents the extended working sample before a production model fit.

## Find a decision

| File | What to look up |
| --- | --- |
| [Pair decisions](pair-decisions.csv) | Search by Census MSA ID, name, and first and last year. The row records the disposition, member CoCs, open questions, and any count or capacity flags. |
| [CoC-year scopes](coc-year-scopes.csv) | Search by HUD CoC ID and count year. The row records PIT and HIC scope separately, the contributing source CoCs, the aggregation operation, and the evidence or remaining question. |
| [Targeted reviews](targeted-reviews.json) | Search by MSA ID or CoC ID for 63 individual reviews. Each entry carries its rationale, dated scope sources, and any structured count override or data annotation. |
| [Source manifest](source-manifest.json) | Find the exact frozen roster of 64 Census MSAs and 84 CoCs, confirmed whole-metro holdouts, source vintages and identifiers, and recorded input fingerprints. |

The pair ledger's `targeted_review` field points to the matching `msa_id`, `first_year`, and `last_year` in the JSON. Confirmed whole-metro holdouts are keyed by `msa_id` in the manifest's `confirmed_msa_holdouts`. The pair ledger marks both those holdouts and individual pairs with a targeted `exclude` decision as `excluded_by_study_decision`. The CoC-year table tracks the availability and scope of each source independently of metro-level inclusion. A blank `working_scope_status` marks retained evidence for a year outside the candidate endpoints. An `unknown` PIT or HIC status calls for evidence before using that source. Fields with several IDs, endpoints, or references use ` | ` as the delimiter. `asset:` references name source or intermediate artifacts stored outside Git. Paths beginning with `docs/` or `data/` are relative to the repository root.

On an accepted York–Hanover pair, `earlier_study_holdout` preserves the historical reason for review. The targeted `supersedes_earlier_study_holdout` field records the decision to accept that specific post-split pair.

### Examples

| MSA and period | Decision |
| --- | --- |
| 39300 Providence–Warwick, 2024–25 | Use the merged MA-505 submission once in 2024 and one submission per member CoC in 2025. |
| 35840 North Port–Bradenton–Sarasota, 2024–25 | Retain the published bed inventory and flag uncertainty in the domestic violence shelter entries. |
| 12540 Bakersfield–Delano, 2024–25 | Accept Kern County scope and the July 2025 local count revision that agrees with HUD. |
| 21500 Erie, 2024–25 | Accept with the documented change in enumeration methodology flagged. |
| 37860 Pensacola–Ferry Pass–Brent and 45940 Trenton–Princeton, 2024–25 | Accept with the documented count timing and classification notes, respectively. |
| 18880 Crestview–Fort Walton Beach–Destin, 2024–25 | Use the CoC's January 31, 2025 PIT figures. Keep HUD's January 24 figures for sensitivity. |
| 12980 Battle Creek and 19660 Deltona–Daytona Beach–Ormond Beach, 2024–25 | Retain the published year-round inventory and flag the temporary beds in Battle Creek and the First Step Shelter entries in Deltona. |
| 19660 Deltona–Daytona Beach–Ormond Beach, 2010–11 through 2014–15 | Accept the five pairs after reviewing the corrected Palm Coast maps and the two-county count records. Flag the subgroup reporting change in 2012–13 and temporary beds from 2010–11 through 2013–14. |
| 34940 Naples–Marco Island and 15980 Cape Coral–Fort Myers, 2024–25 | Assign the presumed duplicate St. Matthew's House facility to Lee using its 65-bed entry. Adjust Collier from 538 to 470 beds and keep 538 for sensitivity. |
| 37340 Palm Bay–Melbourne–Titusville, 2024–25 | Retain the published PIT counts and flag the 213 cold-night shelter guests counted as unsheltered in Brevard in 2025. |
| 31700 Manchester–Nashua, all 11 candidate pairs | Hold out the metro. Review note is “confusing.” The documented service areas cover 74.1% of its residents, and compatible PIT and HIC data for the Balance of State towns remain unverified. |
| 39540 Racine–Mount Pleasant, 2010–11 through 2017–18 and 2024–25 | Accept the six pairs through 2015–16. Exclude 2016–17 for the documented change in unsheltered counting and 2017–18 for a possible change in areas searched. Accept 2024–25 and flag HALO's 27 fewer reported year-round beds. |
| 41420 Salem, 2022–23 | Keep open pending disposition of the earlier whole-site holdout. |
| 49620 York–Hanover, 2015–16 through 2019–20 and 2022–23 through 2024–25 | Accept all eight pairs under the standalone York County reporting scope. Carry youth-count timing notes on 2023–24 and 2024–25 and temporary-bed flags on 2019–20 and 2024–25. |

HUD's FY2025 preliminary funding figure for NH-502 is $762,649, exactly the sum of its Nashua and Hillsborough County geocode amounts. The funding figures identify the geographic codes used for the preliminary calculation. Assessing PIT and HIC coverage still requires the towns and projects behind the reported counts. [HUD FY2025 geographic code report](https://www.hud.gov/sites/default/files/CPD/documents/CoC/FY-2025-GeoCodes-PPRN-Report.pdf) · [HUD FY2025 renewal demand report](https://www.hud.gov/sites/dfiles/CPD/documents/CoC/FY25-CoC-Estimated-Annual-Renewal-Demand-Amount-Report.pdf)

In 2024, NH-500 reported 11 year-round transitional beds at Monadnock Area Transitional Shelter in Peterborough, a town the frozen HUD map assigns to NH-502. The 2025 project reports leave that shelter's inventory treatment unresolved. [2024 NH-500 inventory](https://files.hudexchange.info/reports/published/CoC_HIC_CoC_NH-500-2024_NH_2024.pdf#page=4) · [Shelter provider](https://www.matsnh.org/about-mats) · [2025 NH-500 inventory](https://files.hudexchange.info/reports/published/CoC_HIC_CoC_NH-500-2025_NH_2025.pdf#page=4)

## Review rules

### Preliminary boundary screen

The strict subset retains prior reporting-scope approvals and adds CoC-years supported by dated local reporting documentation or near-equivalent HUD archive maps with matching PIT and HIC figures. The archive screen used two-way comparisons within 5 cm of the archived FY2024 feature and 1 m of the frozen study feature. The ledger retains prior count operations for merged submissions and documented predecessor CoCs. [Historical reconciliations](../../../docs/site-historical-reconciliation-inventory.md) · [Providence reconciliation](../../../docs/providence-historical-reconciliation.md) · [Reviewed 2024 boundary evidence](../coc-boundary-compatibility-shelter-capacity.json)

### Administrative reporting scope

The working sample also accepts cases where dated records support the same administrative reporting jurisdiction despite residual map differences. For the 2022 and 2023 map archives, the attached counts had to match the study inputs. The historical screen routes unresolved changes meeting both 1% and 1 km², gaps in the archive sequence, known changes in operational coverage, and study holdouts to review. The 2025 approvals use dated reporting-scope evidence. The ledger retains uncertainty about how historical counts would change if a shelter or encampment lay in a disputed area. [Perimeter reviews](../../../docs/site-boundary-remaining-geometries.md) · [Casnovia assignment](../../../docs/historical-coc-archive-availability.md)

Compare operational coverage within each pair. Racine's November 2016 approval of combined street and service-based counting falls between the January 2016 and 2017 endpoints. Teams searched previously excluded areas by at least 2018, and January 2017 coverage of those areas is still unverified. [2019 HUD application](https://racinecoc.org/sites/default/files/inline-files/App-Final-with-all-attachments%20NOFA%202019.pdf#page=29) · [June 2024 council minutes](https://racinecoc.org/sites/default/files/inline-files/2024.6.10%20LC%20Minutes.pdf#page=3)

Use CoCs' published classification of cold-night shelter guests as unsheltered unless case-specific evidence warrants further review. Carry documented flags into any reuse. Brevard's `pit_classification_annotation` is scoped to `pit_sheltered` and `pit_unsheltered` for FL-513 in 2025. Attach it with the [HHP-Lab annotation contract](../../../docs/results-recipe-spec_2.md#45-annotations-and-annotation-provenance) when preparing the PIT input so it can follow the figures into derived analyses.

## Reproduce the working sample

1. Use the frozen Census 2023 MSA definition and the manifest's 99% two-way population-overlap roster, with 2024 frozen CoC geography. Select `accepted_on_prior_review` and `accepted_with_map_uncertainty` from the pair ledger for the 533 working keys. Use `strict_boundary_subset=true` for the 101-pair sensitivity fit. The screen begins in 2010–11, masks 2021, and has zero source-complete pairs for 2025–26 at this review. The three early Springfield pairs were screened out under the existing [count-geography decisions](../coc-count-geography-decisions.csv).
2. Acquire the source data identified in the manifest. Use the HUD PIT 2024 publication for observations through 2024 and the 2025 publication for the 2025 observation. Require observed sheltered, unsheltered, and total counts for every contributing row. Use the annual HUD HIC year-round emergency shelter, transitional housing, and safe haven total. For the early HIC years, use the combined field where the publication gives only the aggregate. For each CoC-year, apply the recorded PIT and HIC operations separately. `single` consumes one row, `sum` adds the listed rows, and `preaggregated` consumes one published row covering the listed predecessors.
3. Average the previous and current July county estimates to get January population on the frozen MSA county roster. Use PEP vintage 2020 through January 2020, vintage 2024 for January 2021–24, and vintage 2025 for January 2025. For January 2010, use the intercensal July 2009 series for the earlier endpoint. Use annual BLS LAUS unemployment on the matching metro definition through 2024. For the 2024–25 comparison, sum unemployed people and labor force over January–September and November–December in **each** year, then divide the sums for each year's rate. [BLS metro annual averages](https://www.bls.gov/lau/metro-annual-unemployment-rates.htm) · [BLS methodology](https://www.bls.gov/lau/laumthd.htm)
4. Apply the targeted JSON's PIT and HIC overrides for their exact CoC-year keys before aggregation. Other count and capacity flags supply caveats or sensitivity inputs. When a sensitivity scenario changes an endpoint shared by two pairs, use the same scenario value in both comparisons.

The tracked files contain decisions, identifiers, citations, and fingerprints. Download the bulk input data from the providers listed in the manifest. Some derived geography witnesses and historical download receipts referenced by `asset:` are retained with the working audit. The original boundary fingerprint identifies that audit revision. The CSVs provide a portable review record, while the [results recipe](../../examples/shelter-capacity-results.yaml) declares the executable panel inputs.

When a case is resolved, update its targeted review, affected CoC-year entries, and affected pair dispositions together. Keep the evidence URL, count source and vintage, caveat, and revisit condition with the decision.
