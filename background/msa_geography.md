# MSA And Metro Geography Guide

This note explains when to use `coc`, `metro`, or `msa`, how canonical metro
universe artifacts differ from subset profiles, how MSA artifacts are named,
and how CoC-native PIT counts are allocated into Census MSAs.

## Choosing a Geography

| Target | Use when | Identifier contract | Key artifacts |
| --- | --- | --- | --- |
| `coc` | You need official HUD Continuum of Care units and explicit boundary vintages. | `coc_id` + `boundary_vintage` | `coc__B<year>`, `xwalk__B<year>xT<year>`, `xwalk__B<year>xC<year>`, `panel__Y...@B<year>` |
| `metro` | You need the project's metro analysis surface, either as the full canonical universe or as a named subset profile. | canonical `metro_id` + `definition_version` (for example `census_msa_2023`) | `metro_universe__...`, `metro_subset_membership__...`, `panel__metro__...` |
| `msa` | You need Census Metropolitan Statistical Areas keyed to the official CBSA/MSA delineation. | `msa_id` = 5-digit CBSA/MSA code + `definition_version` (for example `census_msa_2023`) | `msa_definitions__...`, `msa_county_membership__...`, `msa_coc_xwalk__...`, `pit__msa__...`, `panel__msa__...` |

The important distinction is that `metro` and `msa` are not aliases:

- `metro` is the project's metro analysis surface.
- The canonical `metro` universe is derived from Census metro delineations and
  uses canonical CBSA codes as `metro_id`.
- Optional subset profiles such as Glynn/Fox add profile metadata
  (`profile_metro_id`, `profile_metro_name`, `profile_rank`) while preserving
  canonical `metro_id` values.
- `msa` is the Census delineation surface and uses Census CBSA/MSA identifiers.
- Code and artifacts should never reuse `metro_id` for an MSA output or `msa_id`
  for a Glynn/Fox output.

## Canonical Metro Universe Versus Subset Profiles

Use the full canonical metro universe when:

- you want every available Census metro in the chosen delineation version
- you are building a new recipe or transform and do not need a named subset
- you want identifiers to remain canonical across subset definitions

Use a subset profile when:

- you need a published cohort such as the 25 Glynn/Fox metros
- you want canonical `metro_id` values plus profile-specific columns for joins,
  labeling, or regression checks
- you are proving that a historical metro cohort is just a filter over the
  canonical universe rather than a separate geography family

For new recipes, prefer:

```yaml
geometry:
  type: metro
  source: census_msa_2023
  subset_profile: glynn_fox
  subset_profile_definition_version: glynn_fox_v1
```

The legacy form below still works as a compatibility shim:

```yaml
geometry: { type: metro, source: glynn_fox_v1 }
```

Runtime execution resolves that legacy form through the canonical metro
universe plus the Glynn/Fox subset profile. Keep using it only for backward
compatibility, regression coverage, or migration examples.

## Metro And MSA Artifact Families

Canonical metro artifacts:

- `data/curated/metro/metro_universe__<definition>.parquet`
  Canonical metro-universe membership keyed by CBSA code.
- `data/curated/metro/metro_subset_membership__<profile-version>xM<definition>.parquet`
  Subset/profile membership that maps canonical `metro_id` values to profile
  metadata such as `GF01`.

Legacy compatibility artifacts still exist:

- `data/curated/metro/metro_definitions__glynn_fox_v1.parquet`
- `data/curated/metro/metro_coc_membership__glynn_fox_v1.parquet`
- `data/curated/metro/metro_county_membership__glynn_fox_v1.parquet`

Those legacy tables remain for migration safety. New contributor guidance
should assume canonical metro-universe artifacts first and subset profiles
second.

The MSA workflow introduces three curated artifact families:

- `data/curated/msa/msa_definitions__<definition>.parquet`
  Canonical list of MSAs with stable identifiers and names.
- `data/curated/msa/msa_county_membership__<definition>.parquet`
  Official MSA-to-county membership from the Census delineation workbook.
- `data/curated/xwalks/msa_coc_xwalk__B<boundary>xM<definition>xC<counties>.parquet`
  Auditable CoC-to-MSA PIT allocation crosswalk derived from CoC boundaries,
  county geometry, and MSA county membership.

Derived outputs keep the same distinction:

- `pit__msa__P<year>@M<definition>xB<boundary>xC<counties>.parquet`
- `panel__msa__Y<start>-<end>@M<definition>.parquet`

## CoC-to-MSA PIT Allocation Method

PIT counts are published natively at CoC geography, not MSA geography. HHP-Lab
therefore allocates PIT counts into MSAs through the stored CoC-to-MSA
crosswalk:

1. Build the CoC-to-county overlay for the chosen CoC boundary vintage.
2. Keep only counties that belong to each MSA according to the curated MSA
   county-membership artifact.
3. Sum CoC/county intersections to CoC/MSA intersections.
4. Compute `allocation_share = intersection_area / coc_area`.
5. Multiply each CoC PIT measure by `allocation_share`, then sum by `msa_id`.

This is an area-weighted allocation rule. It is explicit in the crosswalk
artifact via:

- `allocation_method = "area"`
- `share_column = "allocation_share"`
- `share_denominator = "coc_area"`

## Prerequisites

For an MSA workflow that consumes CoC PIT, the minimum prerequisites are:

- curated CoC boundary artifact for the boundary vintage
- county geometry for the county vintage used by the crosswalk
- MSA definitions and county membership for the selected `definition_version`
- PIT input at CoC geography

Typical commands:

```bash
hhplab generate msa --definition-version census_msa_2023
hhplab generate msa-xwalk --boundary 2020 --definition-version census_msa_2023 --counties 2020
hhplab build recipe-preflight --recipe recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml --json
hhplab build recipe --recipe recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml --json
```

## Limitations

- PIT is not published natively for MSAs, so MSA PIT values depend on the
  allocation rule and crosswalk vintage.
- CoCs can straddle counties that are outside any MSA. In that case the
  crosswalk can leave an explicit unallocated share rather than forcing a full
  allocation.
- `msa` does not imply the Glynn/Fox `metro` set, even though both ultimately
  rely on CBSA-related concepts in some source products.
