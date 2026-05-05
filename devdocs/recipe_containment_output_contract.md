# Recipe Containment Output Contract

This document defines the v1 recipe contract for geographic containment-list
outputs. It is the implementation target for recipe schema, preflight,
executor, and examples work.

## Scope

Containment outputs are tabular artifacts that list candidate geographies that
overlap a selected container geography above a configured threshold. The first
implementation supports exactly two pairings:

| Container | Candidate | Intended use |
|-----------|-----------|--------------|
| `msa` | `coc` | Find CoCs substantially contained in selected MSAs. |
| `coc` | `county` | Find counties substantially contained in selected CoCs. |

Both pairings are area-overlap queries. They do not allocate measures or
construct analysis panels. Downstream recipes may use the resulting candidate
IDs as selector inputs in later work, but v1 only writes the containment list
artifact and its manifest.

## YAML Shape

Containment outputs live under a target's `outputs` list. A recipe may have
panel, diagnostics, map, and containment outputs side by side.

```yaml
targets:
  selected_msa_containment:
    geometry:
      type: msa
      source: census_msa_2023
      vintage: 2023
    outputs:
      - kind: containment
        id: cleveland_msa_coc_candidates
        containment_spec:
          container:
            type: msa
            source: census_msa_2023
            vintage: 2023
          candidate:
            type: coc
            vintage: 2025
          selector_ids: ["17460"]
          min_share: 0.20
          denominator: candidate_area
          method: planar_intersection
```

`containment_spec` fields:

| Field | Required | Meaning |
|-------|----------|---------|
| `container` | yes | `GeometryRef` for the selected containing geography. |
| `candidate` | yes | `GeometryRef` for possible contained geographies. |
| `selector_ids` | yes | Container IDs to evaluate. Empty lists are invalid. |
| `min_share` | no | Inclusive threshold for `contained_share`; default `0.0`, range `[0, 1]`. |
| `denominator` | no | Area denominator used for thresholding; default `candidate_area`. |
| `method` | no | Geometry operation identifier; default `planar_intersection`. |
| `definition_version` | no | Optional stable version for synthetic or curated definitions. |

Supported `denominator` values:

| Value | Formula | Use |
|-------|---------|-----|
| `candidate_area` | `intersection_area / candidate_area` | Default containment semantics: "how much of the candidate is inside the container?" |
| `container_area` | `intersection_area / container_area` | Coverage semantics: "how much of the container is covered by this candidate?" |

For v1, recipes should use `candidate_area` unless a caller explicitly needs
container coverage diagnostics. Preflight must reject denominator values outside
this table.

## Supported Geometry Pairs

The schema should accept the open `GeometryRef` shape, but semantic validation
must reject unsupported pairs until an implementation exists.

| Pair | Required inputs | Notes |
|------|-----------------|-------|
| `msa -> coc` | MSA boundary artifact and CoC boundary artifact | MSA IDs are CBSA codes. CoC candidates are keyed by `coc_id`. |
| `coc -> county` | CoC boundary artifact and county geometry artifact | County candidates are keyed by county FIPS/GEOID. |

Reverse pairings such as `coc -> msa` and `county -> coc` are not aliases in
v1 because their selector IDs, filenames, and denominator defaults differ.

## Canonical Output Columns

Containment parquet files must contain exactly these core columns, in this
order, followed only by documented extension columns added in later versions:

| Column | Type | Meaning |
|--------|------|---------|
| `container_type` | string | Geometry type for the container, e.g. `msa` or `coc`. |
| `container_id` | string | Selected container ID from `selector_ids`. |
| `candidate_type` | string | Geometry type for the candidate, e.g. `coc` or `county`. |
| `candidate_id` | string | Candidate geography ID. |
| `contained_share` | float | `intersection_area` divided by the configured denominator. |
| `intersection_area` | float | Overlap area in square meters after projection. |
| `candidate_area` | float | Candidate geometry area in square meters after projection. |
| `container_area` | float | Container geometry area in square meters after projection. |
| `method` | string | Geometry operation identifier, initially `planar_intersection`. |
| `container_vintage` | string or integer | Vintage from `containment_spec.container`, if supplied. |
| `candidate_vintage` | string or integer | Vintage from `containment_spec.candidate`, if supplied. |
| `definition_version` | string or null | Stable definition version when the geometry source has one. |

Rows must satisfy:

- `contained_share >= min_share`
- `intersection_area > 0`
- no duplicate `(container_type, container_id, candidate_type, candidate_id)`
  keys within one artifact

Writers may include geometry columns only in debug artifacts, not in the
canonical containment parquet.

## Naming

Containment outputs are downstream-consumable products and should be written
under the configured output root.

Parquet filename:

```text
containment__{container_token}x{candidate_token}__{output_id}.parquet
```

Sidecar manifest filename:

```text
containment__{container_token}x{candidate_token}__{output_id}.manifest.json
```

Tokens use the existing temporal shorthand where available:

| Geometry | Token |
|----------|-------|
| `coc` | `B{vintage}` |
| `county` | `C{vintage}` |
| `msa` | `M{definition_version}` when present, otherwise `M{vintage}` |

Examples:

```text
containment__Mcensus_msa_2023xB2025__cleveland_msa_coc_candidates.parquet
containment__B2025xC2023__la_coc_county_candidates.parquet
```

`output_id` is the output-level `id` if present. If an implementation must
derive an ID, it should use a lowercase slug from the target ID plus
`containment`, and it must be stable across runs.

## Provenance

Containment parquet files must be written with
`write_parquet_with_provenance`. Required provenance metadata:

- recipe name, recipe version, target ID, and output ID
- `containment_spec` as normalized JSON
- resolved input boundary artifact paths
- source geometry types, vintages, and definition versions
- projection used for area calculation
- row count and `min_share`

The sidecar manifest must include every boundary artifact consumed to build the
containment list and the final output path. Missing inputs should fail with an
actionable error that names the ingest or generation command needed to create
the missing geometry.

## Non-Goals

The v1 contract intentionally does not include:

- arbitrary geometry pairs beyond `msa -> coc` and `coc -> county`
- reverse-pair aliases
- measure allocation, rollups, or panel assembly
- polygon geometry output in the canonical parquet
- multi-hop containment such as `msa -> coc -> county` in one output
- automatic selector derivation from ranked panels or query expressions
- topology repair beyond the same boundary-cleaning behavior used by existing
  crosswalk builders
