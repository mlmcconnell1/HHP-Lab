# Recipe site-universe contract

Status: design contract for `coclab-ccwlh.1`. This document specifies the boundary between the existing outcome-free MSA/CoC overlap producer and a RecipeV2 site-year analysis. It does not choose a threshold, study period, outcome categories, or historical comparability claim. Those are research inputs and must be supplied explicitly by the study owner.

## Existing producer contract

The producer is `recipes/msa-coc-overlap-sites-2024.yaml`, implemented by `build_msa_coc_sites` in `hhplab/geographies/msa/coverage.py` and `persist_msa_coc_sites` in `hhplab/recipe/executor/persistence.py`. Its artifact schema is `msa_coc_sites/1`. For each requested threshold it writes:

- a `sites` table with one row for each MSA in the input universe, including rejected and missing candidates, and an `eligible` flag plus a reason;
- a `memberships` table with selected whole-CoC rows, including threshold, `msa_id`, `coc_id`, overlap/population fields and `site_eligible`;
- a JSON summary with producer parameters and counts.

The producer selects a CoC when its population containment share in an MSA is at least the threshold. It marks an MSA eligible only when selected CoCs together cover at least that threshold of the MSA population, after the producer's input and quality checks. This is a population-based geographic selection rule. It does not allocate PIT/HIC outcomes, establish historical boundary comparability, or designate a CoC type as an analytic category. `coc_type` is a provisional name-based description only.

The consumer MUST use the producer's flags and selected memberships. It MUST NOT independently recompute selection from outcome values or reinterpret population overlap as count allocation. The threshold supplied to a recipe MUST be present in both tables, and the selected rows MUST be the exact rows for that threshold. A missing threshold is an error; there is no producer-default fallback.

## Recipe reference and identity

RecipeV2 declares one typed site-universe reference. Its source gives an artifact ID equal to the producer target/output identity and uses exactly one locator form: either a portable bundle path, or explicit paths for the sites and memberships tables. A bundle locator resolves the producer artifact entries through `manifest.json.site_universes[artifact_id]`. A file locator names both tables; an incomplete pair is invalid. File paths must match the canonical names generated for that artifact ID. The resolver derives the paired summary JSON path with the same naming helper; the summary must declare `msa_coc_sites/1` and producer parameters sufficient to verify the selected threshold and vintages. Both Parquet files must carry matching `msa_coc_sites` provenance and parameters. Older producer outputs do not embed `artifact_id` in Parquet metadata; the consumer binds that identity through the canonical filenames, summary and content fingerprints. This belongs to RecipeV2 because it binds a frozen unit of analysis to results planning, sample resolution and run lifecycle. The existing V1 recipe remains the reusable producer and retains its current schema/planner/preflight behavior; its output is consumed by a separate V2 recipe.

Normative planned field nesting (only source IDs, file names, measure columns, threshold and years are placeholders; they must be supplied explicitly):

```yaml
site_universe:
  source:
    artifact_id: overlap_samples
    sites_path: ../outputs/msa-coc-overlap-sites-2024/msa_coc_sites__overlap_samples.parquet
    memberships_path: ../outputs/msa-coc-overlap-sites-2024/msa_coc_memberships__overlap_samples.parquet
  threshold: EXPLICIT_THRESHOLD_REQUIRED
  site_key: msa_id

panels:
  pit_coc:
    ref: {pipeline: pit_by_coc, output: panel}
    geo_key: coc_id
    time_key: year
  hic_coc:
    ref: {pipeline: hic_by_coc, output: panel}
    geo_key: coc_id
    time_key: year
  site_year:
    site_year:
      universe: site_universe
      boundary_evidence:
        path: ../evidence/coc-boundary-compatibility.json
      outcomes:
        pit:
          panel: pit_coc
          measures: [EXPLICIT_COUNT_COLUMN]
        hic:
          panel: hic_coc
          measures: [EXPLICIT_COUNT_COLUMN]
```

The bundle form replaces both file paths with `bundle_path: <bundle-root>` and retains `artifact_id`; no file-locator fields are permitted in that form. The file form requires `sites_path` and `memberships_path`, and forbids `bundle_path`. A bundle manifest maps each artifact ID to schema version and three manifest-relative paths (sites, memberships, summary), each included in the verified inventory; path traversal and missing inventory entries fail. The resolved plan records the manifest-resolved or recipe-resolved paths. V2 `panels` continues to bind raw/build panels to exact pipeline outputs and key columns. A `panels.<name>.site_year` entry is a typed derived binding whose `universe` names the top-level `site_universe`, whose `outcomes.pit` and `outcomes.hic` each name an existing source panel and a non-empty list of measure columns, and whose `boundary_evidence.path` names one evidence artifact. The PIT and HIC bindings may refer to the same source panel when that panel carries both explicitly selected measures. Analyses reference the derived panel by its `panels` key in the existing `input` field. This gives the V2 planner a statically checkable path from source pipeline outputs through site-year aggregation to analyses. The V1 build recipe/preflight validates and produces the reusable artifact; V2 `hhplab results check` resolves the site source and outcome panel references and reports static compatibility/readiness findings, and V2 `hhplab results run` executes the aggregation and analysis. Same-recipe producer references and forward references are outside this contract; a saved producer output or bundle is the supported reusable input.

`site_key` is currently the literal `msa_id`. It is an explicit contract field so that downstream panels and locks cannot silently switch their unit of analysis. A selected site is one eligible MSA at the chosen threshold. Its frozen membership is the sorted set of distinct `(msa_id, coc_id)` pairs where the matching site is eligible and the membership row has `site_eligible=true`.

The planner computes a stable `membership_identity` from canonical JSON containing the artifact schema, artifact ID, threshold, site-key definition and sorted selected pairs. It also records SHA-256 fingerprints for the source summary and both source tables, or the bundle manifest and the three resolved source files. The membership identity captures the analytic set; the input fingerprints detect changed bytes even when the selected pairs happen to remain the same. Both identities travel into the plan, run lock, manifest and replay bundle.

Validation requires finite threshold values in the producer's supported interval `(0.5, 1]`, exact threshold presence in both tables, matching artifact IDs/parameters/vintages, required columns and flags, unique site rows by `(threshold, msa_id)`, unique membership rows by `(threshold, msa_id, coc_id)`, and consistent MSA IDs between eligible sites and selected memberships. Duplicate source keys are hard errors even when duplicate rows contain identical values; conflicting duplicates do not become nulls or get arbitrarily resolved. Unknown schema versions, duplicate pairs, mismatched table identity, missing flags, fingerprint changes, or a selected site with no member rows fail with an actionable error. The resolver reports eligible and excluded site counts. An empty eligible selection is explicit in the plan/preflight and blocks aggregation; it does not silently produce an empty successful panel.

The producer's two-way threshold rule makes an individual CoC's population containment in an MSA greater than one half before it can be selected. The consumer nevertheless enforces a single selected MSA per CoC. Shared CoC membership is rejected because summing that CoC's outcome counts into multiple site series would duplicate counts. The consumer never divides, weights, or otherwise allocates a shared CoC's outcome count.

## Paths and outputs

For a recipe loaded from disk, local source paths are resolved relative to the recipe file's directory, independent of the process working directory. A bundle path is resolved the same way; paths inside it are manifest-relative and must remain contained under the bundle root. Absolute source paths are accepted only as explicit paths and are fingerprinted as inputs. An in-memory recipe must provide an explicit `source_base_dir` before relative input or output paths can be resolved; there is no implicit cwd fallback.

`asset_store_root` continues to locate package/source inputs. Generated artifacts, plans, result sidecars and result bundles use one resolved `output_root`. The shared recipe schema owns the optional recipe-level `output_root` override, inherited by RecipeV2; this keeps producer and consumer destinations under the same rule. Precedence for that root is explicit CLI, environment, recipe override, repository configuration, user configuration, then project default; an explicit CLI override must remain distinguishable from a value already resolved by configuration loading. Recipe-relative output-root values resolve against the recipe file directory. An in-memory recipe needs explicit `source_base_dir` for a relative override. The producer's existing recipe-local site outputs remain in its recipe namespace under the resolved root. Results retain their recipe/run namespaces and fail on collisions rather than replacing existing runs.

The plan and preflight JSON report resolved source files, fingerprints, selected and excluded site counts, membership identity, the resolved output destinations, and blocking findings. A portable replay bundle contains the recipe, lock/manifest lineage, site tables (or their exact immutable copies), and all panel/result inputs required for offline verification. Replay opens files from the bundle root only; it does not consult the original source paths or fetch source data.

## Site-year panel and completeness

The aggregation unit is exactly one selected MSA site by one requested year: `(msa_id, year)`. It is not one CoC-year row and it is not an MSA-year row whose CoC membership is recomputed independently each year. Every requested year reuses the same frozen CoC set and membership identity. The resulting panel declares `geo_key: msa_id` and `time_key: year`; these keys are unique and non-null.

The allowed PIT measure names are the current registered additive count columns `pit_total`, `pit_sheltered`, and `pit_unsheltered`. The allowed HIC measure names are `HIC_PANEL_MEASURE_COLUMNS` from `hhplab/schema/columns.py`, whose current entries are the registered additive bed, unit, and derived-total columns. The recipe must declare a non-empty explicit measure list for each source; this contract does not select a study's bed components or outcomes. A declared measure must belong to the correct source whitelist and its registry definition must declare additive `sum` aggregation. The result panel emits one column per declared measure, retaining its canonical measure name; it does not combine components into a new measure. If downstream analysis needs a combination (for example shelter-only beds), it uses the existing explicit derivation mechanism and carries that definition in the analysis plan.

Apply the known 2021 PIT source limitation per measure: `pit_total` and `pit_unsheltered` receive the `source_limited` status and null aggregate for 2021, even if all source rows are present. If one or more constituents are also missing, use `incomplete_source_limited`; diagnostics still report the observed and missing members. `pit_sheltered` is evaluated normally when its constituents are observed and compatible. HIC measures are also evaluated independently for 2021; the PIT limitation does not discard valid HIC observations. This rule is source-quality handling, not a selected study window, and it does not imply that every other year is comparable.

For each outcome measure, the site-year value is the unweighted sum of one resolved count per frozen member CoC for that MSA and year. Under v2, a member's count is produced only by its declared operation over its explicit source IDs; the resulting member counts are then summed across the frozen membership. No population weighting, partial allocation, imputation, or substitution of a nearby boundary is permitted by this contract. Source-year availability, geometry evidence, and count reconciliation are separately checked. For every measure column, emit deterministic companion fields `<measure>__expected_coc_count`, `<measure>__observed_coc_count`, `<measure>__missing_coc_ids`, `<measure>__unknown_boundary_coc_ids`, `<measure>__incompatible_boundary_coc_ids`, and `<measure>__status`. Expected count is the frozen membership size; observed count is the number of member CoCs with a complete, non-null, valid resolved count, regardless of boundary status; each ID list is sorted. Status is one of `complete`, `incomplete`, `source_limited`, or `incomplete_source_limited`. A value is complete only when observed count equals expected count, all required reconciliation evidence is present, boundary diagnostics permit the selected count basis, and no source limitation applies. The panel keys are `msa_id` and `year`.

A site-year outcome is complete only when every required member CoC has a complete valid count for that year under its declared operation and the required geometry or administrative evidence is supported. The aggregator MUST NOT sum only the available source constituents or frozen members and present that subtotal as a site value. If any constituent is absent, invalid, or unresolved, it records an explicit incomplete status and constituent-level reason/evidence; the outcome value remains null. Duplicate source keys are hard errors for the build, as specified above, rather than a null-valued incomplete observation. Keep the site-year key row where it can be represented without fabricating values. Analysis sample resolution then excludes rows that fail declared required-column completeness, with exclusion counts preserved. If the selected source cannot represent a missing site-year row at all, preflight must identify that absence and block rather than infer a zero.

The compatibility input is a versioned JSON evidence artifact with stable `evidence_id`. The existing `coc_boundary_compatibility/1` schema remains supported for saved locks and bundles. Its header binds it to the exact `site_universe` by producer `artifact_id`, threshold, computed `membership_identity`, and frozen CoC boundary vintage. Each record is keyed by `(source_panel, frozen_coc_id, year)` and repeats `frozen_boundary_vintage`; it also records `source_boundary_vintage` from that named PIT or HIC source panel. These identities must match the producer artifact, selected membership, and exact source-panel provenance. Evidence from another artifact, threshold, membership signature, CoC vintage, source panel or source vintage/year is rejected.

Version 1 records retain their existing statuses: `compatible`, `mapped`, `incompatible`, and `unknown`. A compatible record documents equivalent boundaries for the named source vintage/year and cites evidence. A mapped record resolves exactly one source ID and requires approval and evidence references for a documented one-to-one rename. The v1 validator continues to reject many-to-one mappings. Incompatible and unknown records do not resolve to source IDs; a missing record also resolves as unknown.

The `coc_boundary_compatibility/2` schema adds an explicit `source_coc_ids` list and `count_operation` to each record so the source rows consumed by an outcome are never inferred from a frozen ID. Operations are `single`, `preaggregated`, and `sum`. `single` consumes exactly one named source row. `sum` consumes each explicitly named source row once and requires a reviewed `reconciled` record with evidence, count evidence, an approval reference, and a documented administrative basis. `preaggregated` consumes one named source row that already represents a documented set of predecessor IDs; its count evidence must identify that original predecessor scope, and the record must also be reviewed as `reconciled`. Geometry assessment and evidence are recorded separately from count reconciliation, including any residual or unresolved comparison with the frozen boundary. This allows a documented administrative predecessor union to support a count outcome without claiming its historic polygons are geometrically equal to the frozen polygon.

For either schema, duplicate `(source_panel, frozen_coc_id, year)` records and source IDs assigned to different frozen members for the same panel/year are validation errors. V2 also reserves every ID in a preaggregated predecessor scope, so a predecessor represented by one upstream aggregate cannot also be counted through another member. In v2, source IDs must be non-empty and unique within a record, and every resolved record must declare an operation consistent with its source-ID cardinality. Unknown or incompatible records carry no source IDs or operation. A missing, duplicate, null, or invalid constituent makes the outcome incomplete and null; the aggregator never emits a partial predecessor sum. The artifact records its own identity/fingerprint in preflight, panel provenance, lock and replay bundle.

The policy distinguishes documented geometric compatibility, an approved one-to-one rename, an approved count reconciliation, known incompatibility, and unknown evidence. Only explicitly resolved records may contribute values. A persistent CoC ID alone is not evidence. Unknown or incompatible years stay visible and cannot be silently remapped. Evidence is evaluated per frozen member, panel and requested year. Source coverage is reported independently, so an available source year does not imply compatible geography or an approved count reconciliation.

Each outcome binding names an existing V2 source `PanelBinding` by key, its measure columns, and its declared CoC/year keys. At the source panel, duplicate `(coc_id, year)` keys are hard failures; missing source rows or null/invalid counts create an incomplete site-year outcome with null value and constituent-level reasons. For a v1 mapped member, aggregation reads the exact approved source ID. Under v2, it reads the exact declared source-ID set and operation. It sums one complete value per frozen member only after all source constituents are available and the selected compatibility or reconciliation evidence is supported. It does not fall back from one outcome source to another. Outcome sources may have different year coverage, which is reported by source and year.

## PIT observation ownership

The domain module `hhplab/sources/hud/pit/observation.py` owns interpretation of source Count Types declarations, nullable observation metadata, measure-specific eligibility predicates and complete-member metadata aggregation. Its only internal dependency is `hhplab.schema.columns`. Source parsers and aggregators share these semantics; the module preserves reported values and constituent declarations and does not infer observation from repeated numeric values. Unknown metadata remains unknown. Partial unsheltered counts are observed but do not qualify as complete unsheltered observations; total PIT also requires a complete observed unsheltered component. Sheltered eligibility is evaluated separately from the declaration.

Site-year aggregation consumes these predicates through an explicit application-to-domain dependency. It retains ownership of frozen membership, predecessor count operations, completeness and source-limited statuses, and constituent diagnostics. Geographic/count reconciliation does not establish that a count was observed in the requested year, and observation metadata does not establish historical geographic compatibility. Study geography, research periods and estimator samples remain owned by their existing planning and analysis contracts.

The reviewed amendment is `.elements/plans/pit-observation.intent.json`: it adds `intent:site-universe:pit_observation` revision 1 and revises the existing `intent:site-universe:site_year_aggregation` identity from revision 4 to revision 5. The prior predecessor-reconciliation intent and pin remain preserved; `architecture_layers.toml` selects the new pin receipt and enforces the exact responsibility and dependency declarations. Bead `coclab-93aqk` owns this amendment.

## Analysis and lifecycle

Recipe analyses consume the site-year panel using the existing RecipeV2 sample resolver and correlation executor. The correlation request declares its columns and Pearson or Spearman kind through the existing `CorrelationAnalysis`; this site-universe contract adds no estimand, study years, categories, controls, or research interpretation. A correlation uses the existing resolved sample and row-key contract. Completeness requirements are declared on the analysis sample so the sample resolver records exclusions; the correlation executor must not silently form pair-specific samples outside that resolved sample.

The immutable run identity includes recipe bytes, resolved panel and analysis plan, site artifact identity, threshold, membership identity, source fingerprints, boundary evidence identity, and outcome input fingerprints. The result lock/manifest records these components and the exact output bundle path. Offline replay verifies bundle inventory and all recorded identities before comparing regenerated outputs. Changed source bytes, changed membership, changed boundary evidence, or changed outcomes invalidate the prior run identity and require a new run; they do not rewrite a prior bundle.

## Ownership map

The owners below are implementation boundaries for the remaining child work. They extend the named existing modules and architecture anchors; they do not authorize parallel implementations of schema, resolution, aggregation, or correlation behavior.

| Responsibility | Existing or planned owner | Contract boundary |
|---|---|---|
| Outcome-free overlap calculation and `msa_coc_sites/1` producer | `hhplab/geographies/msa/coverage.py`; `hhplab/recipe/executor/persistence.py`; `hhplab/recipe/recipe_schema.py` | Preserve existing selection, producer flags, paired tables and provenance. No research threshold is chosen here. |
| Typed reference, outcome bindings and derived panel binding | `hhplab/recipe/schema_results.py` | Require source identity, one locator form, threshold, literal `site_key: msa_id`; bind source panels, columns, and compatibility evidence; preserve the V1 producer schema. |
| Artifact/path resolution, fingerprints, membership identity, dependencies and static preflight JSON | `hhplab/recipe/results_plan.py` | Resolve exact paired files against recipe origin or bundle manifest; validate schema/identity/threshold and build dependencies. Child `.2` owns the site artifact consumer. |
| Historical boundary evidence schema and policy | Existing CoC domain validation in `hhplab/geographies/coc/coc_validate.py`, called by V2 result planning/readiness | Validate statuses, one-to-one approved ID mappings and per-member/year evidence. Child `.3` owns the compatibility policy. |
| Actual paired-artifact reads and integrity checks | Existing `hhplab/recipe/executor/inputs.py` | Read exact producer tables and evidence; verify planned fingerprints before execution. |
| PIT source declarations, observation metadata and measure-specific eligibility | Existing `hhplab/sources/hud/pit/observation.py` | Preserve reported counts and constituent declarations; expose source-quality predicates using only the shared column schema. `coclab-93aqk` owns the intent amendment. |
| Whole-CoC site-year aggregation and duplicate/completeness checks | Existing `hhplab/recipe/executor/msa_coc_panel.py` | Consume frozen pairs and source panels; reject duplicate keys; emit one MSA-year row and never emit partial sums. Child `.4` owns the aggregation. |
| Result orchestration and analysis dispatch | Existing `hhplab/results/runner.py` and `hhplab/recipe/executor/analyses.py` | Materialize the derived panel before resolving samples and dispatch to current analysis services. |
| Correlation computation and sample row identity | `hhplab/analysis/correlation.py`; existing sample resolver | Reuse the current resolved-sample and correlation paths. Add no alternate correlation engine. |
| Lock, manifest and offline replay lineage | Existing `hhplab/results/runner.py`, `hhplab/results/bundle.py`, `hhplab/results/verify.py`, and result-lock schema | Preserve artifact/membership/evidence identities and bundle all replay inputs. Child `.5` owns the lifecycle change. |
| Output-root precedence and destinations | Shared `hhplab/recipe/recipe_schema.py` field (`intent:site-universe:recipe_output_root_schema`); existing `hhplab/storage/config.py`, recipe loader/CLI, and result plan/execution destinations | One resolved root for plan through bundle, separate from `asset_store_root`; relative paths use recipe origin, and in-memory recipes require explicit `source_base_dir`. Child `.6` owns it; precedence is CLI > environment > recipe > repository > user > default. |

No same-recipe forward-reference feature is assigned. If future evidence shows that a recipe must produce and consume its site artifact in one graph, that requires a versioned contract and explicit dependency-ordering design before implementation.

## Example workflow and feasibility

`recipes/site-pit-hic-correlation-demo.yaml` is a schema-checked template. Its 0.95 threshold, 2020–2021 interval, `pit_sheltered` and `hic_total_beds` columns, and pooled/per-year sample groups are synthetic demonstration settings only. The template expects the canonical `msa_coc_sites__overlap_samples.parquet` and `msa_coc_memberships__overlap_samples.parquet` files in `recipes/site-inputs/`, plus a separately reviewed boundary-evidence artifact at `recipes/site-inputs/coc-boundary-compatibility.json`. No site tables or approved historical-boundary evidence are checked in, so the checked-in recipe is not expected to pass data readiness until those inputs are supplied. Both `coc_outcomes` and `site_year` are included in the result bundle so offline replay has the source panel used to assemble the derived panel.

To produce a real candidate artifact, run the existing producer workflow and inspect its summary before copying the two canonical tables into the template's `site-inputs/` directory:

```sh
uv run hhplab build recipe-preflight --recipe recipes/msa-coc-overlap-sites-2024.yaml --json
uv run hhplab build recipe-plan --recipe recipes/msa-coc-overlap-sites-2024.yaml --json
uv run hhplab build recipe --recipe recipes/msa-coc-overlap-sites-2024.yaml --json
```

Then provide evidence reviewed for the exact selected membership and source-panel vintages, and check the consumer recipe before running it:

```sh
uv run hhplab results check --recipe recipes/site-pit-hic-correlation-demo.yaml --json --readiness
uv run hhplab results run --recipe recipes/site-pit-hic-correlation-demo.yaml --json --non-interactive
```

The repository's synthetic acceptance fixture is the reproducible executable example; it builds temporary outcome/site inputs and evidence instead of requiring local curated files:

```sh
uv run pytest -q tests/test_site_results_integration.py::test_site_year_bundle_replays_from_bundled_inputs_and_rejects_tampering
```

The source catalog currently declares PIT and HIC as annual January CoC sources beginning in 2007, with `last_year` unset. Registry metadata describes source coverage, and the HUD-linked metadata cites public 2007–2024 counts, but neither establishes which curated rows are present locally or validates CoC boundary equivalence for any historical year. No approved evidence window is established by this contract or the example. Treat source-year availability and boundary comparability as separate checks; readiness and the evidence artifact must support every requested site-year.

Threshold sensitivity must use separate result recipes or runs and separately named samples. The existing producer recipe records 0.90, 0.95 and 0.99 thresholds in the paired tables; for each run, set the matching explicit threshold while keeping the source artifact and outcomes fixed. Compare selected MSA/CoC membership identities and sample counts from the output bundles. The consumer validates that the exact selected rows for that threshold are present, so changing only the recipe threshold is valid only when the referenced artifact actually contains that threshold. Do not interpret threshold differences as causal or as a recommended study setting.

## Evidence and implementation gates

The current producer contract and runtime evidence are in `hhplab/geographies/msa/coverage.py`, `hhplab/recipe/executor/persistence.py`, `hhplab/recipe/executor/manifest.py`, `hhplab/recipe/recipe_schema.py`, `hhplab/recipe/schema_results.py`, `hhplab/recipe/results_plan.py`, `hhplab/recipe/preflight/checks.py`, `hhplab/recipe/executor/msa_coc_panel.py`, `hhplab/recipe/executor/analyses.py`, `hhplab/analysis/correlation.py`, `hhplab/results/runner.py`, `hhplab/results/bundle.py` and `hhplab/results/verify.py`. PIT/HIC measure whitelists are in `hhplab/schema/columns.py`; PIT quality cautions are in `hhplab/schema/measures.py`, and the existing 2021 PIT mask precedent is in `docs/results-recipe-spec-old_1.md`. Existing producer coverage is exercised in `tests/test_msa_coverage.py` and `tests/test_recipe.py`.

Before implementation, Elements must pin the exact anchors, dependencies and ownership table. After implementation, refresh/check observations, reconcile each changed responsibility to the pinned intent, and run the focused schema/planner/preflight/aggregation/lifecycle tests and architecture gates. A mismatch requires code repair or a versioned intent revision with preserved prior evidence; it is not resolved by waiving reconciliation.
