# Target spec: Publishing results bundles to a public findings site

Revision: 3 (adds the permission record format, §6.4)
Status: target specification for implementation
Depends on: `docs/results-recipe-spec_2.md` (rev 4), the Phase-1 results bundle
Applies to: `hhplab/recipe/schema_results.py`, `hhplab/results`, `hhplab/schema`, CLI `results`

---

## 0. How to read this document

- **Goal.** Add one step after `hhplab results run`. It turns a results bundle
  into a small, self-contained **publication package** that a static,
  content-only website can render. The audience is policy people,
  journalists, and interested lay readers, not reviewers.
- **The bundle is still the record.** A publication package is a derived,
  filtered subset of exactly one bundle. It never contains a number that
  can't be traced to that bundle. It never replaces the bundle, which is
  hosted somewhere else and linked from the page.
- **Negative results are first-class.** A finding of "no meaningful effect" or
  "inconclusive" goes through exactly the same recipe, bundle, and publish
  path as a positive one. No step in this spec may treat those results
  differently except in how they are worded.
- **Site-generator neutral.** The package is Markdown with YAML frontmatter,
  SVG, CSV and JSON. This spec does not choose a static site generator, and
  no part of the package may depend on one.
- **Where this spec and the results spec disagree,** the results spec wins for
  existing bundle semantics. This spec proposes an optional recipe field
  (§3), one new bundle file recording redistribution decisions (§6.4,
  `bundle_format_version` 1.1), and publication commands (§9).

## 1. Design principles

1. **Copy, don't link into.** The site owns a copy of the publication package.
   The site build never reads parquet files or bundle internals.
2. **Numbers come from the bundle, words come from the researcher.** Every
   number in a page is filled in from the bundle at publish time (§5). Prose
   never holds a hand-typed result.
3. **The verdict is pre-specified.** How a band reads in plain language is
   declared in the recipe, before the run, and is covered by the lock (§3).
   You can rewrite the narrative after seeing results. You can't relabel
   the verdict.
4. **Redistribution is enforced at publish time,** using the same rules as
   results spec §9.4, whatever the source bundle's `private` flag says (§6).
5. **Keep a public record.** Superseded and retracted findings keep a page
   and a lifecycle notice. Release history is preserved subject to the
   redistribution gate (§8).
6. **Publishing fails loudly.** A missing placeholder value, a withheld
   input, an ambiguous finding, or a failed integrity check stops the publish
   and writes nothing (§7).

## 2. Terms

| Term | Meaning |
| --- | --- |
| bundle | A results bundle as defined in results spec §9. |
| publication | One public page about one research question. It cites one or more findings from **one** bundle. |
| publication source | The researcher-maintained directory `publications/<slug>/` in this repo (§4). |
| publication package | What `results publish` writes into the site's content directory (§4.3). |
| public verdict | The plain-language reading of a finding evaluation, from a closed vocabulary (§3). |
| lifecycle status | Whether a publication is `active`, `superseded`, or `retracted` (§8). Independent of the verdict. |

## 3. Recipe change: `public_verdict` on finding bands

### 3.1 Schema

Add one optional field to `FindingBand` in `recipe/schema_results.py`:

```yaml
findings:
  - id: substitution
    claim: Each additional shelter bed is associated with one fewer unsheltered person.
    criteria_source:
      estimate: {analysis: subst_fd, term: d_beds}
    reference_values: [-1, 0]
    bands:
      - label: full_substitution
        public_verdict: supports_claim
        equivalent_to: {value: -1, margin: {param: delta_theta}}
      - label: no_substitution
        public_verdict: no_meaningful_effect
        equivalent_to: {value: 0, margin: {param: delta_theta}}
      - label: partial_substitution
        public_verdict: partly_supports_claim
        ci_above: {value: -1, margin: {param: delta_theta}}
        ci_below: {value: 0, margin: {param: delta_theta}}
    otherwise: inconclusive
```

`public_verdict` is a closed vocabulary:

| value | When to use it | Default page wording |
| --- | --- | --- |
| `supports_claim` | The evidence matches the claim as stated. | "The evidence supports this." |
| `partly_supports_claim` | There is an effect in the claimed direction, but smaller than the claim. | "The evidence partly supports this." |
| `no_meaningful_effect` | A band requiring the interval to lie within the declared equivalence margin around zero for the stated estimand. | "The range of effects supported by the data lies within the study's equivalence margin." |
| `contradicts_claim` | The evidence points the opposite way. | "The evidence points the other way." |

The wording column is the default. The site may restyle it but must not change
what it means.

### 3.2 Verdicts that do not come from bands

These come from evaluation status, not from a band. Researchers can't assign
them:

| source | public verdict | Default page wording |
| --- | --- | --- |
| `otherwise: inconclusive` | `inconclusive` | "The data can't settle this." Always shown with `compatible_with` (§3.4). |
| `ambiguous_rule` | none: publish refuses (§7) | |
| `not_evaluated` | none: publish refuses (§7) | |

This table governs the base evaluation. A variant that was not evaluated may
appear as an unavailable check with a public-safe reason. It has no verdict
or agreement flag. An ambiguous variant blocks publication of the cited
finding.

### 3.3 Robustness and the displayed verdict

The page headline shows the base evaluation's verdict, adjusted by the finding's
robustness status (results spec §8.4):

| robustness status | Headline | Additional display |
| --- | --- | --- |
| `robust` | base verdict | "Holds up across N alternative analyses." |
| `fragile` | `mixed` | Base verdict shown second, with the variants that disagree. |
| `uninformative` | base verdict | Caveat: "The checks were too noisy to confirm or contradict this." |
| `incomplete` | base verdict | Caveat: "Some checks could not be run," with the reasons. |
| no robustness section | base verdict | "No robustness checks were specified." |

`mixed` is a display verdict only. Use the bundle's robustness result and
the declaration's variant list together. With no declared variants, use the
no-checks wording even if the bundle reports `uninformative`. Count declared
checks and compare their evaluated band labels with the base label. Two bands
can have the same public verdict and still disagree on the band.

### 3.4 Inconclusive results

An inconclusive finding must show the reference values it is compatible with,
from `compatible_with`, in plain language. For example: "The data are
consistent both with no effect and with full one-for-one substitution." This
is the main thing that tells "we couldn't tell" apart from "there is no
effect", and it isn't optional. When the list is empty, say that the interval
contains none of the declared references. If the recipe declares no reference
values, state that explicitly. Never infer a reference or a verdict from an
empty list.

### 3.5 Validation and locking

- If a finding is cited by a publication, every one of its bands must have
  `public_verdict`. Publish fails (§7) if any is missing. Spec preflight
  doesn't require the field, so recipes that are never published don't
  change.
- `public_verdict` is part of the `findings` component hash (results spec
  §9.5). Adding or changing it on a locked recipe makes `lock_status`
  `modified: [findings]`. Omit an unset value from the canonical hash
  payload so existing recipes keep their lock identity. Add verdicts before
  locking.
- A band mapped to `no_meaningful_effect` requires an explicit
  `equivalent_to` condition around zero with a positive resolved margin.
  Publish checks that mechanically. The researcher reviews the wording
  and its fit to the estimand.

## 4. Publication source and package

### 4.1 Source layout (in this repo)

```text
publications/<slug>/
  publication.yaml       # metadata, the bundle pointer, figure choices (§4.2)
  narrative.md           # researcher prose with placeholders (§5)
```

`<slug>` is a permanent identifier matching
`^[a-z0-9]+(?:-[a-z0-9]+)*$`. The source directory name must match the slug.
The source and destination paths must resolve within their requested roots.
Reject traversal, absolute artifact names and symlink escapes.

### 4.2 `publication.yaml`

```yaml
publication_format_version: 1
slug: shelter-beds-and-unsheltered-counts
title: Do more shelter beds reduce street homelessness?
question: >
  When a metro adds shelter beds, does its unsheltered count go down by a
  similar amount?
status: active                     # active | superseded | retracted (§8)
superseded_by: null                # slug, required when status is superseded
retraction_note: null              # required when status is retracted
tags: [shelter, unsheltered, capacity]
geographies: "Census metro sites with frozen CoC membership"

bundle:
  recipe_id: shelter-capacity-substitution
  run_id: <run_id>
  manifest_sha256: <sha256>        # publish checks this against the bundle
  public_url: <url>                # where the full bundle is hosted
  doi: null                        # optional, e.g. a Zenodo DOI

findings: [substitution]           # finding ids cited, in display order
primary_finding: substitution      # drives the headline verdict

variant_descriptions:              # plain language, one line each; required
  lagged_beds: Uses last year's bed count instead of this year's.
  no_right_to_shelter: Leaves out metros with a legal right to shelter.
  coc_geography: Uses Continuum of Care areas instead of metro areas.

figures:                           # closed vocabulary, §6.2
  - kind: estimate_plot
    finding: substitution

change_note: null                  # required for a verdict or lifecycle change (§8)
```

The publication's editorial prose is outside the recipe lock. The cited
findings, claim, bands, result values and sample facts come from the verified
bundle. Select unique findings in display order and include `primary_finding`
in that list. Validate `recipe_id`, `run_id` and the exact manifest SHA-256
against the bundle. The public URL identifies the corresponding downloadable
bundle or archive. Phase A trusts the operator to attach that URL to the
verified bytes. A DOI is an optional citation.

### 4.3 Package layout (written to the site)

```text
<site_content_dir>/<slug>/
  index.md               # frontmatter (§4.4) + rendered narrative
  figures/<name>.svg
  data/<name>.csv        # aggregate tables behind each figure and table
  provenance.json        # §4.5
```

Render and validate a complete package in a staging directory on the
destination filesystem. Readers must observe a complete previous or new
release, including on republish. A plain rename over an existing nonempty
directory cannot provide this guarantee on every filesystem. Use a supported
atomic switch to the staged release, or fail before touching the current
release. Serialize competing writes to the same slug across the history read
and commit. A failed check or interrupted activation must leave the last
committed package and history readable. Keep staged files outside the static
site's content and asset discovery.

### 4.4 `index.md` frontmatter

```yaml
publication_format_version: 1
slug: ...
title: ...
question: ...
headline_verdict: no_meaningful_effect   # after §3.3 adjustment
base_verdict: no_meaningful_effect
robustness_status: robust
headline_text: The range of effects supported by the data lies within the study's equivalence margin.
status: active
superseded_by: null
tags: [...]
geographies: ...
published_at: 2026-11-24T20:00:00Z
lock_status: {status: matched}
provenance_tier: recipe_bundle           # §10
reproducibility: pass                    # pass | partial | fail | not_run, from verify
execution_status: complete
diagnostics_status: pass
findings_status: all_evaluated
distribution_status: full
findings:
  - id: substitution
    claim: ...
    base_verdict: ...
    compatible_with: [...]               # inconclusive only
    variants:
      - id: lagged_beds
        description: Uses last year's bed count instead of this year's.
        evaluation_status: no_substitution
        verdict: no_meaningful_effect
        agrees: true
limitations: [...]                       # copied from the recipe finding
history:                                 # §8
  - {published_at: ..., run_id: ..., manifest_sha256: ..., headline_verdict: ..., lock_status: ..., status: active, change_note: ...}
```

The body of `index.md` is the narrative after placeholder substitution. It is
followed by a generated "For researchers" section. That section holds the
claim, the band definitions with resolved parameters, the coefficient and fit
tables, the lock status, and the bundle link and hash. Include the four run
statuses from results spec §11.3 and applicable public data caveats. Show a
diagnostics failure or incomplete robustness next to the headline, where it
cannot be missed. Use the bundle's lock status, including modified components
when present. A match establishes order in the recipe ledger. An independently
timestamped registration requires separate evidence. The site may collapse
the research details and must render them on active and superseded pages.

### 4.5 `provenance.json`

Record the verified public bundle URL and manifest SHA-256,
`bundle_format_version`, `recipe_id`, `run_id`, `created_at`, `spec_hash`,
`component_hashes`, `lock_status` and the four status fields. Include only
input identities already approved for the public bundle. Publish also records

- `publish_tool_version`
- `published_at`
- repository-relative paths and SHA-256 digests of the two publication source
  files
- for every placeholder, its resolved address and value (§5.4)
- for every figure and CSV, its source table, row filter and redistribution
  decision
- SHA-256 digests for the rendered page and its assets

Record source addresses for values generated by templates as well as for
placeholders, including the underlying rows or criteria for plots and tables.
Include the reproducibility status with public-safe reasons. Exclude private
input paths, row keys, log messages and withheld annotation details. Use the
exact public bundle identity that a reader can use to check these values.

## 5. Narrative placeholders

### 5.1 Syntax

`{{ <field> : <address> [| <format>] }}`

The address is `<analysis>[@<variant>].<term>` for estimate fields,
`<analysis>[@<variant>]` for fit fields, and `<finding>[@<variant>]` for
finding fields. `@base` is the default. Resolve a single exact result key
and a single exact row. Refuse ambiguous matches or references outside
the cited findings and their declared result dependencies.

| field | source table |
| --- | --- |
| `estimate`, `ci_lo`, `ci_hi`, `se`, `p` | regression_estimates |
| `n`, `n_clusters`, `r2`, `r2_within` | regression_fit |
| `r` | correlations (address `<analysis>[@<variant>].<col_a>~<col_b>`, in stored order) |
| `verdict`, `verdict_text`, `label`, `compatible_with` | findings.json |
| `margin` | resolved parameter referenced by a cited band (address `param.<name>`) |

### 5.2 Formats (closed)

Numeric formats are `round:<k>` (default `round:2`), `int` for integral
values, `pct` (×100 with %, for proportions), `pct_points` (×100 with
"percentage points" for differences on the proportion scale), `ci`, `abs`,
and `signed`. `ci` is valid on `estimate` and renders the stored `ci_lo`
and `ci_hi` at the same precision. A percentage conversion requires the
appropriate units. Text and list values use the field's own renderer.

### 5.3 Rules

- An address that doesn't resolve, or a null value, is a publish error. The
  error names the placeholder and the reason recorded in the bundle (for
  example `p_status: not_evaluated`).
- A placeholder that resolves to a withheld artifact is a publish error.
- Escape substituted values for their output context and reject malformed
  placeholders. Only passive content is allowed in the package. The site
  renderer must disable raw HTML in narrative Markdown and allow only
  ordinary web links and contained relative links. SVGs must contain local,
  static markup without scripts or external references.
- **Number lint.** Scan the source narrative outside placeholders for
  numeric literals, including counts, decimals and percentages. Each hit
  is a warning. `--strict` makes it an error. Allow explicitly recognized
  calendar years, citation labels, and clearly marked illustrative examples.
  The review still checks manually written result claims and sample facts.

### 5.4 Recording

Every resolved placeholder goes in `provenance.json` as
`{placeholder, field, address, format, raw_value, rendered}`.

## 6. Redistribution, figures, and tables

### 6.1 Redistribution gate

Publish applies results spec §9.4 to everything it emits, using the lineage in
the source bundle:

- Emit aggregate results only when the contributing sources explicitly
  permit aggregate redistribution. Apply the same rule to headline and
  variant verdicts, limitations, text substitutions, data, figure metadata
  and the provenance sidecar. Use the bundle's annotation redistribution
  rules for caveats, including generic notices where details were withheld.
  Editorial prose also requires review for restricted disclosures.
- Row-level artifacts are never emitted: panel rows, scatters of individual
  places, per-place tables, and min/max/quantiles over groups of fewer than
  10 rows.
- **Portable permission evidence.** Presence in a bundle cannot prove
  permission to republish. In particular, the current writer uses source
  terms while packaging and can include unknown aggregates via an override.
  Publish decides from the bundle's permission record (§6.4) and nothing
  else. Treat missing lineage or permission evidence as unknown.
- **Private computation is Phase B.** `--public-bundle` must supply a
  verified, non-private counterpart with the same recipe, resolved spec,
  component and panel identities. Those identities establish matching
  inputs. Check the actual published rows, findings and caveats against
  the public counterpart as well. Every number needs a matching public
  value or a deterministic derivation from eligible public inputs. When
  replay data are withheld, provide an authorized aggregate companion in
  the public bundle or refuse the figure. Public links and provenance
  identify the verified public counterpart by its own manifest digest.
- Each emitted artifact's decision and source lineage go in `provenance.json`.

### 6.2 Figures (closed vocabulary)

| kind | Phase | Inputs | Notes |
| --- | --- | --- | --- |
| `estimate_plot` | A | regression_estimates, findings.json, resolved parameters | Point and CI for the base and each evaluated, comparable variant. Show unavailable checks in the table. Available by default for findings with an estimate criterion. Applies the redistribution gate. |
| `binned_scatter` | B | a derived/panel column pair from the bundle | Bin means with CIs. Declare the sample, observational unit, deterministic merge order and disclosure threshold before enabling this kind. |
| `group_trend` | B | panel column, group column, year | Group-year means. Declare the sample and suppression rules before enabling this kind. |
| `variant_table` | A | findings.json | Not a figure. A rendered table of each check, its plain description, its verdict, and whether it agrees. |

Use the stored confidence level and measure units. Draw reference lines and
interval boundaries that can be represented on the coefficient axis. If a band
also depends on a fit statistic or another criterion, label any shaded region
as the interval condition and describe the full rule in the research details.
Keep effect scales separate when variants use different units or estimands.
Phase B needs a disclosure rule for distinct contributing units and overlapping
releases. Ten rows alone cannot establish the safety of a group or bin.

Figures are SVG. They are rendered with the `analysis` extra's plotting stack
and come from the same CSV written to `data/`. Styling is minimal and neutral,
so the site's CSS can restyle text. Each SVG has a `<title>` and `<desc>` with
alt text generated from the data, for example "Estimate −0.04, 95% CI −0.21
to 0.13. The declared equivalence margin is ±0.25."

### 6.3 Tables

The "For researchers" section renders coefficients, fit, and variant
comparison from the bundle's result tables. They are filtered to the cited
findings' analyses and use the same redistribution gate.

### 6.4 Permission record (`permissions.json`, bundle format 1.1)

**Why a new file.** The bundle writer already has everything the gate
needs: per-source terms and dated evidence (`SourceCoverageSpec` /
`CovariateSourceSpec` `.redistribution` and `.redistribution_evidence`, or
registry metadata), each artifact's `ArtifactLineage`, and the override flags.
It uses them in `distribution_plan` and then saves only `withheld.json`.
From a bundle alone, a reader can't tell an artifact included because its
sources are allowed from one included because of `--allow-unknown-aggregates`,
and can't see the evidence behind a term. The fix is to persist the record the
writer already computes. It doesn't need a new policy engine.

**Writer change (results spec §9.2 addition).** Every bundle written at
`bundle_format_version` 1.1 contains `permissions.json`, listed in
`manifest.json` like any other file, so integrity verification covers it. It
holds only paths, source ids, statuses and evidence metadata, never source
values, so its own scope is `sample` and it is always included, including in
non-private bundles.

```json
{
  "schema_version": "permissions/1",
  "policy": {
    "small_group_threshold": 10,
    "overrides": {"private": false, "allow_unknown_aggregates": false}
  },
  "sources": {
    "hud/pit": {
      "row_level": "allowed",
      "aggregate": "allowed",
      "terms_origin": "catalog",
      "evidence": [
        {"url": "https://www.hud.gov/sites/documents/webpubstandards.pdf",
         "reviewed_on": "2026-10-20",
         "scope": "HUD-published PIT/HIC statistical workbooks ..."}
      ]
    },
    "zori": {
      "row_level": "unknown",
      "aggregate": "unknown",
      "terms_origin": "catalog",
      "evidence": []
    },
    "unknown-column:foo": {
      "row_level": "unknown",
      "aggregate": "unknown",
      "terms_origin": "default_unknown",
      "evidence": []
    }
  },
  "artifacts": [
    {
      "path": "results/regression_estimates.parquet",
      "scope": "aggregate",
      "sources": ["hud/hic", "hud/pit"],
      "combined_terms": {"row_level": "allowed", "aggregate": "allowed"},
      "strict_disposition": "included",
      "written_disposition": "included",
      "companions": ["results/results.json", "tables/coefficients.csv"]
    }
  ]
}
```

Field rules:

- `sources` lists every source id that appears in any artifact's lineage,
  including the synthetic `unknown-column:<name>` and `unknown-artifact` ids
  that `union_lineage` / `distribution_plan` produce. `terms_origin` is
  `registry` | `catalog` | `default_unknown`, matching which branch of
  `declared_source_terms` supplied the terms. `evidence` copies
  `RedistributionEvidence` records as they stood at run time.
- `artifacts` has one entry per policy artifact passed to
  `distribution_plan`, including the report and `results/results.json`, in
  path order. Companions are listed under their parent and inherit its
  decision, as they do today.
- `strict_disposition` is `artifact_disposition(combined_terms, scope=...,
  private=False, allow_unknown_aggregates=False)`. That is the decision with
  no overrides. `written_disposition` is the decision the writer actually
  applied. Both come from the existing function, called twice. No new
  decision logic is allowed.
- Result-table rows don't get their own entries. Phase A treats each table as
  one unit with whole-table lineage, as `_result_table_lineages` already
  does. Finer-grained (per-analysis) lineage is a later refinement. It only
  makes more things publishable and never fewer.

**Publish rule.** An emitted value, row, figure, or caveat is publishable
only if every artifact it is read from has all of the following in
`permissions.json`:

1. `scope: aggregate` (or `sample` for membership counts),
2. `strict_disposition: included`, and
3. every contributing source with `aggregate: allowed`, `terms_origin` other
   than `default_unknown`, and at least one `evidence` record.

`written_disposition` and `policy.overrides` are ignored by the gate. A
bundle built with `--allow-unknown-aggregates` can still be published from,
but only from artifacts whose strict disposition is `included`. A
`private: true` bundle can be read only under Phase B (§6.1). Publish copies
the source terms and evidence used into `provenance.json`, so a page shows
which review its permission rests on and when it was done.

**Terms change after a run.** A bundle's record is frozen at run time. If a
source is cleared later (ZORI, for example), old bundles stay `unknown`.
Re-run the locked recipe to get a bundle that can publish it. The lock
stays `matched`, because source terms are not part of the spec hash. If
permission is withdrawn later, use retraction (§8). Publish does not re-check
old bundles against the current catalog. Doing so would make the same
bundle's eligibility change silently over time.

**Existing (format 1.0) bundles.** Publish refuses a bundle without
`permissions.json`, with the message "bundle predates permission records;
re-run the recipe with hhplab ≥ <version>". Re-running is the supported
path. Phase-1 acceptance is still open, so very few 1.0 bundles exist that
anyone needs to keep, and every run's inputs are local.

There is no retrofit command in Phase A. If one is ever needed (Phase C, for
example for a bundle whose inputs are gone), it is
`hhplab results attest-permissions <bundle>`. It writes a sidecar
`<bundle_dir>.permissions.json` *outside* the immutable bundle and binds it
to the bundle's `manifest_sha256`. Lineage is recomputed from
`recipe/resolved.json` using the current lineage code, and source terms come
from the current catalog. The sidecar records `attested_after_run: true`, the
tool version and the attestation date, and publish shows "permissions
attested after the run" in the research details. It never modifies the
bundle.

## 7. Publish preconditions

For an active or superseded release, publish refuses, writes nothing, and
exits nonzero if any of these hold:

1. Bundle integrity check fails (results spec §9.6, status 1).
2. The declared recipe ID, run ID or manifest digest differs from the bundle.
3. The manifest reports run-scope failure, or any analysis result required
   for the cited base findings is missing or incomplete. A partial run caused
   by unrelated analyses can still publish. Declared unavailable checks are
   displayed under §3.2–3.3.
4. A cited finding's base evaluation is missing, `ambiguous_rule` or
   `not_evaluated`, or a cited variant evaluation is `ambiguous_rule`.
5. A cited finding has a band without `public_verdict` or an invalid
   `no_meaningful_effect` mapping (§3.5).
6. Any placeholder fails (§5.3), or `--strict` is set and the lint fires.
7. A robustness variant cited in findings.json has no entry in
   `variant_descriptions`.
8. The requested lifecycle status or transition violates §8.
9. The headline verdict or lifecycle status changes and `change_note` is
   empty.
10. Redistribution gate violation (§6.1).

Always display these statuses on active and superseded pages.

- `lock_status`. Show unlocked and modified runs as such.
- Reproducibility. Verify the cited public bundle unless `--skip-reproduce`
  is given. Integrity and permission checks always run. `partial` and
  `not_run` publish with their reason or unavailable scope. A reproducibility
  `fail` refuses unless `--allow-reproducibility-failure` is given, and then
  the page shows the failure.

For a retraction, validate the existing committed package, the requested
transition, both notes and permission to disclose the tombstone fields.
The checks on publishing numerical results apply only to releases that
carry those results.

## 8. Lifecycle and history

- **History** is carried forward from the current committed package at
  `--dest`. A successful publish appends one entry with `published_at`,
  `run_id`, `manifest_sha256`, `headline_verdict`, `lock_status`,
  `status` and `change_note`. Validate the existing package before using
  it as history. Earlier entries retain their order and values, with only
  a public-safe projection if a retraction withdraws permission.
- **A verdict or lifecycle change** needs a `change_note` (§7, item 9).
  The site shows the most recent change note near the headline when it is
  under 12 months old.
- **Superseded:** the page keeps its last content and adds a banner linking to
  `superseded_by`. The replacement must be a distinct committed slug.
  Reject cycles and new links to a retracted replacement. If a replacement
  is later retracted, keep the historical link and show its current status.
  The index derives reverse links from the committed packages.
- **Retracted:** publish a tombstone for an existing slug, with its question,
  `retraction_note` and only the history still safe to disclose. Strip
  previous figures, CSVs, detailed provenance and withdrawn bundle links
  from the public package. Where retention is authorized, keep the prior
  release outside the site's public tree. The site operator must withdraw
  copies still served by the site or its asset host. Retraction uses the
  existing release identity and can proceed when the old bundle is
  unavailable or its permission has been withdrawn. A retracted slug is
  terminal. Publish a correction under a new slug.
- Slugs are permanent (§4.1). Deleting a published package is outside this
  tool. `publish` never deletes another slug's directory.

## 9. CLI

```text
hhplab results publish  --publication publications/<slug>/publication.yaml
                        [--bundle <bundle_dir>]
                        --dest <site_content_dir>
                        [--public-bundle <bundle_dir>]
                        [--strict] [--skip-reproduce]
                        [--allow-reproducibility-failure]
                        [--dry-run] [--json]

hhplab results publish-check --publication <path> --dest <site_content_dir>
                             [--bundle <bundle_dir>] [--public-bundle <bundle_dir>]
                             [--strict] [--skip-reproduce]
                             [--allow-reproducibility-failure] [--json]

hhplab results publish-index --dest <site_content_dir> [--json]
                        # writes <site_content_dir>/findings-index.json:
                        # slug, title, question, headline_verdict, status,
                        # tags, published_at, for every package found
```

`--bundle` is required except for an existing release's retraction.
`publish-check` and `--dry-run` use the same checks as publish, including the
current destination history and supersession graph. Both report planned
relative paths and the §7 outcome without modifying the destination.
`publish-index` validates committed packages, sorts entries
deterministically and atomically replaces the index. A retracted entry may
have a null headline verdict. All commands return
machine-readable status, findings and warnings under `--json`. An invalid
publication, destination or package returns a nonzero exit status.

## 10. Provenance tiers and legacy findings

Most current headline findings came from `results/workflows/` and
`devdocs/*_findings.md`, not from recipe bundles. To publish some of them
before migration:

- `provenance_tier: recipe_bundle`: everything above applies.
- `provenance_tier: legacy_workflow`: the source is a `CovariateFinding`
  sidecar (`results/findings.py`) plus its devdoc. `publication.yaml` replaces
  the `bundle` block with `legacy: {workflow_id, sidecar_path, sidecar_sha256}`.
  It also sets `public_verdict` by hand on the publication, because there are
  no bands. Placeholders resolve only against `key_terms` in the sidecar.
  Always `reproducibility: not_run`. The site must
  show the tier as "Exploratory analysis: not pre-specified".
  The sidecar and devdoc need hashed, publicly eligible source evidence
  before this tier can publish. A nonsignificant sidecar term alone cannot
  support an equivalence verdict.
- A legacy publication can be upgraded to `recipe_bundle` under the same slug
  once the workflow is migrated (`coclab-pjmfl`). The upgrade is a history
  entry, with a required `change_note` if the verdict changes.

Legacy support is Phase C, and can be dropped if the headline findings are
migrated first.

## 11. Page template guidance (for the site, non-normative)

Recommended order for each publication page:

1. The question
2. Verdict badge and `headline_text`, plus the lifecycle banner if any
3. The narrative's opening paragraph: the answer in plain words
4. The primary `estimate_plot` when the finding has a comparable estimate
5. "How sure are we?": robustness summary from §3.3, the `variant_table`, and
   "consistent with" for inconclusive results
6. "What this doesn't mean": a narrative section the researcher writes. The
   template should expect it.
7. Limitations from the recipe
8. "For researchers": collapsed by default
9. Footer: recipe lock status, reproducibility status, bundle link and DOI,
   published date, history

The site index should be filterable by verdict, so that "ruled out" and
"inconclusive" results get the same visibility as positive ones.

## 12. Placement

| Concern | Location | Layer |
| --- | --- | --- |
| `public_verdict` field | `recipe/schema_results.py` (`FindingBand`) | application |
| Publication source and package schemas | new `schema/publication.py` | contracts |
| Verdict mapping, §3.3 adjustment | new `results/verdicts.py` | application |
| Placeholder resolution and lint | new `results/placeholders.py` | application |
| Figures | new `results/figures.py` (optional dependency on the `analysis` extra) | application |
| Publish orchestration, gates, atomic write | new `results/publish.py` | application |
| Permission record model and validation | `schema/redistribution.py` (`PermissionRecord`) | contracts |
| Writing `permissions.json` | `results/bundle.py` (`BundleWriter`), from the existing `distribution_plan` inputs | application |
| CLI | `cli/build_cmds/results.py` / `cli/results.py` | interface |

Record the module ownership in Elements intent and update
`architecture_layers.toml` and the architecture tests. Use the existing
dependency direction. `results/publish.py` reads through `BundleContents`
(`results/bundle.py`). Extend that reader to expose the verified permission,
measure and annotation records publication needs.

## 13. Phasing

- **Phase A (minimum publishable):** §3 schema field and validation; §4 source
  and package schemas; §5 placeholders and lint; §6.1 gate; `estimate_plot` and
  `variant_table`; §7 preconditions; §8 history and lifecycle; `publish`,
  `publish-check`, `publish-index`. Include `permissions.json` in the bundle
  writer (format 1.1, §6.4), `BundleContents` access to it, and verified
  reading of public caveats. Do this writer change first, so every bundle
  produced from then on can be published.
- **Phase B:** `binned_scatter`, `group_trend`, private-source plus
  `--public-bundle` matching (§6.1).
- **Phase C:** legacy tier (§10); `attest-permissions` sidecar only if a
  bundle that can't be re-run needs to be published (§6.4).

Synthetic fixtures can validate the publishing mechanics. Public Phase A
acceptance depends on Phase-1 results acceptance (`coclab-ff6fp.25`) and one
real bundle with nonempty `findings`, resolved parameters and portable
permission evidence. The shelter worked example currently has
`findings: []`. Its acceptance run needs an approved finding declaration
and the research choices tracked by `coclab-ff6fp.24`.

## 14. Non-goals

- Choosing, configuring, or deploying a static site generator.
- Uploading bundles to Zenodo or a GitHub release. Publish only checks the
  hashes it is given.
- Any server-side query or API. The live query interface is a separate
  project. It should consume the same packages and `provenance.json`.
- Intake of user-requested questions. Requests become new recipes and
  publications through the normal workflow.
- Free-text or hand-tuned verdicts for recipe-tier findings.

## 15. Acceptance criteria

Each is a test using synthetic bundles (built with the existing bundle test
fixtures) unless noted.

1. Three bundles from one synthetic recipe, with data producing
   `supports_claim`, `no_meaningful_effect`, and `inconclusive`, all publish
   with the same command and the same `publication.yaml` except
   `bundle`/`change_note`. All three have the same output structure and
   required context.
2. The inconclusive package renders `compatible_with` in frontmatter and in
   the generated "How sure are we?" text.
3. A finding with `robustness: fragile` gets `headline_verdict: mixed` and lists
   the disagreeing variants.
4. Each §7 precondition has a test that triggers it and asserts nothing was
   written to `--dest`.
5. A narrative containing a hand-typed `0.43` warns, and fails under
   `--strict`.
6. A bundle whose only outcome source is `unknown` at the aggregate level is
   refused, even if built with `--allow-unknown-aggregates`.
7. Republishing with a different verdict and no `change_note` fails. With a
   note, it appends exactly one history entry and keeps the previous entries
   in order with their original values.
8. Every reported result number in `index.md` or a figure has a provenance
   address that resolves against the cited public bundle. A test
   re-resolves the placeholders and automatically generated report values.
9. `estimate_plot` SVG and its CSV agree, and the SVG `<desc>` contains the
   estimate, the CI, and the verdict zone.
10. Adding `public_verdict` to a locked recipe yields
    a modified lock status with only `findings` listed. A recipe with the
    field absent retains its previous lock hash.
11. Real data, not required to pass any particular verdict: one publication
    from the shelter-capacity bundle renders end to end into a scratch site
    directory.
12. A missing permission record blocks an otherwise readable aggregate.
    Restricted or unknown data stay outside Markdown, SVG, CSV and
    provenance, including error text and redacted annotation details.
13. A complete base with an unavailable variant can publish. Its page
    preserves the reason and marks the variant unavailable. An ambiguous
    variant stops publication.
14. A retraction with withdrawn permissions publishes from the current
    release record and removes the old numerical assets and bundle link
    from the public package. A failed replace leaves the previous version
    readable. Concurrent publishes preserve a single ordered history.
15. `publish-check`, `publish --dry-run` and `publish` agree on checks
    for the same destination state. Supersession cycles and path escapes
    are refused.
16. A bundle written with `--allow-unknown-aggregates` has an artifact with
    `written_disposition: included` and `strict_disposition: withheld`.
    Publish refuses any value drawn from it, and publishes values from
    artifacts whose strict disposition is `included`.
17. `permissions.json` is in the manifest. Editing one status in it makes
    integrity verification fail, and publish refuses.
18. A format-1.0 bundle (no `permissions.json`) is refused with the re-run
    message. The same recipe re-run at format 1.1 publishes, and its
    `lock_status` is unchanged.
19. For every artifact, `strict_disposition` and `written_disposition` in
    `permissions.json` equal `artifact_disposition` called with the
    recorded terms and the recorded overrides. A property test covers
    random term combinations, scopes and override flags.
20. An allowed source with an empty `evidence` list, or one with
    `terms_origin: default_unknown`, blocks publication of every value that
    depends on it.
