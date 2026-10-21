# Target spec: Recipes that produce standalone results packages

Revision: 4 (supersedes revisions 1–3)
Status: target specification for implementation
Working example: shelter capacity and sheltered/unsheltered homelessness
Applies to: `hhplab/recipe`, `hhplab/analysis`, `hhplab/results`, `hhplab/storage`, CLI `build`/`results`

---

## 0. How to read this document

- **Goal.** Extend the recipe contract so that one YAML file declares a panel
  build, data masks, derived variables, analysis samples, analyses, robustness
  variants, diagnostics, and the claims the analyses test. Running it yields a
  self-contained **results bundle** that a reviewer can read without HHP-Lab
  and re-run at the analysis stage without reacquiring upstream data.
- **The working example is the acceptance test.** If the implementation can't
  express, validate, run, and package §10 exactly as written, it is incomplete.
  If the example is wrong or unworkable, change the spec and give the reason in
  the PR. Don't diverge from it silently.
- **Column names are illustrative.** Map them to the canonical names in
  `schema/columns.py` and the measure registries. Don't rename existing columns.
- **Existing build sections stay as they are.** This spec adds sections; it
  doesn't redesign the current build model.
- **Where this spec states a statistical convention, that convention is the
  contract.** If the existing code does something different, change the code
  or change the spec explicitly. Don't leave the difference in place.

### 0.1 Changes from revision 1

| Rev-1 problem | Resolution |
| --- | --- |
| A calendar difference meant different things depending on the sample step | Time ops declare an explicit `years` interval. Variants switch columns through `substitute`. §4 |
| Undefined baseline/lag sourcing; 2021 handled by sample exclusion | `masks` invalidate cells before derivation. Derivation runs on the full built panel. §4.1, §4.3 |
| `ci_contains` turned uncertainty into a conclusion | Equivalence margins, a separate `compatible_with` report, and exclusive bands. §8 |
| `same_band` could call several inconclusive results robust | Agreement is reported separately from evidential support, with an `uninformative` status. §8.4 |
| Preflight required data-dependent facts | Split into spec preflight (static) and analysis readiness (data-dependent). §11 |
| Analysis-engine capabilities assumed | Capability list and statistical conventions are now explicit. §6 |
| `require` alone defined the sample; estimator could drop rows | Sample contract owns every exclusion; shared samples via `sample_groups` in Phase 1. §5 |
| Verification contract vague | Execution mapping, result schemas, atol+rtol, three verify statuses, manifest self-exclusion. §9.6 |
| Redistribution didn't propagate | Two-level terms per source, with lineage propagation to derived columns and artifacts. §9.4 |
| Ledger was manifest discovery; lock covered only the findings | New append-only run ledger with start/terminal events; lock covers the full resolved spec. §9.5 |
| `override.inference` not allowed; no `outputs` registry; placeholders looked valid; additivity check special-cased; bootstrap status inconsistent | Override semantics defined; panel reference contract; `{unresolved: ...}` markers; closed `diagnostics` section; unsupported capabilities rejected at spec preflight. §3, §7, §7.1, §8.1, §12 |

### 0.2 Changes from revision 2

| Rev-2 problem | Resolution |
| --- | --- |
| Findings didn't follow variant substitutions | All out-of-analysis term references resolve through the variant's substitution map. §7 |
| Sample-group identity per variant undefined | Resolved key is (group, variant, panel identity); membership-affecting fields are listed; subset-targeting rule. §5.3 |
| Readiness errors stopped runs, contradicting partial bundles | Failure scopes (run, panel, analysis); failure bundles; four-part status. §11.3 |
| Running jobs looked abandoned | Lease files with heartbeat; abandonment rule; event schemas; concurrency and torn-write policy. §9.5 |
| Open statistical conventions | Intercept, `design_rank` vs `k_dof`, weights limited to regression, `by` columns required, atol+rtol for LSDV. §6 |
| Grid-based band exclusivity | Exact interval-constraint check, which is an error, not a warning; robustness precedence; `ambiguous_rule` never decisive. §8.4 |
| Lag erased by a missing current value | Lag needs only the earlier cell. §4.3 |
| Verify partial/exit-code mismatch | Partial reasons and exit-code precedence. §9.6 |
| Causal wording for θ | θ is reported as a within-place association; bias direction is no longer asserted. §10 |

### 0.3 Changes from revision 3

| Rev-3 ambiguity | Resolution |
| --- | --- |
| Inference-only variants could lose required cluster columns | They inherit the exact base membership and base required-column set, including a cluster column removed by an inference override. §5.3 |
| Failed base and variant analyses had unclear dependencies | Findings and diagnostics resolve dependencies independently for each result key; a base failure does not automatically invalidate a variant that ran, and vice versa. §8.4, §11.3 |
| Missing, masked, nonfinite, and invalid-weight precedence overlapped | Mask and missing reasons take precedence; invalid/nonfinite weights receive `invalid_weight` at the weight's position in required-column order. §5.2 |
| Undefined arithmetic and statistics could emit NaN or infinity | Domain failures produce null plus an explicit reason; nonfinite diagnostics are `not_evaluated`. JSON never contains NaN or infinity. §4.2, §6.3, §7.1, §9.3 |
| Structural fixed-effect dummy redundancy was confused with unidentified slopes | Dummy/intercept redundancy is expected and reported; fail only when a requested slope is not identified after absorbing fixed effects. §5.4, §6.3 |
| Ledger I/O failure had no feasible terminal-event rule | Failure to record the start event prevents estimation; later I/O failures fail the run and trigger best-effort terminal logging and failure-bundle writing. §9.5, §11.3 |
| Aggregate diagnostics status omitted `not_applicable`; acceptance used an obsolete status name | Aggregate status precedence and acceptance wording now use the four manifest field names. §7.1, §11.3, §16 |
| Synthetic and real-data outcomes were conflated | Synthetic fixtures test arithmetic, status transitions, and failure reporting; real-data diagnostic outcomes are reported as observed and are not required to pass. §16 |

## 1. Design principles

1. **Declarative, with closed vocabularies.** No general expression language,
   no embedded Python, no templating. Logic that doesn't fit stays in
   `results/workflows/`.
2. **Statistics live in `hhplab.analysis`.** The recipe layer validates,
   resolves, dispatches, and packages. It doesn't implement estimators.
3. **The sample contract owns every row exclusion.** No other component may
   drop a row.
4. **Each derived column has exactly one meaning,** fixed by its definition and
   independent of which analysis uses it.
5. **Claims are pre-specified and judged mechanically.** Being compatible with
   a value is reported as such and is never treated as evidence for it.
6. **Failures stay visible.** Unsupported capabilities are rejected before the
   run starts. Failures at runtime are recorded and carried through to every
   dependent result.
7. **Every run is visible.** Each run leaves start and end events in an
   append-only ledger.
8. **Redistribution terms follow lineage,** including through derived values.

## 2. Execution order

```text
build panels (existing)
  → masks            cell-level invalidation (§4.1)
  → derive           full built panel, one meaning per column (§4.2)
  → sample resolution per analysis@variant / sample group (§5)
  → analysis readiness (data-dependent checks, §11.2)
  → estimation       hhplab.analysis; consumes resolved sample, drops nothing (§6)
  → diagnostics      (§7.1)
  → findings         (§8)
  → package bundle   (§9)
```

## 3. `panels`

Each entry names a panel that a build section of the recipe produces.

```yaml
panels:
  cbsa: {ref: <panel reference>}
  coc:  {ref: <panel reference>}
```

**Reference contract.** There's no `outputs` registry in the current recipe
model. The implementer defines the `ref` form against the current model (for
example, a pipeline id plus its panel output) and updates §10 to use it. The
requirements:

- The ref resolves at plan time to exactly one panel artifact. Otherwise it is
  a spec-preflight error.
- The resolved panel declares a geography key column and a time key column. If
  the existing output spec doesn't carry them, the panel entry declares them
  (`geo_key`, `time_key`).
- The panel's identity, meaning its build spec plus its declared source
  vintages, is computable at plan time for locking (§9.5). Its content hash is
  recorded when the run happens.

Duplicate `(geo_key, time_key)` rows in a built panel are a readiness error
(§11.2), never an exclusion.

## 4. Masks and derivation

### 4.1 `masks`

Masks mark cells as invalid before any derivation. A masked cell is null for
every later purpose, it can never supply a lag or baseline value, and rows
excluded because of it carry the reason `masked:<col>`.

```yaml
masks:
  - id: covid_unsheltered_2021
    panels: [cbsa, coc]                  # default: all panels
    columns: [pit_unsheltered, pit_total]
    years: [2021]
    reason: Many CoCs did not conduct a 2021 unsheltered count.
```

Phase 1 supports year-based masks across all geographies, plus an optional
`geographies: {include: [...]}` list to limit them.

### 4.2 `derive`

Derivation runs once per panel on the **full built panel** (every year built,
not only the years in a sample), after masks are applied. Every derived column
has one fixed meaning. Each entry may include `panels: [...]` (default: all
panels).

| op | fields | semantics |
| --- | --- | --- |
| `sum` | `of: [cols]`, `require_all: bool` | With `require_all: true`, any null input gives null. Otherwise sum present inputs; all-null gives null, never zero. |
| `ratio` | `num`, `den`, `zero_den: null\|error` | num / den; zero denominator follows `zero_den`; negative finite denominators are allowed. |
| `per_capita` | `of`, `pop`, `scale` | of / pop × scale; population ≤ 0 gives null with `domain:nonpositive_population`. |
| `diff` | `of`, `years` | x(g, t) − x(g, t − years). |
| `lag` | `of`, `years` | x(g, t − years). |
| `growth` | `of`, `years`, `kind: pct\|log` | (x_t / x_{t−years}) − 1, or ln x_t − ln x_{t−years}. |
| `log` | `of`, `offset` | ln(of + offset); argument ≤ 0 gives null with `domain:log_nonpositive`. |
| `indicator` | `of`, `in: [values]` or `gte`/`lte` | 0/1 flag; a null input stays null. |
| `share` | `of`, `total`, `zero_den: null\|error` | A ratio carrying share semantics; `zero_den` defaults to `null`. |

Except where an operation defines different null behavior, null inputs
propagate to null. In particular, `sum(require_all: false)` sums present
inputs, `lag` depends only on its earlier cell, and `diff`/`growth` need both
cells. Finite-input arithmetic that overflows to a nonfinite value also yields
null with a domain reason. `ratio` and `share` use `zero_den: null|error`
(default `null`); `per_capita` has the population-specific rule above. For
`growth`, a zero earlier value makes
`pct` undefined and either nonpositive endpoint makes `log` undefined. The
current or earlier endpoint may be negative for `diff`; `diff` has no sign
restriction. These rules describe missing/undefined results, not sample
exclusions by themselves: a required derived null is handled by §5.2.

### 4.3 Time-operation semantics

- `years` is a calendar interval and is required. There's no implicit step.
- `lag` at (g, t) needs only the earlier cell (g, t − years). It is null if
  that row doesn't exist in the built panel, or if that cell is null or masked.
  A missing or masked value at (g, t) itself doesn't affect the lag.
- `diff` and `growth` at (g, t) need both cells. Each is null if either row is
  absent or either cell is null or masked, and `growth` is also null when the
  earlier value is 0 (`pct`) or ≤ 0 (`log`).
- A derived null caused by a masked input carries mask provenance (§5.2,
  rule 4).
- A derived null caused by an arithmetic domain condition carries its named
  domain reason in the derived artifact. If required by an analysis, it is
  excluded as `missing:<derived-column>` unless mask provenance takes
  precedence; sample reasons remain in the closed vocabulary in §5.2.
- **Baseline values may come from outside a sample.** A row excluded from an
  analysis sample, or a year before the sample window, can supply the t − years
  value, provided it exists in the built panel and isn't masked. Sample
  selection (§5) filters rows only by each row's own year and attributes.
- Build the panel far enough back to cover the earliest baseline any analysis
  needs. Readiness reports the rows lost to missing baselines.

### 4.4 Measure contracts for derived columns

Every derived column gets an entry in the bundle's `measures.json` with its
definition, inputs, units, aggregation semantics (extensive, intensive, or
rate), and source lineage (§9.4). Spec preflight rejects an op whose input
semantics are incompatible (for example a `sum` of rates) and warns when the
combination is merely questionable. Derived values are never re-aggregated
across geographies.

## 5. The sample contract

### 5.1 Sample spec

```yaml
sample:
  years: {from: 2011, to: 2024}      # optional step; or an explicit list
  exclude_years: []
  geographies:
    exclude: {column: right_to_shelter, equals: 1}
  require: [<extra cols>]            # adds to the inferred set; doesn't replace it
  min_obs_per_geography: 2
  drop_singletons: true              # default true when fixed_effects is non-empty
```

**Required columns are inferred**, in this order: outcome, predictors, controls,
weights, fixed-effect columns, cluster column, then `require`. For descriptive
analyses it is `columns` followed by the `by` columns. For correlation analyses
it is `columns`.

### 5.2 Exclusion reasons and order

Each panel row receives `included` and, when excluded, exactly one reason. The
rules are applied in this order:

1. `year_out_of_range`
2. `year_excluded`
3. `geography_excluded`
4. `masked:<col>`: the first required column, in inferred-column order, whose
   cell is masked, or whose cell is a derived value that is null *because* an
   input cell was masked. Derivation carries mask provenance so that this can
   be told apart from ordinary missingness.
5. `missing:<col>`: the first null required column. A storage null is missing;
   IEEE NaN is not a null and is classified under rule 6.
6. Nonfinite values: scan required numeric columns in order. The first ±inf or
   IEEE NaN in a non-weight column gives `nonfinite:<col>`; a nonfinite weight
   gives `invalid_weight` at that position.
7. `invalid_weight`: a finite weight is ≤ 0.
8. `insufficient_obs` and `singleton_fe`: applied **iteratively until nothing
   more changes**. A row gets the reason for the rule that removed it, in the
   first iteration it was removed. Within an iteration, `insufficient_obs` is
   applied before `singleton_fe`.

A non-numeric value in a required numeric column isn't an exclusion. It's a
spec-preflight error if the column's type contract is known, and a readiness
error otherwise.

For each row, apply the rule classes in the numbered order: scan all required
columns for masks, then storage nulls, then nonfinite numeric values, each
scan using deterministic inferred-column order. IEEE NaN is always
nonfinite, including when a dataframe library might otherwise coerce it to a
missing marker; implementations must preserve this distinction through sample
resolution, using a nullable representation that keeps NaN distinct from null
(for example an Arrow floating dtype). A masked or null weight therefore gets
`masked:<weight>` or `missing:<weight>` under rules 4–5; a nonfinite weight
gets `invalid_weight` during rule 6; and only a finite weight ≤ 0 reaches rule
7. This makes duplicate bad conditions resolve to exactly one stable reason.

### 5.3 Sample groups

Analyses whose results are compared algebraically must share their exact
membership:

```yaml
sample_groups:
  fd_main:
    sample: { ... }
analyses:
  - id: subst_fd
    sample: {group: fd_main}
```

**Resolved identity.** A resolved sample group is keyed by
`(group_id, variant_id, panel_identity)`. Each resolved group has one
membership, and every member analysis evaluated under that variant uses it.

**Required columns.** The group's set is the union of its members' inferred
sets (§5.1), with each member's spec *as modified by the variant*, followed by
the group's own `require`. The order is deterministic: members in declaration
order, each member's columns in inferred order, keeping the first occurrence of
any duplicate. The exclusion rules and their order (§5.2) come from the group's
sample spec, after any variant override.

**Membership-affecting fields:** `input`, `sample` and `sample.*`, `weights`,
`outcome`, `predictors`, `controls`, `fixed_effects`, `inference.cluster_by`,
and any `substitute` entry that matches a column referenced by a member.

**Variants that target a subset of a group:**

- If a variant changes a membership-affecting field for any member, it must
  list every member of that group in `applies_to`. Otherwise it is a
  spec-preflight error.
- A variant that changes only fields that don't affect membership
  (`inference.method`, `inference.reps`, `inference.seed`,
  `inference.weight_type`, `ci_level`) may target a subset. Such an
  inference-only variant always reuses that member's `@base` required columns
  and membership, even if the replacement inference method makes a base
  `cluster_by` column unnecessary. This preserves the base sample contract;
  the sample file records `sample_source: <group>@base`. A variant that also
  changes any membership-affecting field follows the all-members rule above.

**Single panel.** Every member of a group must resolve to the same panel under
every variant. Spec preflight rejects any variant under which members would
resolve to different panels.

**Fixed-effect exclusions.** Members of a sample group must have identical
fixed-effect exclusion policies after applying the variant: the same resolved
`fixed_effects` dimensions, `drop_singletons` setting, and
`min_obs_per_geography` threshold. Otherwise spec preflight rejects the group.
The shared membership then applies iterative minimum-observation and
singleton removal once using that common policy; estimators consume that
same membership. This prevents a shared group's members from claiming
different samples because their fixed-effect structures drop different rows.

### 5.4 Estimator contract

The estimator receives the resolved frame and must not drop or duplicate any
rows. Each row carries its immutable panel key `(geo_key, time_key)` from
sample resolution. The returned row-key multiset must equal the included
row-key multiset exactly; ordering may change. Any missing, added, or repeated
key fails with `sample_contract_violation`, even if the row count is
unchanged. A requested slope that is not identifiable after absorbing fixed
effects fails with `rank_deficient`, naming the unidentified terms. Columns
are never dropped silently. The intercept and full set of
fixed-effect dummy columns are structurally redundant by construction; this
expected redundancy is reported as absorbed structure and does not itself
fail the analysis. The rank check applies to the estimable slope space after
fixed-effect absorption.

## 6. Analyses and statistical conventions

### 6.1 Common fields

```yaml
- id: <unique>
  method: descriptive | correlation | regression | lagged_association
  input: panels.<name>
  sample: <sample spec> | {group: <name>}
  weights: none | <column>        # regression only in Phase 1
  report: [<keys>]
```

### 6.2 Methods (Phase 1)

**descriptive:** `columns`, `stats: [n, mean, sd, min, p10, p25, median, p75, p90, max, share_gte: {col: x}, share_lte: {col: x}]`, `by: [year | <col>]`. Unweighted only in Phase 1; `weights` is a spec-preflight error.

**correlation:** `columns`, `kind: pearson | spearman`. Listwise over the resolved sample. Unweighted only in Phase 1; `weights` is a spec-preflight error.

**regression:** `estimator: ols`, `outcome`, `predictors`, `controls`,
`fixed_effects` (any categorical columns; `geography` and `year` are aliases
for the panel keys), `inference: {method, cluster_by, reps, seed, weight_type}`,
`ci_level`.

### 6.3 Conventions (the contract)

- **Weights:** analytic weights. Weighted least squares minimizes Σ wᵢeᵢ².
  Coefficients and standard errors don't change when the weights are rescaled.
- **Intercept:** always fitted. Phase 1 has no `intercept: false`. Without
  fixed effects, it is reported as the term `(intercept)` with role
  `intercept`. With any absorbed fixed effect it is absorbed and not reported.
  In both cases it counts toward `design_rank`.
- **Fixed effects:** absorbed exactly. Coefficients must match the
  least-squares dummy-variable (LSDV) estimate within |a − b| ≤ 1e-10 +
  1e-10·|b|. Multi-way absorption may use alternating projections run to
  convergence (tolerance ≤ 1e-12).
- **Fixed-effect redundancy:** an intercept plus a complete set of category
  dummies, and overlapping dummy bases across absorbed dimensions, have
  expected redundant columns. Report absorbed dimensions and the full design
  rank; do not label these structural redundancies unidentified slopes. A
  predictor/control whose residualized column is linearly dependent on other
  requested slopes is unidentified and triggers `rank_deficient`.
- **`design_rank`:** the numerical rank of the full design, meaning the
  regressors plus the intercept plus all fixed-effect dummies. This is the
  parameter count for classical and HC1 inference. `df_resid` = N −
  `design_rank`.
- **`k_dof`:** the parameter count used in the clustered finite-sample
  correction. `k_dof` = `design_rank` − r_nested, where r_nested =
  `design_rank` − (rank of the design after every fixed-effect dimension nested
  within the cluster variable is replaced by a single intercept column). This
  follows the reghdfe convention. Without nested fixed effects, `k_dof` =
  `design_rank`.
- **classical:** σ̂² = SSR/`df_resid`; t distribution with `df_resid` degrees
  of freedom.
- **hc1:** HC0 × N/`df_resid`; t with `df_resid`.
- **clustered (CR1):** the sandwich estimator × G/(G − 1) × (N − 1)/(N −
  `k_dof`); t with G − 1 degrees of freedom.
- **Confidence intervals (Wald):** estimate ± t_{df, 1 − α/2} × se, with
  α = 1 − `ci_level`. p-values are two-sided and use the same distribution.
- **wild_cluster_bootstrap (Phase 2):** restricted (WCR), with Rademacher
  weights by default and Webb six-point weights when G < 12 or when requested.
  CIs come from inverting the bootstrap test at level α, with the root bracketed
  to within 1e-6 × se. Record the seed, reps, weight type, and grid. **Phase 1
  rejects this method at spec preflight** with `capability_unavailable`.
- **r2:** weighted, centered, 1 − SSR/TSS around the weighted mean of y, for
  the full model including fixed effects.
- **r2_within:** 1 − SSR/TSS_w, where TSS_w is the weighted sum of squares of y
  after projecting out all absorbed fixed effects. Without fixed effects,
  r2_within = r2.
- A zero denominator in either r2 definition makes that statistic undefined:
  serialize `value: null`, `status: not_evaluated`, and a specific reason
  (`zero_total_sum_squares` or `zero_within_sum_squares`). The analysis may
  still succeed and report its estimates. Constant-column correlations are
  likewise `not_evaluated` with reason `constant_input`; their coefficient
  is null. Any other undefined/nonfinite statistic uses null and an explicit
  reason, never JSON NaN or infinity.
- **Descriptive statistics:** sd uses ddof = 1. Quantiles use linear
  interpolation (Hyndman–Fan type 7).
- **Spearman correlation:** ties get average ranks.

### 6.4 Capability additions required in `hhplab.analysis`

The current regression API (`analysis/regression.py`) supports entity/year
dummy fixed effects and classical or clustered standard errors. Phase 1
requires adding:

1. Analytic weights.
2. Arbitrary absorbed fixed effects, with the equivalence to LSDV above.
3. HC1.
4. Configurable `ci_level` and CI construction as above.
5. `r2_within` and K/df reporting per §6.3.
6. An explicit rank check that fails rather than dropping columns.
7. An interface that accepts a resolved sample frame and asserts it drops no
   rows.

Phase 2: bootstrap CIs by test inversion, which extends the existing wild
cluster p-value code. The implementer should confirm this list against the
current API and extend it if anything else is missing.

## 7. `variants`

```yaml
variants:
  - id: <unique>
    applies_to: [<analysis ids>]
    override:                     # dotted paths; each value replaces that node whole
      sample.years: {from: 2013, to: 2023, step: 2}
      inference: {method: hc1}
    substitute:                   # column renames applied to every column reference
      d_beds: d2_beds
```

- **Allowed override roots:** `input`, `sample` and `sample.*`, `weights`,
  `outcome`, `predictors`, `controls`, `fixed_effects`, `inference` and
  `inference.*`, `ci_level`. Any other path is a spec-preflight error.
- **`substitute`** rewrites column references (outcome, predictors, controls,
  weights, fixed effects, cluster, and sample `require`) in each affected
  analysis, or in the shared spec of a group. A substitution that matches
  nothing in an analysis gives a warning.
- Substitutions are simultaneous mappings from names in the already-overridden
  spec; a destination is never substituted again. Spec preflight rejects a
  collision where two distinct referenced inputs become the same term, or a
  renamed term collides with an untouched term. This avoids silently changing
  the design by collapsing duplicate columns.
- **Substitution resolves everywhere.** Every reference to an analysis term or
  column made *outside* the analysis (diagnostic `terms`, finding
  `criteria_source`, table rows) resolves through the substitution map of the
  variant being evaluated, applied to that analysis. Result rows carry both
  `term`, the name actually estimated, and `base_term`, the name in the base
  spec. Findings and diagnostics match on `base_term`.
- `override` is applied first, then `substitute`.
- Variants don't stack in Phase 1. Results are keyed
  `<analysis_id>@<variant_id>`, and the unmodified run is `@base`.
- If a variant changes `input`, every derived column it needs must exist for
  that panel.

### 7.1 `diagnostics`

These are closed operations. They are not special cases keyed to particular
analysis ids.

```yaml
diagnostics:
  - id: <unique>
    op: linear_identity
    terms:                         # Σ sign × coefficient = 0
      - {analysis: a, term: x, sign: 1}
      - {analysis: b, term: x, sign: 1}
      - {analysis: c, term: x, sign: -1}
    variants: all                  # every variant where all referenced analyses ran
    require_same_design: true      # same sample group, regressors, FE, weights; outcome may differ
  - id: <unique>
    op: column_identity            # lhs = Σ rhs, row-wise
    panel: cbsa
    lhs: pit_total
    rhs: [pit_sheltered, pit_unsheltered]
    rows: unmasked                 # or {group: <sample group>}
```

**`variants: all`** means every variant that applies to *all* the referenced
analyses, plus `@base`. A variant that applies to only some of them makes the
diagnostic `not_applicable` for that variant. For example, `hc1` targets only
`subst_fd`, so `fd_additivity@hc1` is `not_applicable`.

Each diagnostic reports a status (`pass`, `fail`, `not_evaluated`, or
`not_applicable`), the value, the tolerance (§9.6), an explicit reason when
not evaluated, and details: the per-variant residual for
`linear_identity`, and the number of discrepant rows plus up to 20 example keys
for `column_identity`. A `linear_identity` failure doesn't necessarily mean the
samples differ. It can also come from inconsistent outcome totals in the data,
which `column_identity` is there to detect, or from different design matrices,
which `require_same_design` rules out before estimation.

If any input value or computed diagnostic value is nonfinite, the diagnostic
is `not_evaluated`, its value is JSON null, and its reason identifies the
nonfinite input or operation. Undefined results follow the same contract.
`not_applicable` means the diagnostic's declared applicability condition is
false (for example, a variant does not include all referenced analyses); it
does not mean an analysis failed. Per-result dependency failures use
`not_evaluated` with the failed result keys listed.

## 8. Parameters and findings

### 8.1 `parameters` and unresolved values

```yaml
parameters:
  delta_theta: {unresolved: "Equivalence margin for θ, in persons per bed"}
```

Anywhere a scalar is expected, `{param: <name>}` refers to a parameter and
`{unresolved: "<description>"}` marks a value that hasn't been decided. Spec
preflight lists every unresolved value. `results run` and `results lock` refuse
to run while any remain.

Resolved parameters must be finite JSON numbers unless the consuming field
requires another scalar type. Equivalence margins are nonnegative; in the
shelter example, `0 < delta_theta < 0.5` and `delta_slope > 0`. `ci_level` is
strictly between 0 and 1. `fit_threshold` and `fit_floor` are finite and must
satisfy `fit_floor <= fit_threshold`; they remain researcher-supplied values.
The spec does not assign defaults or silently choose any research threshold.
Every use is type-checked against its field, and a type/domain violation is a
spec-preflight error.

### 8.2 Finding structure

```yaml
findings:
  - id: <unique>
    claim: <plain language>
    criteria_source:                 # what the bands evaluate
      estimate: {analysis: <id>, term: <term>}      # gives estimate, ci_lo, ci_hi
      stats:                                        # optional named scalars
        fit: {analysis: <id>, stat: r2_within}
    reference_values: [-1, 0]        # reported as compatible / incompatible; never a band
    bands: [...]
    otherwise: inconclusive
    robustness: {variants: [...]}
    limitations: [...]
```

### 8.3 Band conditions (all conditions in a band must hold)

| condition | meaning |
| --- | --- |
| `equivalent_to: {value: v, margin: m}` | The CI lies entirely within [v − m, v + m]. |
| `ci_above: {value: v, margin: m}` | ci_lo > v + m (m defaults to 0). |
| `ci_below: {value: v, margin: m}` | ci_hi < v − m. |
| `ci_excludes: v` | v lies outside the CI. |
| `stat_gte: {stat: <name>, value: x}` / `stat_lte` | Threshold on a named stat. |

There are no point-estimate-only conditions in Phase 1.

### 8.4 Evaluation

- All bands are evaluated. Exactly one match gives that label. No match gives
  `inconclusive`. More than one match gives `ambiguous_rule`. An undefined
  estimate, interval endpoint, or required statistic makes the finding
  `not_evaluated` with a reason and null criterion value; it is not coerced to
  false and is not reported as `ambiguous_rule`. Each base and variant
  evaluation records its sanitized `criteria` mapping: criterion name to
  `{value, reason}`. A finite numeric value has `reason: null`; an undefined
  value is `null` with a nonempty reason. Raw NaN or infinity is never emitted.
- **Exact exclusivity check (spec preflight error).** With the resolved
  parameters, each band becomes a disjunction of conjunctions of interval
  constraints on the variables (ci_lo, ci_hi, s₁…sₙ):
  - `equivalent_to` → ci_lo ≥ v − m ∧ ci_hi ≤ v + m
  - `ci_above` → ci_lo > v + m
  - `ci_below` → ci_hi < v − m
  - `ci_excludes v` → (ci_hi < v) ∨ (ci_lo > v)
  - `stat_gte` → s ≥ x; `stat_lte` → s ≤ x

  Two bands overlap if any conjunct of one, intersected with any conjunct of
  the other, is satisfiable together with ci_lo ≤ ci_hi. With only per-variable
  bounds plus that one ordering constraint, satisfiability is decided exactly:
  - Each variable's intersected interval (tracking open and closed endpoints)
    must be non-empty.
  - The ci_lo interval's lower end must be ≤ the ci_hi interval's upper end,
    strictly when either endpoint is open.

  Any overlap is a spec-preflight error naming both bands and a witness point.
- `compatible_with` lists each reference value that the CI contains, with the
  label "compatible with". It is descriptive only and never sets a status.
- A finding is `not_evaluated` if any analysis it depends on failed.
- Dependencies are evaluated per result key (`analysis@variant`). A base
  finding depends on base analysis results; a finding's variant evaluation
  depends on that variant's analysis result. Failure of the base analysis does
  not invalidate a variant analysis that ran, and failure of a variant does
  not rewrite the base finding. The robustness summary still follows the
  precedence below: a failed base finding is `not_evaluated`; for an evaluated
  base, an inconclusive base is `uninformative`; otherwise a failed listed
  variant makes robustness `incomplete`.
- **Decisive** means a declared band label. `inconclusive`, `ambiguous_rule`,
  and `not_evaluated` are never decisive.
- **Robustness** is reported separately from support. The first rule that
  matches wins:
  1. `not_evaluated`: the base finding is `not_evaluated`.
  2. `uninformative`: the base result isn't decisive. Variant outcomes are
     still listed but don't affect the status.
  3. `incomplete`: any listed variant failed or wasn't evaluated.
  4. `fragile`: any listed variant has a different result, including a
     non-decisive one.
  5. `robust`: every listed variant has the same decisive band as the base.

  The band and `compatible_with` for every variant are always listed. Spec
  preflight requires every robustness variant to apply to the finding's
  estimand analysis.

## 9. `results` and the bundle

### 9.1 Recipe section

```yaml
results:
  bundle_dir: results/{id}/{run_id}
  include_panels: [cbsa, coc]
  tables: [coefficients, fit, sample_summary, exclusions, variant_comparison, diagnostics]
  report: {title: <title>}
```

### 9.2 Layout

```text
<bundle_dir>/
  README.md
  manifest.json            # inventory of all files EXCEPT itself and manifest.sha256
  manifest.sha256
  recipe/
    original.yaml          # byte-exact submission
    resolved.json          # canonical resolved spec, with original provenance paths
    execution.json         # bundle-local mapping: panel name → panels/<name>.parquet; spec hash
    lock.json              # if a lock exists
  panels/<name>.parquet    # base (built) columns only, unmasked, with hhplab_provenance
  derived/<name>.parquet   # keys + derived columns (for readers; verify recomputes)
  measures.json
  samples/<analysis>@<variant>.parquet   # every panel row: keys, included, reason
  results/
    regression_estimates.parquet
    regression_fit.parquet
    descriptive_stats.parquet
    correlations.parquet
    results.json           # all of the above, full finite float precision
  diagnostics.json
  findings.json
  withheld.json            # artifacts withheld under §9.4, with reasons and rebuild instructions
  tables/  report/index.html (self-contained)  environment/  logs/
```

### 9.3 Result schemas (contracts in `schema/`)

- **regression_estimates:** analysis, variant, term, base_term, role
  (predictor|control|intercept), estimate, se, df, ci_lo, ci_hi, ci_level,
  ci_method, p, p_status, p_reason, inference_method. Estimates, standard
  errors, degrees of freedom, and CI endpoints must be finite or the analysis
  fails. If p is undefined, serialize it as null with `p_status:
  not_evaluated` and an explicit `p_reason`; otherwise use
  `p_status: evaluated` and `p_reason: null`. This covers, for example, a
  zero estimate with zero standard error without emitting NaN.
- **regression_fit:** analysis, variant, sample_source, n, n_clusters,
  design_rank, k_dof, df_resid, r2, r2_status, r2_reason, r2_within,
  r2_within_status, r2_within_reason, absorbed (list). Undefined fit
  statistics have null values, `not_evaluated` status, and a reason; this does
  not by itself fail estimation.
- **descriptive_stats:** analysis, variant, column, group (canonical JSON
  string), stat, value, status, reason. Undefined values serialize as JSON
  null with `not_evaluated` and a reason; valid values have status `evaluated`
  and a null reason. No JSON artifact may contain NaN or infinity.
- **correlations:** analysis, variant, col_a, col_b, kind, r, n, status,
  reason. A constant input has `r: null`, `status: not_evaluated`, and reason
  `constant_input`.
- **diagnostics.json** and **findings.json:** JSON schemas defined in
  `schema/`, versioned with `bundle_format_version`.
  Each finding base/variant evaluation has `status`, `label`,
  `compatible_with`, `reason`, and `criteria`. `criteria` is a mapping from
  criterion names to `{value, reason}` records; undefined required criteria
  retain a null value and explicit reason.

`manifest.json` fields: `bundle_format_version`, `recipe_id`, `run_id`,
`created_at`, the four status fields of §11.3, per-analysis@variant status
and errors, `spec_hash`, `component_hashes`, `lock_status`, `input_identities`
(build spec hash, declared vintages, content sha256 per panel), `private`
(bool), and `files` (path, sha256, bytes, role).

### 9.4 Redistribution

- Each source spec carries `redistribution: {row_level: allowed|restricted|unknown, aggregate: allowed|restricted|unknown}`.
- **Lineage:** each panel column has a set of sources. A derived column's set
  is the union of its inputs' sets. The most restrictive status in the set
  applies.
- **Row-level artifacts:** panels, derived columns, row-level figures such as
  scatters, and descriptive `min`/`max`/quantiles computed over groups with
  fewer than 10 rows.
- **Aggregate artifacts:** estimates, fit statistics, other descriptive
  statistics, correlations.
- **Samples files** contain keys, inclusion flags, and reasons, but no source
  values, so they're treated as allowed.
- `restricted` or `unknown` row-level data is withheld. Aggregates that are
  `restricted` are withheld. Aggregates that are `unknown` are withheld unless
  `--allow-unknown-aggregates` is passed. `--private` ships everything and sets
  `private: true`.
- Everything withheld is listed in `withheld.json`, with its source lineage and
  rebuild instructions.
- The implementer must confirm each source's actual terms. ZORI and
  IPUMS/NHGIS-derived products must not be marked `allowed` until confirmed.

### 9.5 Run ledger and locking

- **New component:** an append-only JSONL **run ledger** at
  `<outputs_root>/ledger/runs.jsonl`, living in the infrastructure layer (for
  example `storage/run_ledger.py`). It is separate from the existing
  manifest-discovery "ledger".
- **Event schemas.** Every event carries `event`, `schema_version`, `ts`
  (UTC, RFC 3339), and its identity field.
  - `run_started`: run_id, recipe_id, recipe sha256, spec_hash,
    component_hashes, lock_status, lock_id (if matched), host, pid,
    process_start_time, environment summary.
  - `run_finished`: run_id, the four status fields (§11.3), bundle path,
    manifest sha256.
  - `run_failed`: run_id, failure scope (§11.3), error class, message, failure
    bundle path (if written).
  - `lock_created`: lock_id, recipe_id, recipe sha256, spec_hash,
    component_hashes, export path (if any).
  - `ledger_repair`: byte offset and length of an isolated torn fragment.
- **Ledger storage failure:** if the initial `run_started` append cannot be
  durably written, do not begin analysis. Return a run-scope failure and make
  a failure bundle if bundle storage is available. If ledger storage fails
  after `run_started`, stop the run at the next safe boundary, mark the run
  failed, and attempt `run_failed` logging once on a best-effort basis; never
  recursively retry a failed append. A successful analysis without a durable
  terminal event is not reported as complete. Logging failure details to the
  failure bundle/log is best effort too.
- **Running state.** While running, a run holds a lease file at
  `<outputs_root>/ledger/running/<run_id>.json` (run_id, host, pid,
  process_start_time, heartbeat_ts), rewritten atomically every 30 s. The run
  removes it after writing its terminal event.
- **Abandonment.** `history` reports a run with no terminal event as
  `running` while its lease is live, and as `abandoned` when either:
  - the lease heartbeat is older than 5 minutes, or
  - the lease's host is the current host and no process with the recorded pid
    and process_start_time exists.

  If the lease file is missing, the run is `abandoned`. Marking a run abandoned
  never writes a terminal event on its behalf.
- **Concurrency and torn writes.**
  - Each append takes an exclusive advisory lock (`flock`) on the ledger file
    and writes one serialized JSON line, newline-terminated, in a single
    `write`, followed by `fsync`.
  - Before appending, if the file doesn't end with a newline, the writer
    appends a newline and a `ledger_repair` event, which isolates the torn
    fragment.
  - Readers skip unparseable lines and report them. A malformed line that
    isn't at the end is reported as `ledger_corrupt` and is never rewritten.
  - The ledger assumes a local filesystem, because `flock` is unreliable on
    network filesystems. Preflight warns if `outputs_root` is on one.
- **Lock identity:** the spec hash covers the canonical resolved analysis spec,
  with component hashes for `build` (build sections plus declared source
  vintages), `masks`, `derive`, `samples` (sample specs and groups),
  `analyses`, `variants`, `diagnostics`, `parameters`, and `findings`.
  Content hashes of the inputs aren't known at lock time. They're recorded per
  run, and `history` flags runs that share a spec hash but have different input
  content.
- `results lock` refuses while unresolved values remain. Each run reports
  `lock_status`: `matched`, `modified: [components]`, or `unlocked`. The status
  appears in the manifest and the report header.
- **What a lock establishes:** the order of events within this ledger. It
  doesn't show that the researcher hadn't looked at the data or tried other
  specifications elsewhere. `results lock --export` writes `lock.json` for the
  researcher to publish somewhere with an independent timestamp (for example a
  public repository commit or a registry) before running. The report states
  this limitation in plain language.

### 9.6 Verification

`hhplab results verify <bundle_dir>` uses `recipe/resolved.json` together with
`recipe/execution.json`, never the original provenance paths. It produces three
separate statuses:

1. **integrity:** `manifest.sha256` matches, and every file matches the
   manifest.
2. **reproducibility** (`pass` | `fail` | `partial` | `not_run`): masks, derive,
   samples, estimation, diagnostics, and findings are recomputed from the
   bundled base panels. Derived columns are compared with `derived/`, sample
   membership exactly, and results within tolerance. Findings must produce the
   same bands.
3. **unavailable:** inputs withheld under §9.4, and the outputs that therefore
   couldn't be checked.

**Tolerance:** floats pass when |a − b| ≤ atol + rtol·|b|, with defaults
atol = 1e-10 and rtol = 1e-9. Both are recorded in the bundle and can be
overridden from the CLI. Integers, booleans, strings, and membership must match
exactly. Bootstrap results must match exactly under the same recorded
environment. Under a different environment the result is `partial`, with the
reason `environment_differs`.

**Partial reasons:** `unavailable_inputs`, `environment_differs`. Both can
apply at once, and all that apply are listed. If integrity fails,
reproducibility is `not_run`.

**Exit codes, by precedence (highest first):** 1 = integrity failure; 2 =
reproducibility failure; 3 = partial (any reason); 0 = everything passes. The
JSON output always reports all three statuses in full, whichever code is
returned.

## 10. Working example: shelter capacity

### 10.1 Question

The sheltered PIT count and the HIC are taken on the same night, so across
places the sheltered count largely measures capacity. The open question: when
a place's shelter capacity changes, how does its unsheltered count change? Let θ
be the **within-place association** between year-to-year changes in beds and
changes in the unsheltered count, conditional on the controls and year effects.
θ is reported as an association. This recipe declares no identification
assumptions that would license a causal reading.

- θ ≈ −1: changes in beds are matched by offsetting changes in the unsheltered
  count. This is consistent with absorption of people already unsheltered.
- −1 < θ < 0, distinguishable from both ends: partial offset.
- θ ≈ 0: no offset. Measured total homelessness moves with capacity.

Under explicit identification assumptions, which are out of scope here, these
cases would correspond to full, partial, and no absorption.

Because total = sheltered + unsheltered, OLS on a shared sample and design
gives β_total = β_sheltered + θ exactly. That identity is a declared diagnostic.

### 10.2 Recipe

```yaml
version: 2
id: shelter-capacity-substitution
title: Shelter capacity and the PIT count
authors: [Matt McConnell]

# --- existing build sections (current syntax; abbreviated) ---
# CBSA-year and CoC-year panels, 2010–2024:
#   pit_sheltered, pit_unsheltered, pit_total   (HUD PIT)
#   hic_es_beds, hic_th_beds, hic_sh_beds        (HUD HIC, year-round beds)
#   population (PEP), unemployment_rate (BLS LAUS), median_gross_rent (ACS)
#   right_to_shelter                             (static 0/1 flag; §10.4)
# ----------------------------------------------------------------

panels:
  cbsa: {ref: {unresolved: "Implementer: reference to the CBSA panel output"}}
  coc:  {ref: {unresolved: "Implementer: reference to the CoC panel output"}}

parameters:
  delta_theta:   {unresolved: "Equivalence margin for θ (persons per bed); must be < 0.5"}
  delta_slope:   {unresolved: "Equivalence margin for the levels slope around 1"}
  fit_threshold: {unresolved: "Minimum r2_within for the capacity claim"}
  fit_floor:     {unresolved: "r2_within at or below which the capacity claim is contradicted"}

masks:
  - id: covid_unsheltered_2021
    columns: [pit_unsheltered, pit_total]
    years: [2021]
    reason: Many CoCs did not conduct a 2021 unsheltered count.

derive:
  - {id: beds_shelter, op: sum, of: [hic_es_beds, hic_th_beds, hic_sh_beds], require_all: true}
  - {id: utilization,  op: ratio, num: pit_sheltered, den: beds_shelter, zero_den: "null"}
  # annual differences
  - {id: d_beds,        op: diff, of: beds_shelter,      years: 1}
  - {id: d_sheltered,   op: diff, of: pit_sheltered,     years: 1}
  - {id: d_unsheltered, op: diff, of: pit_unsheltered,   years: 1}
  - {id: d_total,       op: diff, of: pit_total,         years: 1}
  - {id: d_population,  op: diff, of: population,        years: 1}
  - {id: d_unemployment, op: diff, of: unemployment_rate, years: 1}
  - {id: lag_d_beds,    op: lag,  of: d_beds,            years: 1}
  # biennial differences (odd-year robustness)
  - {id: d2_beds,        op: diff, of: beds_shelter,      years: 2}
  - {id: d2_sheltered,   op: diff, of: pit_sheltered,     years: 2}
  - {id: d2_unsheltered, op: diff, of: pit_unsheltered,   years: 2}
  - {id: d2_total,       op: diff, of: pit_total,         years: 2}
  - {id: d2_population,  op: diff, of: population,        years: 2}
  - {id: d2_unemployment, op: diff, of: unemployment_rate, years: 2}

sample_groups:
  fd_main:
    sample:
      years: {from: 2011, to: 2024}   # 2011 differences use 2010 baselines
      # 2021 and 2022 differences that touch masked 2021 cells are excluded as masked:<col>

analyses:
  - id: levels_fit
    method: regression
    input: panels.cbsa
    estimator: ols
    outcome: pit_sheltered
    predictors: [beds_shelter]
    fixed_effects: [year]
    inference: {method: clustered, cluster_by: geography}
    sample: {years: {from: 2010, to: 2024}}
    report: [coefficients, ci, r2, r2_within, n, n_clusters]

  - id: utilization_dist
    method: descriptive
    input: panels.cbsa
    columns: [utilization]
    stats: [n, median, p10, p25, p75, p90, {share_gte: {utilization: 0.9}}]
    by: [year]
    sample: {years: {from: 2010, to: 2024}}

  - id: subst_fe
    method: regression
    input: panels.cbsa
    estimator: ols
    outcome: pit_unsheltered
    predictors: [beds_shelter]
    controls: [population, unemployment_rate, median_gross_rent]
    fixed_effects: [geography, year]
    inference: {method: clustered, cluster_by: geography}
    sample: {years: {from: 2010, to: 2024}, min_obs_per_geography: 2}
    report: [coefficients, ci, r2_within, n, n_clusters]

  - id: subst_fd
    method: regression
    input: panels.cbsa
    estimator: ols
    outcome: d_unsheltered
    predictors: [d_beds]
    controls: [d_population, d_unemployment]
    fixed_effects: [year]
    inference: {method: clustered, cluster_by: geography}
    sample: {group: fd_main}
    report: [coefficients, ci, r2, n, n_clusters]

  - id: sheltered_fd
    method: regression
    input: panels.cbsa
    estimator: ols
    outcome: d_sheltered
    predictors: [d_beds]
    controls: [d_population, d_unemployment]
    fixed_effects: [year]
    inference: {method: clustered, cluster_by: geography}
    sample: {group: fd_main}
    report: [coefficients, ci, n]

  - id: total_fd
    method: regression
    input: panels.cbsa
    estimator: ols
    outcome: d_total
    predictors: [d_beds]
    controls: [d_population, d_unemployment]
    fixed_effects: [year]
    inference: {method: clustered, cluster_by: geography}
    sample: {group: fd_main}
    report: [coefficients, ci, n]

variants:
  - id: odd_years_fe
    applies_to: [subst_fe]
    override:
      sample.years: {from: 2011, to: 2023, step: 2}
  - id: odd_years_fd
    applies_to: [subst_fd, sheltered_fd, total_fd]       # whole group
    override:
      sample.years: {from: 2013, to: 2023, step: 2}      # 2023 drops: its baseline is masked 2021
    substitute:
      d_beds: d2_beds
      d_sheltered: d2_sheltered
      d_unsheltered: d2_unsheltered
      d_total: d2_total
      d_population: d2_population
      d_unemployment: d2_unemployment
  - id: no_right_to_shelter
    applies_to: [levels_fit, subst_fe, subst_fd, sheltered_fd, total_fd]
    override:
      sample.geographies: {exclude: {column: right_to_shelter, equals: 1}}
  - id: coc_geography
    applies_to: [levels_fit, subst_fe, subst_fd, sheltered_fd, total_fd]
    override:
      input: panels.coc
  - id: pop_weighted
    applies_to: [subst_fe, subst_fd, sheltered_fd, total_fd]
    override:
      weights: population
  - id: lagged_beds
    applies_to: [subst_fd, sheltered_fd, total_fd]
    substitute:
      d_beds: lag_d_beds
  - id: hc1
    applies_to: [subst_fd]
    override:
      inference: {method: hc1}

diagnostics:
  - id: fd_additivity
    op: linear_identity
    terms:
      - {analysis: sheltered_fd, term: d_beds, sign: 1}
      - {analysis: subst_fd,     term: d_beds, sign: 1}
      - {analysis: total_fd,     term: d_beds, sign: -1}
    variants: all          # substitute renames the term in odd_years_fd / lagged_beds
    require_same_design: true
  - id: pit_components_cbsa
    op: column_identity
    panel: cbsa
    lhs: pit_total
    rhs: [pit_sheltered, pit_unsheltered]
    rows: unmasked
  - id: fd_components
    op: column_identity
    panel: cbsa
    lhs: d_total
    rhs: [d_sheltered, d_unsheltered]
    rows: {group: fd_main}

findings:
  - id: capacity_measure
    claim: >
      Across CBSA-years, shelter bed capacity accounts for nearly all variation
      in the sheltered PIT count beyond year effects, at close to one sheltered
      person per bed.
    criteria_source:
      estimate: {analysis: levels_fit, term: beds_shelter}
      stats:
        fit: {analysis: levels_fit, stat: r2_within}
    reference_values: [1]
    bands:
      - label: supported
        equivalent_to: {value: 1, margin: {param: delta_slope}}
        stat_gte: {stat: fit, value: {param: fit_threshold}}
      - label: contradicted
        stat_lte: {stat: fit, value: {param: fit_floor}}
    otherwise: inconclusive
    robustness: {variants: [no_right_to_shelter, coc_geography]}
    limitations:
      - The sheltered count and the HIC are taken on the same night, so a close fit is partly mechanical; the claim concerns interpretation of the count, not a causal effect.
      - Overflow and seasonal beds can push utilization above 1; see utilization_dist.

  - id: substitution
    claim: >
      Within CBSAs, year-to-year changes in shelter beds are associated with
      offsetting changes in the unsheltered count that are measurably smaller
      in magnitude than the change in beds, but not zero.
    criteria_source:
      estimate: {analysis: subst_fd, term: d_beds}
    reference_values: [-1, 0]
    bands:
      - label: near_full_offset
        equivalent_to: {value: -1, margin: {param: delta_theta}}
      - label: near_zero_offset
        equivalent_to: {value: 0, margin: {param: delta_theta}}
      - label: partial_offset
        ci_above: {value: -1, margin: {param: delta_theta}}
        ci_below: {value: 0, margin: {param: delta_theta}}
    otherwise: inconclusive
    robustness:
      variants: [odd_years_fd, no_right_to_shelter, coc_geography, pop_weighted, lagged_beds, hc1]
    limitations:
      - θ is a within-place association, not a causal effect. Bed changes may respond to rising unsheltered homelessness, which would tend to make θ understate any offset. Other mechanisms, such as new capacity drawing people into the count or common local shocks, can push it in either direction. lagged_beds is a partial check on timing, not an identification strategy.
      - Many CoCs count unsheltered people only in odd years, and even-year values may be carried forward; odd_years_fd addresses this.
      - Changes in PIT methodology within a CoC can move counts independently of capacity.
      - CBSA-level estimates describe places, not individuals.

results:
  bundle_dir: results/{id}/{run_id}
  include_panels: [cbsa, coc]
  tables: [coefficients, fit, sample_summary, exclusions, variant_comparison, diagnostics]
  report: {title: Shelter capacity and the PIT count}
```

Notes on the example:

- With δ < 0.5, the three substitution bands are mutually exclusive. The full
  and partial bands can't both match, because partial requires
  ci_lo > −1 + δ while full requires ci_hi ≤ −1 + δ. The same argument applies
  at the 0 end. Spec preflight confirms exclusivity with the resolved δ.
- A CI of [−2, 1] produces `inconclusive`, with
  `compatible_with: [-1, 0]`.
- In the `lagged_beds` and `odd_years_fd` variants, `substitute` also renames
  the diagnostic term and the `substitution` finding's estimand (`d_beds` →
  `lag_d_beds` / `d2_beds`). Both match on `base_term` (§7).
- `hc1` targets only `subst_fd` and changes only `inference.method`, so
  `subst_fd@hc1` reuses `fd_main@base` membership, and `fd_additivity@hc1` is
  `not_applicable`.
- `odd_years_fd`, `no_right_to_shelter`, `coc_geography`, `pop_weighted`, and
  `lagged_beds` change membership-affecting fields, so each lists all three
  `fd_main` members, and each resolves its own `(fd_main, variant, panel)`
  group.

### 10.3 Phase-2 extension (not part of Phase-1 acceptance)

```yaml
  - id: wild_bootstrap
    applies_to: [subst_fd]
    override:
      inference: {method: wild_cluster_bootstrap, cluster_by: geography, reps: 9999, seed: 20261020}
```

In Phase 1, adding this variant must fail at spec preflight with
`capability_unavailable`. That failure is itself an acceptance test (§14).

### 10.4 Research inputs Matt supplies before locking

1. The definition and source of the `right_to_shelter` flag.
2. `delta_theta`, `delta_slope`, `fit_threshold`, and `fit_floor`.
3. The primary geography (CBSA is assumed) and the year range.
4. The bed definition: ES + TH + SH year-round beds, excluding seasonal and
   overflow beds, is assumed.

Before a real-data lock, verify that every requested source and measure is
available at each panel's declared geography and year range. Availability at
CBSA level does not establish availability at CoC level. The `coc_geography`
variant is a planned robustness analysis; do not claim that a real-data run
can execute it until all its required inputs are supported at CoC-year level.
Record unavailable inputs as research/source gaps and keep real-data
acceptance distinct from the synthetic contract fixtures.

## 11. Preflight and readiness

### 11.1 Spec preflight (static; no panel data needed)

Checks references (panels, columns after derivation, analyses, variants,
groups, terms, parameters), the derive graph (acyclic), op compatibility with
measure semantics, the variant override allow-list, `substitute` targets, group
consistency (§5.3), `require_same_design` for `linear_identity`, band
well-formedness and exact exclusivity (§8.4), that each robustness variant
applies to its finding's estimand, capability availability (for
example `capability_unavailable` for a bootstrap method in Phase 1),
unresolved values, redistribution status of all bundled lineage, and seeds for
stochastic methods.

### 11.2 Analysis readiness (needs built or cached panels)

This stage runs after the panels exist, or earlier against cached panels whose
identity matches the planned build. Per analysis@variant, it checks or reports:

- Duplicate keys (error); non-numeric values in numeric roles (error).
- Resolved sample size, with exclusion counts by reason.
- Cluster count, with a warning below 30 that suggests a bootstrap method once
  available.
- Within-fixed-effect variation of every predictor (error if there's none).
- Iterative singleton and `insufficient_obs` losses (warning above 10% of the
  rows remaining after rule 7).
- Rank of the design (error if deficient).
- Weight validity counts.
- Rows lost to missing baselines in time-derived columns.

`results run` runs readiness automatically, applying the failure scopes in
§11.3. `hhplab results check --recipe <path> [--readiness]` runs either stage
on its own.

### 11.3 Failure scopes and status

**Failure scopes:**

| Scope | Causes | Effect |
| --- | --- | --- |
| run | Spec-preflight error; unresolved values; failure to append `run_started`; bundle I/O failure; ledger I/O failure after start | A start-append failure prevents analysis. A later bundle/ledger I/O failure stops work at a safe boundary and marks the run failed. Attempt one `run_failed` append when ledger access remains possible. A **failure bundle** is written if possible: manifest, `recipe/`, preflight report, logs. |
| panel | Panel build failure; duplicate keys; panel-level contract violation | Every analysis@variant on that panel fails with `panel_unavailable:<reason>`. Analyses on other panels continue. |
| analysis | Readiness error (no within-FE variation, rank deficiency, non-numeric value in a numeric role, empty sample); estimation error; sample-contract violation | That analysis@variant fails. Diagnostics and finding evaluations depending on that exact result key become `not_evaluated`; independent base/variant results continue. |

**Status fields.** Every manifest, ledger terminal event, and report header
records four statuses independently:

| Field | Values | Rule |
| --- | --- | --- |
| `execution_status` | `complete` \| `partial` \| `failed` | `failed` on a run-scope failure; `partial` if any analysis@variant failed or was skipped; otherwise `complete`. |
| `diagnostics_status` | `pass` \| `fail` \| `incomplete` \| `not_applicable` \| `none` | See aggregate precedence below; `not_applicable` rows do not count as failures. |
| `findings_status` | `all_evaluated` \| `some_not_evaluated` \| `ambiguous` | `ambiguous` if any finding (base or variant) is `ambiguous_rule`; it takes precedence over `some_not_evaluated`. |
| `distribution_status` | `full` \| `withheld` \| `private` | `withheld` if `withheld.json` is non-empty; `private` under `--private`. |

`execution_status: complete` says nothing about whether diagnostics passed.
The report header shows all four fields before any finding, and a diagnostics
failure appears there as prominently as an execution failure.

For `diagnostics_status`, `fail` takes precedence over `incomplete`, which
takes precedence over `pass`. A declared diagnostic set whose evaluated rows
are all `not_applicable` has status `not_applicable`; a set with at least one
applicable row and no failures/incomplete rows has status `pass`. `none` means
there are no diagnostic declarations. `not_applicable` rows do not make a
mixed set incomplete.

## 12. CLI

```text
hhplab results check    --recipe <path> [--readiness] [--json]
hhplab results run      --recipe <path> [--private] [--allow-unknown-aggregates] [--json]
hhplab results lock     --recipe <path> [--export <path>]
hhplab results verify   <bundle_dir> [--atol x] [--rtol y] [--json]
hhplab results history  --recipe-id <id> [--json]
```

`build recipe-preflight` handles version-2 recipes by delegating to
`results check`.

## 13. Placement

| Concern | Location | Layer |
| --- | --- | --- |
| v2 schema models | `recipe/recipe_schema.py` or new `recipe/schema_results.py` | application |
| Masks and derive | new `recipe/derive.py` | application |
| Sample resolution | new `analysis/sample.py` | application |
| Estimator capability additions | `analysis/regression.py` and focused modules | application |
| Analysis dispatch | new `recipe/executor/analyses.py` | application |
| Diagnostics and findings evaluation | `results/diagnostics.py`, `results/findings.py` | application |
| Bundle, verify | `results/bundle.py`, `results/verify.py` | application |
| Run ledger | `storage/run_ledger.py` | infrastructure |
| Redistribution fields | source specs, `covariates/catalog.py` | domain |
| Result and bundle schema contracts | `schema/` | contracts |
| CLI | `cli/build_cmds/results.py` | interface |

Update `architecture_layers.toml` and the architecture tests for every new
module. Add no new upward edges.

## 14. Phasing

**Phase 1.** Everything needed for §10.2 as written:

- Recipe sections: `version: 2`, `panels` reference contract, `parameters`,
  unresolved markers, `masks`, `derive`, sample contract and groups.
- Estimation: both preflight stages, the §6.4 capability additions,
  descriptive/correlation/regression, `variants` (override + substitute),
  `diagnostics`, findings with exclusive bands and separate robustness
  statuses.
- Bundle and tooling: result schemas, bundle, redistribution propagation, run
  ledger, lock, history, verify.
- Rejection of unsupported capabilities at spec preflight.

**Phase 2.** Wild-cluster bootstrap CIs by test inversion; `lagged_association`;
weighted descriptive statistics; figures.

**Phase 3.** 2SLS; stacked variants and robustness grids; analyses that depend
on other analyses; migrating suitable `results/workflows/`.

## 15. Non-goals

- A general expression or scripting language.
- Estimators implemented outside `hhplab.analysis`.
- Bundles that rebuild panels from raw sources.
- Serving bundles on the web.

## 16. Acceptance criteria

1. Every version-1 recipe and test passes unchanged.
2. Spec preflight on §10.2 lists each unresolved value and fails. With every
   value resolved, it passes. Adding §10.3 makes it fail with
   `capability_unavailable`.
3. Readiness on the built panels reports sample sizes, exclusions by reason,
   clusters, and baseline losses for every analysis@variant.
4. `results run` produces a bundle with `execution_status: complete` and
   `diagnostics_status: pass` on the synthetic acceptance fixture and a bundle
   that matches §9.2 and validates against the §9.3
   schemas.
5. In the synthetic acceptance fixture, `fd_additivity` passes for every
   applicable variant within tolerance and both `column_identity` checks pass.
   For real data, report each diagnostic's observed result and discrepant
   keys; a substantive failure is a research/data outcome, not a failed
   software acceptance check.
6. On synthetic fixtures, absorbed-FE coefficients equal LSDV within
   1e-10 + 1e-10·|b|. The classical, HC1, and CR1 standard errors, CIs,
   `design_rank`, and `k_dof` match a reference implementation (fixture values
   committed with their provenance). The fixtures cover weights, fixed effects
   nested in clusters, the no-FE intercept case, and a coefficient that is
   exactly zero.
7. `verify` passes after the bundle directory is moved. Editing an estimate
   and updating the manifest gives integrity pass and reproducibility fail.
   Editing any file without updating the manifest gives integrity fail.
8. A bundle with a withheld panel verifies with exit code 3, and `unavailable`
   lists the affected outputs.
9. A derived column whose lineage includes a `restricted` row-level source is
   withheld along with its source column.
10. Changing a sample spec after `results lock` gives
    `lock_status: modified: [samples]` in both the manifest and the report.
11. A run in progress appears in `history` as `running`. After it is killed,
    it appears as `abandoned`: immediately on the same host (the pid check),
    and after lease expiry when checked from elsewhere. Two concurrent runs
    produce well-formed, interleaved ledger lines. A truncated final line is
    isolated by a `ledger_repair` event on the next append.
12. Forcing one analysis to fail gives `execution_status: partial`, `not_evaluated` for
    each dependent finding, and a report showing the failure before any
    finding.
13. Band evaluation tests:
    - CI [−2, 1] → `inconclusive`, `compatible_with: [-1, 0]`.
    - Overlapping bands → a spec-preflight error with a witness point. This
      includes an overlap narrower than 1e-9 and an overlap only at a shared
      closed endpoint.
    - Bands that touch only at an open endpoint → no error.
    - Inconclusive base plus a failed variant → `uninformative`, not
      `incomplete`.
    - Decisive base plus a failed variant → `incomplete`.
    - A variant that differs → `fragile`.
14. `substitution@odd_years_fd` and `substitution@lagged_beds` are evaluated
    against `d2_beds` and `lag_d_beds` respectively, and appear in the
    robustness listing.
15. `subst_fd@hc1` reports `sample_source: fd_main@base`, and
    `fd_additivity@hc1` is `not_applicable`. A variant that changes `weights`
    for only one `fd_main` member is rejected at spec preflight.
16. A duplicate key injected into the CoC panel fails every `@coc_geography`
    analysis with `panel_unavailable`. The CBSA analyses complete, and the
    bundle reports `execution_status: partial`. A spec-preflight error
    produces `run_failed` and a failure bundle.
17. A deliberately inconsistent `pit_total` in a fixture yields
    `execution_status: complete` with `diagnostics_status: fail`.
18. A `lag` at a row whose current value is missing still returns the earlier
    value.
19. `verify` returns exit code 3 with both partial reasons listed when inputs
    are withheld and the environment differs. It returns 1, with
    reproducibility `not_run`, when integrity also fails.
20. The targeted contract cases are explicit: `hc1` subset reuse preserves
    base membership and required columns; a lag at a masked current cell still
    uses an unmasked earlier cell; a failed robustness variant yields
    `incomplete` for a decisive base and `uninformative` for an inconclusive
    base; undefined r2 and constant correlation produce null plus
    `not_evaluated` reasons; duplicate panel keys fail the panel; and bad
    weights receive exactly one reason according to §5.2. Synthetic fixtures
    use named, test-only parameter values and state their expected results in
    fixture constants; those values do not set research thresholds.
21. Unit tests cover:
    - Every derive op, including baselines drawn from outside the sample and
      masked cells.
    - The exclusion-reason order, including iterative singleton removal.
    - Sample-group union semantics.
    - `substitute` propagation to diagnostics.
    - The tolerance rule near zero.
    - Manifest self-exclusion.
    - Ledger event pairing.

    Fixtures are synthetic and need no network access.
