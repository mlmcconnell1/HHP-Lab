# State-Level Robustness Rechecks (2026-07-07)

Prompted by the Vera jail work finding that a state x year FE check can
either reveal or erase an MSA-panel signal depending on how well multi-MSA
states identify it (`devdocs/vera_jail_hic_pit_longitudinal.md` and
`..._pooled.md`). Went back through four prior headline-adjacent findings to
see which are exposed to the same confound: shared state-level policy or
economic shocks hitting every MSA in a state at once, which plain MSA
entity+year FE cannot separate from a genuine city-specific relationship.

Two different techniques used depending on data shape: **state x year FE**
for panel specs (multiple years per MSA, mirroring the Vera work exactly),
and **state-clustered SE + leave-CA-out** for the sanctuary long-difference
specs, which are pure cross-sections (one row per MSA, no year dimension --
a `state` fixed effect in a cross-section with mostly-one-MSA-per-state
coverage is close to unidentifiable, see below).

## 1. Sanctuary status -> sheltered/unsheltered/beds growth (top-50, long-diff 2015-2025)

**Checked first whether a state-FE approach is even feasible.** It mostly
isn't: of 29 states represented in the top-50 cohort, only **2 (Missouri,
Pennsylvania)** have both a sanctuary and a non-sanctuary MSA. California
alone supplies 7 of 50 MSAs, all sanctuary-coded; Florida and Texas supply 4
each, none sanctuary-coded. A literal state-FE regression would identify the
sanctuary coefficient almost entirely off 4 MSAs (the MO and PA pairs) and
silently drop every single-MSA state as a zero-residual passenger observation
-- not a meaningful check. Used two better-suited substitutes instead:

| Outcome | Original (HC1) | State-clustered SE | Excluding CA (n=43) |
| --- | ---: | ---: | ---: |
| Unsheltered growth | b=-0.016, p=0.932 | p=0.938 | b=-0.057, p=0.805 |
| Sheltered growth | b=+0.512, p<0.0001 | p<0.0001 | **b=+0.395, p=0.0001** |
| Beds growth | b=+0.526, p<0.0001 | p<0.0001 | **b=+0.398, p=0.0001** |

**Result: reassuring.** State-clustering SE (which properly accounts for
sanctuary status being correlated within state) barely moves the p-values --
the sheltered/beds effect was never a borderline result, so the wider SE
doesn't threaten it. More importantly, **excluding California entirely --
removing the single state that could most plausibly be driving this on its
own -- shrinks the effect by about 22-24% (0.51->0.40, 0.53->0.40) but it
stays highly significant (p=0.0001) on the remaining 43 MSAs.** This is not
purely a California story. Unsheltered stays null throughout, as originally
found.

## 2. Core rent-shock elasticity (FD ZORI -> unsheltered growth, pooled top-150)

Pooled top-50 + rank-51-150 annual first-difference panel (n=1090, 137
MSAs, 29 of 43 states with 2+ MSAs -- well-identified, matching the Vera
pooled check's power).

| Spec | Estimate | SE | p |
| --- | ---: | ---: | ---: |
| Plain year FE | +1.921 | 0.425 | <0.0001 |
| State x year FE | +1.795 | 0.693 | 0.0095 |

**Result: reassuring, and the opposite pattern from the jail check.** The
elasticity barely moves (1.92 -> 1.80, a 7% change) despite a much more
demanding specification that would absorb any shared state-level rent or
economic policy shock (state rent-control laws, state eviction moratoria,
etc.). It stays solidly significant. This is the headline finding of the
whole project and it does not appear to be a state-policy artifact.

## 3. Overdose deaths vs PSH beds (pooled top-150, entity+year-FE lag design)

This result was already only marginal before this check
(`devdocs/overdose_homelessness_lag_screen.md`).

| Spec | Plain year FE | State x year FE |
| --- | ---: | ---: |
| Levels, PSH lag1 (n=372, 99/113 MSAs in multi-MSA states) | b=+0.094, p=0.081 | b=+0.019, p=0.562 |
| FD, PSH lag1 (n=175) | b=+0.102, p=0.066 | b=+0.039, p=0.430 |

**Result: the marginal signal is fully explained away.** Consistent with a
state-level drug-policy confound (naloxone access laws, Good Samaritan laws,
opioid settlement fund allocation are all state-level and could move
multiple metros in a state together). Reinforces the existing writeup's
conclusion that this result was never solid enough to lean on; this closes
the loop rather than reversing anything.

## 4. IRS migration churn x rent-shock interaction (top-50 only)

Reproduced the recorded interaction (memory: -0.085, p=0.073; this run:
-0.090, p=0.057, same ballpark) then added state x year FE. **Caveat up
front: this is top-50-only, and like the top-50 jail check, identification
is thin** -- only 11 of 29 states have 2+ MSAs.

| Term | Plain year FE | State x year FE |
| --- | ---: | ---: |
| `d_log_zori` (base rent effect) | b=+3.974, p=0.0028 | b=+5.455, p=0.0511 |
| `d_log_zori x churn_rate` (interaction) | b=-0.090, p=0.057 | b=-0.113, p=0.163 |

**Result: weakens on both terms, loses significance on both.** The
interaction was already only marginal; state x year FE pushes it further
from significance (p=0.057->0.163) and pushes the base rent effect right to
the edge (p=0.0028->0.0511) too, though the point estimates don't collapse
toward zero (if anything the base effect gets larger). Given the thin
top-50-only identification (mirroring the jail check's finding that the
top-50 cohort's state x year FE is a much weaker instrument than the pooled
cohort's), this result should be treated with real caution either way -- not
confidently reversed, but not confidently defended either. A pooled
top-150 version of the migration panel (not built; IRS SOI migration work
was top-50-only in this project to date) would be the way to actually
resolve it, mirroring how pooling helped the core rent elasticity check.

## Summary

| Finding | Pre-check status | Post-check status |
| --- | --- | --- |
| Sanctuary -> sheltered/beds growth | Strong | **Confirmed** (survives state-clustered SE and CA exclusion, ~25% attenuation) |
| Core rent-shock elasticity (~1.6-1.9) | Strong | **Confirmed** (barely moves under state x year FE) |
| Overdose deaths vs PSH beds | Marginal | **Explained away** (state x year FE erases it) |
| IRS migration churn x rent-shock | Marginal | **Weakened, unresolved** (thin top-50 identification; pooling would help) |

The two flagship findings (sanctuary, core elasticity) hold up. The two
findings that were already marginal before this pass get materially weaker
evidence, one decisively (overdose/PSH) and one inconclusively due to sample
thinness (IRS churn). No finding flipped sign or reversed direction the way
the jail-vs-unsheltered correlation did in the original Vera work -- this
pass didn't find a new "looked real, was actually a state artifact"
surprise, just confirmed two strong results and further weakened two that
were already flagged as marginal.

## Artifacts

Ad hoc, not saved as tracked parquet outputs except where noted -- these
were direct `statsmodels`/`regress_panel` comparisons against existing
panels (`outputs/tot_longdiff.parquet`, `outputs/top50_msa_beds_longdiff.parquet`,
pooled FD panel built inline from `outputs/top50_msa_longitudinal_2010_2025.parquet`
+ `outputs/msa_rank51_150_replication/fd__msa_rank51_150__Y2015-2025.parquet`,
`outputs/overdose_lag/spec_{b,d}_psh_stateyear_fe.parquet` (saved),
`outputs/top50_msa_migration_fd.parquet`). Rerun by reconstructing
`primary_state`/`state_year` columns inline per the pattern in
`scripts/build_vera_hic_pit_longitudinal_pooled.py` if this needs to be
reproduced.
