# Vera Jail Population vs HIC Beds and PIT Counts: Pooled Top-150 Longitudinal Design

Date: 2026-07-07

## Design

Same entity+year-FE lag scaffold as `devdocs/vera_jail_hic_pit_longitudinal.md`
(top-50-only, 2010-2023), rerun on the **pooled top-50 + rank-51-150 cohort**
(150 MSAs, no overlap) to trade the time depth of that design for
cross-sectional power. Window: 2015-2020 + 2022-2023 (rank-51-150's PIT base
panel doesn't reach before 2015; 2021 excluded as always). This is the same
window the earlier pooled correlation screen used, and -- per the design
correction already noted in the top-50 doc -- not actually shorter than what
the rent-controlled top-50 specs could use anyway (ZORI itself starts 2015).
So this version simply adds the 100 rank-51-150 MSAs across the identical
years, roughly 2.7x the sample.

Build script: `scripts/build_vera_hic_pit_longitudinal_pooled.py`. Same 6
outcomes (unsheltered/total/sheltered PIT; Emergency Shelter/PSH/All HIC
beds), same 4 spec forms (levels contemp/lag1, FD contemp/lag1), same
`primary_state` x year FE robustness layer -- but this time **32 of 45
states have 2+ MSAs, covering 137 of 150 MSAs** (versus top-50's 11 of 29
states / 32 of 50 MSAs), so the state x year FE check is much better
identified here than it was in the top-50-only version.

## Primary Results: Entity+Year FE, Rent-Controlled

Jail coefficient only (n=803-1078 depending on spec, vs. top-50's n=250-400):

| Outcome | Levels contemp | Levels lag1 | FD contemp | FD lag1 |
| --- | ---: | ---: | ---: | ---: |
| Unsheltered PIT | **-0.223 (p=0.0093)** | **-0.234 (p=0.0079)** | -0.021 (p=0.811) | **-0.264 (p=0.0174)** |
| Total PIT | **-0.121 (p=0.0186)** | **-0.097 (p=0.0277)** | +0.052 (p=0.299) | -0.068 (p=0.186) |
| Sheltered PIT | -0.100 (p=0.084) | -0.025 (p=0.598) | -0.006 (p=0.894) | +0.017 (p=0.673) |
| Emergency Shelter beds | -0.099 (p=0.265) | +0.038 (p=0.674) | -0.212 (p=0.337) | +0.176 (p=0.351) |
| PSH beds | +0.048 (p=0.339) | +0.136 (p=0.112) | -0.266 (p=0.251) | +0.148 (p=0.456) |
| All HIC beds | -0.027 (p=0.627) | +0.072 (p=0.357) | -0.273 (p=0.241) | +0.238 (p=0.229) |

**Unsheltered and total PIT come back significantly negative in 3 of 4 and
2 of 4 spec forms respectively -- a striking contrast with the top-50-only
version, where all 24 regressions were null.** All three HIC bed categories
stay null throughout, as does sheltered PIT (one borderline p=0.084).

**This is also a sign flip, not just a signal appearing**: the earlier
pooled-150 same-year correlation found unsheltered *positively* correlated
with jail population (r=+0.113, partial r=+0.142 controlling population).
Here, with entity+year FE and a rent control, the same cohort and years give
a *negative* coefficient (-0.22 to -0.26). Between-metro variation
(bigger/denser metros have more of both jail population and unsheltered
homelessness) and within-metro variation (year-to-year, a metro's own jail
population moving inversely with its own unsheltered count) point opposite
directions here -- exactly the between/within split this project's earlier
FE checks (PSH-overdose, jail-vs-beds-top50) have been finding, except this
time the within-metro signal is real and opposite-signed rather than merely
absent.

## Robustness Checks

**Excluding 2020**: the negative unsheltered/total result gets *stronger*,
not weaker -- unsheltered b=-0.271 (p=0.0028, n=938), total b=-0.165
(p=0.0010, n=938). Not a COVID-decarceration artifact; if anything 2020 was
diluting it.

**State x year FE** (well-identified here: 137 of 150 MSAs in states with
2+ MSAs, vs. top-50's fragile 32 of 50): **the negative unsheltered/total
result collapses to null.** Unsheltered: b=-0.074 (p=0.483, n=1072, down
from p=0.0093). Total: b=-0.018 (p=0.750, n=1073, down from p=0.0186). The
FD-lag1 version of unsheltered also flattens and even flips sign:
b=+0.007 (p=0.899, down from b=-0.264, p=0.0174). All HIC beds stays null
under state x year FE too (b=+0.136, p=0.282), consistent with (not
contradicting) the fragile top-50 state x year FE result on beds, which was
identified off a much thinner, CA/FL/TX/OH-dominated sample.

## Interpretation

**Three different fixed-effects structures give three different answers,
and the most informative one is the null.** Reading them in order of how
much of the confounding structure each one removes:

1. Raw same-year correlation (no FE): unsheltered positively correlated
   with jail population -- almost certainly a between-metro size/density
   artifact (bigger cities have more of both).
2. MSA entity + calendar-year FE: unsheltered *negatively* and
   significantly related to jail population, robust to dropping 2020.
   This removes each MSA's own fixed level and common national-year shocks,
   but not shocks shared by multiple MSAs *within the same state* -- e.g. a
   state-level sentencing reform, jail population cap, or homelessness
   funding bill that hits every metro in that state in the same year.
3. MSA entity + state-year FE: the negative result disappears entirely.

Given (3) additionally removes exactly the kind of confound (2) is exposed
to, and given this pooled cohort's state x year FE is well-identified
(unlike top-50's version), **the honest read is that the entity+year-FE
negative result was very likely a state-level policy co-movement, not a
city-specific relationship between jail population and unsheltered
homelessness.** This doesn't prove no relationship exists -- it means this
design cannot distinguish "MSA A's jail population and MSA A's unsheltered
count move together for city-specific reasons" from "the state MSA A sits in
passed a policy that moved both, and so did every other metro in that
state." A design that could separate those (e.g. explicit state-level
policy-event dates as a shock/instrument) is the natural next step, not
attempted here.

The methodological lesson from doing this at two cohort sizes: the
state-level confound the original design proposal flagged as a risk turned
out to matter substantively, but in the *opposite* direction from what the
top-50-only check suggested (there, state x year FE *revealed* a fragile
signal plain FE missed on HIC beds; here, in a much better-identified
cohort, state x year FE *removes* a real-looking signal plain FE found on
unsheltered PIT). Don't assume a state-level robustness check will push a
result in either particular direction -- run it, and trust the version with
better identifying variation when they disagree, which here is unambiguously
the pooled cohort's state x year FE (137/150 MSAs) over the top-50 version's
(32/50 MSAs).

## Caveats

- Same `primary_state` crude first-listed-state parsing as the top-50 doc
  (imprecise for genuinely multi-state MSAs).
- Connecticut's 4 rank-51-150 MSAs still contribute zero Vera coverage
  (state-run jails).
- Same stock-vs-flow limitation as before: `total_jail_pop` says nothing
  about admissions/turnover, and Vera's admits coverage degrades to ~50%
  from 2020 on if that's pursued later.
- This design still cannot separate a genuine within-city mechanism from a
  shared-state-policy artifact for the (already null-once-controlled)
  unsheltered result -- see Interpretation.

## Artifacts

`outputs/vera_hic_pit_longitudinal_pooled/` (gitignored):
`vera_hic_pit_longitudinal_pooled_{levels,fd}.parquet`,
`spec_{a,b,c,d}_{outcome}_*.parquet` + `{A,B,C,D}_{outcome}_result.{parquet,json}`,
`key_coefficients.csv`, plus the 2020-exclusion and state-year-FE sensitivity
parquets. Build script: `scripts/build_vera_hic_pit_longitudinal_pooled.py`.
Reuses the same `outputs/overdose_lag/hic_by_category/` rollups as the
top-50 longitudinal design.
