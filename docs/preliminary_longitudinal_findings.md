# Preliminary longitudinal findings

Updated 2026-10-25. This summarizes the corrected results; earlier estimates that treated carried-forward unsheltered counts as observations are superseded. Details and artifacts are in [the longitudinal audit](site-pit-hic-2024-input-audit.md).

## Design and measurement

Whole-CoC regional sums use two-way population overlap with MSAs. The 99% universe has **60 sites / 77 CoCs**; the 95% sensitivity has **67 / 86**. Bridgeport, Salem, York, Sacramento, Boston, and NYC remain excluded. Boston fails the 95% rule anyway; NYC qualifies but is held out.

PIT/HIC checks cover **2010–2024, excluding 2021**. ACS rent is preceding-year 1-year median gross rent, including utilities. ZORI uses **2015–2023 calendar-year rents aligned with January PIT2016–2024**, with fixed county composition/weights and complete calendar years.

Main checks correlate annual log changes after site/year fixed effects; log-level models are secondary. Counts use log(1+x), rents log(x). We examine lags −2 through +2, pre-2020 and leave-one-site-out sensitivities, with **1,999 whole-site bootstrap draws** and nominal 95% intervals. Positive lag means the predictor precedes PIT, additional to the preceding-year rent alignment. Annual differences never bridge missing years.

HUD **Count Types** determines count occurrence. Carried-forward, partial, and unknown unsheltered observations are excluded from unsheltered/total analyses; reported values are preserved. All constituent CoCs must qualify, and total additionally requires observed sheltered counts. Genuine observed zeroes remain eligible.

## Findings

- **Shelter capacity (ES+TH+SH beds) versus unsheltered PIT:** no consistent negative association across timings or increases-only/declines-only subsets. The pooled same-year annual correlation is **−0.005 [−0.099, 0.107]**.
- **PSH capacity versus unsheltered PIT:** no consistent detectable reduction. The pooled two-year-leading correlation is **−0.025 [−0.120, 0.045]**. An earlier nominal negative association weakened after the observation correction; the increases-only result also no longer excludes zero.
- **Shelter capacity versus sheltered/total PIT:** positive same-year associations: sheltered PIT **0.476** for bed increases and **0.319** for declines; total PIT **0.383** for increases, with declines uncertain. These are capacity/occupancy diagnostics, not evidence of reduced regional homelessness.
- **ACS gross rent versus unsheltered PIT:** weak annual evidence: primary **r = 0.067 [−0.047, 0.161]**. Some level models show modest positive associations, but results depend on specification and timing.

ZORI annual-change results are similar at 99% and 95% overlap:

| Outcome / timing | 99% r | 95% r [nominal 95% interval] |
| --- | ---: | --- |
| Unsheltered, primary alignment | 0.100 | 0.103 [−0.106, 0.313] |
| Unsheltered, +1 year | 0.189 | **0.206 [0.070, 0.328]** |
| Total, primary alignment | 0.213 | **0.217 [0.0002, 0.392]** |
| Total, +1 year | 0.042 | 0.076 [−0.093, 0.223] |

At 95%, the primary comparison has **243 pairs / 53 represented sites**; +1 year has **153 / 47**. Matched ACS observations do not reproduce ZORI's one-year unsheltered signal. Pre-2020 forward annual intervals include zero. Some total-PIT level and reverse-timing associations are positive. The near-zero primary total-PIT lower endpoint is fragile. Differences between outcomes, measures, and cohorts have not been formally tested.

## Interpretation and work not yet done

We have **not found a consistent regional decline in unsheltered homelessness following shelter or PSH bed expansion**. This does not establish zero causal benefit: capacity is not placements, PIT measures net counts, and capacity may respond to need. ZORI suggests a possible delayed association between market rent increases and unsheltered growth.

These exploratory correlations involve many unadjusted comparisons, incomplete observations, and provisional historical geography. **PSH models jointly controlling for rent have not been run.** Odd-to-odd designs, household-matched headcount decomposition, and broader demand-side models also remain outstanding. Rent-adjusted PSH checks should compare models on identical observations.
