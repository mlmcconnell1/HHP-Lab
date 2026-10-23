# Response to the preliminary longitudinal review

Date: 2026-10-22

This responds to [longitudinal_review_notes.md](longitudinal_review_notes.md). It describes the current evidence and proposed revisions to the analysis plan; the additional analyses below have not been run. The current results and review evidence are documented in [the PIT–HIC audit](site-pit-hic-2024-input-audit.md).

The notes contain several strong suggestions. The most useful additions are the headcount decomposition, matching beds and people by household type, and an odd-year sensitivity check. Parts of the document predate the latest Count Types correction, and several design choices need revision before implementation.

## 1. Carry-forward correction: substantially completed

We now retain HUD's original Count Types declarations and separate reported numbers from eligibility for analysis. The unsheltered and total-PIT analyses exclude carried-forward, partial, and unknown observations and require complete observed counts at both exact calendar endpoints of every annual difference. Equal consecutive counts remain eligible when HUD declares that an actual count occurred; equality alone is not evidence of a carry-forward.

The source audit and site/year observation-availability tables already exist. In the frozen 60-site/77-CoC universe, the correction leaves 711 eligible unsheltered site-years and 508 exact annual changes. Only 55 sites contribute same-year unsheltered annual changes.

The annual exclusion sensitivity in P1 has therefore been run. The earlier pooled two-year PSH-leading correlation weakened from **−0.094 to −0.025**, with the corrected 95% site-bootstrap interval **−0.120 to +0.045**. The increases-only estimate is now **−0.083**, with interval **−0.270 to +0.022**. Neither excludes zero.

These changes reflect both improved observation eligibility and a different sample composition; they do not isolate the numerical bias caused by carry-forwards.

## 2. Odd-year design: useful, but define exact windows

An odd-to-odd design is a worthwhile sensitivity analysis. It should retain the source-declaration checks rather than assume that every odd-year count is complete.

For the existing 2010–2024 study period, excluding 2021, the usable candidate two-year outcome windows are:

- 2011→2013
- 2013→2015
- 2015→2017
- 2017→2019

The 2019→2023 change spans four years and cannot replace a missing two-year window. Both 2019→2021 and 2021→2023 must be omitted. There is no subsequent complete odd-to-odd window within the current study period. Consequently, this design also restricts outcomes to the pre-COVID period; differences from the full annual results could reflect timing, period, and sample changes together.

Match bed changes over equally long windows, and specify lag hypotheses with explicit dates. For example, PSH growth during 2011–2013 predicting unsheltered change during 2013–2015 is an unambiguous one-step lead in this design. The phrase “PSH increases at t−2” does not by itself distinguish that hypothesis from other window definitions.

There are at most four outcome windows per site before observation exclusions, and fewer matched windows for lagged comparisons. This makes sample counts and power especially important.

## 3. Shelter headcount decomposition: a high-priority addition

Regressing changes in sheltered, unsheltered, and total people on changes in shelter beds would make the results easier to interpret than correlations alone. Report slopes in people per additional bed, alongside the existing log-change results, with site and year effects and whole-site uncertainty estimates.

Use identical observations, regressors, weights, and bootstrap draws across all three outcomes. Because total equals sheltered plus unsheltered, the fitted slopes then satisfy:

`total slope = sheltered slope + unsheltered slope`

Testing whether the total slope equals the sheltered slope is therefore equivalent to testing whether the unsheltered slope is zero. The decomposition is useful presentation and an accounting check, but these are not independent pieces of evidence.

The proposed mechanisms are possible interpretations, not identified flows. A negative unsheltered slope alongside a positive sheltered slope would be consistent with substitution, but aggregate changes cannot establish where individual shelter residents came from. Likewise, equal sheltered and total slopes would not establish that new residents came specifically from doubled-up households or institutions.

## 4. Household matching: potentially important, with additional data work

Adult-only shelter capacity versus unsheltered people in adult-only households is a better matched question than all shelter beds versus all unsheltered people. Analyze families separately as well; do not assume that family capacity cannot affect unsheltered families. Keep child-only households distinct where the source permits it.

The current curated HIC schema retains project-type bed totals and family **units**, not the full household-specific bed breakdown. The curated PIT schema retains overall sheltered, unsheltered, and total counts. This extension therefore needs source coverage checks and additional ingestion, rather than just another regression on the current panel. Family units cannot substitute directly for family beds, and PIT household counts cannot substitute for person counts.

## 5. Utilization: align the numerator and denominator first

The current shelter measure is year-round **ES+TH+SH**. The notes switch to ES+TH, which would change the measure and omit Safe Haven capacity while retaining its occupants in overall sheltered PIT.

Sheltered PIT can also include occupants of seasonal and overflow beds. Dividing that count by year-round beds can produce a ratio above one without establishing an error. HUD distinguishes year-round, seasonal, and overflow inventories and describes utilization using occupancy on the inventory night. See [HUD HIC/PIT guidance, particularly pages 39 and 45–47](https://files.hudexchange.info/resources/documents/HIC-and-PIT-Count-Data-Submission-Guidance.pdf).

Until coverage is aligned, label the proposed ratio “sheltered PIT per year-round shelter bed,” rather than treating it as a complete utilization measure. Values above one or very low values warrant investigation, not automatic rejection.

I support presenting same-year shelter-bed/sheltered-count associations primarily as capacity and occupancy diagnostics. They should not be presented as evidence that additional capacity reduces regional homelessness.

For the proposed slack/full interaction models, prefer utilization measured before the outcome window. Classifying a year using its contemporaneous sheltered outcome risks building an association into the regime definition. Earlier utilization does not eliminate endogeneity, but avoids that immediate construction problem.

## 6. Multiple testing, timing, and confirmation

A complete results table and multiple-comparison adjustments would improve transparency. The current reviewed output contains 70 comparisons across levels, adjusted log levels, annual changes, and sign subsets. Define the testing family explicitly, retain the exploratory history, and identify which new analyses are primary versus secondary before running them.

Bootstrap p-values require a specified, null-calibrated testing procedure; they do not follow automatically from the saved percentile intervals. BH and Holm adjustments should be applied to valid underlying p-values.

Writing down the PSH hypothesis now is useful discipline, but the same timing has already been explored. A new specification on the existing universe is a planned robustness check, not independent confirmation. Reverse timing is already included in the lag grid and should remain visible. An unsheltered-leading association alone does not establish a funding-response mechanism, particularly within signed declines-only subsets.

Bridgeport, Salem, York, and Sacramento were excluded because of data or geographic comparability problems. They are unsuitable as a clean confirmation sample merely because they were excluded. Preserve their exclusions. Any future analysis of those sites would first need to resolve their comparability problems and would have limited precision with so few clusters.

## 7. Robustness and power

Exact-calendar annual differences and lag joins are already implemented and tested. The annual analysis does not treat 2020→2022 as a one-year change. A genuine calendar lag between otherwise valid observations is a separate concept from fabricating an annual difference across a missing year; any additional COVID-crossing exclusion should be stated as a sensitivity rule.

Pre-2020 point estimates already exist for all current comparisons, including shelter analyses. Adding uncertainty intervals for those estimates would be new work. Size weighting, small-site exclusions, and an inventory of unusually large changes are useful additions. Weighting also changes the question: a typical-region association differs from an association emphasizing regions with more people experiencing homelessness. Choose weights explicitly and show both interpretations.

Power simulations should preserve the actual observation pattern, within-site dependence, fixed-effect specification, and reduced odd-year sample. Ordinary resampling alone does not specify what a nonzero effect would look like; the simulation needs an explicit signal-generating model. Report detectable effects in people per bed as well as correlation units. A universal threshold such as r = 0.1 does not, by itself, define policy relevance. Prespecify a meaningful effect size if the goal is to assess whether the data can rule it out.

## 8. Additional outcomes and demand-side extension

PSH versus sheltered and total PIT are useful additions. Capacity is still a stock rather than a measure of placements or turnover, so these associations would not establish the route through which PSH operates. RRH, separate TH analyses, and shelter-quality measures should remain secondary and begin with source and historical-comparability checks.

The demand-side extension addresses a major omission in the present analysis. Start with a small set of explicit rent and labor-market hypotheses and check existing package registries before adding sources. Use the project's temporal alignment rules and document how each covariate maps to the whole-CoC site footprints. Medians and rates cannot simply be summed; an average of CBSA rent medians is not the site's median rent.

The proposed 2020 ACS1 treatment needs correction. Standard 2020 ACS1 estimates were not released. Published experimental tables cover national and state geographies, not CBSAs, and Census advises against comparisons with standard ACS estimates. This is a metro-level availability gap, not merely a flag to attach to otherwise ordinary observations. See [Census experimental-data FAQs](https://www.census.gov/programs-surveys/acs/data/experimental-data/faq.html). Any alternative source or temporal treatment must be explicit; do not bridge a missing covariate year silently.

The capacity-regime and shelter-quality hypotheses are worth exploring, but historical coverage of non-congregate and low-barrier measures must be established before promising a full-period test. Finding that rents explain more variation than beds would describe predictive association, not their relative causal importance. Compare explanatory performance on common observations and specifications.

## Recommended sequence and interpretation

1. Update the notes and summary to reflect the completed Count Types correction and weakened PSH result.
2. Specify and run the exact odd-year sensitivity, reporting the substantially smaller sample.
3. Run the common-sample headcount decomposition.
4. Audit and ingest the household-specific measures, then repeat the matched analyses.
5. Add the focused robustness, multiplicity, and power work.
6. Develop a separate, narrowly specified demand-side extension.

Keep the existing site universe fixed. Add its share of national PIT and a description of how selected regions differ from excluded regions, without presenting the sample as nationally representative.

The current finding remains: **shelter and PSH capacity changes have not shown a consistent detectable relationship with subsequent reductions in regional unsheltered counts in this sample.** The proposed work can test whether that conclusion survives better measurement, household matching, and alternative timing. It cannot, by itself, convert these observational associations into estimates of program effects.
