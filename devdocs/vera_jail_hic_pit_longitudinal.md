# Vera Jail Population vs HIC Beds and PIT Counts: Longitudinal Design (top-50, 2010-2023)

Date: 2026-07-07

## Design

Follow-up to `devdocs/vera_jail_hic_pit_correlations.md` (pooled top-150,
2015-2020+2022-23, contemporaneous correlations only). This trades
cross-sectional breadth for time depth: **top-50 only**, extended back to
**2010** (rank-51-150's PIT base panel doesn't reach past 2015, so it's
dropped here), entity+year FE, clustered by `msa_id`, with the standard
`log_zori` rent control and both contemporaneous and lag-1 terms in levels
and first-difference form -- the same scaffold used throughout the
overdose-lag work.

**Correction to the design as proposed**: extending to 2010 does not
actually buy more usable rows once the rent control is included, because
**ZORI itself only starts in 2015** (Zillow's series begins Jan 2015,
per this project's standard temporal-coverage facts) -- exactly the same
constraint that bounded the earlier pooled-150 screen. So the
rent-controlled primary specs below are effectively a **2015-2023 top-50-only**
panel (n=400 levels, n=300 FD-contemporaneous, n=250 FD-lag1), not a true
2010-2023 one. To actually use 2010-2014, a second family of specs drops the
rent control (see below). Top-50 has full Vera jail coverage in every year
2010-2023 (no missing MSAs, unlike the pooled screen's CT gap -- none of the
top-50 are Connecticut MSAs).

Two robustness layers beyond the primary spec, both flagged in the earlier
design proposal:
- **2020 exclusion**: most US jails saw large, policy-driven population
  drops in 2020 (COVID decarceration), a plausible confound for anything
  that changed abruptly that year.
- **State x year FE**: jail population is driven substantially by
  state-level sentencing/release policy, which MSA entity+year FE alone
  won't absorb. Constructed a coarse `primary_state` (first state code in
  the MSA name, e.g. "TX" from "Austin-Round Rock-San Marcos, TX" --
  imprecise for multi-state MSAs, noted as a limitation) and used
  `state_year = primary_state + "_" + year` as the year-FE column instead
  of plain year.

Build script: `scripts/build_vera_hic_pit_longitudinal.py` (tracked,
reproducible). Outcomes: 3 PIT margins (unsheltered/total/sheltered) + 3 HIC
categories (Emergency Shelter, PSH, All HIC beds -- chosen as the 3 that
moved most in the earlier correlation screen; full category set not rerun
here). **Emergency Shelter is only available 2014-2023**: 2010-2013 HUD
sheets don't publish an ES-specific column (filled 0 -> log undefined),
matching the documented HIC backfill history.

## Primary Results: Entity+Year FE, Rent-Controlled (effectively 2015-2023)

Jail coefficient only, all 4 specs x 6 outcomes (24 regressions):

| Outcome | Levels contemp | Levels lag1 | FD contemp | FD lag1 |
| --- | ---: | ---: | ---: | ---: |
| Unsheltered PIT | -0.220 (p=0.331) | -0.214 (p=0.328) | -0.142 (p=0.359) | +0.034 (p=0.890) |
| Total PIT | -0.057 (p=0.594) | -0.078 (p=0.450) | -0.002 (p=0.978) | -0.034 (p=0.719) |
| Sheltered PIT | -0.056 (p=0.648) | +0.045 (p=0.728) | +0.009 (p=0.878) | +0.074 (p=0.342) |
| Emergency Shelter beds | +0.104 (p=0.525) | +0.139 (p=0.351) | +0.066 (p=0.331) | -0.028 (p=0.767) |
| PSH beds | -0.044 (p=0.601) | -0.019 (p=0.842) | -0.004 (p=0.949) | +0.067 (p=0.594) |
| All HIC beds | +0.018 (p=0.857) | +0.067 (p=0.459) | -0.079 (p=0.181) | +0.026 (p=0.697) |

n=400 (levels), 300 (FD contemp), 250 (FD lag1) throughout.

**Every single coefficient is statistically indistinguishable from zero.**
This is a much cleaner null than the PSH-overdose lag design found earlier
(which had two marginal p~0.07 results) -- here nothing comes close. The
striking contemporaneous correlations found in the pooled-150 screen
(jail vs PSH beds r=-0.166, jail vs sheltered PIT r=-0.243, both p<0.0001)
essentially disappear once MSA entity+year fixed effects remove between-metro
differences and control for rent shocks. That earlier screen's correlations
were almost entirely a between-MSA pattern, not a within-MSA one.

## Robustness Checks

**Excluding 2020** (headline outcomes, levels-contemporaneous): still null.
Total HIC beds b=+0.035 (p=0.743, n=350); sheltered PIT b=-0.069 (p=0.634,
n=350); unsheltered PIT b=-0.250 (p=0.367, n=350). 2020 was not masking a
relationship that appears once removed, nor was it manufacturing the null.

**Full 2010-2023 window, no rent control** (uses all 650 levels rows,
including 2010-2014 where ZORI is unavailable): still null for total beds
(p=0.957) and unsheltered (p=0.751); PSH null (p=0.204); **sheltered edges
toward marginal** (b=-0.150, p=0.081, n=650) -- same negative sign as the
correlation screen, more data moves it closer to significance but not
across the line.

**State x year FE** (levels-contemporaneous, rent-controlled, n=400): this
is where something survives. All HIC beds: **b=-0.321, p=0.0013** -- sign
matches the original correlation, and it's significant here despite being
null under plain entity+year FE. Sheltered and unsheltered stay null
(p=0.49, p=0.77). The FD-contemporaneous version of the beds result also
leans the same way: b=-0.112, p=0.061 (n=300), weaker but consistent.

**This result needs a strong caveat, not a headline.** `state_year` FE with
`msa_id` entity FE is a design_rank=255 model on n=400 rows -- barely more
identifying degrees of freedom than parameters. Only 11 of 29 primary states
in the top-50 cohort have 2+ MSAs (32 of 50 MSAs); the other 18 MSAs are
their state's sole representative and contribute zero within-state-year
identifying variation once the interaction dummies absorb their MSA-state
cell entirely. The result is therefore identified almost entirely off
California (7 MSAs), Florida/Texas (4 each), and Ohio (3) -- comparing large
multi-MSA states' internal cross-metro variation, not a broad national
pattern. Plausible reading: MSA-level jail population moves too slowly
year-to-year for within-MSA-over-time comparison (plain entity+year FE) to
have any power, while within-state-year cross-MSA comparison has more usable
variation -- but that variation comes from a small, non-representative set
of large states, and it's the opposite of what a within-MSA "does this metro's
own jail population predict its own subsequent bed capacity" test would
show, since plain entity+year FE found nothing.

## Interpretation

**The pooled-150 contemporaneous correlations do not survive a proper
longitudinal test.** Once local fixed effects are controlled, there is no
detectable relationship -- in either direction, at any lag -- between jail
population and HIC bed capacity or PIT homelessness in this cohort. The one
partial exception (state x year FE on total HIC beds) is real enough to
flag but too fragile (thin identifying sample, dominated by a handful of
large states) to treat as a finding. This mirrors, more starkly, what the
PSH-vs-overdose FE-lag check found earlier: cross-sectional correlations in
this class of MSA-year panel data are frequently a between-metro artifact
that a same-year Pearson correlation cannot distinguish from a genuine
within-metro relationship, and this time the within-metro signal doesn't
survive at all in the primary design.

**What this doesn't rule out**: (1) a faster-cycling mechanism than an
annual panel can see (the admits/discharges flow-vs-stock point from the
design proposal -- not tested here, and Vera's admits coverage itself
degrades to ~50% starting 2020, a data-quality obstacle to that follow-up);
(2) a genuine state-level policy tradeoff between incarceration and housing
investment, which the state x year FE result gestures at but can't establish
given the small effective sample; (3) relationships outside the top-50 cohort
-- this design deliberately traded cross-sectional breadth for time depth,
so a pooled top-150 version of this same FE+lag design (not run here) would
have more cross-sectional power at the cost of the shorter 2015-2023 window
the pooled cohort is stuck with anyway.

## Caveats

- `primary_state` is a coarse first-listed-state parse from MSA name;
  wrong or incomplete for genuinely cross-state MSAs (e.g. it assigns
  Washington-Arlington-Alexandria, DC-VA-MD-WV to "DC" alone, discarding
  its VA/MD/WV population). Treat the state x year FE check as suggestive
  of the right kind of confound to worry about, not a precise test of it.
- Emergency Shelter beds are 2014-2023 only (see Design); its coefficients
  above are estimated on a 1-year-shorter effective window than the other
  outcomes within the same n, since HIC's ES/TH/SH split wasn't published
  before 2014 (rows filled 0, logged out as NaN).
- `total_jail_pop` is a stock measure; this whole design cannot speak to a
  revolving-door (admissions/turnover) mechanism, only to standing
  population levels.

## Artifacts

`outputs/vera_hic_pit_longitudinal/` (gitignored):
`vera_hic_pit_longitudinal_{levels,fd}.parquet`, `spec_{a,b,c,d}_{outcome}_*.parquet`
+ `{A,B,C,D}_{outcome}_result.{parquet,json}`, `key_coefficients.csv`, plus the
2020-exclusion/no-rent-control/state-year-FE sensitivity parquets named
accordingly. Build script: `scripts/build_vera_hic_pit_longitudinal.py`.
Reuses `outputs/overdose_lag/hic_by_category/` rollups, now extended back to
2010 (this work added the 2010-2014 B2018-boundary years).
