# Housing-Supply Instrument for Rent Growth (bead coclab-znltg)

Date: 2026-07-07

## Question

Can supply-side variation instrument metro rent growth and firm up the causal
reading of the rent -> unsheltered elasticity (OLS FD ~1.58, long difference
~1.61)?

## Data and Tooling Added

- `census_bps` county building permits now ingest directly from raw
  `co{YYYY}a.txt` annual files (`hhplab ingest covariate --source census_bps
  --raw-path data/raw/census_bps`), curated as
  `covariate__census_bps__Y2000-2024.parquet` (75,709 county-years), and
  aggregate to MSA via `hhplab aggregate covariate --source census_bps
  --target-geo msa --years 2010-2024`.
- `hhplab analyze regress` gained 2SLS: `--endogenous <term> --instruments
  <cols>`, with first-stage instrument coefficients in the output table
  (`stage == "first_stage"`) and a cluster-robust first-stage F in metadata.
  Verified against `linearmodels.IV2SLS` (long-diff estimates match to 4
  decimals; FD SEs differ only by finite-sample cluster correction, matching
  the package's existing convention).
- Saiz (2010) supply elasticities staged at
  `data/raw/saiz_elasticity/saiz2010_supply_elasticity.dta` (269 metros, from
  MIT Urban Economics Lab); 49/50 top MSAs matched by principal city
  (Sacramento absent from the source).

## Instruments

Exposure measures (z-scored across the top 50):

- `supply_constraint_bps`: -log mean 2010-2014 permitted units per 1,000
  residents (pre-sample window fully covered by PEP weights).
- `supply_constraint_bps_long`: same using mean 2000-2014 permits per 2010
  resident (county rollup for pre-PEP years).
- `saiz_inv_elasticity_z`, `saiz_unaval_z`: inverse Saiz elasticity and
  undevelopable-land share.

Annual instruments interact exposure with leave-one-out population-weighted
national January ZORI growth over all 393 MSAs (`bartik_*`). Long-difference
instruments use the static exposures directly.

Panels: `outputs/supply_iv/top50_msa_supply_iv_fd.parquet` (400 rows),
`..._longdiff.parquet` (n=50), built by `outputs/supply_iv/build_supply_iv_panel.py`.

## Results

First differences, year FE, MSA-clustered (outcome `d_log_unshelt_rate`):

| Spec | Term | Estimate | SE | p | n | First-stage F |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| OLS baseline | `d_log_zori` | 1.580 | 0.545 | 0.004 | 400 | - |
| 2SLS bartik_bps (+constraint main) | `d_log_zori` | -11.43 | 7.94 | 0.151 | 400 | 13.3 |
| 2SLS bartik_bps (no main; invalid design) | `d_log_zori` | 4.86 | 2.42 | 0.045 | 400 | 26.7 |
| 2SLS bartik_bps_long | `d_log_zori` | -18.60 | 15.36 | 0.227 | 400 | 1.9 |
| 2SLS bartik_saiz | `d_log_zori` | 1.05 | 11.83 | 0.930 | 392 | 1.3 |
| 2SLS bartik_unaval | `d_log_zori` | 6.23 | 11.62 | 0.592 | 392 | 2.0 |
| Reduced form | `bartik_bps` | 2.78 | 2.03 | 0.172 | 400 | - |
| Pre-COVID (<=2020) OLS | `d_log_zori` | 1.09 | 0.74 | 0.144 | 250 | - |
| Pre-COVID 2SLS bartik_bps | `d_log_zori` | -5.29 | 17.12 | 0.757 | 250 | 2.6 |

2015 -> 2025 long differences (n=50, homoskedastic SEs):

| Spec | Term | Estimate | SE | p | First-stage F |
| --- | --- | ---: | ---: | ---: | ---: |
| OLS | `d_log_zori_15_25` | 1.605 | 0.932 | 0.092 | - |
| 2SLS supply_constraint_bps | `d_log_zori_15_25` | 0.949 | 1.801 | 0.601 | 17.5 |
| 2SLS saiz_inv_elasticity | `d_log_zori_15_25` | -129.7 | 2218.6 | 0.954 | 0.0 |
| 2SLS saiz_unaval | `d_log_zori_15_25` | -78.4 | 798.1 | 0.922 | 0.0 |
| Reduced form | `supply_constraint_bps` | 0.087 | 0.168 | 0.609 | - |
| Reduced form | `saiz_inv_elasticity_z` | -0.126 | 0.107 | 0.245 | - |

Anderson-Rubin 95% confidence set for the long-difference IV elasticity
(inverting the reduced form): **[-3.45, +4.80]**; AR p-value at beta=0 is
0.61 and at the OLS 1.605 is 0.72.

## Interpretation

1. **The IV program neither overturns nor confirms the OLS elasticity.** The
   only instrument with a genuine first stage - the permits-based constraint in
   the 10-year long difference (F=17.5, first-stage loading +0.091 log-points
   of rent growth per SD of constraint) - delivers +0.95, the same sign and
   magnitude class as OLS (1.61), but the AR confidence set spans [-3.5, +4.8].
   At n=50 the design is honest but uninformative.
2. **The annual shift-share design fails its identifying premise in this era.**
   Conditional on the constraint main effect and year FE, the first-stage
   bartik loading is *negative* (-0.24, p<0.001; pre-COVID -0.28): constrained
   metros' rents co-moved *less* with national rent booms in 2015-2025. The
   2016-2025 national booms were elastic-sunbelt, migration-driven demand
   episodes (Austin, Phoenix, Tampa), inverting the textbook exposure logic and
   simultaneously undermining the exclusion restriction (the same migration
   directly moves homelessness risk). The wrong-signed 2SLS estimates
   (-11 to -19) should not be read causally. The no-main-effect variant
   (+4.9, p=0.045) is reported only to show sensitivity - omitting the
   exposure main effect is not a defensible design.
3. **Saiz (2010) elasticities are dead as instruments for this window**: raw
   correlation with 2015-2025 rent growth is -0.03; first-stage F <= 1.3 in
   every variant. Geography-based supply elasticity from 1970-2000 no longer
   predicts current rent growth in the top-50 sample - consistent with the
   declining-elasticity literature and itself a finding worth keeping.
4. **Where this leaves the causal claim:** rent -> unsheltered continues to
   rest on the FD timing asymmetry (contemporaneous+lag positive, lead ~0) and
   the robustness battery, not on IV. The practical route to a powered IV is
   the top-150 long-difference sample (3x cross-section for the permits
   instrument), not better annual shift-shares.

## Caveats

- The permits-intensity exposure is measured 2010-2014, after the 2000s boom
  and bust; the 2000-2014 long window loses the first stage entirely (F=1.9),
  so the short-window result should be treated as one draw of an exposure
  definition, not a robust family.
- Long-difference exclusion requires supply constraint to affect unsheltered
  growth only through rent growth - density, land values, and shelter siting
  costs are plausible violations; the AR set already prices in none of that.
- ZORI asking rents; PIT 2021 excluded throughout; DC MSA MD-508 gap notes
  from the main longitudinal record still apply.

## Artifacts

All under `outputs/supply_iv/` (gitignored): `build_supply_iv_panel.py`,
`top50_msa_supply_iv_fd.parquet`, `top50_msa_supply_iv_fd_precovid.parquet`,
`top50_msa_supply_iv_longdiff.parquet`, `saiz_match_audit.csv`, and one
`<spec>.parquet` + `<spec>.json` + manifest per regression named
`fd_*` / `ld_*` as in the tables above.
