"""Recompute the report's headline statistics from the panel artifacts.

Each block prints a labeled result; expected values from the session record
are noted in comments so mismatches are obvious.
"""
# ruff: noqa: E501
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf

from hhplab.results.workflows.analyze_sanctuary_longdiff_robustness import (
    build_longdiff_inputs,
)

OUT = "outputs"

# ---------- 1. Capacity: beds vs sheltered PIT, 2024 cross-section ----------
xs = pd.read_parquet(f"{OUT}/top50_msa_untangle_A2024.parquet")
m = xs.dropna(subset=["shelter_beds_per_1000", "pit_shelt_per_1000"])
r = np.corrcoef(np.log(m.shelter_beds_per_1000), np.log(m.pit_shelt_per_1000))[0, 1]
lin = np.corrcoef(m.shelter_beds_per_1000, m.pit_shelt_per_1000)[0, 1]
occ = (m.pit_sheltered.sum() / (m.shelter_beds_per_1000 * m.total_population / 1000).sum()
       if "pit_sheltered" in m else np.nan)
print(f"[1] beds~sheltered log r={r:.3f} (expect ~0.95+), linear r={lin:.3f}, n={len(m)}")

# ---------- 2. Cross-section: rent level vs unsheltered rate ----------
m2 = xs.dropna(subset=["msa_median_rent", "pit_unshelt_per_1000"])
r2 = np.corrcoef(np.log(m2.msa_median_rent), np.log(m2.pit_unshelt_per_1000))[0, 1]
print(f"[2] log median rent ~ log unshelt rate r={r2:.3f} (expect ~0.69), n={len(m2)}")

# ---------- 3. Annual FD with year FE, ZORI, clustered (expect ~+1.58 p=0.004) ----------
fd = pd.read_parquet(f"{OUT}/top50_msa_fd_2015_2025.parquet")
fd1 = fd[(fd.year_gap == 1)].dropna(subset=["d_log_unshelt_rate", "d_log_zori", "d_log_pop"])
mfd = smf.ols("d_log_unshelt_rate ~ d_log_zori + d_log_pop + C(year)", data=fd1).fit(
    cov_type="cluster", cov_kwds={"groups": fd1.msa_id})
print(f"[3] FD ZORI elasticity={mfd.params['d_log_zori']:.3f} p={mfd.pvalues['d_log_zori']:.4f} n={int(mfd.nobs)}")

# ---------- 4. Levels FE ACS1 (expect +2.70 p<0.001 n=744) ----------
lv = pd.read_parquet(f"{OUT}/top50_msa_longitudinal_2010_2025.parquet")
lv1 = lv.dropna(subset=["log_unshelt_rate", "log_acs1_rent", "log_pop"])
mlv = smf.ols("log_unshelt_rate ~ log_acs1_rent + log_pop + C(msa_id) + C(year)", data=lv1).fit(
    cov_type="cluster", cov_kwds={"groups": lv1.msa_id})
print(f"[4] levels FE ACS1 elasticity={mlv.params['log_acs1_rent']:.3f} p={mlv.pvalues['log_acs1_rent']:.4f} n={int(mlv.nobs)}")

# ---------- 5. Lag/lead asymmetry ACS1 (expect contemp +0.68, lag1 +0.93, lead1 +0.08, n=544) ----------
ll = lv[(lv.year_gap == 1) & (lv.lag1_gap == 1) & (lv.lead1_gap == 1)].dropna(
    subset=["d_log_unshelt_rate", "d_log_acs1_rent", "d_log_acs1_rent_lag1", "d_log_acs1_rent_lead1"])
mll = smf.ols("d_log_unshelt_rate ~ d_log_acs1_rent + d_log_acs1_rent_lag1 + d_log_acs1_rent_lead1 + C(year)",
              data=ll).fit(cov_type="cluster", cov_kwds={"groups": ll.msa_id})
for t in ["d_log_acs1_rent", "d_log_acs1_rent_lag1", "d_log_acs1_rent_lead1"]:
    ci = mll.conf_int().loc[t]
    print(f"[5] {t}: b={mll.params[t]:+.3f} p={mll.pvalues[t]:.3f} ci=({ci[0]:+.2f},{ci[1]:+.2f}) n={int(mll.nobs)}")

# ---------- 6. Sanctuary long-diff margins (expect unshelt ~0 p~1.0; shelt +0.53; beds +0.54) ----------
ld = pd.read_parquet(f"{OUT}/tot_longdiff.parquet")
_, bl, _ = build_longdiff_inputs()
for name, frame, dep in [("unsheltered", ld, "d_log_unshelt_rate_15_25"),
                         ("sheltered", ld, "d_log_shelt_rate_15_25"),
                         ("beds", bl, "d_log_beds_15_25")]:
    f2 = frame.dropna(subset=[dep, "sanctuary"])
    mm = smf.ols(f"{dep} ~ sanctuary", data=f2).fit(cov_type="HC1")
    ci = mm.conf_int().loc["sanctuary"]
    print(f"[6] sanctuary→{name} growth 15-25: b={mm.params['sanctuary']:+.3f} p={mm.pvalues['sanctuary']:.4f} "
          f"ci=({ci[0]:+.2f},{ci[1]:+.2f}) n={len(f2)}")

# ---------- 7. Long diff rent growth vs unsheltered growth (expect ~+1.6 p~0.08-0.16) ----------
f3 = ld.dropna(subset=["d_log_unshelt_rate_15_25", "d_log_zori_15_25"])
m7 = smf.ols("d_log_unshelt_rate_15_25 ~ d_log_zori_15_25", data=f3).fit(cov_type="HC1")
print(f"[7] longdiff 15-25 elasticity={m7.params['d_log_zori_15_25']:.3f} p={m7.pvalues['d_log_zori_15_25']:.4f} n={len(f3)}")
