"""Generate the report figures as SVG into the ignored outputs dir."""
# ruff: noqa: B007, E402, E501, E702, I001
import os
import numpy as np
import pandas as pd
import statsmodels.formula.api as smf
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from hhplab.results.workflows.analyze_sanctuary_longdiff_robustness import (
    build_longdiff_inputs,
)

OUT = "outputs"
FIGDIR = os.path.join(OUT, "report_src", "figs")
os.makedirs(FIGDIR, exist_ok=True)

# palette roles (light mode, print target)
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
BASELINE = "#c3c2b7"
BLUE = "#2a78d6"    # categorical slot 1
AQUA = "#1baf7a"    # categorical slot 2

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE,
    "savefig.facecolor": SURFACE,
    "font.family": "sans-serif", "font.size": 9.5,
    "axes.edgecolor": BASELINE, "axes.linewidth": 1.0,
    "axes.labelcolor": SECONDARY,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "xtick.labelsize": 8.5, "ytick.labelsize": 8.5,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "grid.linestyle": "-",
    "axes.spines.top": False, "axes.spines.right": False,
    "axes.titlesize": 10.5, "axes.titlecolor": INK,
    "svg.fonttype": "none",
})

DOT = dict(s=64, color=BLUE, edgecolors=SURFACE, linewidths=1.6, zorder=3)


def style_ax(ax):
    ax.set_axisbelow(True)
    ax.spines["left"].set_color(BASELINE)
    ax.spines["bottom"].set_color(BASELINE)


def annotate(ax, x, y, text, dx=6, dy=4, ha="left"):
    ax.annotate(text, (x, y), textcoords="offset points", xytext=(dx, dy),
                fontsize=8, color=SECONDARY, ha=ha, zorder=4)


def fit_line(ax, x, y, log=False):
    xx = np.log(x) if log else np.asarray(x, dtype=float)
    b, a = np.polyfit(xx, np.log(y) if log else np.asarray(y, dtype=float), 1)
    xs = np.linspace(xx.min(), xx.max(), 100)
    ys = a + b * xs
    if log:
        ax.plot(np.exp(xs), np.exp(ys), color=BLUE, lw=2, alpha=0.55, zorder=2)
    else:
        ax.plot(xs, ys, color=BLUE, lw=2, alpha=0.55, zorder=2)


def short(name):
    return name.split("-")[0].split(",")[0]


xs = pd.read_parquet(f"{OUT}/top50_msa_untangle_A2024.parquet")

# ---------- Figure 1: shelter beds vs sheltered PIT ----------
fig, ax = plt.subplots(figsize=(6.2, 4.0))
m = xs.dropna(subset=["shelter_beds_per_1000", "pit_shelt_per_1000"]).copy()
ax.scatter(m.shelter_beds_per_1000, m.pit_shelt_per_1000, **DOT)
ax.set_xscale("log"); ax.set_yscale("log")
lims = [0.25, 12]
ax.plot(lims, lims, color=MUTED, lw=1.2, ls=(0, (4, 3)), zorder=1)
ax.text(4.4, 3.0, "dashed line: one sheltered\nperson per bed", fontsize=8, color=MUTED)
for _, r in m.iterrows():
    nm = short(r.msa_name)
    if nm in ("New York", "Boston", "Las Vegas"):
        annotate(ax, r.shelter_beds_per_1000, r.pit_shelt_per_1000, nm)
ax.set_xlim(0.4, 12); ax.set_ylim(0.25, 12)
from matplotlib.ticker import FixedLocator, NullFormatter, ScalarFormatter
for axis in (ax.xaxis, ax.yaxis):
    axis.set_major_locator(FixedLocator([0.5, 1, 2, 5, 10]))
    axis.set_major_formatter(ScalarFormatter())
    axis.set_minor_formatter(NullFormatter())
ax.set_xlabel("Shelter beds per 1,000 residents (HIC, 2024)")
ax.set_ylabel("Sheltered homeless per 1,000 residents (PIT, 2024)")
ax.set_title("Sheltered counts track shelter capacity almost perfectly (r = 0.97)")
style_ax(ax)
fig.tight_layout(); fig.savefig(f"{FIGDIR}/fig1_capacity.svg"); plt.close(fig)

# ---------- Figure 2: rent level vs unsheltered rate, 2024 cross-section ----------
fig, ax = plt.subplots(figsize=(6.2, 4.0))
m = xs.dropna(subset=["msa_median_rent", "pit_unshelt_per_1000"]).copy()
ax.scatter(m.msa_median_rent, m.pit_unshelt_per_1000, **DOT)
ax.set_yscale("log")
b, a = np.polyfit(m.msa_median_rent, np.log(m.pit_unshelt_per_1000), 1)
xr = np.linspace(m.msa_median_rent.min(), m.msa_median_rent.max(), 50)
ax.plot(xr, np.exp(a + b * xr), color=BLUE, lw=2, alpha=0.55, zorder=2)
for _, r in m.iterrows():
    nm = short(r.msa_name)
    if nm in ("Los Angeles", "San Jose", "New York", "Detroit", "Seattle", "Fresno"):
        annotate(ax, r.msa_median_rent, r.pit_unshelt_per_1000, nm,
                 **({"dx": -6, "ha": "right"} if nm in ("San Jose", "Los Angeles") else {}))
ax.yaxis.set_major_locator(FixedLocator([0.03, 0.1, 0.3, 1, 3]))
ax.yaxis.set_major_formatter(ScalarFormatter())
ax.yaxis.set_minor_formatter(NullFormatter())
ax.set_xlabel("Median gross rent, ACS 2020–2024 ($/month)")
ax.set_ylabel("Unsheltered homeless per 1,000 residents (log scale)")
ax.set_title("Expensive metros have far more people sleeping outside (r = 0.68 in logs)")
style_ax(ax)
fig.tight_layout(); fig.savefig(f"{FIGDIR}/fig2_crosssection.svg"); plt.close(fig)

# ---------- Figure 3: long difference 2015-2025 ----------
ld = pd.read_parquet(f"{OUT}/tot_longdiff.parquet")
names = xs[["msa_id", "msa_name"]]
ld = ld.merge(names, on="msa_id", how="left")
fig, ax = plt.subplots(figsize=(6.2, 4.0))
x = ld.d_log_zori_15_25 * 100
y = ld.d_log_unshelt_rate_15_25 * 100
ax.scatter(x, y, **DOT)
b, a = np.polyfit(x, y, 1)
xsr = np.linspace(x.min(), x.max(), 50)
ax.plot(xsr, a + b * xsr, color=BLUE, lw=2, alpha=0.55, zorder=2)
ax.axhline(0, color=BASELINE, lw=1)
for _, r in ld.iterrows():
    nm = short(str(r.msa_name))
    if nm in ("Phoenix", "Sacramento", "New Orleans", "Louisville", "Tampa", "Austin", "San Francisco"):
        kw = {}
        if nm in ("Sacramento", "Tampa"):
            kw = {"dx": -8, "ha": "right"}
        elif nm == "Phoenix":
            kw = {"dx": -4, "dy": 9, "ha": "right"}
        annotate(ax, r.d_log_zori_15_25 * 100, r.d_log_unshelt_rate_15_25 * 100, nm, **kw)
ax.set_xlabel("Rent growth 2015 → 2025 (ZORI, log points × 100)")
ax.set_ylabel("Unsheltered growth 2015 → 2025 (log points × 100)")
ax.set_title("Metros where rents rose most saw unsheltered homelessness grow most")
style_ax(ax)
fig.tight_layout(); fig.savefig(f"{FIGDIR}/fig3_longdiff.svg"); plt.close(fig)

# ---------- Figure 4: lag/lead coefficient plot ----------
lv = pd.read_parquet(f"{OUT}/top50_msa_longitudinal_2010_2025.parquet")
ll = lv[(lv.year_gap == 1) & (lv.lag1_gap == 1) & (lv.lead1_gap == 1)].dropna(
    subset=["d_log_unshelt_rate", "d_log_acs1_rent", "d_log_acs1_rent_lag1", "d_log_acs1_rent_lead1"])
mll = smf.ols("d_log_unshelt_rate ~ d_log_acs1_rent + d_log_acs1_rent_lag1 + d_log_acs1_rent_lead1 + C(year)",
              data=ll).fit(cov_type="cluster", cov_kwds={"groups": ll.msa_id})
rows = [
    ("NEXT year's rent change\n(placebo — should be zero)", "d_log_acs1_rent_lead1"),
    ("Same year's rent change", "d_log_acs1_rent"),
    ("PRIOR year's rent change", "d_log_acs1_rent_lag1"),
]
fig, ax = plt.subplots(figsize=(6.2, 2.9))
ypos = np.arange(len(rows))
for i, (label, term) in enumerate(rows):
    lo, hi = mll.conf_int().loc[term]
    ax.plot([lo, hi], [i, i], color=BLUE, lw=2, zorder=2, solid_capstyle="round")
    ax.scatter([mll.params[term]], [i], **DOT)
    ax.annotate(f"{mll.params[term]:+.2f}", (mll.params[term], i),
                textcoords="offset points", xytext=(0, 9), ha="center",
                fontsize=8.5, color=INK)
ax.axvline(0, color=BASELINE, lw=1.2, zorder=1)
ax.set_yticks(ypos, [r[0] for r in rows], fontsize=9, color=INK)
ax.set_ylim(-0.55, 2.75)
ax.margins(x=0.06)
ax.set_xlabel("Effect on unsheltered change (dot = estimate, bar = 95% CI)")
ax.set_title("Rent moves first, homelessness follows", loc="left")
ax.grid(axis="y", visible=False)
style_ax(ax)
fig.tight_layout(); fig.savefig(f"{FIGDIR}/fig4_laglead.svg"); plt.close(fig)
print("fig4 n =", int(mll.nobs))

# ---------- Figure 5: sanctuary margins coefficient plot ----------
_, bl, _ = build_longdiff_inputs()
specs = [
    ("Unsheltered growth", ld, "d_log_unshelt_rate_15_25"),
    ("Sheltered growth", ld, "d_log_shelt_rate_15_25"),
    ("All-program bed growth", bl, "d_log_beds_15_25"),
]
fig, ax = plt.subplots(figsize=(6.2, 2.9))
for i, (label, frame, dep) in enumerate(specs):
    f2 = frame.dropna(subset=[dep, "sanctuary"])
    mm = smf.ols(f"{dep} ~ sanctuary", data=f2).fit(cov_type="HC1")
    lo, hi = mm.conf_int().loc["sanctuary"]
    ax.plot([lo * 100, hi * 100], [i, i], color=BLUE, lw=2, zorder=2, solid_capstyle="round")
    ax.scatter([mm.params["sanctuary"] * 100], [i], **DOT)
    ax.annotate(f"{mm.params['sanctuary']*100:+.0f}", (mm.params["sanctuary"] * 100, i),
                textcoords="offset points", xytext=(0, 9), ha="center", fontsize=8.5, color=INK)
ax.axvline(0, color=BASELINE, lw=1.2, zorder=1)
ax.set_yticks(range(len(specs)), [s[0] for s in specs], fontsize=9, color=INK)
ax.set_ylim(-0.55, 2.75)
ax.margins(x=0.06)
ax.set_xlabel("Sanctuary-metro growth difference (log pts × 100, 95% CI)")
ax.set_title("Sanctuary predicts shelter growth only", loc="left")
ax.grid(axis="y", visible=False)
style_ax(ax)
fig.tight_layout(); fig.savefig(f"{FIGDIR}/fig5_sanctuary.svg"); plt.close(fig)

print("figures written to", FIGDIR)
