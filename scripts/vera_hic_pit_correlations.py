"""Contemporaneous cross-sectional correlations between Vera county jail
population and (a) HIC bed categories, (b) PIT homelessness counts.

Same-year Pearson correlations across MSAs -- no fixed effects, no lag, no
rent control, same caveats as scripts/overdose_hic_category_correlations.py.
Run after build_vera_hic_pit_panel.py.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

ROOT = Path(__file__).resolve().parents[1]
PANEL = ROOT / "outputs" / "vera_hic_pit" / "vera_hic_pit_levels.parquet"

HIC_CATEGORIES = ("es", "th", "sh", "rrh", "psh", "oph", "total_beds")
HIC_LABELS = {
    "es": "Emergency Shelter",
    "th": "Transitional Housing",
    "sh": "Safe Haven",
    "rrh": "Rapid Re-Housing",
    "psh": "Permanent Supportive Housing",
    "oph": "Other Permanent Housing",
    "total_beds": "All HIC beds",
}
PIT_MARGINS = ("unshelt", "total", "shelt")
PIT_LABELS = {
    "unshelt": "Unsheltered PIT",
    "total": "Total PIT",
    "shelt": "Sheltered PIT",
}
PIT_LOG_COLUMN = {
    "unshelt": "log_unshelt_rate",
    "total": "log_total_rate",
    "shelt": "log_shelt_rate",
}


def _clean_pair(df: pd.DataFrame, col: str, other: str) -> pd.DataFrame:
    sub = df.dropna(subset=[col, other])
    return sub[(sub[col] > 0) & (sub[other] > 0)]


def partial_corr_controlling_pop(x: np.ndarray, y: np.ndarray, pop: np.ndarray):
    design = np.column_stack([np.ones(len(pop)), pop])
    bx, *_ = np.linalg.lstsq(design, x, rcond=None)
    by, *_ = np.linalg.lstsq(design, y, rcond=None)
    return stats.pearsonr(x - design @ bx, y - design @ by)


def run_family(df: pd.DataFrame, keys: tuple, labels: dict, rate_col_fn, log_source_fn):
    print(f"\n=== POOLED (all years) ===")
    pooled_rows = []
    for key in keys:
        col = rate_col_fn(key)
        sub = _clean_pair(df, col, "jail_per_1000")
        r_log, p_log = stats.pearsonr(np.log(sub[col]), np.log(sub["jail_per_1000"]))
        r_partial, p_partial = partial_corr_controlling_pop(
            np.log(sub[col]).to_numpy(), np.log(sub["jail_per_1000"]).to_numpy(),
            sub["log_pop"].to_numpy(),
        )
        pooled_rows.append(
            {"category": labels[key], "r_log_log": r_log, "p_log_log": p_log,
             "r_partial_pop": r_partial, "p_partial_pop": p_partial, "n": len(sub)}
        )
        print(f"{labels[key]:30s} log-log r={r_log:+.3f} (p={p_log:.4f})  "
              f"partial-r|pop={r_partial:+.3f} (p={p_partial:.4f})  n={len(sub)}")

    print("\n=== BY YEAR (log-log r) ===")
    years = sorted(df.year.unique())
    header = f"{'Category':30s}" + "".join(f"{y:>10d}" for y in years)
    print(header)
    by_year_rows = []
    for key in keys:
        col = rate_col_fn(key)
        line = f"{labels[key]:30s}"
        for year in years:
            sub = _clean_pair(df[df.year == year], col, "jail_per_1000")
            if len(sub) < 5:
                line += f"{'n/a':>10s}"
                continue
            r, p = stats.pearsonr(np.log(sub[col]), np.log(sub["jail_per_1000"]))
            by_year_rows.append({"category": labels[key], "year": year, "r": r, "p": p, "n": len(sub)})
            marker = "*" if p < 0.05 else ""
            line += f"{r:+.2f}{marker}".rjust(10)
        print(line)
    return pooled_rows, by_year_rows


def main() -> None:
    df = pd.read_parquet(PANEL)

    print("############ Vera jail vs HIC bed categories ############")
    hic_pooled, hic_by_year = run_family(
        df, HIC_CATEGORIES, HIC_LABELS, lambda k: f"{k}_per_1000", None
    )

    print("\n\n############ Vera jail vs PIT homelessness margins ############")
    df_pit = df.copy()
    for margin, log_col in PIT_LOG_COLUMN.items():
        rate_col = f"{margin}_pit_per_1000"
        df_pit[rate_col] = np.exp(df_pit[log_col])
    pit_pooled, pit_by_year = run_family(
        df_pit, PIT_MARGINS, PIT_LABELS, lambda k: f"{k}_pit_per_1000", None
    )

    out_dir = ROOT / "outputs" / "vera_hic_pit"
    pd.DataFrame(hic_pooled).to_csv(out_dir / "vera_hic_correlations_pooled.csv", index=False)
    pd.DataFrame(hic_by_year).to_csv(out_dir / "vera_hic_correlations_by_year.csv", index=False)
    pd.DataFrame(pit_pooled).to_csv(out_dir / "vera_pit_correlations_pooled.csv", index=False)
    pd.DataFrame(pit_by_year).to_csv(out_dir / "vera_pit_correlations_by_year.csv", index=False)


if __name__ == "__main__":
    main()
