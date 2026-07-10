"""Contemporaneous cross-sectional correlations between HIC bed categories
and CDC overdose deaths, pooled and by year.

Unlike the entity+year-FE lag specs in build_overdose_lag_panel.py, these are
simple same-year Pearson correlations across MSAs -- no fixed effects, no
lag, no rent control. They answer "do MSAs with more of a given bed type per
capita also have more overdose deaths per capita, in the same year," which is
a much weaker, more confounded question (bigger/denser metros plausibly have
more of both). Run after build_overdose_lag_panel.py.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT

ROOT = REPO_ROOT
LEVELS_PANEL = OUTPUTS_ROOT / "overdose_lag" / "overdose_lag_levels.parquet"

CATEGORIES = ("es", "th", "sh", "rrh", "psh", "oph", "total_beds")
LABELS = {
    "es": "Emergency Shelter",
    "th": "Transitional Housing",
    "sh": "Safe Haven",
    "rrh": "Rapid Re-Housing",
    "psh": "Permanent Supportive Housing",
    "oph": "Other Permanent Housing",
    "total_beds": "All HIC beds",
}
MIN_OVERDOSE_COVERAGE = 0.8


def _clean_pair(df: pd.DataFrame, col: str) -> pd.DataFrame:
    sub = df.dropna(subset=[col, "overdose_per_1000"])
    return sub[(sub[col] > 0) & (sub["overdose_per_1000"] > 0)]


def partial_corr_controlling_pop(x: np.ndarray, y: np.ndarray, pop: np.ndarray):
    design = np.column_stack([np.ones(len(pop)), pop])
    bx, *_ = np.linalg.lstsq(design, x, rcond=None)
    by, *_ = np.linalg.lstsq(design, y, rcond=None)
    return stats.pearsonr(x - design @ bx, y - design @ by)


def run() -> dict[str, object]:
    df = pd.read_parquet(LEVELS_PANEL)
    df = df[df.overdose_coverage_ratio >= MIN_OVERDOSE_COVERAGE].copy()

    print(f"=== POOLED (all years, coverage>={MIN_OVERDOSE_COVERAGE}) ===")
    rows = []
    for cat in CATEGORIES:
        col = f"{cat}_per_1000"
        sub = _clean_pair(df, col)
        r_log, p_log = stats.pearsonr(np.log(sub[col]), np.log(sub["overdose_per_1000"]))
        r_partial, p_partial = partial_corr_controlling_pop(
            np.log(sub[col]).to_numpy(),
            np.log(sub["overdose_per_1000"]).to_numpy(),
            sub["log_pop"].to_numpy(),
        )
        rows.append(
            {
                "category": LABELS[cat],
                "r_log_log": r_log,
                "p_log_log": p_log,
                "r_partial_pop": r_partial,
                "p_partial_pop": p_partial,
                "n": len(sub),
            }
        )
        print(
            f"{LABELS[cat]:30s} log-log r={r_log:+.3f} (p={p_log:.4f})  "
            f"partial-r|pop={r_partial:+.3f} (p={p_partial:.4f})  n={len(sub)}"
        )

    print("\n=== BY YEAR (log-log r) ===")
    years = sorted(df.year.unique())
    header = f"{'Category':30s}" + "".join(f"{y:>10d}" for y in years)
    print(header)
    by_year_rows = []
    for cat in CATEGORIES:
        col = f"{cat}_per_1000"
        line = f"{LABELS[cat]:30s}"
        for year in years:
            sub = _clean_pair(df[df.year == year], col)
            if len(sub) < 5:
                line += f"{'n/a':>10s}"
                continue
            r, p = stats.pearsonr(np.log(sub[col]), np.log(sub["overdose_per_1000"]))
            by_year_rows.append(
                {"category": LABELS[cat], "year": year, "r": r, "p": p, "n": len(sub)}
            )
            marker = "*" if p < 0.05 else ""
            line += f"{r:+.2f}{marker}".rjust(10)
        print(line)

    out_dir = OUTPUTS_ROOT / "overdose_lag"
    pooled_path = out_dir / "hic_category_correlations_pooled.csv"
    by_year_path = out_dir / "hic_category_correlations_by_year.csv"
    pd.DataFrame(rows).to_csv(pooled_path, index=False)
    pd.DataFrame(by_year_rows).to_csv(by_year_path, index=False)
    return {
        "minimum_overdose_coverage": MIN_OVERDOSE_COVERAGE,
        "pooled_rows": len(rows),
        "by_year_rows": len(by_year_rows),
        "pooled": rows,
        "by_year": by_year_rows,
        "outputs": {
            "pooled_csv": str(pooled_path),
            "by_year_csv": str(by_year_path),
        },
    }


def main() -> None:
    run()


if __name__ == "__main__":
    main()
