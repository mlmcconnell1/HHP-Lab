"""Render reviewed observed-count PIT/HIC results without rerunning estimates."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_RESULTS_DIR = Path(
    "/home/matt/HHP-Output/site-boundary-history-audit/"
    "exploratory-without-sacramento/observed-counts/final-attempt-02"
)
KEY_COLUMNS = ["bed_measure", "pit_measure", "analysis", "sign", "lag"]
VALUE_COLUMNS = [
    "n_pairs", "n_sites", "pearson_r", "site_bootstrap_low", "site_bootstrap_high",
]
LAGS = (-2, -1, 0, 1, 2)
SHELTER = "hic_shelter_year_round_beds"
PSH = "hic_psh_year_round_beds"
UNSHELTERED = "pit_unsheltered"
ANNUAL = "annual_log1p_changes_fe"
LEVELS = "log1p_levels_fe"
PLOT_SPECS = (
    (
        "lead_lag_check", "Shelter and PSH beds versus unsheltered PIT", 2, 2,
        (
            (SHELTER, UNSHELTERED, LEVELS, "all", "Shelter beds: log levels"),
            (SHELTER, UNSHELTERED, ANNUAL, "all", "Shelter beds: annual changes"),
            (PSH, UNSHELTERED, LEVELS, "all", "PSH beds: log levels"),
            (PSH, UNSHELTERED, ANNUAL, "all", "PSH beds: annual changes"),
        ),
    ),
    (
        "psh_lead_lag_changes", "PSH beds versus unsheltered PIT: annual changes", 1, 3,
        (
            (PSH, UNSHELTERED, ANNUAL, "all", "All PSH changes"),
            (PSH, UNSHELTERED, ANNUAL, "increases", "PSH increases"),
            (PSH, UNSHELTERED, ANNUAL, "declines", "PSH declines"),
        ),
    ),
    (
        "shelter_lead_lag_changes", "Shelter beds versus PIT: annual changes", 3, 2,
        (
            (SHELTER, UNSHELTERED, ANNUAL, "increases", "Unsheltered PIT: bed increases"),
            (SHELTER, UNSHELTERED, ANNUAL, "declines", "Unsheltered PIT: bed declines"),
            (SHELTER, "pit_total", ANNUAL, "increases", "Total PIT: bed increases"),
            (SHELTER, "pit_total", ANNUAL, "declines", "Total PIT: bed declines"),
            (SHELTER, "pit_sheltered", ANNUAL, "increases", "Sheltered PIT: bed increases"),
            (SHELTER, "pit_sheltered", ANNUAL, "declines", "Sheltered PIT: bed declines"),
        ),
    ),
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_reviewed_results(results_dir: Path) -> tuple[pd.DataFrame, dict]:
    manifest = json.loads((results_dir / "observed_longitudinal_manifest.json").read_text())
    results = pd.read_csv(results_dir / "observed_longitudinal_results.csv")
    if results.duplicated(KEY_COLUMNS).any():
        raise ValueError("Duplicate result keys; restore the reviewed results before plotting.")
    expected = pd.DataFrame(manifest["results"])
    columns = [*KEY_COLUMNS, *VALUE_COLUMNS]
    pd.testing.assert_frame_equal(
        results.sort_values(KEY_COLUMNS)[columns].reset_index(drop=True),
        expected.sort_values(KEY_COLUMNS)[columns].reset_index(drop=True),
        check_dtype=False, check_exact=False, rtol=1e-12, atol=1e-12,
    )
    if not np.isfinite(results[VALUE_COLUMNS].to_numpy(dtype=float)).all():
        raise ValueError("Nonfinite estimates or intervals; review the results before plotting.")
    if results.site_bootstrap_low.gt(results.site_bootstrap_high).any():
        raise ValueError("Reversed confidence limits; review the results before plotting.")
    return results, manifest


def panel_rows(results: pd.DataFrame, spec: tuple) -> pd.DataFrame:
    bed, outcome, analysis, sign, _title = spec
    selected = results.loc[
        results.bed_measure.eq(bed) & results.pit_measure.eq(outcome)
        & results.analysis.eq(analysis) & results.sign.eq(sign)
    ].sort_values("lag")
    if tuple(selected.lag) != LAGS:
        raise ValueError(f"Expected exactly five calendar lags for panel {spec!r}.")
    return selected


def draw_plot(
    results: pd.DataFrame, manifest: dict, spec: tuple, output_dir: Path, source_digest: str,
) -> dict:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    stem, title, nrows, ncols, panels = spec
    tables = [panel_rows(results, panel) for panel in panels]
    height = 5.0 if nrows == 1 else 4.0 * nrows + 1.0
    width = 14.0 if ncols == 3 else 12.0
    fig, axes = plt.subplots(nrows, ncols, figsize=(width, height), sharey=True, squeeze=False)
    plotted = []
    try:
        lower = min(float(table.site_bootstrap_low.min()) for table in tables)
        upper = max(float(table.site_bootstrap_high.max()) for table in tables)
        lower = min(lower, *(float(table.pearson_r.min()) for table in tables), 0.0)
        upper = max(upper, *(float(table.pearson_r.max()) for table in tables), 0.0)
        padding = max(0.05, 0.12 * (upper - lower))
        for ax, panel, table in zip(axes.flat, panels, tables, strict=True):
            lag = table.lag.to_numpy(dtype=float)
            low = table.site_bootstrap_low.to_numpy(dtype=float)
            high = table.site_bootstrap_high.to_numpy(dtype=float)
            point = table.pearson_r.to_numpy(dtype=float)
            # Draw intervals independently: a percentile interval need not bracket the point.
            ax.vlines(lag, low, high, color="#2563a6", linewidth=1.6)
            ax.hlines(low, lag - 0.07, lag + 0.07, color="#2563a6", linewidth=1.6)
            ax.hlines(high, lag - 0.07, lag + 0.07, color="#2563a6", linewidth=1.6)
            ax.plot(lag, point, "o-", color="#123f6b", linewidth=1.3, markersize=5)
            ax.axhline(0, color="#666666", linewidth=0.9, linestyle="--")
            ax.set_title(panel[-1], fontsize=11, pad=10)
            ax.set_xticks(lag, [
                f"{int(row.lag):+d}\n{int(row.n_pairs)} / {int(row.n_sites)}"
                for row in table.itertuples(index=False)
            ])
            ax.set_ylim(max(-1.0, lower - padding), min(1.0, upper + padding))
            ax.set_xlim(-2.35, 2.35)
            ax.grid(axis="y", color="#dddddd", linewidth=0.6)
            ax.spines[["top", "right"]].set_visible(False)
            ax.tick_params(labelsize=9)
            plotted.extend(table.assign(plot=stem, panel=panel[-1]).to_dict("records"))
        fig.suptitle(f"{title}\nObserved counts · 2010–2024, excluding 2021", fontsize=14)
        fig.supylabel("Site/year-adjusted Pearson correlation", x=0.012, fontsize=11)
        fig.supxlabel(
            "PIT year − bed year: positive = beds precede PIT\n"
            "Below each lag: paired observations / contributing sites",
            y=0.14 if nrows == 1 else 0.075, fontsize=10,
        )
        fig.text(
            0.5, 0.018,
            f"Frozen universe: {manifest['sites']} sites / {manifest['cocs']} CoCs; "
            f"95% intervals from {manifest['bootstrap_replicates']:,} site bootstraps.\n"
            "PIT endpoints must be observed; annual changes use log(1 + count).\n"
            "Exploratory; intervals unadjusted for multiple comparisons; geography provisional.",
            ha="center", va="bottom", fontsize=8.5, color="#444444",
        )
        fig.tight_layout(rect=(0.025, 0.17 if nrows == 1 else 0.14, 1, 0.9))
        description = f"Reviewed observed-count PIT/HIC results. CSV SHA-256: {source_digest}"
        outputs = []
        for suffix in ("png", "svg"):
            path = output_dir / f"{stem}.{suffix}"
            metadata = {"Title": title, "Description": description}
            fig.savefig(path, dpi=180, metadata=metadata)
            outputs.append({"path": str(path), "sha256": sha256(path)})
    finally:
        plt.close(fig)
    return {"name": stem, "files": outputs, "points": plotted}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", type=Path, default=DEFAULT_RESULTS_DIR)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--non-interactive", action="store_true")
    args = parser.parse_args()
    results_dir = args.results_dir.expanduser().resolve()
    output_dir = (
        args.output_dir.expanduser().resolve()
        if args.output_dir else results_dir / "graphs" / "reviewed"
    )
    if output_dir.exists() and any(output_dir.iterdir()):
        raise SystemExit(
            f"Choose a fresh --output-dir; existing graphs are preserved: {output_dir}"
        )
    results, manifest = load_reviewed_results(results_dir)
    source_digest = sha256(results_dir / "observed_longitudinal_results.csv")
    output_dir.mkdir(parents=True, exist_ok=True)
    plots = [
        draw_plot(results, manifest, spec, output_dir, source_digest) for spec in PLOT_SPECS
    ]
    points = pd.DataFrame([row for plot in plots for row in plot.pop("points")])
    points_path = output_dir / "plotted_estimates.csv"
    points.to_csv(points_path, index=False)
    snapshot_path = output_dir / "plot_script.py"
    shutil.copyfile(__file__, snapshot_path)
    record = {
        "source_results": str(results_dir / "observed_longitudinal_results.csv"),
        "source_results_sha256": source_digest,
        "source_manifest": str(results_dir / "observed_longitudinal_manifest.json"),
        "source_manifest_sha256": sha256(results_dir / "observed_longitudinal_manifest.json"),
        "membership_identity": manifest["membership_identity"],
        "script": str(Path(__file__).resolve()), "script_sha256": sha256(Path(__file__)),
        "script_snapshot": str(snapshot_path), "script_snapshot_sha256": sha256(snapshot_path),
        "source_rows": len(results),
        "unique_plotted_rows": len(points.drop_duplicates(KEY_COLUMNS)),
        "scope": "Adjusted log levels and annual changes; raw-level tables remain in source CSV.",
        "plotted_estimates": str(points_path), "plotted_estimates_sha256": sha256(points_path),
        "plots": plots,
    }
    record_path = output_dir / "plot_manifest.json"
    record_path.write_text(json.dumps(record, indent=2) + "\n")
    if args.json:
        print(json.dumps({"status": "complete", "manifest": str(record_path), "plots": plots}))
    else:
        print(f"Wrote {len(plots)} graphs as PNG and SVG under {output_dir}")


if __name__ == "__main__":
    main()
