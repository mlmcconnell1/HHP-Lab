"""Reproduce Census BPS mix-adjusted valuation benchmark checks.

The workflow tracks two checks that justified adding
``bps_mix_adjusted_permit_value_per_unit_thousands``:

* national fixed-mix BPS valuation index versus BLS PPI WPUSI012011;
* cross-MSA distinctness versus the existing 2010-2014 BPS supply-constraint
  exposure.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

from hhplab.covariates.census_bps_contract import (
    CENSUS_BPS_CLASS_UNIT_COLUMNS,
    CENSUS_BPS_CLASS_VALUE_COLUMNS,
    CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
)
from hhplab.results.workflows._paths import OUTPUTS_ROOT, REPO_ROOT

ROOT = REPO_ROOT
OUT = OUTPUTS_ROOT / "bps_valuation_benchmark"

BPS_COUNTY_PATH = REPO_ROOT / "data/curated/covariates/covariate__census_bps__Y2000-2024.parquet"
BPS_MSA_PANEL_PATH = (
    REPO_ROOT / "data/curated/covariates/covariate_panel__census_bps__Y2000-2024.parquet"
)
BPS_COUNTY_PATH_ENV = "HHPLAB_BPS_COUNTY_PATH"
BPS_MSA_PANEL_PATH_ENV = "HHPLAB_BPS_MSA_PANEL_PATH"
BPS_FETCH_LIVE_PPI_ENV = "HHPLAB_BPS_FETCH_LIVE_PPI"

BLS_PPI_SERIES_ID = "WPUSI012011"
BLS_PPI_SERIES_LABEL = "PPI inputs to construction industries, goods"
BLS_PUBLIC_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/{series_id}"
BENCHMARK_YEARS = tuple(range(2000, 2025))
DISTINCTNESS_YEARS = tuple(range(2010, 2015))

# Official BLS annual-average observations fetched from the public API on
# 2026-07-09. Set HHPLAB_BPS_FETCH_LIVE_PPI=1 to refresh from BLS instead.
EMBEDDED_WPUSI012011_ANNUAL_AVERAGES: dict[int, float] = {
    2000: 144.1,
    2001: 142.8,
    2002: 144.0,
    2003: 147.1,
    2004: 161.5,
    2005: 169.6,
    2006: 180.2,
    2007: 183.2,
    2008: 196.4,
    2009: 189.2,
    2010: 194.5,
    2011: 201.1,
    2012: 205.8,
    2013: 209.3,
    2014: 214.4,
    2015: 213.6,
    2016: 213.9,
    2017: 221.6,
    2018: 235.7,
    2019: 235.7,
    2020: 239.2,
    2021: 303.418,
    2022: 341.533,
    2023: 331.784,
    2024: 328.522,
}


def _as_path(value: str | Path | None, default: Path) -> Path:
    return Path(value) if value else default


def _zscore(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return (numeric - numeric.mean()) / numeric.std(ddof=0)


def _safe_log(values: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").astype("float64")
    return np.log(numeric.where(numeric > 0))


def fetch_bls_ppi_annual_averages(
    *,
    years: tuple[int, ...] = BENCHMARK_YEARS,
    chunk_size: int = 10,
) -> pd.DataFrame:
    """Fetch annual-average PPI observations from the public BLS API."""
    rows: list[dict[str, object]] = []
    for start in range(min(years), max(years) + 1, chunk_size):
        end = min(start + chunk_size - 1, max(years))
        params = urllib.parse.urlencode(
            {"startyear": start, "endyear": end, "annualaverage": "true"}
        )
        url = f"{BLS_PUBLIC_API_URL.format(series_id=BLS_PPI_SERIES_ID)}?{params}"
        with urllib.request.urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if payload.get("status") != "REQUEST_SUCCEEDED":
            raise RuntimeError(
                "BLS PPI request failed. Use the embedded fallback by unsetting "
                f"{BPS_FETCH_LIVE_PPI_ENV}, or retry later. Response: {payload!r}"
            )
        for observation in payload["Results"]["series"][0]["data"]:
            if observation["period"] != "M13":
                continue
            year = int(observation["year"])
            if year in years:
                rows.append({"year": year, "ppi": float(observation["value"])})
    return pd.DataFrame(rows).drop_duplicates("year").sort_values("year").reset_index(drop=True)


def embedded_bls_ppi_annual_averages() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"year": year, "ppi": value}
            for year, value in EMBEDDED_WPUSI012011_ANNUAL_AVERAGES.items()
        ]
    ).sort_values("year", ignore_index=True)


def load_ppi_annual_averages(*, fetch_live: bool = False) -> pd.DataFrame:
    ppi = fetch_bls_ppi_annual_averages() if fetch_live else embedded_bls_ppi_annual_averages()
    missing = sorted(set(BENCHMARK_YEARS) - set(ppi["year"]))
    if missing:
        raise ValueError(f"BLS PPI series {BLS_PPI_SERIES_ID} missing benchmark years: {missing}")
    base = ppi.loc[ppi["year"].eq(min(BENCHMARK_YEARS)), "ppi"].iloc[0]
    result = ppi.copy()
    result["ppi_index_2000"] = result["ppi"] / base * 100.0
    return result


def build_national_fixed_mix_bps_index(county_bps: pd.DataFrame) -> pd.DataFrame:
    required = {"year", *CENSUS_BPS_CLASS_UNIT_COLUMNS, *CENSUS_BPS_CLASS_VALUE_COLUMNS}
    missing = sorted(required - set(county_bps.columns))
    if missing:
        raise ValueError(f"Census BPS county data missing required columns: {missing}")

    annual = (
        county_bps[county_bps["year"].isin(BENCHMARK_YEARS)]
        .groupby("year", as_index=False)[
            [*CENSUS_BPS_CLASS_UNIT_COLUMNS, *CENSUS_BPS_CLASS_VALUE_COLUMNS]
        ]
        .sum(min_count=1)
    )
    base_row = annual[annual["year"].eq(min(BENCHMARK_YEARS))]
    if base_row.empty:
        raise ValueError("Census BPS county data has no 2000 base year for fixed-mix index.")
    base_units = base_row.loc[:, list(CENSUS_BPS_CLASS_UNIT_COLUMNS)].iloc[0].astype("float64")
    base_weights = base_units / base_units.sum()

    rows: list[dict[str, float | int]] = []
    for row in annual.itertuples(index=False):
        row_data = row._asdict()
        units = np.array([float(row_data[column]) for column in CENSUS_BPS_CLASS_UNIT_COLUMNS])
        values = np.array([float(row_data[column]) for column in CENSUS_BPS_CLASS_VALUE_COLUMNS])
        value_per_unit = values / np.where(units > 0, units, np.nan)
        rows.append(
            {
                "year": int(row_data["year"]),
                "national_fixed_mix_value_per_unit": float(
                    np.nansum(value_per_unit * base_weights.to_numpy())
                ),
            }
        )

    result = pd.DataFrame(rows).sort_values("year", ignore_index=True)
    base_value = result.loc[
        result["year"].eq(min(BENCHMARK_YEARS)),
        "national_fixed_mix_value_per_unit",
    ].iloc[0]
    result["bps_index_2000"] = result["national_fixed_mix_value_per_unit"] / base_value * 100.0
    result["d_log_bps_index"] = _safe_log(result["bps_index_2000"]).diff()
    return result


def build_ppi_benchmark(county_bps: pd.DataFrame, ppi: pd.DataFrame) -> pd.DataFrame:
    benchmark = build_national_fixed_mix_bps_index(county_bps).merge(
        ppi[["year", "ppi", "ppi_index_2000"]],
        on="year",
        how="inner",
        validate="one_to_one",
    )
    benchmark["d_log_ppi_index"] = _safe_log(benchmark["ppi_index_2000"]).diff()
    return benchmark


def build_distinctness_frame(msa_bps: pd.DataFrame) -> pd.DataFrame:
    required = {
        "msa_id",
        "year",
        "permitted_units",
        "population_weight_denominator",
        CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
    }
    missing = sorted(required - set(msa_bps.columns))
    if missing:
        raise ValueError(f"Census BPS MSA panel missing required columns: {missing}")

    short = msa_bps[msa_bps["year"].isin(DISTINCTNESS_YEARS)].copy()
    short["msa_id"] = short["msa_id"].astype(str)
    short["permits_per_1000"] = (
        pd.to_numeric(short["permitted_units"], errors="coerce")
        / pd.to_numeric(short["population_weight_denominator"], errors="coerce")
        * 1000.0
    )
    grouped = short.groupby("msa_id").agg(
        observed_years=("year", "count"),
        bps_permits_per_1000_1014=("permits_per_1000", "mean"),
        mean_mix_adjusted_value_per_unit_1014=(
            CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN,
            "mean",
        ),
    )
    grouped["supply_constraint_bps"] = _zscore(-_safe_log(grouped["bps_permits_per_1000_1014"]))
    grouped["log_mean_mix_adjusted_value_per_unit_1014"] = _safe_log(
        grouped["mean_mix_adjusted_value_per_unit_1014"]
    )
    return grouped.reset_index().dropna(
        subset=["supply_constraint_bps", "log_mean_mix_adjusted_value_per_unit_1014"]
    )


def summarize_benchmark(
    ppi_benchmark: pd.DataFrame,
    distinctness: pd.DataFrame,
    msa_bps: pd.DataFrame,
) -> dict[str, object]:
    annual_changes = ppi_benchmark[["d_log_bps_index", "d_log_ppi_index"]].dropna()
    latest_year = int(ppi_benchmark["year"].max())
    latest = ppi_benchmark[ppi_benchmark["year"].eq(latest_year)].iloc[0]
    msa_panel = msa_bps.copy()
    has_value = msa_panel[CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN].notna()
    by_year = has_value.groupby(msa_panel["year"]).mean()
    missing_rate_by_msa = (
        msa_panel.groupby("msa_id")[CENSUS_BPS_MIX_ADJUSTED_VALUE_PER_UNIT_COLUMN]
        .apply(lambda values: float(values.isna().mean()))
        .rename("nan_rate")
    )
    by_msa = msa_panel.groupby("msa_id").agg(
        avg_permitted_units=("permitted_units", "mean"),
    )
    by_msa = by_msa.join(missing_rate_by_msa)
    return {
        "bls_ppi_series_id": BLS_PPI_SERIES_ID,
        "bls_ppi_series_label": BLS_PPI_SERIES_LABEL,
        "benchmark_year_range": [min(BENCHMARK_YEARS), max(BENCHMARK_YEARS)],
        "latest_year": latest_year,
        "bps_index_2000_latest": float(latest["bps_index_2000"]),
        "ppi_index_2000_latest": float(latest["ppi_index_2000"]),
        "annual_log_change_correlation": float(
            annual_changes["d_log_bps_index"].corr(annual_changes["d_log_ppi_index"])
        ),
        "annual_log_change_rows": int(len(annual_changes)),
        "distinctness_year_range": [min(DISTINCTNESS_YEARS), max(DISTINCTNESS_YEARS)],
        "distinctness_msa_count": int(len(distinctness)),
        "distinctness_correlation": float(
            distinctness["supply_constraint_bps"].corr(
                distinctness["log_mean_mix_adjusted_value_per_unit_1014"]
            )
        ),
        "msa_panel_rows": int(len(msa_panel)),
        "msa_panel_non_null_mix_adjusted_rows": int(has_value.sum()),
        "msa_panel_mix_adjusted_non_null_share": float(has_value.mean()),
        "lowest_year_non_null_share": {
            "year": int(by_year.idxmin()),
            "share": float(by_year.min()),
        },
        "msa_nan_rate_log_units_correlation": float(
            by_msa["nan_rate"].corr(_safe_log(by_msa["avg_permitted_units"]))
        ),
    }


def run_benchmark(
    *,
    bps_county_path: Path | str | None = None,
    bps_msa_panel_path: Path | str | None = None,
    fetch_live_ppi: bool = False,
) -> dict[str, object]:
    county_path = _as_path(bps_county_path, BPS_COUNTY_PATH)
    msa_path = _as_path(bps_msa_panel_path, BPS_MSA_PANEL_PATH)
    if not county_path.exists():
        raise FileNotFoundError(
            f"Census BPS county curated file not found: {county_path}. Run "
            "`uv run hhplab ingest covariate --source census_bps --raw-path <raw-dir>` first."
        )
    if not msa_path.exists():
        raise FileNotFoundError(
            f"Census BPS MSA panel not found: {msa_path}. Run "
            "`uv run hhplab aggregate covariate --source census_bps --target-geo msa "
            "--years 2000-2024` first."
        )

    county = pd.read_parquet(county_path)
    msa = pd.read_parquet(msa_path)
    ppi = load_ppi_annual_averages(fetch_live=fetch_live_ppi)
    ppi_benchmark = build_ppi_benchmark(county, ppi)
    distinctness = build_distinctness_frame(msa)
    summary = summarize_benchmark(ppi_benchmark, distinctness, msa)

    OUT.mkdir(parents=True, exist_ok=True)
    ppi_path = OUT / "bps_valuation_ppi_benchmark.parquet"
    ppi_csv_path = OUT / "bps_valuation_ppi_benchmark.csv"
    distinctness_path = OUT / "bps_valuation_distinctness.parquet"
    distinctness_csv_path = OUT / "bps_valuation_distinctness.csv"
    summary_path = OUT / "bps_valuation_benchmark_summary.json"

    ppi_benchmark.to_parquet(ppi_path, index=False)
    ppi_benchmark.to_csv(ppi_csv_path, index=False)
    distinctness.to_parquet(distinctness_path, index=False)
    distinctness.to_csv(distinctness_csv_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    return {
        "summary": summary,
        "ppi_benchmark_path": str(ppi_path),
        "ppi_benchmark_csv_path": str(ppi_csv_path),
        "distinctness_path": str(distinctness_path),
        "distinctness_csv_path": str(distinctness_csv_path),
        "summary_path": str(summary_path),
    }


def run() -> dict[str, object]:
    result = run_benchmark(
        bps_county_path=os.environ.get(BPS_COUNTY_PATH_ENV),
        bps_msa_panel_path=os.environ.get(BPS_MSA_PANEL_PATH_ENV),
        fetch_live_ppi=os.environ.get(BPS_FETCH_LIVE_PPI_ENV) == "1",
    )
    return {
        **result["summary"],
        "outputs": {
            "ppi_benchmark_parquet": result["ppi_benchmark_path"],
            "ppi_benchmark_csv": result["ppi_benchmark_csv_path"],
            "distinctness_parquet": result["distinctness_path"],
            "distinctness_csv": result["distinctness_csv_path"],
            "summary_json": result["summary_path"],
        },
    }


def main() -> None:
    result = run_benchmark(
        bps_county_path=os.environ.get(BPS_COUNTY_PATH_ENV),
        bps_msa_panel_path=os.environ.get(BPS_MSA_PANEL_PATH_ENV),
        fetch_live_ppi=os.environ.get(BPS_FETCH_LIVE_PPI_ENV) == "1",
    )
    summary = result["summary"]
    print(f"summary -> {result['summary_path']}")
    print(f"ppi benchmark -> {result['ppi_benchmark_path']}")
    print(f"distinctness -> {result['distinctness_path']}")
    print(
        "annual log-change correlation: "
        f"{summary['annual_log_change_correlation']:.3f} "
        f"({summary['annual_log_change_rows']} rows)"
    )
    print(
        "distinctness correlation: "
        f"{summary['distinctness_correlation']:.3f} "
        f"({summary['distinctness_msa_count']} MSAs)"
    )
    print(
        "MSA panel mix-adjusted non-null share: "
        f"{summary['msa_panel_mix_adjusted_non_null_share']:.3f}"
    )


if __name__ == "__main__":
    main()
