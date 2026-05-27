"""MSA unemployment source resolution for MSA-CoC panels."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import pandas as pd

import hhplab.naming as naming
from hhplab.paths import curated_dir

MsaUnemploymentSource = Literal["acs5", "laus"]

MSA_UNEMPLOYMENT_COMMON_COLUMNS: tuple[str, ...] = (
    "msa_id",
    "year",
    "msa_unemployment",
    "unemployment_source",
)

MSA_ACS5_UNEMPLOYMENT_COLUMNS: tuple[str, ...] = (
    *MSA_UNEMPLOYMENT_COMMON_COLUMNS,
    "msa_unemployment_acs5",
    "msa_civilian_labor_force",
    "msa_unemployed_count",
)

MSA_LAUS_UNEMPLOYMENT_COLUMNS: tuple[str, ...] = (
    *MSA_UNEMPLOYMENT_COMMON_COLUMNS,
    "msa_unemployment_laus",
    "msa_laus_unemployment_rate_percent",
    "msa_laus_labor_force",
    "msa_laus_employed",
    "msa_laus_unemployed",
    "laus_vintage_used",
)


def resolve_msa_unemployment(
    source: MsaUnemploymentSource,
    *,
    acs5_msa: pd.DataFrame | None = None,
    laus_msa: pd.DataFrame | None = None,
    years: range | list[int] | tuple[int, ...] | None = None,
) -> pd.DataFrame:
    """Resolve the selected MSA unemployment source for an MSA-CoC panel.

    The output ``msa_unemployment`` is always a fraction. ACS5 aggregate
    unemployment rates are already fractions; BLS LAUS publishes percentages,
    so LAUS rates are divided by 100 while the raw percentage is preserved in
    ``msa_laus_unemployment_rate_percent``.
    """
    if source == "acs5":
        if acs5_msa is None:
            raise ValueError("unemployment_source='acs5' requires an ACS5 MSA DataFrame.")
        return _resolve_acs5_unemployment(acs5_msa, years=years)
    if source == "laus":
        if laus_msa is None:
            raise ValueError(
                "unemployment_source='laus' requires BLS LAUS MSA data. "
                "Run `hhplab ingest laus-metro --year YEAR` for each requested year "
                "or set unemployment_source='acs5'."
            )
        return _resolve_laus_unemployment(laus_msa, years=years)
    raise ValueError("unemployment_source must be one of: acs5, laus.")


def load_laus_msa_unemployment(
    years: range | list[int] | tuple[int, ...],
    definition_version: str,
    *,
    laus_dir: Path | None = None,
) -> pd.DataFrame:
    """Load required yearly LAUS metro artifacts or raise an actionable error."""
    if laus_dir is None:
        laus_dir = curated_dir("laus")

    frames: list[pd.DataFrame] = []
    missing: list[int] = []
    for year in _requested_years(years):
        artifact_path = laus_dir / naming.laus_metro_filename(year, definition_version)
        if not artifact_path.exists():
            canonical = naming.laus_metro_path(
                year,
                definition_version,
                base_dir=laus_dir.parent.parent if laus_dir.name == "laus" else None,
            )
            artifact_path = canonical if canonical.exists() else artifact_path
        if not artifact_path.exists():
            missing.append(year)
            continue
        frame = pd.read_parquet(artifact_path)
        frame["year"] = year if "year" not in frame.columns else frame["year"]
        frames.append(frame)

    if missing:
        _raise_missing_laus_years(missing, definition_version=definition_version)
    if not frames:
        _raise_missing_laus_years(_requested_years(years), definition_version=definition_version)
    return pd.concat(frames, ignore_index=True)


def validate_laus_years_available(
    laus_msa: pd.DataFrame,
    years: range | list[int] | tuple[int, ...],
    *,
    definition_version: str | None = None,
) -> None:
    """Validate that LAUS rows cover every requested reference year."""
    if "year" not in laus_msa.columns:
        raise ValueError(
            "BLS LAUS MSA data is missing required column 'year'. "
            "Recreate LAUS artifacts with `hhplab ingest laus-metro --year YEAR`."
        )
    available = set(pd.to_numeric(laus_msa["year"], errors="coerce").dropna().astype(int))
    missing = [year for year in _requested_years(years) if year not in available]
    if missing:
        _raise_missing_laus_years(missing, definition_version=definition_version)


def _resolve_acs5_unemployment(
    acs5_msa: pd.DataFrame,
    *,
    years: range | list[int] | tuple[int, ...] | None,
) -> pd.DataFrame:
    frame = _normalize_msa_year_frame(acs5_msa, source_label="ACS5 MSA")
    _validate_years_if_requested(frame, years, source_label="ACS5 MSA unemployment")

    if "unemployment_rate" in frame.columns:
        rate = pd.to_numeric(frame["unemployment_rate"], errors="coerce")
    elif {"civilian_labor_force", "unemployed_count"}.issubset(frame.columns):
        labor_force = pd.to_numeric(frame["civilian_labor_force"], errors="coerce")
        unemployed = pd.to_numeric(frame["unemployed_count"], errors="coerce")
        rate = unemployed / labor_force.where(labor_force > 0)
    else:
        raise ValueError(
            "ACS5 MSA unemployment requires 'unemployment_rate' or both "
            "'civilian_labor_force' and 'unemployed_count'. "
            f"Available columns: {list(acs5_msa.columns)}"
        )

    result = frame[["msa_id", "year"]].copy()
    result["msa_unemployment"] = rate.astype("Float64")
    result["unemployment_source"] = "acs5"
    result["msa_unemployment_acs5"] = result["msa_unemployment"]
    result["msa_civilian_labor_force"] = _optional_numeric(frame, "civilian_labor_force")
    result["msa_unemployed_count"] = _optional_numeric(frame, "unemployed_count")
    return result[list(MSA_ACS5_UNEMPLOYMENT_COLUMNS)]


def _resolve_laus_unemployment(
    laus_msa: pd.DataFrame,
    *,
    years: range | list[int] | tuple[int, ...] | None,
) -> pd.DataFrame:
    frame = _normalize_msa_year_frame(laus_msa, source_label="BLS LAUS MSA")
    if years is not None:
        validate_laus_years_available(frame, years)
    if "unemployment_rate" not in frame.columns:
        raise ValueError(
            "BLS LAUS MSA unemployment requires 'unemployment_rate'. "
            "Recreate LAUS artifacts with `hhplab ingest laus-metro --year YEAR`."
        )

    percent_rate = pd.to_numeric(frame["unemployment_rate"], errors="coerce")
    result = frame[["msa_id", "year"]].copy()
    result["msa_unemployment"] = (percent_rate / 100.0).astype("Float64")
    result["unemployment_source"] = "laus"
    result["msa_unemployment_laus"] = result["msa_unemployment"]
    result["msa_laus_unemployment_rate_percent"] = percent_rate.astype("Float64")
    result["msa_laus_labor_force"] = _optional_numeric(frame, "labor_force")
    result["msa_laus_employed"] = _optional_numeric(frame, "employed")
    result["msa_laus_unemployed"] = _optional_numeric(frame, "unemployed")
    result["laus_vintage_used"] = frame.get("laus_vintage_used", frame["year"]).astype("string")
    return result[list(MSA_LAUS_UNEMPLOYMENT_COLUMNS)]


def _normalize_msa_year_frame(frame: pd.DataFrame, *, source_label: str) -> pd.DataFrame:
    id_column = _msa_id_column(frame, source_label=source_label)
    if "year" not in frame.columns:
        raise ValueError(
            f"{source_label} unemployment data is missing required column 'year'. "
            f"Available columns: {list(frame.columns)}"
        )
    normalized = frame.copy()
    raw_msa_id = normalized[id_column].astype(str)
    normalized["msa_id"] = raw_msa_id.mask(
        raw_msa_id.str.fullmatch(r"\d+"),
        raw_msa_id.str.zfill(5),
    )
    normalized["year"] = pd.to_numeric(normalized["year"], errors="raise").astype(int)
    return normalized


def _msa_id_column(frame: pd.DataFrame, *, source_label: str) -> str:
    if "msa_id" in frame.columns:
        return "msa_id"
    if "metro_id" in frame.columns:
        return "metro_id"
    raise ValueError(
        f"{source_label} unemployment data requires 'msa_id' or 'metro_id'. "
        f"Available columns: {list(frame.columns)}"
    )


def _optional_numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column in frame.columns:
        return pd.to_numeric(frame[column], errors="coerce").astype("Float64")
    return pd.Series(pd.NA, index=frame.index, dtype="Float64")


def _validate_years_if_requested(
    frame: pd.DataFrame,
    years: range | list[int] | tuple[int, ...] | None,
    *,
    source_label: str,
) -> None:
    if years is None:
        return
    available = set(frame["year"].astype(int))
    missing = [year for year in _requested_years(years) if year not in available]
    if missing:
        raise ValueError(
            f"{source_label} data is missing requested year(s) {missing}. "
            "Build or load the matching ACS5 MSA aggregate before assembling the MSA-CoC panel."
        )


def _requested_years(years: range | list[int] | tuple[int, ...]) -> list[int]:
    return [int(year) for year in years]


def _raise_missing_laus_years(
    years: list[int],
    *,
    definition_version: str | None,
) -> None:
    definition_hint = (
        f" with definition_version='{definition_version}'" if definition_version else ""
    )
    commands = ", ".join(f"`hhplab ingest laus-metro --year {year}`" for year in years)
    raise ValueError(
        f"Missing BLS LAUS MSA unemployment artifact(s) for year(s) {years}{definition_hint}. "
        f"Run {commands} first, or set unemployment_source='acs5' in the MSA-CoC panel spec."
    )
