"""Dataset-year metadata helpers for recipe execution."""

from __future__ import annotations

import re

import pandas as pd

from hhplab.recipe.executor_core import ExecutionContext, ExecutorError
from hhplab.recipe.executor_inputs import _resolve_year_column
from hhplab.recipe.planner import ResampleTask

_ACS_PATH_VINTAGE_RE = re.compile(r"__A(\d{4})")


def _normalize_acs5_vintage(value: object) -> str:
    """Normalize an ACS5 vintage value to its end-year string."""
    text = str(value).strip()
    if not text:
        raise ExecutorError("Empty ACS5 vintage value cannot be normalized.")
    if "-" in text:
        end = text.split("-")[-1]
        if end.isdigit() and len(end) == 4:
            return end
    if text.isdigit() and len(text) == 4:
        return text
    match = re.search(r"(\d{4})$", text)
    if match is not None:
        return match.group(1)
    raise ExecutorError(
        f"Could not normalize ACS5 vintage value {value!r} to a 4-digit end year."
    )


def _single_string_value(values: pd.Series, label: str) -> str | None:
    """Return a single distinct non-null string value, or None when absent."""
    distinct = values.dropna().astype(str).unique().tolist()
    if not distinct:
        return None
    if len(distinct) > 1:
        raise ExecutorError(
            f"{label} has multiple distinct values in one dataset-year slice: "
            f"{sorted(distinct)}."
        )
    return str(distinct[0])


def _fallback_dataset_year_value(
    *,
    df: pd.DataFrame,
    task: ResampleTask,
) -> str | None:
    """Return a single year-like value from the active dataset slice."""
    year_column = _resolve_year_column(df, task.year_column)
    if year_column is None or year_column not in df.columns:
        if "year" not in df.columns:
            return str(task.year)
        year_column = "year"
    return _single_string_value(
        df[year_column],
        f"Dataset '{task.dataset_id}' year {task.year}: {year_column}",
    )


def record_dataset_year_metadata(
    *,
    task: ResampleTask,
    ctx: ExecutionContext,
    df: pd.DataFrame,
) -> None:
    """Capture dataset/year provenance needed during panel assembly."""
    ds = ctx.recipe.datasets.get(task.dataset_id)
    if ds is None:
        return

    metadata: dict[str, str] = {}

    if ds.provider == "census" and ds.product in {"acs", "acs5"}:
        acs5_vintage = None
        if "acs_vintage" in df.columns:
            acs5_vintage = _single_string_value(
                df["acs_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs_vintage",
            )
            if acs5_vintage is not None:
                acs5_vintage = _normalize_acs5_vintage(acs5_vintage)
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs5_vintage = match.group(1)
        if acs5_vintage is None:
            fallback_year = _fallback_dataset_year_value(df=df, task=task)
            if fallback_year is not None:
                acs5_vintage = _normalize_acs5_vintage(fallback_year)
        if acs5_vintage is not None:
            metadata["acs5_vintage_used"] = acs5_vintage

    if ds.provider == "census" and ds.product == "acs1":
        acs1_vintage = None
        if "acs1_vintage" in df.columns:
            acs1_vintage = _single_string_value(
                df["acs1_vintage"],
                f"Dataset '{task.dataset_id}' year {task.year}: acs1_vintage",
            )
        elif task.input_path is not None:
            match = _ACS_PATH_VINTAGE_RE.search(task.input_path)
            if match is not None:
                acs1_vintage = match.group(1)
        if acs1_vintage is None:
            acs1_vintage = _fallback_dataset_year_value(df=df, task=task)
        if acs1_vintage is not None:
            metadata["acs1_vintage_used"] = acs1_vintage

    if metadata:
        ctx.dataset_year_metadata[(task.dataset_id, task.year)] = metadata
