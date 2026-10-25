"""Lagged panel associations."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .contracts import AnalysisError, AnalysisResult
from .persistence import _persist_result, _read_panel, _require_columns


def lagged_associations_panel(
    panel_path: Path,
    *,
    outcome: str,
    predictors: list[str],
    lags: list[int],
    entity_column: str = "geo_id",
    year_column: str = "year",
    output_path: Path | None = None,
) -> AnalysisResult:
    """Correlate an outcome with lagged predictor values by entity-year.

    Lags align by exact calendar year: for lag ``L`` the outcome at year ``Y``
    is paired with the predictor at year ``Y - L`` for the same entity. Missing
    intermediate years are never bridged -- e.g. if 2021 is excluded from the
    panel, 2020 and 2022 pair only at lag 2, not lag 1.
    """
    if not lags or any(lag < 1 for lag in lags):
        raise AnalysisError("lagged associations require one or more positive lags.")
    df = _read_panel(panel_path)
    _require_columns(
        df,
        [outcome, *predictors, entity_column, year_column],
        context="lagged",
    )
    work = df[[outcome, *predictors, entity_column, year_column]].copy()
    work = _normalize_lagged_panel_keys(work, entity_column=entity_column, year_column=year_column)
    for column in [outcome, *predictors]:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    rows: list[dict[str, Any]] = []
    base = work[[entity_column, year_column, outcome]]
    for predictor in predictors:
        source = work[[entity_column, year_column, predictor]]
        for lag in sorted(set(lags)):
            # Predictor at year Y - lag pairs with the outcome at year Y, so the
            # lagged source key is source_year + lag.
            lagged = source.assign(__lag_source_year=source[year_column] + lag)
            pair = (
                base.merge(
                    lagged[[entity_column, "__lag_source_year", predictor]],
                    left_on=[entity_column, year_column],
                    right_on=[entity_column, "__lag_source_year"],
                    how="inner",
                )[[outcome, predictor]]
                .dropna()
            )
            rows.append(
                {
                    "outcome": outcome,
                    "predictor": predictor,
                    "lag": lag,
                    "n": int(len(pair)),
                    "correlation": (
                        float(pair[outcome].corr(pair[predictor])) if len(pair) > 1 else np.nan
                    ),
                }
            )
    table = pd.DataFrame(rows)
    return _persist_result(
        table,
        panel_path=panel_path,
        output_path=output_path,
        analysis_type="lagged",
        parameters={
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "entity_column": entity_column,
            "year_column": year_column,
        },
        metadata={
            "analysis_type": "lagged",
            "outcome": outcome,
            "predictors": predictors,
            "lags": sorted(set(lags)),
            "association_count": len(rows),
        },
    )


def _python_scalar(value: Any) -> Any:
    return value.item() if isinstance(value, np.generic) else value


def _key_is_null(value: Any) -> bool:
    return value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value))


def _is_valid_entity_key(value: Any) -> bool:
    return not _key_is_null(value) and not isinstance(value, bool) and isinstance(value, (str, int))


def _reject_duplicate_keys(work: pd.DataFrame, *, entity_column: str, year_column: str) -> None:
    duplicate_rows = work[work.duplicated(subset=[entity_column, year_column], keep=False)]
    if duplicate_rows.empty:
        return
    repeated_keys = duplicate_rows[[entity_column, year_column]].drop_duplicates()
    first = tuple(repeated_keys.iloc[0].tolist())
    raise AnalysisError(
        f"lagged panel contains duplicate {entity_column}/{year_column} keys "
        f"({len(duplicate_rows)} rows spanning {len(repeated_keys)} repeated key pairs; "
        f"first repeated key: {first!r}). Aggregate or deduplicate the panel so each "
        f"entity-year appears once before lagged analysis."
    )


def _normalize_lagged_panel_keys(
    work: pd.DataFrame, *, entity_column: str, year_column: str
) -> pd.DataFrame:
    """Validate entity/year key columns and normalize years to integers.

    Rows with null, non-string/non-integer entity keys, or null, non-numeric,
    or non-integral years are rejected because exact calendar alignment is
    meaningless without well-formed keys.
    """
    # Reset to a positional index so error reporting and example extraction use
    # row positions rather than whatever index the caller's panel carries.
    work = work.copy().reset_index(drop=True)
    entities = work[entity_column].map(_python_scalar)
    years_raw = work[year_column].map(_python_scalar)

    bad_entity = entities.map(lambda value: not _is_valid_entity_key(value))
    if bad_entity.any():
        positions = [int(index) for index in bad_entity[bad_entity].index[:5]]
        raise AnalysisError(
            f"lagged entity column {entity_column!r} must contain non-null string or "
            f"integer values; found invalid values at row positions {positions}. "
            f"Clean the entity column before lagged analysis."
        )

    boolean_years = years_raw.map(lambda value: isinstance(value, bool))
    numeric_years = pd.to_numeric(years_raw, errors="coerce")
    non_numeric = numeric_years.isna()
    non_integral = ~non_numeric & (numeric_years % 1 != 0)
    invalid_years = boolean_years | non_numeric | non_integral
    if invalid_years.any():
        examples = [years_raw.iloc[index] for index in invalid_years[invalid_years].index[:5]]
        raise AnalysisError(
            f"lagged year column {year_column!r} must contain non-null integral years; "
            f"found {int(invalid_years.sum())} invalid value(s), e.g. {examples}. "
            f"Normalize the year column to integer years before lagged analysis."
        )

    work[entity_column] = entities
    work[year_column] = numeric_years.astype("int64")
    _reject_duplicate_keys(work, entity_column=entity_column, year_column=year_column)
    return work
