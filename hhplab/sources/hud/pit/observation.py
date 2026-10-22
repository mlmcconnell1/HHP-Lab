"""Source-declared PIT count occurrence, independent of numeric count patterns.

Reported values remain intact. Nullable booleans distinguish absent metadata
from an explicit non-count. Partial counts are observed but not complete.
"""

from __future__ import annotations

import json
import re

import pandas as pd

from hhplab.schema.columns import PIT_OBSERVATION_COLUMNS

COUNT_TYPES_FULL = "Sheltered and Unsheltered Count"
COUNT_TYPES_SHELTERED_ONLY = "Sheltered-Only Count"
COUNT_TYPES_PARTIAL = "Sheltered and Partial Unsheltered Count"
COUNT_TYPES_POLICY_VERSION = "hud_count_types/1"
COUNT_TYPES_POLICY_REF = (
    "https://www.huduser.gov/portal/sites/default/files/pdf/2024-AHAR-Part-1.pdf"
)
PIT_BOOLEAN_OBSERVATION_COLUMNS = PIT_OBSERVATION_COLUMNS[1:]
PIT_SHELTERED_OBSERVED_COLUMN = "pit_sheltered_count_observed"
_NO_COUNT = re.compile(r"^no (\d{4}) pit count$")


def _normalized_count_type(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).casefold().strip()
    return re.sub(r"[\s\-\u2010-\u2015]+", " ", text)


def _sheltered_declaration_status(value: object) -> object:
    """Classify one declaration, including nested aggregate JSON maps."""
    allowed = {
        _normalized_count_type(item)
        for item in (COUNT_TYPES_FULL, COUNT_TYPES_PARTIAL, COUNT_TYPES_SHELTERED_ONLY)
    }
    if value is None or pd.isna(value):
        return pd.NA
    text = str(value)
    try:
        declarations = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        key = _normalized_count_type(text)
        if key in allowed:
            return True
        if _NO_COUNT.fullmatch(key):
            return False
        return pd.NA
    if not isinstance(declarations, dict) or not declarations:
        return pd.NA
    statuses = [_sheltered_declaration_status(item) for item in declarations.values()]
    if any(status is False for status in statuses):
        return False
    if any(pd.isna(status) for status in statuses):
        return pd.NA
    return True


def pit_count_metadata(
    count_types: object, *, year: int | None, has_unsheltered_value: bool
) -> dict[str, object]:
    """Classify HUD's declaration; never compare counts between years.

    AHAR carries forward non-count estimates under its published policy.
    Sheltered-only 2021 zeros/values do not establish reuse: that exceptional
    year remains unknown for carry-forward status. A complete observed-only
    sample must also exclude partial counts and require a usable value.
    """
    raw = pd.NA if count_types is None or pd.isna(count_types) else str(count_types)
    key = _normalized_count_type(raw)
    observed: object = pd.NA
    carried: object = pd.NA
    partial: object = pd.NA
    if key == _normalized_count_type(COUNT_TYPES_FULL):
        observed, carried, partial = True, False, False
    elif key == _normalized_count_type(COUNT_TYPES_PARTIAL):
        observed, carried, partial = True, False, True
    else:
        no_count = _NO_COUNT.fullmatch(key)
        declared_no_count = no_count is not None and (
            year is None or int(no_count.group(1)) == year
        )
        if key == _normalized_count_type(COUNT_TYPES_SHELTERED_ONLY) or declared_no_count:
            observed, partial = False, False
            if not has_unsheltered_value:
                carried = False
            elif year is not None and year != 2021:
                carried = True
    return dict(zip(PIT_OBSERVATION_COLUMNS, (raw, observed, carried, partial), strict=True))


def with_pit_count_metadata(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive canonical flags from raw Count Types on CoC source rows."""
    result = frame.copy()
    types = result.get("count_types", pd.Series(pd.NA, index=result.index))
    years = result.get("pit_year", result.get("year", pd.Series(pd.NA, index=result.index)))
    values = result.get("pit_unsheltered", pd.Series(pd.NA, index=result.index))
    records = [
        pit_count_metadata(
            raw,
            year=None if pd.isna(year) else int(year),
            has_unsheltered_value=not pd.isna(value),
        )
        for raw, year, value in zip(types, years, values, strict=True)
    ]
    metadata = pd.DataFrame(records, index=result.index, columns=PIT_OBSERVATION_COLUMNS)
    result["count_types"] = metadata["count_types"].astype("string")
    for column in PIT_BOOLEAN_OBSERVATION_COLUMNS:
        result[column] = metadata[column].astype("boolean")
    return result


def observed_pit_mask(
    frame: pd.DataFrame, measure: str = "pit_unsheltered", *, require_value: bool = True
) -> pd.Series:
    """Select complete current-year observations; missing metadata fails closed.

    Total PIT requires an observed unsheltered component too. Sheltered PIT is
    eligible in full, partial-unsheltered and sheltered-only counts, but not in
    a declared no-PIT-count year. No reported values are altered by this helper.
    """
    if measure not in {"pit_total", "pit_sheltered", "pit_unsheltered"}:
        raise ValueError(f"Unsupported PIT measure {measure!r}; select a PIT count column.")
    available = (
        frame.get(measure, pd.Series(pd.NA, index=frame.index)).notna()
        if require_value else pd.Series(True, index=frame.index)
    )
    if measure == "pit_sheltered":
        declared_observed = frame.get(PIT_SHELTERED_OBSERVED_COLUMN)
        if declared_observed is not None:
            return (available & declared_observed.eq(True)).fillna(False)
        types = frame.get("count_types", pd.Series(pd.NA, index=frame.index))
        return (available & types.map(_sheltered_declaration_status).eq(True)).fillna(False)
    observed = frame.get(
        "unsheltered_count_observed", pd.Series(pd.NA, index=frame.index, dtype="boolean")
    )
    partial = frame.get(
        "unsheltered_count_partial", pd.Series(pd.NA, index=frame.index, dtype="boolean")
    )
    carried = frame.get(
        "unsheltered_count_carried_forward",
        pd.Series(pd.NA, index=frame.index, dtype="boolean"),
    )
    return (available & observed.eq(True) & partial.eq(False) & carried.eq(False)).fillna(False)


def aggregate_pit_count_metadata(
    frame: pd.DataFrame,
    *,
    expected_coc_count: int,
    expected_coc_ids: set[str] | None = None,
) -> dict[str, object]:
    """Describe a reported sum: all members observed, any carried/partial.

    Aggregate Count Types is a JSON map preserving each contributing CoC's
    original declaration. Unknown or missing members cannot produce an
    observed=True total merely because other members have numbers.
    """
    if not set(PIT_BOOLEAN_OBSERVATION_COLUMNS).issubset(frame.columns):
        frame = with_pit_count_metadata(frame)
    complete = len(frame) == expected_coc_count and expected_coc_count > 0
    raw = frame.get("count_types", pd.Series(pd.NA, index=frame.index))
    identifiers = frame.get("coc_id", pd.Series(frame.index.astype(str), index=frame.index))
    declarations = {
        str(coc): None if pd.isna(value) else str(value)
        for coc, value in zip(identifiers, raw, strict=True)
    }
    unique_identifiers = len(declarations) == len(frame)
    if expected_coc_ids is not None:
        declarations = {
            coc: declarations.get(coc)
            for coc in sorted(expected_coc_ids)
        }
        unique_identifiers = unique_identifiers and set(identifiers.astype(str)) == expected_coc_ids
    result: dict[str, object] = {
        "count_types": json.dumps(declarations, sort_keys=True, ensure_ascii=False)
    }
    for column in PIT_BOOLEAN_OBSERVATION_COLUMNS:
        flags = frame.get(column, pd.Series(pd.NA, index=frame.index, dtype="boolean"))
        if column == "unsheltered_count_observed":
            if flags.eq(False).fillna(False).any():
                value: object = False
            elif not complete or not unique_identifiers or flags.isna().any():
                value = pd.NA
            elif "pit_unsheltered" not in frame or frame.pit_unsheltered.isna().any():
                value = pd.NA
            else:
                value = True
        elif flags.eq(True).fillna(False).any():
            value = True
        elif not complete or not unique_identifiers or flags.isna().any():
            value = pd.NA
        else:
            value = False
        result[column] = value
    sheltered_flags = frame.get(PIT_SHELTERED_OBSERVED_COLUMN)
    if sheltered_flags is None:
        sheltered_flags = raw.map(_sheltered_declaration_status).astype("boolean")
    if sheltered_flags.eq(False).fillna(False).any():
        sheltered_value: object = False
    elif (
        not complete
        or not unique_identifiers
        or sheltered_flags.isna().any()
        or "pit_sheltered" not in frame
        or frame.pit_sheltered.isna().any()
    ):
        sheltered_value = pd.NA
    else:
        sheltered_value = True
    result[PIT_SHELTERED_OBSERVED_COLUMN] = sheltered_value
    return result
