"""Annotation propagation for recipe preparation operations.

The helpers here deliberately operate on exact ``(geography, year)`` cells.
They carry immutable records and artifact identities while rebuilding bindings
from the rows and columns that an operation actually retains.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import pandas as pd

from hhplab.schema.contracts import (
    annotation_identity_key,
    annotation_source_identity,
    annotations_for_source_cell,
    frame_annotation_contract,
    serialize_annotation_contract,
)

_GEOGRAPHY_COLUMNS = (
    "geo_id", "coc_id", "msa_id", "metro_id", "county_fips", "tract_geoid",
    "county_id", "geoid", "geo",
)


def resolve_annotation_geo_column(
    frame: pd.DataFrame,
    declared: str | None = None,
) -> str | None:
    """Resolve a geography key without choosing between differing identities."""
    if declared is not None:
        if declared not in frame.columns:
            raise ValueError(f"annotated preparation input is missing geography key {declared!r}")
        return declared
    candidates = [column for column in _GEOGRAPHY_COLUMNS if column in frame.columns]
    if len(candidates) > 1:
        reference = frame[candidates[0]].reset_index(drop=True)
        if any(
            not reference.equals(frame[column].reset_index(drop=True))
            for column in candidates[1:]
        ):
            raise ValueError(
                "annotated preparation input has multiple geography keys with different "
                "values; pass the recipe's resolved geo column explicitly"
            )
    return candidates[0] if candidates else None


def _source_contract(
    frame: pd.DataFrame,
    *,
    year_column: str,
) -> dict[str, Any] | None:
    contract = frame_annotation_contract(frame)
    if contract is None:
        return None
    if frame.empty:
        return contract
    if year_column not in frame.columns:
        raise ValueError(
            f"annotated preparation input has no year key {year_column!r}; "
            "preserve source year keys before filtering or joining"
        )
    bound_columns = {
        column
        for binding in contract["bindings"].values()
        for column in binding["columns"]
    }
    if bound_columns - set(frame.columns):
        raise ValueError(
            "annotated preparation input is missing a bound column; remap annotation "
            "bindings before dropping or renaming source columns"
        )
    return contract


def _source_lineage(
    frame: pd.DataFrame,
    contract: Mapping[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    lineage = {key: list(entries) for key, entries in contract.get("sources", {}).items()}
    digest = frame.attrs.get("results_content_sha256")
    if not isinstance(digest, str):
        return lineage
    provenance = frame.attrs.get("results_panel_provenance", {})
    if not isinstance(provenance, Mapping):
        raise ValueError("annotated source provenance must be a mapping")
    identity = annotation_source_identity(digest, provenance)
    for record in contract["records"]:
        key = annotation_identity_key(record["id"], record.get("revision", 1))
        entries = lineage.setdefault(key, [])
        if identity not in entries:
            entries.append(identity)
    return lineage


def _integer_year(value: Any, *, role: str) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            f"annotated preparation {role} year values must be integers"
        ) from exc
    if not numeric.is_integer():
        raise ValueError(
            f"annotated preparation {role} year values must be integers"
        )
    return int(numeric)


def _set_output_contract(frame: pd.DataFrame, contract: Mapping[str, Any] | None) -> None:
    frame.attrs.pop("results_source_parquet_bytes", None)
    frame.attrs.pop("results_content_sha256", None)
    provenance = frame.attrs.get("results_panel_provenance")
    if isinstance(provenance, Mapping):
        provenance = dict(provenance)
        provenance.pop("annotations", None)
        extra = provenance.get("extra")
        if isinstance(extra, Mapping):
            extra = dict(extra)
            extra.pop("annotations", None)
            provenance["extra"] = extra
        frame.attrs["results_panel_provenance"] = provenance
    if contract is None:
        frame.attrs.pop("annotations", None)
    else:
        frame.attrs["annotations"] = contract


def propagate_annotations(
    output: pd.DataFrame,
    sources: Sequence[tuple],
    *,
    output_geo_column: str = "geo_id",
    year_column: str = "year",
) -> pd.DataFrame:
    """Rebind annotations from source columns onto exact retained output cells.

    Each source tuple maps source column names to output column names. Rows map
    only when their geography and year values match exactly; row aggregation or
    geography changes need a separate contributor-aware mapping.
    """
    contracts: list[
        tuple[pd.DataFrame, dict[str, Any], Mapping[str, str], str | None, str]
    ] = []
    records: dict[str, dict[str, Any]] = {}
    lineage: dict[str, list[dict[str, Any]]] = {}
    uncertain_ids: set[str] = set()
    for source in sources:
        if len(source) == 2:
            frame, column_map = source
            source_geo_column = None
            source_year_column = "year"
        elif len(source) == 4:
            frame, column_map, source_geo_column, source_year_column = source
        else:
            raise ValueError(
                "annotation source mapping must provide frame/columns, and may also "
                "provide explicit geography and year keys"
            )
        contract = _source_contract(frame, year_column=source_year_column)
        if contract is None:
            continue
        geo_column = resolve_annotation_geo_column(frame, source_geo_column)
        if not frame.empty and geo_column is None:
            raise ValueError(
                "annotated preparation input has no supported geography key; "
                "pass the recipe's resolved geo column and retain it through the operation"
            )
        source_lineage = _source_lineage(frame, contract)
        for record in contract["records"]:
            key = annotation_identity_key(record["id"], record.get("revision", 1))
            prior = records.get(key)
            if prior is not None and prior != record:
                raise ValueError(
                    f"conflicting annotation content for {key!r}; use a new revision"
                )
            records[key] = record
            for entry in source_lineage.get(key, []):
                entries = lineage.setdefault(key, [])
                if entry not in entries:
                    entries.append(entry)
        contracts.append((frame, contract, column_map, geo_column, source_year_column))

    if not contracts:
        _set_output_contract(output, None)
        return output
    if not output.empty and (
        output_geo_column not in output.columns or year_column not in output.columns
    ):
        raise ValueError(
            "annotated preparation output requires geography and year keys; "
            "keep geo_id and year through panel assembly"
        )

    bindings: dict[str, dict[str, dict[str, list[list[Any]]]]] = {
        key: {} for key in records
    }
    output_cells: set[tuple[Any, int]] = set()
    if not output.empty:
        for geography, year in zip(
            output[output_geo_column], output[year_column], strict=True,
        ):
            if pd.isna(geography) or pd.isna(year):
                continue
            year_value = _integer_year(year, role="output")
            output_cells.add((geography, year_value))

    for frame, contract, column_map, geo_column, source_year_column in contracts:
        if geo_column is None:
            continue
        exact_matches: dict[tuple[str, Any, int], list[tuple[str, bool]]] = {}
        global_matches: list[tuple[str, bool]] = []
        if contract["schema_version"] == 1:
            for record in contract["records"]:
                identity = annotation_identity_key(record["id"], record["revision"])
                uncertain = identity in contract["uncertain_ids"]
                columns = contract["bindings"].get(identity, {"columns": {}})["columns"]
                if uncertain and not columns:
                    global_matches.append((identity, True))
                for column, cells in columns.items():
                    for geography, year in cells:
                        exact_matches.setdefault((column, geography, year), []).append(
                            (identity, uncertain)
                        )
        for source_column, output_column in column_map.items():
            if source_column not in frame.columns or output_column not in output.columns:
                continue
            for geography, year in zip(
                frame[geo_column], frame[source_year_column], strict=True,
            ):
                if pd.isna(geography) or pd.isna(year):
                    continue
                year_value = _integer_year(year, role="source")
                cell = (geography, year_value)
                if cell not in output_cells:
                    continue
                matches = (
                    [*exact_matches.get((source_column, geography, year_value), ()),
                     *global_matches]
                    if contract["schema_version"] == 1 else
                    annotations_for_source_cell(contract, geography, year_value, source_column)
                )
                for identity, uncertain in matches:
                    cells = bindings[identity].setdefault(output_column, [])
                    encoded = [geography.item() if hasattr(geography, "item") else geography,
                               year_value]
                    if encoded not in cells:
                        cells.append(encoded)
                    if uncertain:
                        uncertain_ids.add(identity)

    _set_output_contract(output, serialize_annotation_contract(
        list(records.values()),
        bindings={identity: {"columns": columns}
                  for identity, columns in bindings.items()},
        uncertain_ids=sorted(uncertain_ids),
        sources=lineage,
    ))
    return output


def frame_column_map(
    source: pd.DataFrame,
    output: pd.DataFrame,
    *,
    aliases: Mapping[str, str] | None = None,
    suffix: str | None = None,
) -> dict[str, str]:
    """Map unchanged source columns through projection and declared aliases."""
    declared = aliases or {}
    mapped: dict[str, str] = {}
    for column in source.columns:
        target = declared.get(column, column)
        suffixed_target = f"{target}{suffix}" if suffix is not None else None
        if target not in output.columns and suffixed_target in output.columns:
            target = f"{target}{suffix}"
        if target in output.columns:
            mapped[column] = target
    return mapped


def renamed_column_map(source: pd.DataFrame, output: pd.DataFrame) -> dict[str, str]:
    """Map source names through a rename when the output name adds a suffix."""
    mapped: dict[str, str] = {}
    for column in source.columns:
        if column in output.columns:
            mapped[column] = column
            continue
        candidates = [
            name for name in output.columns
            if name.startswith(f"{column}_")
        ]
        if len(candidates) == 1:
            mapped[column] = candidates[0]
    return mapped


def has_annotations(frame: pd.DataFrame) -> bool:
    """Return whether a frame carries a valid annotation contract."""
    return frame_annotation_contract(frame) is not None


