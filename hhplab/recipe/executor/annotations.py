"""Annotation propagation for recipe preparation operations.

The helpers here deliberately operate on exact ``(geography, year)`` cells.
They carry immutable records and artifact identities while rebuilding bindings
from the rows and columns that an operation actually retains.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from hhplab.artifacts.naming.naming import coc_base_path
from hhplab.geographies.xwalks.boundary_annotations import (
    B2010_COVERAGE_ANNOTATION,
    read_b2010_boundary_coverage,
)
from hhplab.recipe.executor.core import ExecutionContext, _classify_path
from hhplab.recipe.manifest import AssetRecord
from hhplab.schema.contracts import (
    annotation_identity_key,
    annotation_source_identity,
    annotations_for_source_cell,
    frame_annotation_contract,
    serialize_annotation_contract,
)
from hhplab.storage.config import load_config

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


def clear_frame_annotation_contract(frame: pd.DataFrame) -> None:
    """Remove inherited annotation metadata before rebuilding output bindings."""
    _set_output_contract(frame, None)


def _annotation_matcher(
    contract: Mapping[str, Any],
) -> Callable[[Any, int, str], list[tuple[str, bool]]]:
    """Index cell bindings once before matching the rows of a preparation step."""
    if contract["schema_version"] == 0:
        return lambda geography, year, column: annotations_for_source_cell(
            contract, geography, year, column,
        )

    exact: dict[tuple[str, Any, int], list[tuple[str, bool]]] = {}
    global_matches: list[tuple[str, bool]] = []
    uncertain_ids = set(contract["uncertain_ids"])
    for identity, binding in contract["bindings"].items():
        uncertain = identity in uncertain_ids
        if uncertain and not binding["columns"]:
            global_matches.append((identity, True))
        for column, cells in binding["columns"].items():
            for geography, year in cells:
                exact.setdefault((column, geography, year), []).append((identity, uncertain))
    for identity in uncertain_ids - contract["bindings"].keys():
        global_matches.append((identity, True))

    def matches(geography: Any, year: int, column: str) -> list[tuple[str, bool]]:
        return [*exact.get((column, geography, year), ()), *global_matches]

    return matches


def has_applicable_annotation_on_columns(
    frame: pd.DataFrame,
    columns: Sequence[str],
    *,
    geography_column: str,
    year_column: str,
) -> bool:
    """Check whether any actual source cell is annotated on selected columns."""
    contract = frame_annotation_contract(frame)
    if contract is None:
        return False
    if geography_column not in frame or year_column not in frame:
        raise ValueError("annotated source needs exact geography/year keys")
    match_annotation = _annotation_matcher(contract)
    for geography, year in zip(
        frame[geography_column], frame[year_column], strict=True,
    ):
        if pd.isna(geography) or pd.isna(year):
            continue
        geography_value = geography.item() if hasattr(geography, "item") else geography
        year_value = _integer_year(year, role="source")
        if any(
            match_annotation(geography_value, year_value, column)
            for column in columns if column in frame.columns
        ):
            return True
    return False


def is_b2010_coverage_contract(contract: Mapping[str, Any]) -> bool:
    """Recognize the artifact-wide limitation, separate from pair-specific notes."""
    key = annotation_identity_key(B2010_COVERAGE_ANNOTATION, 1)
    return (
        contract["schema_version"] == 1
        and len(contract["records"]) == 1
        and contract["records"][0]["id"] == B2010_COVERAGE_ANNOTATION
        and contract["records"][0]["revision"] == 1
        and contract["uncertain_ids"] == [key]
        and contract["bindings"] == {key: {"columns": {}}}
    )


def validate_annual_crosswalk_contract(
    crosswalk: pd.DataFrame, path: Path, ctx: ExecutionContext
) -> dict[str, Any] | None:
    """Accept only generated annual HUD crosswalk annotations with matching vintages."""
    provenance = crosswalk.attrs.get("results_panel_provenance", {})
    boundary_vintage = provenance.get("boundary_vintage")
    match = re.fullmatch(r"HUDAnnual_(\d{4})", str(boundary_vintage))
    contract = frame_annotation_contract(crosswalk)
    if contract is None:
        return None
    if match is None:
        raise ValueError(
            f"Annotated crosswalk {path.name!r} requires a supported annual HUD geometry vintage."
        )
    geometry_year = int(match[1])
    config = ctx.storage_config or load_config(
        project_root=ctx.project_root,
        recipe_output_root=ctx.recipe.output_root,
        recipe_base_dir=ctx.recipe.source_base_dir,
    )
    boundary_path = coc_base_path(str(boundary_vintage), config.asset_store_root)
    if not boundary_path.is_file():
        raise ValueError(
            f"Annotated crosswalk {path.name!r} requires registered source geometry "
            f"{boundary_vintage}; ingest or restore {boundary_path}."
        )
    boundary_identity = ctx.cache.file_identity(boundary_path)
    boundary_root, boundary_relative_path = _classify_path(boundary_path, ctx)
    ctx.consumed_assets.append(AssetRecord(
        role="geometry", path=boundary_relative_path, sha256=boundary_identity.sha256,
        size=boundary_identity.size, root=boundary_root,
    ))
    crosswalk_identity = annotation_source_identity(
        ctx.cache.file_identity(path).sha256, provenance,
    )
    for record in contract["records"]:
        key = annotation_identity_key(record["id"], record.get("revision", 1))
        sources = contract["sources"].get(key, [])
        if not any(
            source.get("sha256") == boundary_identity.sha256
            and source.get("vintages", {}).get("boundary_vintage") == str(boundary_vintage)
            for source in sources
        ):
            raise ValueError(
                f"Annotated crosswalk {path.name!r} has no verified source identity for "
                f"{boundary_vintage} on {key!r}."
            )
        for cells in contract["bindings"].get(key, {}).get("columns", {}).values():
            if any(len(cell) != 2 or int(cell[1]) != geometry_year for cell in cells):
                raise ValueError(
                    f"Annotated crosswalk {path.name!r} binds {key!r} to a year "
                    f"different from its {boundary_vintage} geometry vintage."
                )
        if crosswalk_identity not in sources:
            contract["sources"].setdefault(key, []).append(crosswalk_identity)
    _set_output_contract(crosswalk, serialize_annotation_contract(
        contract["records"], bindings=contract["bindings"],
        uncertain_ids=contract["uncertain_ids"], sources=contract["sources"],
    ))
    return contract


def resolve_b2010_crosswalk_coverage(
    crosswalk: pd.DataFrame, path: Path, ctx: ExecutionContext
) -> dict | None:
    """Resolve a generated note or the verified registered boundary for older B2010 weights."""
    provenance = crosswalk.attrs.get("results_panel_provenance", {})
    contract = frame_annotation_contract(crosswalk)
    if contract is not None and provenance.get("boundary_vintage") != "2010":
        validate_annual_crosswalk_contract(crosswalk, path, ctx)
        return None
    if contract is not None and not is_b2010_coverage_contract(contract):
        raise ValueError(f"Annotated crosswalk {path.name!r} has unsupported annotation lineage.")
    if provenance.get("boundary_vintage") != "2010":
        return None
    if contract is None:
        config = ctx.storage_config or load_config(
            project_root=ctx.project_root,
            recipe_output_root=ctx.recipe.output_root,
            recipe_base_dir=ctx.recipe.source_base_dir,
        )
        boundary_path = coc_base_path("2010", config.asset_store_root)
        contract = read_b2010_boundary_coverage(boundary_path)
        if contract is None:
            return None
        boundary = ctx.cache.file_identity(boundary_path)
        root, rel = _classify_path(boundary_path, ctx)
        ctx.consumed_assets.append(
            AssetRecord(role="geometry", path=rel, sha256=boundary.sha256, size=boundary.size,
                        root=root)
        )

    identity = ctx.cache.file_identity(path)
    key = annotation_identity_key(B2010_COVERAGE_ANNOTATION, 1)
    sources = list(contract["sources"].get(key, []))
    crosswalk_source = annotation_source_identity(identity.sha256, provenance)
    if crosswalk_source not in sources:
        sources.append(crosswalk_source)
    return serialize_annotation_contract(
        contract["records"], uncertain_ids=[key], sources={key: sources}
    )


def bind_b2010_panel_coverage(
    panel: pd.DataFrame, coverage: Mapping[str, Any] | None, *,
    geo_column: str, columns: Sequence[str],
) -> None:
    """Bind the coverage caveat to the result cells derived using B2010 geography."""
    if coverage is None or panel.empty:
        return
    if not is_b2010_coverage_contract(coverage):
        raise ValueError("Expected the artifact-wide B2010 archive coverage annotation")
    prior = frame_annotation_contract(panel) or serialize_annotation_contract([])
    key = annotation_identity_key(B2010_COVERAGE_ANNOTATION, 1)
    cells = {(geo, int(year)) for geo, year in zip(
        panel[geo_column], panel["year"], strict=True
    )}
    if key in prior["bindings"]:
        previous = prior["bindings"][key]["columns"]
    else:
        previous = {}
    bindings = {column: list(existing) for column, existing in previous.items()}
    for column in columns:
        if column in panel:
            bound_cells = cells.union(map(tuple, bindings.get(column, [])))
            bindings[column] = [
                list(cell) for cell in sorted(bound_cells, key=lambda cell: (str(cell[0]), cell[1]))
            ]
    sources = list(prior["sources"].get(key, []))
    for entry in coverage["sources"].get(key, []):
        if entry not in sources:
            sources.append(entry)
    _set_output_contract(panel, serialize_annotation_contract(
        [*prior["records"], *coverage["records"]],
        bindings={**prior["bindings"], key: {"columns": bindings}},
        uncertain_ids=sorted({*prior["uncertain_ids"], key}),
        sources={**prior["sources"], key: sources},
    ))


def bind_crosswalk_aggregate_contributors(
    output: pd.DataFrame,
    contract: Mapping[str, Any],
    contributors: Mapping[str, tuple[str | None, pd.DataFrame]],
    *,
    target_geo_column: str,
    output_geo_column: str = "geo_id",
) -> pd.DataFrame:
    """Map CoC weight bindings onto actual contributor geography and year pairs."""
    output_cells = set(output[[output_geo_column, "year"]].itertuples(index=False, name=None))
    bindings: dict[str, dict] = {}
    uncertain = set()
    for key, binding in contract["bindings"].items():
        weight_bindings = binding["columns"]
        by_column = {}
        for column, (weight_column, rows) in contributors.items():
            if column not in output:
                continue
            scoped = (weight_bindings.values() if weight_column is None
                      else [weight_bindings.get(weight_column, [])])
            cocs = {str(coc) for group in scoped for coc, _ in group}
            matching = rows.loc[rows["coc_id"].astype(str).isin(cocs)]
            cells = output_cells.intersection(
                matching[[target_geo_column, "year"]].itertuples(index=False, name=None)
            )
            if cells:
                by_column[column] = [list(cell) for cell in sorted(cells)]
        bindings[key] = {"columns": by_column}
        if by_column and key in contract["uncertain_ids"]:
            uncertain.add(key)
    _set_output_contract(output, serialize_annotation_contract(
        contract["records"],
        bindings=bindings,
        uncertain_ids=sorted(uncertain),
        sources=contract["sources"],
    ))
    return output


def bind_source_aggregate_contributors(
    output: pd.DataFrame,
    source: pd.DataFrame,
    contributors: Mapping[str, tuple[str, pd.DataFrame]],
    *,
    source_geo_column: str,
    source_year_column: str,
    output_geo_column: str = "geo_id",
    output_year_column: str = "year",
    target_geo_column: str = "geo_id",
    target_year_column: str = "year",
) -> pd.DataFrame:
    """Bind source caveats to output cells using actual contributing rows.

    Each contributor frame has source and output geography/year columns. The
    mapping key is the output measure and its value is the source measure plus
    the exact rows consumed for that output measure.
    """
    contract = _source_contract(source, year_column=source_year_column)
    if contract is None:
        return output
    source_geo = resolve_annotation_geo_column(source, source_geo_column)
    if source_geo is None:
        raise ValueError("annotated aggregation source has no resolved geography key")
    current = frame_annotation_contract(output)
    records: dict[str, dict[str, Any]] = {}
    bindings: dict[str, dict[str, list[list[Any]]]] = {}
    lineage: dict[str, list[dict[str, Any]]] = {}
    uncertain: set[str] = set()

    def add_contract(
        value: Mapping[str, Any] | None,
        *,
        preserve_bindings: bool,
        preserve_uncertain: bool,
    ) -> None:
        if value is None:
            return
        for record in value["records"]:
            identity = annotation_identity_key(record["id"], record.get("revision", 1))
            prior = records.get(identity)
            if prior is not None and prior != record:
                raise ValueError(
                    f"conflicting annotation content for {identity!r}; use a new revision"
                )
            records[identity] = record
            if preserve_bindings:
                binding = value.get("bindings", {}).get(identity, {"columns": {}})
                for column, cells in binding.get("columns", {}).items():
                    target = bindings.setdefault(identity, {}).setdefault(column, [])
                    for cell in cells:
                        if cell not in target:
                            target.append(cell)
            if preserve_uncertain and identity in value.get("uncertain_ids", []):
                uncertain.add(identity)
            for entry in value.get("sources", {}).get(identity, []):
                entries = lineage.setdefault(identity, [])
                if entry not in entries:
                    entries.append(entry)

    add_contract(current, preserve_bindings=True, preserve_uncertain=True)
    add_contract(contract, preserve_bindings=False, preserve_uncertain=False)
    source_lineage = _source_lineage(source, contract)
    for identity, entries in source_lineage.items():
        target = lineage.setdefault(identity, [])
        for entry in entries:
            if entry not in target:
                target.append(entry)

    if not output.empty and {target_geo_column, target_year_column} - set(output.columns):
        raise ValueError("annotated aggregate output needs exact geography/year keys")
    output_cells = {
        (
            geography.item() if hasattr(geography, "item") else geography,
            _integer_year(year, role="output"),
        )
        for geography, year in zip(
            output[target_geo_column], output[target_year_column], strict=True,
        )
        if not pd.isna(geography) and not pd.isna(year)
    } if (
        target_geo_column in output.columns and target_year_column in output.columns
    ) else set()
    if not output.empty and not output_cells:
        raise ValueError("annotated aggregate output needs exact geography/year keys")

    match_annotation = _annotation_matcher(contract)
    for output_column, (source_column, rows) in contributors.items():
        if output_column not in output.columns:
            continue
        if source_column not in source.columns:
            raise ValueError(f"annotated aggregation source is missing measure {source_column!r}")
        required = {
            source_geo_column, source_year_column,
            output_geo_column, output_year_column,
        }
        if required - set(rows.columns):
            raise ValueError("aggregate annotation contributors are missing exact cell keys")
        for source_value, source_year, output_value, output_year in rows[
                [source_geo_column, source_year_column, output_geo_column, output_year_column]
            ].itertuples(index=False, name=None):
            if (
                pd.isna(source_value) or pd.isna(source_year)
                or pd.isna(output_value) or pd.isna(output_year)
            ):
                continue
            source_geography = (
                source_value.item() if hasattr(source_value, "item") else source_value
            )
            source_year_value = _integer_year(source_year, role="source")
            output_cell = (
                output_value.item() if hasattr(output_value, "item") else output_value,
                _integer_year(output_year, role="output"),
            )
            if output_cell not in output_cells:
                raise ValueError(
                    "aggregate annotation contributor maps outside the exact output cells"
                )
            matches = match_annotation(source_geography, source_year_value, source_column)
            for identity, applies_uncertainly in matches:
                if identity not in records:
                    continue
                encoded_output_cell = [output_cell[0], output_cell[1]]
                cells = bindings.setdefault(identity, {}).setdefault(output_column, [])
                if encoded_output_cell not in cells:
                    cells.append(encoded_output_cell)
                if applies_uncertainly:
                    uncertain.add(identity)

    _set_output_contract(output, serialize_annotation_contract(
        list(records.values()),
        bindings={identity: {"columns": columns} for identity, columns in bindings.items()},
        uncertain_ids=sorted(uncertain),
        sources=lineage,
    ))
    return output


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
        match_annotation = _annotation_matcher(contract)
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
                matches = match_annotation(geography, year_value, source_column)
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


def reassign_annotation_year(
    source: pd.DataFrame,
    *,
    geography_column: str,
    source_year_column: str,
    target_year: int,
) -> pd.DataFrame:
    """Move the bindings of a known single-vintage source to its panel year."""
    rebound = propagate_annotations(
        source.copy(),
        [(source, frame_column_map(source, source), geography_column, source_year_column)],
        output_geo_column=geography_column,
        year_column=source_year_column,
    )
    contract = frame_annotation_contract(rebound)
    if contract is not None:
        for binding in contract["bindings"].values():
            for column, cells in binding["columns"].items():
                binding["columns"][column] = [[geography, target_year] for geography, _ in cells]
        _set_output_contract(rebound, contract)
    return rebound.assign(year=target_year)


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


