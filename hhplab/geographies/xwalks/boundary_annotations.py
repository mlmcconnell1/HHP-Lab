"""Carry source geometry annotations into generated CoC crosswalk weights."""

import re
from pathlib import Path

import pandas as pd

from hhplab.schema.contracts import (
    annotation_identity_key,
    annotation_source_identity,
    parse_annotation_contract,
    serialize_annotation_contract,
)
from hhplab.storage.provenance import read_provenance
from hhplab.storage.raw_snapshot import hash_file


def bind_annual_boundary_annotations(
    crosswalk: pd.DataFrame,
    boundary_path: Path,
    boundary_vintage: str,
    weight_columns: tuple[str, ...],
) -> None:
    """Bind affected weights at their CoC and geometry year.

    All pair rows for a selected CoC use its boundary. The destination's
    independent denominators are outside the supplied weight columns.
    """
    annual = re.fullmatch(r"HUDAnnual_(\d{4})", boundary_vintage)
    if annual is None:
        return
    provenance = read_provenance(boundary_path)
    if provenance is None or "annotations" not in provenance.extra:
        return
    source = parse_annotation_contract(provenance.extra["annotations"])
    if not source["records"]:
        return
    year = int(annual[1])
    output_cocs = set(crosswalk["coc_id"].dropna().astype(str))
    columns = [column for column in weight_columns if column in crosswalk]
    boundary_identity = annotation_source_identity(
        hash_file(boundary_path)[0], provenance.to_dict()
    )
    bindings = {}
    sources = {}
    uncertain = []
    for record in source["records"]:
        key = annotation_identity_key(record["id"], record["revision"])
        source_columns = source["bindings"].get(key, {}).get("columns", {})
        cells = sorted({
            (str(coc), year) for coc, source_year in source_columns.get("geometry", [])
            if source_year == year and str(coc) in output_cocs
        })
        bindings[key] = {
            "columns": {column: [list(cell) for cell in cells] for column in columns if cells}
        }
        sources[key] = list(source["sources"].get(key, []))
        if boundary_identity not in sources[key]:
            sources[key].append(boundary_identity)
        if key in source["uncertain_ids"] or source["schema_version"] == 0:
            uncertain.append(key)
    crosswalk.attrs["annotations"] = serialize_annotation_contract(
        source["records"], bindings=bindings, sources=sources, uncertain_ids=uncertain
    )
