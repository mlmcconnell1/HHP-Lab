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

B2010_COVERAGE_ANNOTATION = "hud-b2010-available-archive-coverage"
B2010_COVERAGE_TEXT = (
    "This calculation uses the available 2010 HUD state-archive CoC polygons. "
    "Historical archive and registration rosters differ, some reporting IDs lack "
    "source polygons, and the extra Massachusetts shared-jurisdiction feature is "
    "withheld because its allocation is unresolved. Coverage, selection and "
    "aggregates that depend on this geography are conditional on the available "
    "polygons. The archive alone cannot establish whether a particular metro or "
    "outcome year is affected. Revisit the relevant geography or reporting scope "
    "if it matters to a specific output."
)


def read_b2010_boundary_coverage(path: Path) -> dict | None:
    """Describe incomplete historic coverage from the actual boundary artifact."""
    if not path.is_file():
        return None
    provenance = read_provenance(path)
    if (
        provenance is None
        or provenance.boundary_vintage != "2010"
        or provenance.extra.get("coverage_status") != "partial_unresolved"
        or provenance.extra.get("coverage_reference") != "native_2010_listing"
    ):
        return None
    record = {
        "id": B2010_COVERAGE_ANNOTATION,
        "revision": 1,
        "category": "geographic_approximation",
        "summary": "Conditional coverage of the available national 2010 CoC archive",
        "text": B2010_COVERAGE_TEXT,
        "public_text": B2010_COVERAGE_TEXT,
        "evidence": [{
            "name": "2010 CoC archive acquisition and reconciliation",
            "ref": "docs/historical-coc-2010-acquisition.md",
        }],
        "scope": {"years": [2010], "columns": ["geometry"]},
    }
    key = annotation_identity_key(B2010_COVERAGE_ANNOTATION, 1)
    identity = annotation_source_identity(hash_file(path)[0], provenance.to_dict())
    # Missing footprints cannot be localized by inspecting the surviving rows.
    return serialize_annotation_contract([record], uncertain_ids=[key], sources={key: [identity]})


def bind_boundary_annotations(
    crosswalk: pd.DataFrame,
    boundary_path: Path,
    boundary_vintage: str,
    weight_columns: tuple[str, ...],
) -> None:
    """Bind affected weights at their CoC and geometry year.

    All pair rows for a selected CoC use its boundary. The destination's
    independent denominators are outside the supplied weight columns.
    """
    if boundary_vintage == "2010":
        coverage = read_b2010_boundary_coverage(boundary_path)
        if coverage is not None:
            crosswalk.attrs["annotations"] = coverage
        return
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
