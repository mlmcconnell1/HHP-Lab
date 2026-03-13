"""CLI command for listing available artifacts for agent-safe discovery."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Annotated

import pyarrow.parquet as pq
import typer

from coclab.builds import require_build_dir, resolve_build_dir
from coclab.provenance import read_provenance


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _classify_role(path: Path) -> str:
    p = path.as_posix()
    starts = (
        (("base/",), "base_asset"),
        (("coc_boundaries/",), "boundary"),
        (("tiger/",), "census_geometry"),
        (("xwalks/",), "crosswalk"),
        (("panel/",), "panel"),
        (("pit/",), "pit"),
        (("measures/",), "measures"),
        (("zori/",), "zori"),
        (("pep/",), "pep"),
        (("acs/",), "acs"),
    )
    for prefixes, role in starts:
        if any(p.startswith(prefix) for prefix in prefixes):
            return role

    if "/base/" in p or p.startswith("base/"):
        return "base_asset"
    if "/coc_boundaries/" in p:
        return "boundary"
    if "/tiger/" in p:
        return "census_geometry"
    if "/xwalks/" in p:
        return "crosswalk"
    if "/panel/" in p:
        return "panel"
    if "/pit/" in p:
        return "pit"
    if "/measures/" in p:
        return "measures"
    if "/zori/" in p:
        return "zori"
    if "/pep/" in p:
        return "pep"
    if "/acs/" in p:
        return "acs"
    if p.endswith("_registry.parquet"):
        return "registry"
    return "artifact"


def _safe_mtime(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime).isoformat()


def _parquet_meta(path: Path) -> dict[str, object]:
    meta: dict[str, object] = {}
    try:
        pf = pq.ParquetFile(path)
        schema = pf.schema_arrow
        meta["rows"] = pf.metadata.num_rows if pf.metadata else None
        meta["columns"] = len(schema.names)
        meta["schema_hash"] = _sha256_text(str(schema))
    except Exception:
        meta["rows"] = None
        meta["columns"] = None
        meta["schema_hash"] = None

    try:
        prov = read_provenance(path)
        if prov is not None:
            pdict = prov.to_dict()
            meta["provenance"] = {
                "boundary_vintage": pdict.get("boundary_vintage"),
                "tract_vintage": pdict.get("tract_vintage"),
                "county_vintage": pdict.get("county_vintage"),
                "acs_vintage": pdict.get("acs_vintage"),
                "weighting": pdict.get("weighting"),
                "notation": pdict.get("notation"),
                "geo_type": pdict.get("geo_type"),
                "definition_version": pdict.get("definition_version"),
            }
        else:
            meta["provenance"] = None
    except Exception:
        meta["provenance"] = None

    return meta


def _scan_scope(root: Path, scope: str) -> list[dict[str, object]]:
    if not root.exists():
        return []

    items: list[dict[str, object]] = []
    for path in sorted(root.rglob("*.parquet")):
        rel = path.relative_to(root)
        entry: dict[str, object] = {
            "scope": scope,
            "role": _classify_role(rel),
            "path": str(path),
            "relative_path": rel.as_posix(),
            "bytes": path.stat().st_size,
            "modified_at": _safe_mtime(path),
        }
        entry.update(_parquet_meta(path))
        items.append(entry)

    items.sort(key=lambda x: (str(x["role"]), str(x["relative_path"])))
    return items


def _matches_definition_version(entry: dict[str, object], defn_version: str) -> bool:
    """Check if an artifact matches the requested definition_version.

    Matches against provenance metadata and, as a fallback, against the
    ``@D{token}`` segment in the filename.
    """
    from coclab.naming import _normalize_definition_version

    norm = _normalize_definition_version(defn_version)

    # Check provenance metadata first
    prov = entry.get("provenance")
    if isinstance(prov, dict) and prov.get("definition_version") is not None:
        prov_defn = prov["definition_version"]
        return _normalize_definition_version(str(prov_defn)) == norm

    # Fallback: match @D{token} in filename
    rel = str(entry.get("relative_path", ""))
    return f"@D{norm}" in rel.lower()


def list_artifacts(
    build: Annotated[
        str,
        typer.Option(
            "--build",
            "-b",
            help="Named build directory to inventory.",
        ),
    ],
    include_global: Annotated[
        bool,
        typer.Option(
            "--include-global/--build-only",
            help="Also include global data/curated artifacts.",
        ),
    ] = True,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Emit machine-readable JSON output.",
        ),
    ] = False,
    definition_version: Annotated[
        str | None,
        typer.Option(
            "--definition-version",
            help="Filter metro artifacts to this definition version.",
        ),
    ] = None,
    geo_type: Annotated[
        str | None,
        typer.Option(
            "--geo-type",
            help="Filter artifacts by geo type ('coc' or 'metro').",
        ),
    ] = None,
) -> None:
    """List build and curated artifacts with lightweight metadata.

    Intended for automation/agent use where deterministic artifact discovery
    is preferable to ad-hoc path guessing.
    """
    try:
        build_dir = require_build_dir(build)
    except FileNotFoundError:
        build_path = resolve_build_dir(build)
        typer.echo(f"Error: Build '{build}' not found at {build_path}", err=True)
        typer.echo("Run: coclab build create --name <build>", err=True)
        raise typer.Exit(2) from None

    artifacts: list[dict[str, object]] = []
    artifacts.extend(_scan_scope(build_dir, scope="build"))

    if include_global:
        artifacts.extend(_scan_scope(Path("data/curated"), scope="global"))

    # Filter by geo_type when requested
    if geo_type is not None:
        is_metro = geo_type == "metro"
        filtered: list[dict[str, object]] = []
        for art in artifacts:
            rel = str(art.get("relative_path", ""))
            prov = art.get("provenance")
            prov_geo = prov.get("geo_type") if isinstance(prov, dict) else None

            if prov_geo is not None:
                if prov_geo == geo_type:
                    filtered.append(art)
            elif is_metro:
                if "__metro__" in rel:
                    filtered.append(art)
            else:
                if "__metro__" not in rel:
                    filtered.append(art)
        artifacts = filtered

    # Filter by definition_version when requested
    if definition_version is not None:
        artifacts = [
            art for art in artifacts
            if _matches_definition_version(art, definition_version)
        ]

    artifacts.sort(key=lambda x: (str(x["scope"]), str(x["role"]), str(x["path"])))

    if json_output:
        payload = {
            "status": "ok",
            "build": build,
            "build_dir": str(build_dir),
            "include_global": include_global,
            "count": len(artifacts),
            "artifacts": artifacts,
        }
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return

    if not artifacts:
        typer.echo("No artifacts found.")
        return

    typer.echo(f"Artifacts for build '{build}' (include_global={include_global}):\n")
    typer.echo(
        f"{'Scope':<8} {'Role':<16} {'Rows':>10} {'Cols':>6} {'Size':>10} {'Path'}"
    )
    typer.echo("-" * 120)
    for art in artifacts:
        rows = "?" if art["rows"] is None else f"{int(art['rows']):,}"
        cols = "?" if art["columns"] is None else f"{int(art['columns'])}"
        size = f"{int(art['bytes']):,}"
        typer.echo(
            f"{art['scope']:<8} {art['role']:<16} {rows:>10} {cols:>6} {size:>10} {art['path']}"
        )

    typer.echo(f"\nTotal: {len(artifacts)} artifact(s)")
