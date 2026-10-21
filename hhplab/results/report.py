"""Offline result reports from structured, distribution-filtered payloads."""

from __future__ import annotations

import csv
import html
import io
import re
from collections.abc import Mapping, Sequence
from typing import Any

from hhplab.schema.results import RESULT_TABLE_SCHEMAS, canonical_json, validate_result_rows

TABLE_NAMES = {
    "regression_estimates": "coefficients",
    "regression_fit": "fit",
    "descriptive_stats": "descriptive",
    "correlations": "correlations",
}
STATUS_FIELDS = ("execution_status", "diagnostics_status", "findings_status", "distribution_status")
STYLE = """body{font-family:system-ui,sans-serif;max-width:1100px;margin:2rem auto;padding:0 1rem;
line-height:1.5;color:#17212b}table{border-collapse:collapse;width:100%;margin:1rem 0}
th,td{border:1px solid #ccd3da;padding:.4rem;text-align:left;vertical-align:top}
th{background:#edf2f6}.statuses{border:2px solid #40566c;padding:1rem}
pre{white-space:pre-wrap;overflow-wrap:anywhere}h2{margin-top:2rem}.muted{color:#53616d}"""


def _safe_text(value: Any) -> str:
    text = str(value)
    # Error messages can contain local runtime paths, which are not portable.
    return re.sub(r"(?<![\w:])(?:/[A-Za-z0-9_.~-]+){2,}|[A-Za-z]:\\[^\s]+", "[local path]", text)


def _escape(value: Any) -> str:
    return html.escape(_safe_text(value), quote=True)


def _display(value: Any) -> str:
    if value is None:
        return "Not evaluated"
    if isinstance(value, (Mapping, list, tuple)):
        return canonical_json(value)
    return str(value)


def _table(rows: Sequence[Mapping[str, Any]], columns: Sequence[str]) -> str:
    header = "".join(f"<th>{_escape(column)}</th>" for column in columns)
    body = "".join(
        "<tr>"
        + "".join(f"<td>{_escape(_display(row.get(column)))}</td>" for column in columns)
        + "</tr>"
        for row in rows
    )
    return f"<table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>"


def render_tables(tables: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, bytes]:
    """Render only supplied permitted rows, with identical CSV and HTML content."""
    artifacts = {}
    for name, rows in tables.items():
        if name not in TABLE_NAMES:
            raise ValueError(f"unsupported result table: {name!r}")
        validate_result_rows(name, rows)
        columns = RESULT_TABLE_SCHEMAS[name].column_names
        stream = io.StringIO(newline="")
        writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _display(row[column]) for column in columns})
        filename = TABLE_NAMES[name]
        artifacts[f"{filename}.csv"] = stream.getvalue().encode("utf-8")
        artifacts[f"{filename}.html"] = _document(filename, _table(rows, columns)).encode("utf-8")
    return artifacts


def _document(title: str, body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        f"<title>{_escape(title)}</title><style>{STYLE}</style></head><body>{body}</body></html>"
    )


def render_report(payload: Mapping[str, Any]) -> str:
    """Render every variant and status before claims, without remote assets.

    Packaging supplies already-filtered payloads. Explicit withheld entries
    receive status-only placeholders even if a caller accidentally includes
    value fields alongside them.
    """
    title = payload.get("title", payload.get("recipe_id", "Results"))
    statuses = payload.get("statuses", {})
    body = f'<h1>{_escape(title)}</h1><section class="statuses"><h2>Run status</h2>'
    body += _table(
        [{field: statuses.get(field, "unknown") for field in STATUS_FIELDS}], STATUS_FIELDS
    )
    body += f"<p>Lock: {_escape(_display(payload.get('lock_status', 'unlocked')))}</p></section>"
    body += (
        '<p class="muted">A lock records event order in this ledger. It does not establish '
        "that the researcher had not previously examined the data or tried other specifications. "
        "An independently published lock provides an external timestamp.</p>"
    )
    if payload.get("private"):
        body += "<p><strong>Private bundle: included data may not be redistributable.</strong></p>"
    body += "<h2>Analyses and variants</h2>"
    analyses = payload.get("analyses", {})
    for key, result in analyses.items():
        body += f"<h3>{_escape(key)}</h3>"
        if result.get("withheld") or result.get("distribution") == "withheld":
            body += "<p>Withheld under source redistribution terms.</p>"
            continue
        body += f"<p>Status: {_escape(result.get('status', 'unknown'))}</p>"
        if result.get("status") != "complete":
            body += f"<pre>{_escape(_display(result.get('error', 'Not evaluated')))}</pre>"
            continue
        for table_name in TABLE_NAMES:
            rows = result.get(table_name)
            if rows is None:
                continue
            if isinstance(rows, Mapping):
                rows = [rows]
            validate_result_rows(table_name, rows)
            body += _table(rows, RESULT_TABLE_SCHEMAS[table_name].column_names)
        for warning in result.get("warnings", ()):
            body += f"<p>{_escape(warning)}</p>"
    for label, field in (
        ("Diagnostics", "diagnostics"),
        ("Findings", "findings"),
        ("Research claims", "finding_claims"),
        ("Sample exclusions", "exclusions"),
        ("Errors", "errors"),
        ("Limitations", "limitations"),
        ("Withheld artifacts", "withheld"),
    ):
        body += f"<h2>{label}</h2><pre>{_escape(_display(payload.get(field, [])))}</pre>"
    body += (
        "<h2>Verification</h2><p>Move the complete bundle directory as one unit. "
        "From its parent directory, run <code>hhplab results verify BUNDLE --json</code>. "
        "Verification reports integrity, environment compatibility, and reproducibility "
        "separately. Withheld replay inputs produce a partial result.</p>"
    )
    return _document(str(title), body)


def render_readme(payload: Mapping[str, Any]) -> str:
    statuses = payload.get("statuses", {})
    lines = [_safe_text(payload.get("title", payload.get("recipe_id", "Results bundle"))), ""]
    lines.extend(f"{field}: {statuses.get(field, 'unknown')}" for field in STATUS_FIELDS)
    lines.extend(
        [
            "",
            f"Lock: {_safe_text(_display(payload.get('lock_status', 'unlocked')))}",
            "A ledger lock establishes event order, not absence of prior data inspection.",
            "Publish the exported lock independently for an external timestamp.",
            "",
            "Open report/index.html offline. Keep the complete directory together when moving it.",
            "Verify from the parent directory: hhplab results verify BUNDLE --json",
            "Withheld inputs prevent full replay; inspect withheld.json for rebuild instructions.",
        ]
    )
    return "\n".join(lines) + "\n"
