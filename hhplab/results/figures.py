"""Public, permission-filtered figures and research tables for result bundles."""

from __future__ import annotations

import csv
import io
import math
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

_ESTIMATE_TABLE = "results/regression_estimates.parquet"
_FIT_TABLE = "results/regression_fit.parquet"
_SVG_NS = "http://www.w3.org/2000/svg"
_ID_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")
_CSV_NUMBER_PATTERN = re.compile(r"^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


@dataclass(frozen=True)
class ResearchAssets:
    """Generated publication assets and the exact values that support them."""

    assets: dict[str, bytes]
    figures_markdown: str
    research_markdown: str
    result_values: tuple[Mapping[str, Any], ...]
    artifacts: tuple[Mapping[str, Any], ...]


class FigureError(ValueError):
    """A safe, stable failure while building public figure or table content."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def render_research_assets(
    *,
    figure_findings: tuple[str, ...],
    findings: Mapping[str, Mapping[str, Any]],
    declarations: Mapping[str, Mapping[str, Any]],
    references: Sequence[Mapping[str, Any]],
    analyses: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any],
    rows: Callable[[str], Sequence[Mapping[str, Any]]],
    optional_rows: Callable[[str], Sequence[Mapping[str, Any]]],
    unit_for: Callable[[str, str, str | None], str | None],
    escape_text: Callable[[str], str],
) -> ResearchAssets:
    """Render aggregate estimate plots and bundle-backed research tables.

    Required rows come only through ``rows``. Optional research tables come
    through ``optional_rows`` so denied optional material can be omitted. The
    callbacks are deliberately invoked outside plotting error handling so
    their permission and publication exceptions keep their original types.
    """
    if not isinstance(figure_findings, tuple) or any(
        not isinstance(item, str) or not item for item in figure_findings
    ):
        raise FigureError("invalid_figure_request", "Figure requests are invalid.")
    if len(figure_findings) != len(set(figure_findings)):
        raise FigureError("invalid_figure_request", "Figure requests must be unique.")

    cited_findings = tuple(findings)
    if any(key not in declarations for key in cited_findings):
        raise FigureError("invalid_finding", "A cited finding lacks its declaration.")
    if any(key not in findings for key in figure_findings):
        raise FigureError("invalid_figure_request", "A figure references an uncited finding.")

    estimate_rows = tuple(rows("regression_estimates")) if figure_findings else tuple(
        optional_rows("regression_estimates")
    )
    fit_rows = tuple(optional_rows("regression_fit"))
    _validate_rows(estimate_rows, "regression_estimates")
    _validate_rows(fit_rows, "regression_fit")

    result_values: list[Mapping[str, Any]] = []
    seen_values: set[tuple[str, str]] = set()
    assets: dict[str, bytes] = {}
    artifacts: list[Mapping[str, Any]] = []
    figure_links: list[tuple[str, str]] = []

    figure_rows: dict[str, list[dict[str, Any]]] = {}
    asset_ids: dict[str, str] = {}
    for finding_id in figure_findings:
        asset_id = _asset_id(finding_id)
        if asset_id in asset_ids and asset_ids[asset_id] != finding_id:
            raise FigureError(
                "asset_name_collision",
                "Finding identifiers map to the same public asset name.",
            )
        asset_ids[asset_id] = finding_id
    for finding_id in figure_findings:
        finding_references = [
            reference
            for reference in references
            if reference.get("owner_id") == finding_id
            and reference.get("base_term") is not None
        ]
        if not finding_references:
            raise FigureError(
                "missing_estimate_reference",
                "An estimate plot requires a cited estimate criterion.",
            )
        selected = [
            _resolve_estimate_row(reference, estimate_rows)
            for reference in finding_references
        ]
        plot_rows = []
        for reference, row in zip(finding_references, selected, strict=True):
            key = reference["result_key"]
            base_term = reference["base_term"]
            analysis = analyses.get(key)
            if not isinstance(analysis, Mapping):
                raise FigureError("invalid_analysis", "A cited analysis is unavailable.")
            unit = unit_for("estimate", key, base_term)
            if unit is not None and (not isinstance(unit, str) or not unit.strip()):
                raise FigureError("invalid_unit", "A measure unit is invalid.")
            estimate = _finite_number(row, "estimate")
            ci_lo = _finite_number(row, "ci_lo")
            ci_hi = _finite_number(row, "ci_hi")
            ci_level = _finite_number(row, "ci_level")
            if ci_lo > ci_hi or not 0 < ci_level < 1:
                raise FigureError("invalid_interval", "A cited estimate interval is invalid.")
            result_key = str(reference["result_key"])
            analysis_id, variant_id = _split_result_key(result_key)
            term = row.get("term")
            if not isinstance(term, str) or not term:
                raise FigureError("invalid_estimate_row", "A cited estimate row is invalid.")
            row_address = _table_row_address(
                _ESTIMATE_TABLE,
                {
                    "analysis": analysis_id,
                    "variant": variant_id,
                    "term": term,
                    "base_term": base_term,
                },
            )
            row_identity = {
                "analysis": analysis_id,
                "variant": variant_id,
                "result_key": result_key,
                "term": term,
                "base_term": base_term,
            }
            plot_rows.append(
                {
                    **row_identity,
                    "finding_id": finding_id,
                    "estimate": estimate,
                    "ci_lo": ci_lo,
                    "ci_hi": ci_hi,
                    "ci_level": ci_level,
                    "units": unit or "units not declared",
                    "row_address": row_address,
                    "panel_name": analysis.get("panel_name"),
                    "outcome": analysis.get("effective_spec", {}).get("outcome"),
                }
            )
            _append_result_value(
                result_values,
                seen_values,
                row_address,
                {
                    **row_identity,
                    "estimate": estimate,
                    "ci_lo": ci_lo,
                    "ci_hi": ci_hi,
                    "ci_level": ci_level,
                    "units": unit,
                },
            )
        figure_rows[finding_id] = plot_rows

    try:
        csv_paths: dict[str, str] = {}
        for finding_id, plot_rows in figure_rows.items():
            csv_path = f"data/{asset_ids[finding_id]}-estimates.csv"
            assets[csv_path] = _estimate_csv(plot_rows)
            csv_paths[finding_id] = csv_path
            artifacts.append(
                {
                    "path": csv_path,
                    "source_table": _ESTIMATE_TABLE,
                    "row_filter": {
                        "finding_id": finding_id,
                        "result_keys": list(dict.fromkeys(row["result_key"] for row in plot_rows)),
                        "base_terms": list(dict.fromkeys(row["base_term"] for row in plot_rows)),
                    },
                    "strict_disposition": "included",
                }
            )

        for finding_id, plot_rows in figure_rows.items():
            declaration = declarations[finding_id]
            criteria, criteria_text = _figure_criteria(
                finding_id,
                declaration,
                declarations,
                parameters,
                result_values,
                seen_values,
            )
            svg_path = f"figures/{asset_ids[finding_id]}-estimates.svg"
            svg_bytes, description = _render_estimate_svg(
                finding_id,
                findings[finding_id],
                declaration,
                plot_rows,
                criteria,
                criteria_text,
            )
            assets[svg_path] = svg_bytes
            figure_links.append((svg_path, description))
            artifacts.append(
                {
                    "path": svg_path,
                    "csv_path": csv_paths[finding_id],
                    "source_table": _ESTIMATE_TABLE,
                    "row_filter": {
                        "finding_id": finding_id,
                        "result_keys": list(dict.fromkeys(
                            row["result_key"] for row in plot_rows
                        )),
                        "base_terms": list(dict.fromkeys(
                            row["base_term"] for row in plot_rows
                        )),
                    },
                    "strict_disposition": "included",
                }
            )
    except FigureError:
        raise
    except (
        ArithmeticError,
        csv.Error,
        ET.ParseError,
        ImportError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        raise FigureError(
            "figure_render_failed", "A public figure could not be rendered safely."
        ) from exc

    allowed_keys = _analysis_result_keys(references)
    fit_rows = tuple(row for row in fit_rows if _result_key(row) in allowed_keys)
    coefficient_rows = tuple(row for row in estimate_rows if _result_key(row) in allowed_keys)
    research_markdown, table_values = _render_research_markdown(
        findings=findings,
        declarations=declarations,
        references=references,
        analyses=analyses,
        parameters=parameters,
        coefficient_rows=coefficient_rows,
        fit_rows=fit_rows,
        escape_text=escape_text,
        unit_for=unit_for,
    )
    figure_link_text = [
        f"![{escape_text(description)}](<{path}>)" for path, description in figure_links
    ]
    for address, value in table_values["values"]:
        _append_result_value(result_values, seen_values, address, value)
    for path, content in table_values["csvs"].items():
        if path in assets:
            raise FigureError("asset_name_collision", "Generated publication assets share a path.")
        assets[path] = content
    artifacts.extend(table_values["artifacts"])

    return ResearchAssets(
        assets=assets,
        figures_markdown="\n\n".join(figure_link_text),
        research_markdown=research_markdown,
        result_values=tuple(result_values),
        artifacts=tuple(artifacts),
    )


def _validate_rows(rows: Sequence[Mapping[str, Any]], table: str) -> None:
    if any(not isinstance(row, Mapping) for row in rows):
        raise FigureError("invalid_result_table", f"The {table} result table is invalid.")


def _resolve_estimate_row(
    reference: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]:
    key = reference.get("result_key")
    base_term = reference.get("base_term")
    term = reference.get("term")
    if not all(isinstance(value, str) and value for value in (key, base_term, term)):
        raise FigureError("invalid_reference", "An estimate reference is invalid.")
    matches = [
        row
        for row in rows
        if _result_key(row) == key and row.get("base_term") == base_term and row.get("term") == term
    ]
    if len(matches) != 1:
        raise FigureError("missing_estimate", "A cited estimate row is missing or ambiguous.")
    return matches[0]


def _result_key(row: Mapping[str, Any]) -> str:
    analysis = row.get("analysis")
    variant = row.get("variant")
    if not isinstance(analysis, str) or not isinstance(variant, str):
        return ""
    return f"{analysis}@{variant}"


def _analysis_result_keys(references: Sequence[Mapping[str, Any]]) -> frozenset[str]:
    return frozenset(
        value
        for reference in references
        if isinstance((value := reference.get("result_key")), str)
    )


def _split_result_key(value: str) -> tuple[str, str]:
    if "@" not in value:
        raise FigureError("invalid_result_key", "A cited result key is invalid.")
    analysis, variant = value.rsplit("@", 1)
    if not analysis or not variant:
        raise FigureError("invalid_result_key", "A cited result key is invalid.")
    return analysis, variant


def _table_row_address(path: str, identity: Mapping[str, str]) -> str:
    return f"{path}?{urlencode(identity)}"


def _finite_number(row: Mapping[str, Any], key: str) -> float:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise FigureError("invalid_estimate_row", "A cited estimate value is invalid.")
    return float(value)


def _append_result_value(
    records: list[Mapping[str, Any]],
    seen: set[tuple[str, str]],
    address: str,
    raw_value: Any,
) -> None:
    identity = (address, repr(raw_value))
    if identity in seen:
        return
    seen.add(identity)
    records.append({"address": address, "raw_value": raw_value})


def _estimate_csv(rows: Sequence[Mapping[str, Any]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output,
        fieldnames=(
            "finding_id",
            "analysis",
            "variant",
            "result_key",
            "term",
            "base_term",
            "units",
            "estimate",
            "ci_lo",
            "ci_hi",
            "ci_level",
            "row_address",
        ),
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    for row in rows:
        safe_row = {
            key: value if key in {"estimate", "ci_lo", "ci_hi", "ci_level"} else _csv_text(value)
            for key, value in row.items()
        }
        writer.writerow(
            {
                **safe_row,
                "finding_id": _csv_text(row.get("finding_id", "")),
                "estimate": _csv_number(row["estimate"]),
                "ci_lo": _csv_number(row["ci_lo"]),
                "ci_hi": _csv_number(row["ci_hi"]),
                "ci_level": _csv_number(row["ci_level"]),
            }
        )
    return output.getvalue().encode("utf-8")


def _csv_number(value: float) -> str:
    if not math.isfinite(value):
        raise FigureError("invalid_estimate_row", "A cited estimate value is invalid.")
    return format(value, ".17g")


def _figure_criteria(
    finding_id: str,
    declaration: Mapping[str, Any],
    declarations: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any],
    result_values: list[Mapping[str, Any]],
    seen_values: set[tuple[str, str]],
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    finding_index = list(declarations).index(finding_id)
    condition_specs = (
        ("equivalent_to", "equivalent to"),
        ("ci_above", "above"),
        ("ci_below", "below"),
        ("ci_excludes", "excludes"),
    )
    criteria = []
    lines = []
    bands = declaration.get("bands", ())
    if not isinstance(bands, Sequence) or isinstance(bands, str | bytes):
        raise FigureError("invalid_criteria", "A finding band declaration is invalid.")
    for band_index, band in enumerate(bands):
        if not isinstance(band, Mapping):
            raise FigureError("invalid_criteria", "A finding band declaration is invalid.")
        label = str(band.get("label", f"band {band_index + 1}"))
        for condition_name, _display_name in condition_specs:
            raw_condition = band.get(condition_name)
            if raw_condition is None:
                continue
            condition_address = (
                f"recipe/resolved.json#/recipe/findings/{finding_index}/bands/"
                f"{band_index}/{condition_name}"
            )
            if condition_name == "ci_excludes":
                value, value_address = _resolve_criterion_value(
                    raw_condition,
                    parameters,
                    condition_address,
                    result_values,
                    seen_values,
                )
                criteria.append(
                    {
                        "kind": condition_name,
                        "label": label,
                        "value": value,
                        "address": value_address,
                    }
                )
                lines.append(
                    f"Band {label!r} requires the confidence interval to exclude "
                    f"{_format(value)}."
                )
                continue
            if not isinstance(raw_condition, Mapping):
                raise FigureError("invalid_criteria", "A finding interval condition is invalid.")
            value, value_address = _resolve_criterion_value(
                raw_condition.get("value"),
                parameters,
                f"{condition_address}/value",
                result_values,
                seen_values,
            )
            margin_source = raw_condition.get("margin", 0)
            margin, margin_address = _resolve_criterion_value(
                margin_source,
                parameters,
                f"{condition_address}/margin",
                result_values,
                seen_values,
            )
            if margin < 0:
                raise FigureError("invalid_criteria", "A finding margin is invalid.")
            criterion = {
                "kind": condition_name,
                "label": label,
                "value": value,
                "margin": margin,
                "address": value_address,
                "margin_address": margin_address,
            }
            if condition_name == "equivalent_to":
                criterion["lower"] = value - margin
                criterion["upper"] = value + margin
                lines.append(
                    f"Band {label!r} requires the CI to lie within "
                    f"[{_format(value - margin)}, {_format(value + margin)}]."
                )
            elif condition_name == "ci_above":
                criterion["boundary"] = value + margin
                lines.append(
                    f"Band {label!r} requires the lower CI endpoint to exceed "
                    f"{_format(value + margin)}."
                )
            else:
                criterion["boundary"] = value - margin
                lines.append(
                    f"Band {label!r} requires the upper CI endpoint to be below "
                    f"{_format(value - margin)}."
                )
            criteria.append(criterion)
        statistic_rules = []
        for condition_name, operator in (("stat_gte", "at least"), ("stat_lte", "at most")):
            raw_condition = band.get(condition_name)
            if raw_condition is None:
                continue
            if not isinstance(raw_condition, Mapping):
                raise FigureError("invalid_criteria", "A finding statistic condition is invalid.")
            condition_address = (
                f"recipe/resolved.json#/recipe/findings/{finding_index}/bands/"
                f"{band_index}/{condition_name}/value"
            )
            value, _ = _resolve_criterion_value(
                raw_condition.get("value"),
                parameters,
                condition_address,
                result_values,
                seen_values,
            )
            statistic_rules.append(f"{raw_condition['stat']} {operator} {_format(value)}")
        if statistic_rules:
            lines.append(f"Band {label!r} also requires {' and '.join(statistic_rules)}.")
    return criteria, tuple(lines)


def _resolve_criterion_value(
    source: Any,
    parameters: Mapping[str, Any],
    source_address: str,
    result_values: list[Mapping[str, Any]],
    seen_values: set[tuple[str, str]],
) -> tuple[float, str]:
    if isinstance(source, Mapping) and set(source) == {"param"}:
        name = source.get("param")
        if not isinstance(name, str) or name not in parameters:
            raise FigureError("invalid_criteria", "A finding parameter is unresolved.")
        value = parameters[name]
        address = f"recipe/resolved.json#/plan/parameters/{_json_pointer_token(name)}"
    else:
        value = source
        address = source_address
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise FigureError("invalid_criteria", "A numeric finding criterion is invalid.")
    numeric = float(value)
    _append_result_value(result_values, seen_values, address, numeric)
    return numeric, address


def _render_estimate_svg(
    finding_id: str,
    finding: Mapping[str, Any],
    declaration: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    criteria: Sequence[Mapping[str, Any]],
    criteria_text: Sequence[str],
) -> tuple[bytes, str]:
    from matplotlib import rc_context
    from matplotlib.backends.backend_svg import FigureCanvasSVG
    from matplotlib.figure import Figure

    groups: dict[tuple[str, str, str, str], list[Mapping[str, Any]]] = {}
    for row in rows:
        group_key = (
            str(row.get("panel_name") or "panel not declared"),
            str(row.get("outcome") or "outcome not declared"),
            str(row["term"]),
            str(row["units"]),
        )
        groups.setdefault(group_key, []).append(row)
    if not groups:
        raise FigureError("missing_estimate", "An estimate plot has no eligible estimates.")

    group_items = list(groups.items())
    figure = Figure(figsize=(8.2, max(3.0, 1.9 * len(rows) + 0.8 * len(group_items))))
    FigureCanvasSVG(figure)
    axes = figure.subplots(len(group_items), 1, squeeze=False)[:, 0]
    alt_lines = []
    reference_values = declaration.get("reference_values", ())
    if not isinstance(reference_values, Sequence) or isinstance(reference_values, str | bytes):
        raise FigureError("invalid_reference_values", "Finding reference values are invalid.")
    refs = [_safe_number(value) for value in reference_values]

    for axis, (group_key, group_rows) in zip(axes, group_items, strict=True):
        panel_name, outcome, term, unit = group_key
        positions = list(range(len(group_rows)))
        estimates = [float(row["estimate"]) for row in group_rows]
        lows = [float(row["ci_lo"]) for row in group_rows]
        highs = [float(row["ci_hi"]) for row in group_rows]
        for reference_value in refs:
            axis.axvline(reference_value, color="#777777", linewidth=0.8, linestyle=":")
        for condition in criteria:
            if condition.get("kind") == "equivalent_to":
                axis.axvspan(
                    condition["lower"],
                    condition["upper"],
                    color="#9bbf9b",
                    alpha=0.22,
                    linewidth=0,
                )
            elif condition.get("kind") in {"ci_above", "ci_below", "ci_excludes"}:
                boundary = (
                    condition["value"]
                    if condition["kind"] == "ci_excludes"
                    else condition["boundary"]
                )
                axis.axvline(boundary, color="#777777", linewidth=0.8, linestyle="--")
        for position, estimate, low, high, row in zip(
            positions, estimates, lows, highs, group_rows, strict=True
        ):
            axis.errorbar(
                estimate,
                position,
                xerr=[[estimate - low], [high - estimate]],
                fmt="o" if row["variant"] == "base" else "s",
                color="#245b45" if row["variant"] == "base" else "#386b8a",
                ecolor="#555555",
                capsize=3,
                markersize=5,
            )
            level = _format(float(row["ci_level"] * 100), 0)
            alt_lines.append(
                f"{_variant_label(finding, row['variant'])} estimate {_format(estimate)}, "
                f"{level}% CI {_format(low)} to {_format(high)} "
                f"{row['units']}."
            )
        labels = [_variant_label(finding, row["variant"]) for row in group_rows]
        axis.set_yticks(positions, labels)
        axis.set_xlabel(f"Estimate ({unit})")
        axis.set_title(f"{outcome} · {term} · {panel_name}", fontsize=9)
        axis.grid(axis="x", color="#dddddd", linewidth=0.5)
        for spine_name in ("top", "right", "left"):
            axis.spines[spine_name].set_visible(False)
        axis.tick_params(axis="y", length=0)

    criteria_lines = list(criteria_text)
    if criteria_lines:
        alt_lines.extend(criteria_lines)
        alt_lines.append(
            "Shaded regions and dashed boundaries mark the interval conditions. "
            "The research details list all criteria."
        )
    if refs:
        alt_lines.append(
            "The declared reference values are "
            + ", ".join(_format(item) for item in refs)
            + "."
        )
    if len(group_items) > 1:
        alt_lines.append("Separate panels preserve different outcomes, terms, or effect scales.")
    description = " ".join(alt_lines)
    title = f"Estimate intervals for finding {finding_id}"
    buffer = io.BytesIO()
    try:
        with rc_context({"svg.hashsalt": "hhplab-results-publication-v1"}):
            figure.savefig(buffer, format="svg", metadata={"Date": None, "Creator": "HHP-Lab"})
        raw_svg = buffer.getvalue()
    finally:
        figure.clear()
    return _accessible_static_svg(raw_svg, title, description), description


def _accessible_static_svg(raw_svg: bytes, title: str, description: str) -> bytes:
    root = ET.fromstring(raw_svg)
    if root.tag != f"{{{_SVG_NS}}}svg":
        raise FigureError("invalid_svg", "The rendered figure is not an SVG document.")
    title_id = "figure-title"
    description_id = "figure-description"
    root.set("role", "img")
    root.set("aria-labelledby", f"{title_id} {description_id}")
    root.insert(0, ET.Element(f"{{{_SVG_NS}}}desc", {"id": description_id}))
    root[0].text = description
    root.insert(0, ET.Element(f"{{{_SVG_NS}}}title", {"id": title_id}))
    root[0].text = title
    for element in root.iter():
        local_name = element.tag.rsplit("}", 1)[-1].lower()
        if local_name in {"script", "foreignobject", "iframe", "object", "embed"}:
            raise FigureError("unsafe_svg", "The rendered figure contains active content.")
        for attribute, value in element.attrib.items():
            local_attribute = attribute.rsplit("}", 1)[-1].lower()
            if local_attribute.startswith("on"):
                raise FigureError("unsafe_svg", "The rendered figure contains active content.")
            if local_attribute == "href" and not value.startswith("#"):
                raise FigureError("unsafe_svg", "The rendered figure contains an external link.")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _render_research_markdown(
    *,
    findings: Mapping[str, Mapping[str, Any]],
    declarations: Mapping[str, Mapping[str, Any]],
    references: Sequence[Mapping[str, Any]],
    analyses: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any],
    coefficient_rows: Sequence[Mapping[str, Any]],
    fit_rows: Sequence[Mapping[str, Any]],
    escape_text: Callable[[str], str],
    unit_for: Callable[[str, str, str | None], str | None],
) -> tuple[str, dict[str, Any]]:
    sections = []
    result_values: list[tuple[str, Any]] = []
    variant_csv_rows: list[dict[str, Any]] = []
    for finding_id in findings:
        declaration = declarations[finding_id]
        finding = findings[finding_id]
        claim = _md(escape_text, str(finding.get("claim", "")))
        sections.extend((f"### {_md(escape_text, finding_id)}", claim))
        bands = declaration.get("bands", ())
        if bands:
            sections.append("**Declared criteria**")
            sections.append(
                _band_table(finding_id, bands, declarations, parameters, escape_text, result_values)
            )
        variants = finding.get("variants", ())
        if variants:
            sections.append("**Variant comparison**")
            sections.append(
                _variant_table(
                    finding_id, finding, escape_text, result_values, variant_csv_rows
                )
            )

    keys = _analysis_result_keys(references)
    selected_coefficients = [row for row in coefficient_rows if _result_key(row) in keys]
    if selected_coefficients:
        sections.append("## Coefficients")
        sections.append(
            _coefficient_table(
                selected_coefficients, escape_text, result_values, unit_for
            )
        )
    selected_fit = [row for row in fit_rows if _result_key(row) in keys]
    if selected_fit:
        sections.append("## Model fit")
        sections.append(_fit_table(selected_fit, escape_text, result_values))
    if not selected_coefficients and not selected_fit:
        sections.append("No optional coefficient or fit tables were eligible for this release.")
    csvs: dict[str, bytes] = {}
    artifacts: list[Mapping[str, Any]] = []
    if selected_coefficients:
        path = "data/research-coefficients.csv"
        csvs[path] = _dict_csv(
            selected_coefficients,
            (
                "analysis", "variant", "result_key", "term", "base_term", "estimate",
                "se", "ci_level", "ci_lo", "ci_hi", "units", "row_address",
            ),
            lambda row: _coefficient_csv_row(row, unit_for),
        )
        artifacts.append(
            _research_table_artifact(path, _ESTIMATE_TABLE, selected_coefficients, "estimate")
        )
    if selected_fit:
        path = "data/research-fit.csv"
        csvs[path] = _dict_csv(
            selected_fit,
            (
                "analysis", "variant", "result_key", "n", "n_clusters", "r2",
                "r2_within", "row_address",
            ),
            _fit_csv_row,
        )
        artifacts.append(_research_table_artifact(path, _FIT_TABLE, selected_fit, "fit"))
    if variant_csv_rows:
        path = "data/research-variants.csv"
        csvs[path] = _records_csv(
            variant_csv_rows,
            (
                "finding_id", "variant", "description", "status", "verdict", "agrees",
                "reason", "row_address",
            ),
        )
        artifacts.append({
            "path": path,
            "source_table": "findings.json",
            "row_filter": {"finding_ids": list(findings)},
            "strict_disposition": "included",
        })
    return "\n\n".join(section for section in sections if section), {
        "values": result_values,
        "csvs": csvs,
        "artifacts": artifacts,
    }


def _band_table(
    finding_id: str,
    bands: Any,
    declarations: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any],
    escape_text: Callable[[str], str],
    result_values: list[tuple[str, Any]],
) -> str:
    finding_index = list(declarations).index(finding_id)
    lines = ["| Band | Public verdict | Rule |", "| --- | --- | --- |"]
    for band_index, band in enumerate(bands):
        label = str(band.get("label", f"band {band_index + 1}"))
        rules = []
        for name in (
            "equivalent_to", "ci_above", "ci_below", "ci_excludes", "stat_gte", "stat_lte"
        ):
            if name not in band or band[name] is None:
                continue
            condition = band[name]
            path = (
                f"recipe/resolved.json#/recipe/findings/{finding_index}/bands/"
                f"{band_index}/{name}"
            )
            if name == "ci_excludes":
                value, address = _criterion_for_table(condition, parameters, f"{path}")
                result_values.append((address, value))
                rules.append(f"CI excludes {_format(value)}")
            elif name.startswith("stat_"):
                if not isinstance(condition, Mapping):
                    continue
                value, address = _criterion_for_table(
                    condition.get("value"), parameters, f"{path}/value"
                )
                result_values.append((address, value))
                operator = "≥" if name == "stat_gte" else "≤"
                rules.append(f"{condition.get('stat')} {operator} {_format(value)}")
            else:
                value, address = _criterion_for_table(
                    condition.get("value"), parameters, f"{path}/value"
                )
                margin_source = condition.get("margin", 0)
                margin, margin_address = _criterion_for_table(
                    margin_source, parameters, f"{path}/margin"
                )
                result_values.extend(((address, value), (margin_address, margin)))
                if name == "equivalent_to":
                    rules.append(
                        f"CI within [{_format(value - margin)}, {_format(value + margin)}]"
                    )
                elif name == "ci_above":
                    rules.append(f"CI lower bound > {_format(value + margin)}")
                else:
                    rules.append(f"CI upper bound < {_format(value - margin)}")
        lines.append(
            "| "
            + " | ".join(
                _md(escape_text, cell)
                for cell in (
                    label,
                    str(band.get("public_verdict", "inconclusive")),
                    " and ".join(rules),
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _variant_table(
    finding_id: str,
    finding: Mapping[str, Any],
    escape_text: Callable[[str], str],
    result_values: list[tuple[str, Any]],
    csv_rows: list[dict[str, Any]],
) -> str:
    lines = [
        "| Check | Description | Status | Verdict | Agrees with base | Reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for variant in finding.get("variants", ()):
        if not isinstance(variant, Mapping):
            raise FigureError("invalid_variant", "A public variant record is invalid.")
        variant_id = str(variant.get("id", ""))
        status = str(variant.get("evaluation_status", variant.get("status", "unknown")))
        verdict = variant.get("verdict")
        agrees = variant.get("agrees")
        reason = variant.get("unavailable_reason") or ""
        identity = urlencode({"finding": finding_id, "variant": variant_id})
        address = f"findings.json#{identity}"
        csv_rows.append({
            "finding_id": finding_id,
            "variant": variant_id,
            "description": str(variant.get("description", "")),
            "status": status,
            "verdict": verdict or "",
            "agrees": "yes" if agrees is True else "no" if agrees is False else "",
            "reason": str(reason),
            "row_address": address,
        })
        result_values.append(
            (
                address,
                {"status": status, "verdict": verdict, "agrees": agrees, "reason": reason},
            )
        )
        lines.append(
            "| "
            + " | ".join(
                _md(escape_text, str(value if value is not None else "not evaluated"))
                for value in (
                    variant_id,
                    str(variant.get("description", "")),
                    status,
                    str(verdict or "not evaluated"),
                    "yes" if agrees is True else "no" if agrees is False else "—",
                    str(reason),
                )
            )
            + " |"
        )
    return "\n".join(lines)


def _coefficient_table(
    rows: Sequence[Mapping[str, Any]],
    escape_text: Callable[[str], str],
    result_values: list[tuple[str, Any]],
    unit_for: Callable[[str, str, str | None], str | None],
) -> str:
    lines = [
        "| Analysis | Variant | Term | Estimate | SE | CI level | CI low | CI high | Units |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in rows:
        key = _result_key(row)
        analysis_id, variant = _split_result_key(key)
        base_term = str(row.get("base_term", ""))
        term = str(row.get("term", ""))
        address = _table_row_address(
            _ESTIMATE_TABLE,
            {
                "analysis": analysis_id,
                "variant": variant,
                "term": term,
                "base_term": base_term,
            },
        )
        numeric = {
            name: value
            for name, value in row.items()
            if name in {"estimate", "se", "ci_level", "ci_lo", "ci_hi", "p"}
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        }
        result_values.append((address, numeric))
        unit = unit_for("estimate", key, base_term)
        unit = unit.strip() if isinstance(unit, str) and unit.strip() else "units not declared"
        cells = (
            analysis_id,
            variant,
            term,
            _format_optional(row.get("estimate")),
            _format_optional(row.get("se")),
            _format_optional(row.get("ci_level"), percent=True),
            _format_optional(row.get("ci_lo")),
            _format_optional(row.get("ci_hi")),
            unit,
        )
        lines.append("| " + " | ".join(_md(escape_text, str(cell)) for cell in cells) + " |")
    return "\n".join(lines)


def _fit_table(
    rows: Sequence[Mapping[str, Any]],
    escape_text: Callable[[str], str],
    result_values: list[tuple[str, Any]],
) -> str:
    lines = [
        "| Analysis | Variant | n | Clusters | R² | Within R² |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        key = _result_key(row)
        analysis_id, variant = _split_result_key(key)
        address = _table_row_address(_FIT_TABLE, {"analysis": analysis_id, "variant": variant})
        numeric = {
            name: value
            for name, value in row.items()
            if name in {"n", "n_clusters", "r2", "r2_within"}
            and isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
        }
        result_values.append((address, numeric))
        cells = (
            analysis_id,
            variant,
            _format_optional(row.get("n"), precision=0),
            _format_optional(row.get("n_clusters"), precision=0),
            _format_optional(row.get("r2")),
            _format_optional(row.get("r2_within")),
        )
        lines.append("| " + " | ".join(_md(escape_text, str(cell)) for cell in cells) + " |")
    return "\n".join(lines)


def _coefficient_csv_row(
    row: Mapping[str, Any],
    unit_for: Callable[[str, str, str | None], str | None],
) -> dict[str, Any]:
    key = _result_key(row)
    analysis_id, variant = _split_result_key(key)
    base_term = str(row.get("base_term", ""))
    identity = {
        "analysis": analysis_id,
        "variant": variant,
        "term": str(row.get("term", "")),
        "base_term": base_term,
    }
    unit = unit_for("estimate", key, base_term)
    unit = unit.strip() if isinstance(unit, str) and unit.strip() else "units not declared"
    return {
        **identity,
        "result_key": key,
        **{
            name: _csv_number(float(row[name])) if _is_finite(row.get(name)) else ""
            for name in ("estimate", "se", "ci_level", "ci_lo", "ci_hi")
        },
        "units": unit,
        "row_address": _table_row_address(_ESTIMATE_TABLE, identity),
    }


def _fit_csv_row(row: Mapping[str, Any]) -> dict[str, Any]:
    key = _result_key(row)
    analysis_id, variant = _split_result_key(key)
    identity = {"analysis": analysis_id, "variant": variant}
    return {
        **identity,
        "result_key": key,
        **{
            name: _csv_number(float(row[name])) if _is_finite(row.get(name)) else ""
            for name in ("n", "n_clusters", "r2", "r2_within")
        },
        "row_address": _table_row_address(_FIT_TABLE, identity),
    }


def _research_table_artifact(
    path: str, source_table: str, rows: Sequence[Mapping[str, Any]], table_kind: str
) -> Mapping[str, Any]:
    result_keys = list(dict.fromkeys(_result_key(row) for row in rows))
    row_filter: dict[str, Any] = {"table": table_kind, "result_keys": result_keys}
    if table_kind == "estimate":
        row_filter["terms"] = list(dict.fromkeys(str(row.get("term", "")) for row in rows))
        row_filter["base_terms"] = list(
            dict.fromkeys(str(row.get("base_term", "")) for row in rows)
        )
    return {
        "path": path,
        "source_table": source_table,
        "row_filter": row_filter,
        "strict_disposition": "included",
    }


def _dict_csv(
    rows: Sequence[Mapping[str, Any]],
    fieldnames: Sequence[str],
    transform: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> bytes:
    return _records_csv([transform(row) for row in rows], fieldnames)


def _records_csv(rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output, fieldnames=fieldnames, lineterminator="\n", extrasaction="ignore"
    )
    writer.writeheader()
    for row in rows:
        writer.writerow({key: _csv_text(value) for key, value in row.items()})
    return output.getvalue().encode("utf-8")


def _csv_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return _csv_number(value)
    text = str(value)
    if _CSV_NUMBER_PATTERN.fullmatch(text):
        return text
    if text.lstrip().startswith(("=", "+", "-", "@", "\t", "\r")):
        return "'" + text
    return text


def _is_finite(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _criterion_for_table(
    source: Any,
    parameters: Mapping[str, Any],
    address: str,
) -> tuple[float, str]:
    if isinstance(source, Mapping) and set(source) == {"param"}:
        name = source.get("param")
        if not isinstance(name, str) or name not in parameters:
            raise FigureError("invalid_criteria", "A finding parameter is unresolved.")
        value = parameters[name]
        address = f"recipe/resolved.json#/plan/parameters/{_json_pointer_token(name)}"
    else:
        value = source
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise FigureError("invalid_criteria", "A numeric finding criterion is invalid.")
    return float(value), address


def _variant_label(finding: Mapping[str, Any], variant_id: str) -> str:
    if variant_id == "base":
        return "Base"
    for variant in finding.get("variants", ()):
        if isinstance(variant, Mapping) and variant.get("id") == variant_id:
            return str(variant.get("description") or variant_id)
    return variant_id


def _asset_id(value: str) -> str:
    normalized = _ID_PATTERN.sub("-", value).strip("-.")
    return normalized or "finding"


def _json_pointer_token(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _format(value: float, precision: int = 2) -> str:
    return f"{value:.{precision}f}"


def _format_optional(value: Any, precision: int = 2, *, percent: bool = False) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return "—"
    numeric = float(value) * 100 if percent else float(value)
    return f"{numeric:.{precision}f}%" if percent else f"{numeric:.{precision}f}"


def _safe_number(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise FigureError("invalid_reference_values", "Finding reference values are invalid.")
    return float(value)


def _md(escape_text: Callable[[str], str], value: str) -> str:
    escaped = escape_text(value)
    if not isinstance(escaped, str):
        raise FigureError("invalid_escaped_text", "Public text could not be escaped safely.")
    return escaped.replace("|", "\\|").replace("\n", " ").replace("\r", " ")
