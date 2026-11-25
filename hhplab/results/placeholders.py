"""Resolve publication narrative placeholders against eligible bundle values."""

from __future__ import annotations

import html
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote, urlsplit

from markdown_it import MarkdownIt

_PLACEHOLDER_RE = re.compile(r"\{\{\s*(.*?)\s*\}\}", re.DOTALL)
_ANALYSIS_ESTIMATE_ADDRESS_RE = re.compile(
    r"^(?P<analysis>[A-Za-z0-9_-]+)(?:@(?P<variant>[A-Za-z0-9_-]+))?\.(?P<term>[A-Za-z0-9_-]+)$"
)
_ANALYSIS_ADDRESS_RE = re.compile(
    r"^(?P<analysis>[A-Za-z0-9_-]+)(?:@(?P<variant>[A-Za-z0-9_-]+))?$"
)
_FINDING_ADDRESS_RE = re.compile(
    r"^(?P<finding>[A-Za-z0-9_-]+)(?:@(?P<variant>[A-Za-z0-9_-]+))?$"
)
_NUMBER_RE = re.compile(
    r"(?<![A-Za-z0-9_])[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?%?(?![A-Za-z0-9_])"
)
_CITATION_LABEL_RE = re.compile(r"\[\^?\d+(?:\s*[,;]\s*\d+)*\]")
_MARKDOWN_LINK_RE = re.compile(r"!?\[[^\]]*\]\((?:\\.|[^)])*\)")
_LINK_DESTINATION_CANDIDATE_RE = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)")
_NUMERIC_FIELDS = frozenset(
    {"estimate", "ci_lo", "ci_hi", "se", "p", "n", "n_clusters", "r2", "r2_within", "r", "margin"}
)
_ESTIMATE_FIELDS = frozenset({"estimate", "ci_lo", "ci_hi", "se", "p"})
_FIT_FIELDS = frozenset({"n", "n_clusters", "r2", "r2_within"})
_FINDING_FIELDS = frozenset({"verdict", "verdict_text", "label", "compatible_with"})
_TABLE_FOR_FIELD = {
    **{field: "regression_estimates" for field in _ESTIMATE_FIELDS},
    **{field: "regression_fit" for field in _FIT_FIELDS},
    "r": "correlations",
}


@dataclass(frozen=True)
class RenderedNarrative:
    markdown: str
    placeholders: tuple[Mapping[str, Any], ...]
    warnings: tuple[str, ...]


class PlaceholderError(ValueError):
    """A stable, public-safe error for a malformed or unresolved placeholder."""

    def __init__(self, code: str, placeholder: str, reason: str) -> None:
        self.code = code
        self.placeholder = placeholder
        self.reason = reason
        super().__init__(f"{placeholder}: {reason} ({code})")


def escape_markdown_text(value: str) -> str:
    """Escape a plain-text value before inserting it into Markdown prose."""
    if not isinstance(value, str):
        raise TypeError("Markdown text value must be a string")
    escaped = html.escape(value, quote=False)
    return re.sub(r"([\\`*_{}\[\]()#+\-!|~])", r"\\\1", escaped)


def render_narrative(
    narrative: str,
    *,
    rows: Callable[[str], Sequence[Mapping[str, Any]]],
    allowed_estimates: frozenset[tuple[str, str]],
    allowed_result_keys: frozenset[str] | None = None,
    evaluations: Mapping[tuple[str, str], Mapping[str, Any]],
    parameters: Mapping[str, Any],
    unit_for: Callable[[str, str, str | None], str | None],
    strict: bool = False,
) -> RenderedNarrative:
    """Resolve every placeholder once and reject active or ambiguous Markdown."""
    if not isinstance(narrative, str):
        raise TypeError("narrative must be a string")
    if not isinstance(strict, bool):
        raise TypeError("strict must be boolean")
    if allowed_result_keys is None:
        allowed_result_keys = frozenset(key for key, _term in allowed_estimates)

    _validate_markdown(narrative)
    matches = tuple(_PLACEHOLDER_RE.finditer(narrative))
    residue = _PLACEHOLDER_RE.sub("", narrative)
    if "{{" in residue or "}}" in residue:
        malformed = "{{" if "{{" in residue else "}}"
        raise PlaceholderError("malformed_placeholder", malformed, "malformed placeholder syntax")

    warnings = _number_lint(narrative, matches)
    if strict and warnings:
        raise PlaceholderError("number_lint", "narrative", "numeric text outside placeholders")

    replacements: list[str] = []
    records: list[Mapping[str, Any]] = []
    for match in matches:
        raw_placeholder = match.group(0)
        parsed = _parse_placeholder(match.group(1), raw_placeholder)
        field_name, address, format_name = parsed
        raw_value, rendered_value, source_address = _resolve_placeholder(
            field_name,
            address,
            format_name,
            raw_placeholder,
            rows=rows,
            allowed_estimates=allowed_estimates,
            allowed_result_keys=allowed_result_keys,
            evaluations=evaluations,
            parameters=parameters,
            unit_for=unit_for,
        )
        escaped = escape_markdown_text(rendered_value)
        replacements.append(escaped)
        records.append(
            {
                "placeholder": raw_placeholder,
                "field": field_name,
                "address": address,
            "source_address": source_address,
                "format": format_name,
                "raw_value": _json_safe(raw_value),
                "rendered": escaped,
            }
        )

    pieces: list[str] = []
    cursor = 0
    for match, replacement in zip(matches, replacements, strict=True):
        pieces.append(narrative[cursor : match.start()])
        pieces.append(replacement)
        cursor = match.end()
    pieces.append(narrative[cursor:])
    rendered = "".join(pieces)
    _validate_markdown(rendered)
    return RenderedNarrative(rendered, tuple(records), warnings)


def _parse_placeholder(
    body: str, raw_placeholder: str
) -> tuple[str, str, str | None]:
    if "\n" in body or "\r" in body:
        raise PlaceholderError(
            "malformed_placeholder", raw_placeholder, "placeholder must be on one line"
        )
    parts = body.split("|")
    if len(parts) > 2:
        raise PlaceholderError(
            "malformed_placeholder", raw_placeholder, "placeholder has too many format separators"
        )
    expression = parts[0].strip()
    format_name = parts[1].strip() if len(parts) == 2 else None
    if ":" not in expression:
        raise PlaceholderError(
            "malformed_placeholder", raw_placeholder, "placeholder requires field and address"
        )
    field_name, address = (part.strip() for part in expression.split(":", 1))
    if not field_name or not address:
        raise PlaceholderError(
            "malformed_placeholder", raw_placeholder, "placeholder requires field and address"
        )
    if field_name not in _NUMERIC_FIELDS | _FINDING_FIELDS:
        raise PlaceholderError("invalid_field", raw_placeholder, "unsupported placeholder field")
    if format_name is not None and field_name not in _NUMERIC_FIELDS:
        raise PlaceholderError(
            "invalid_format", raw_placeholder, "text fields do not accept formats"
        )
    if format_name is not None:
        _validate_format_name(format_name, raw_placeholder)
    elif field_name in _NUMERIC_FIELDS:
        format_name = "round:2"
    return field_name, address, format_name


def _validate_format_name(format_name: str, placeholder: str) -> None:
    if format_name in {"int", "pct", "pct_points", "ci", "abs", "signed"}:
        return
    match = re.fullmatch(r"round:(\d+)", format_name)
    if match and 0 <= int(match.group(1)) <= 12:
        return
    raise PlaceholderError("invalid_format", placeholder, "unsupported numeric format")


def _resolve_placeholder(
    field_name: str,
    address: str,
    format_name: str | None,
    placeholder: str,
    *,
    rows: Callable[[str], Sequence[Mapping[str, Any]]],
    allowed_estimates: frozenset[tuple[str, str]],
    allowed_result_keys: frozenset[str],
    evaluations: Mapping[tuple[str, str], Mapping[str, Any]],
    parameters: Mapping[str, Any],
    unit_for: Callable[[str, str, str | None], str | None],
) -> tuple[Any, str, str]:
    if field_name in _FINDING_FIELDS:
        if "." in address:
            raise PlaceholderError("malformed_address", placeholder, "invalid finding address")
        found = _FINDING_ADDRESS_RE.fullmatch(address)
        if found is None:
            raise PlaceholderError("malformed_address", placeholder, "invalid finding address")
        finding_id = found.group("finding")
        variant_id = found.group("variant") or "base"
        evaluation = evaluations.get((finding_id, variant_id))
        if evaluation is None:
            raise PlaceholderError(
                "missing_result", placeholder, "finding evaluation is unavailable"
            )
        if field_name == "compatible_with":
            raw_value = evaluation.get("compatible_with")
            value = evaluation.get("compatible_text")
            if value is None:
                value = _render_list(raw_value, placeholder)
        else:
            raw_value = evaluation.get(field_name)
            value = raw_value
        if value is None:
            raise PlaceholderError(
                "missing_result", placeholder, _safe_evaluation_reason(evaluation)
            )
        if not isinstance(value, str):
            raise PlaceholderError("invalid_value", placeholder, "finding value is not text")
        return raw_value, value, f"findings.json#finding={finding_id}&variant={variant_id}"

    if field_name == "margin":
        match = re.fullmatch(r"param\.([A-Za-z0-9_-]+)", address)
        if match is None:
            raise PlaceholderError("malformed_address", placeholder, "invalid parameter address")
        name = match.group(1)
        if name not in parameters:
            raise PlaceholderError("missing_result", placeholder, "cited parameter is unavailable")
        value = _number(parameters[name], placeholder)
        return (
            parameters[name],
            _format_number(value, field_name, format_name, None, placeholder),
            f"recipe/resolved.json#/plan/parameters/{name}",
        )

    if field_name == "r":
        found = re.fullmatch(
            r"(?P<analysis>[A-Za-z0-9_-]+)(?:@(?P<variant>[A-Za-z0-9_-]+))?\.(?P<a>[A-Za-z0-9_-]+)~(?P<b>[A-Za-z0-9_-]+)",
            address,
        )
        if found is None:
            raise PlaceholderError("malformed_address", placeholder, "invalid correlation address")
        result_key = _result_key(found.group("analysis"), found.group("variant"))
        if result_key not in allowed_result_keys:
            raise PlaceholderError(
                "disallowed_reference", placeholder, "analysis is outside cited findings"
            )
        matches = [
            row
            for row in rows("correlations")
            if _row_key(row) == result_key
            and row.get("col_a") == found.group("a")
            and row.get("col_b") == found.group("b")
        ]
        row = _one_row(matches, placeholder)
        value = _present_value(row, "r", "status", placeholder)
        return (
            value,
            _format_number(
                _number(value, placeholder),
                field_name,
                format_name,
                unit_for(field_name, result_key, None),
                placeholder,
            ),
            f"results/correlations.parquet#analysis={found.group('analysis')}"
            f"&variant={found.group('variant') or 'base'}"
            f"&col_a={found.group('a')}&col_b={found.group('b')}",
        )

    if field_name in _ESTIMATE_FIELDS:
        found = _ANALYSIS_ESTIMATE_ADDRESS_RE.fullmatch(address)
        if found is None:
            raise PlaceholderError("malformed_address", placeholder, "invalid estimate address")
        result_key = _result_key(found.group("analysis"), found.group("variant"))
        base_term = found.group("term")
        if (result_key, base_term) not in allowed_estimates:
            raise PlaceholderError(
                "disallowed_reference", placeholder, "estimate is outside cited findings"
            )
        matches = [
            row
            for row in rows("regression_estimates")
            if _row_key(row) == result_key and row.get("base_term") == base_term
        ]
        row = _one_row(matches, placeholder)
        if format_name == "ci":
            estimate = _number(_present_value(row, "estimate", None, placeholder), placeholder)
            ci_lo = _number(_present_value(row, "ci_lo", None, placeholder), placeholder)
            ci_hi = _number(_present_value(row, "ci_hi", None, placeholder), placeholder)
            precision = 2
            if format_name is not None:
                precision = _precision(format_name, placeholder)
            raw = {"estimate": estimate, "ci_lo": ci_lo, "ci_hi": ci_hi}
            return (
                raw,
                _format_interval(estimate, ci_lo, ci_hi, precision),
                _result_row_address("regression_estimates", result_key, base_term),
            )
        value = _present_value(
            row,
            field_name,
            "p_status" if field_name == "p" else None,
            placeholder,
        )
        number = _number(value, placeholder)
        unit = unit_for(field_name, result_key, base_term)
        return (
            value,
            _format_number(number, field_name, format_name, unit, placeholder),
            _result_row_address("regression_estimates", result_key, base_term),
        )

    if field_name in _FIT_FIELDS:
        found = _ANALYSIS_ADDRESS_RE.fullmatch(address)
        if found is None:
            raise PlaceholderError("malformed_address", placeholder, "invalid fit address")
        result_key = _result_key(found.group("analysis"), found.group("variant"))
        if result_key not in allowed_result_keys:
            raise PlaceholderError(
                "disallowed_reference", placeholder, "analysis is outside cited findings"
            )
        row = _one_row(
            [item for item in rows("regression_fit") if _row_key(item) == result_key],
            placeholder,
        )
        value = _present_value(
            row,
            field_name,
            f"{field_name}_status" if field_name in {"r2", "r2_within"} else None,
            placeholder,
        )
        return (
            value,
            _format_number(
                _number(value, placeholder),
                field_name,
                format_name,
                unit_for(field_name, result_key, None),
                placeholder,
            ),
            _result_row_address("regression_fit", result_key),
        )

    raise PlaceholderError("invalid_field", placeholder, "unsupported placeholder field")


def _result_row_address(table: str, result_key: str, term: str | None = None) -> str:
    analysis, variant = result_key.split("@", 1)
    address = f"results/{table}.parquet#analysis={analysis}&variant={variant}"
    if term is not None:
        address += f"&base_term={term}"
    return address


def _result_key(analysis: str, variant: str | None) -> str:
    return f"{analysis}@{variant or 'base'}"


def _row_key(row: Mapping[str, Any]) -> str:
    return f"{row.get('analysis')}@{row.get('variant')}"


def _one_row(rows: Sequence[Mapping[str, Any]], placeholder: str) -> Mapping[str, Any]:
    if not rows:
        raise PlaceholderError("missing_result", placeholder, "result row is unavailable")
    if len(rows) != 1:
        raise PlaceholderError(
            "ambiguous_result", placeholder, "more than one exact result row matched"
        )
    return rows[0]


def _present_value(
    row: Mapping[str, Any], field_name: str, status_field: str | None, placeholder: str
) -> Any:
    value = row.get(field_name)
    if value is not None:
        return value
    if status_field is not None and row.get(status_field) == "not_evaluated":
        raise PlaceholderError(
            "missing_result", placeholder, f"{status_field}: not_evaluated"
        )
    raise PlaceholderError("missing_result", placeholder, "required result value is unavailable")


def _safe_evaluation_reason(evaluation: Mapping[str, Any]) -> str:
    status = evaluation.get("status", evaluation.get("evaluation_status"))
    if status == "not_evaluated":
        return "finding_status: not_evaluated"
    return "finding value is unavailable"


def _number(value: Any, placeholder: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise PlaceholderError("invalid_value", placeholder, "result value is not numeric")
    number = float(value)
    if not math.isfinite(number):
        raise PlaceholderError("invalid_value", placeholder, "result value is not finite")
    return number


def _format_number(
    value: float,
    field_name: str,
    format_name: str | None,
    unit: str | None,
    placeholder: str,
) -> str:
    selected = format_name or "round:2"
    if selected == "ci":
        raise PlaceholderError("invalid_format", placeholder, "ci is valid only for estimate")
    if selected == "int":
        if not value.is_integer():
            raise PlaceholderError("invalid_format", placeholder, "int requires an integral value")
        return str(int(value))
    if selected in {"pct", "pct_points"}:
        if not isinstance(unit, str) or not _is_proportion_unit(unit):
            raise PlaceholderError(
                "invalid_units", placeholder, "percentage format requires proportion units"
            )
        converted = value * 100
        rendered = _fixed(converted, 2)
        suffix = "%" if selected == "pct" else " percentage points"
        return rendered + suffix
    if selected == "abs":
        return _fixed(abs(value), 2)
    if selected == "signed":
        return _signed(value, 2)
    precision = _precision(selected, placeholder)
    return _fixed(value, precision)


def _precision(format_name: str, placeholder: str) -> int:
    match = re.fullmatch(r"round:(\d+)", format_name)
    if match is None:
        return 2
    precision = int(match.group(1))
    if not 0 <= precision <= 12:
        raise PlaceholderError(
            "invalid_format", placeholder, "round precision must be 0 through 12"
        )
    return precision


def _fixed(value: float, precision: int) -> str:
    return f"{value:.{precision}f}"


def _signed(value: float, precision: int) -> str:
    return f"{value:+.{precision}f}"


def _format_interval(estimate: float, ci_lo: float, ci_hi: float, precision: int) -> str:
    return f"{_fixed(estimate, precision)} [{_fixed(ci_lo, precision)}, {_fixed(ci_hi, precision)}]"


def _is_proportion_unit(unit: str) -> bool:
    normalized = unit.strip().lower()
    return normalized == "proportion" or normalized.startswith(
        ("proportion per ", "share", "fraction")
    )


def _render_list(value: Any, placeholder: str) -> str:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise PlaceholderError("invalid_value", placeholder, "list value is unavailable")
    if not value:
        return "none"
    if any(isinstance(item, (Mapping, list, tuple, set)) for item in value):
        raise PlaceholderError("invalid_value", placeholder, "list value has an unsupported item")
    rendered = [str(item) for item in value]
    if len(rendered) == 1:
        return rendered[0]
    if len(rendered) == 2:
        return f"{rendered[0]} and {rendered[1]}"
    return f"{', '.join(rendered[:-1])}, and {rendered[-1]}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _validate_markdown(markdown: str) -> None:
    # CommonMark leaves malformed destinations such as
    # ``[label](javascript:alert(1))`` as plain text. Treat link-shaped
    # syntax as an attempted link anyway, so invalid parsing cannot bypass
    # the destination allowlist.
    for candidate in _LINK_DESTINATION_CANDIDATE_RE.finditer(markdown):
        destination = candidate.group(1).strip("<>")
        if not _safe_link(destination):
            raise PlaceholderError("unsafe_markdown", "narrative", "link target is not allowed")
    parser = MarkdownIt("commonmark", {"html": True, "linkify": False})
    for token in parser.parse(markdown):
        _validate_token(token)


def _validate_token(token: Any) -> None:
    if token.type.startswith("html_"):
        raise PlaceholderError("unsafe_markdown", "narrative", "raw HTML is not allowed")
    if token.type == "image":
        raise PlaceholderError(
            "unsafe_markdown", "narrative", "images are not allowed in narrative Markdown"
        )
    if token.type == "link_open":
        href = token.attrGet("href")
        if not isinstance(href, str) or not _safe_link(href):
            raise PlaceholderError("unsafe_markdown", "narrative", "link target is not allowed")
    for child in token.children or ():
        _validate_token(child)


def _safe_link(href: str) -> bool:
    if any(character.isspace() or ord(character) < 32 for character in href):
        return False
    try:
        parsed = urlsplit(href)
        _ = parsed.port
    except ValueError:
        return False
    if parsed.scheme:
        return (
            parsed.scheme.lower() in {"http", "https"}
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and "@" not in parsed.netloc
        )
    if parsed.netloc or href.startswith("/") or "\\" in href:
        return False
    decoded_path = unquote(parsed.path)
    if decoded_path.startswith("/") or "\\" in decoded_path:
        return False
    if any(part in {"", ".."} for part in decoded_path.split("/")) and decoded_path:
        return False
    return True


def _number_lint(narrative: str, matches: Sequence[re.Match[str]]) -> tuple[str, ...]:
    outside = list(narrative)
    for match in matches:
        for index in range(match.start(), match.end()):
            outside[index] = " "
    text = "".join(outside)
    text = _MARKDOWN_LINK_RE.sub(lambda match: _link_label(match.group(0)), text)
    text = re.sub(r"https?://\S+", " ", text)
    warnings: list[str] = []
    for match in _NUMBER_RE.finditer(text):
        literal = match.group(0)
        if _is_calendar_year(literal) or _is_citation_label(
            text, match.start(), match.end()
        ):
            continue
        line = text.count("\n", 0, match.start()) + 1
        if _is_illustrative_number(text, match.start()):
            continue
        warnings.append(f"numeric literal outside placeholder at line {line}")
    return tuple(warnings)


def _link_label(markdown_link: str) -> str:
    opening = markdown_link.find("[")
    separator = markdown_link.find("](", opening)
    if opening < 0 or separator < 0:
        return " "
    return markdown_link[opening + 1 : separator]


def _is_calendar_year(token: str) -> bool:
    if not token.isdigit() or len(token) != 4:
        return False
    return 1000 <= int(token) <= 2999


def _is_citation_label(text: str, start: int, end: int) -> bool:
    return any(
        match.start() <= start and end <= match.end()
        for match in _CITATION_LABEL_RE.finditer(text)
    )


def _is_illustrative_number(text: str, start: int) -> bool:
    line_start = text.rfind("\n", 0, start) + 1
    line_end = text.find("\n", start)
    if line_end == -1:
        line_end = len(text)
    prefix = text[line_start:start].lower()
    return bool(re.search(r"\billustrative\s+example\s*:\s*$", prefix))


__all__ = ["PlaceholderError", "RenderedNarrative", "escape_markdown_text", "render_narrative"]
