"""Mandatory architecture gate for the results-recipe Phase-1 intent.

For divergence, classify the exact intent ID, path, or edge as missing, moved,
wrong-layer, unexpected, or dependency drift; inspect the cause; then repair
implementation or revise the same intent ID with a new revision and rationale.
For intent revisions, dry-run and import, validate, repin, update the mapping,
and rerun scaffold, reconcile, and architecture checks while retaining prior
pin and evidence. Review the implemented responsibilities directly: matching
the authored responsibility text here does not prove behavioral conformance.
"""

from __future__ import annotations

import ast
import json
import subprocess
import tomllib
from collections import defaultdict
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

REPO_ROOT = Path(__file__).parents[1]
POLICY_PATH = REPO_ROOT / "architecture_layers.toml"
POLICY = tomllib.loads(POLICY_PATH.read_text(encoding="utf-8"))[
    "results_v2"
]
ELEMENT_EXPECTATIONS: dict[str, dict[str, str]] = POLICY["elements"]
IMPORT_PREFIXES: dict[str, list[str]] = POLICY["import_prefixes"]
FORBIDDEN_IMPORTS: dict[str, list[str]] = POLICY["forbidden_imports"]
LAYER_ORDER = tuple(tomllib.loads(POLICY_PATH.read_text(encoding="utf-8"))["layers"]["order"])


def _json_command(*args: str, timeout: int = 45) -> dict[str, Any]:
    completed = subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    try:
        response = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise AssertionError(f"{args[0]} returned invalid JSON: {completed.stderr}") from error
    if completed.returncode:
        raise AssertionError(f"{args[0]} rejected current architecture state: {response}")
    return response


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if line.strip():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as error:
                raise AssertionError(f"invalid JSONL at {path}:{line_number}: {error}") from error
    return records


def _intent_records() -> dict[str, dict[str, Any]]:
    path = REPO_ROOT / POLICY["intent_file"]
    return {
        record["intent_element_id"]: record
        for record in _read_jsonl(path)
        if record.get("content", {}).get("plan") == POLICY["plan"]
    }


def _plan_bead_owners() -> dict[str, set[str]]:
    owners: dict[str, set[str]] = defaultdict(set)
    for record in _read_jsonl(REPO_ROOT / POLICY["bead_map"]):
        for intent_id in record.get("intent_ids", []):
            owners[intent_id].add(record["id"])
    return owners


def _bead_statuses() -> dict[str, str]:
    records = _read_jsonl(REPO_ROOT / POLICY["bead_export"])
    return {record["id"]: record["status"] for record in records if "id" in record}


def _is_unfinished(status: str | None) -> bool:
    return status in {"open", "in_progress", "blocked"}


def _active_pending_ids(
    expectations: dict[str, dict[str, str]],
    statuses: dict[str, str],
    *,
    final_release: bool,
) -> set[str]:
    if final_release:
        return set()
    return {
        intent_id
        for intent_id, expected in expectations.items()
        if expected["kind"] == "semantic/module"
        and expected["change"] == "new"
        and _is_unfinished(statuses.get(expected["bead"]))
    }


def _within(path: str, parent: str) -> bool:
    candidate = PurePosixPath(path)
    parent_path = PurePosixPath(parent)
    return candidate != parent_path and parent_path in candidate.parents


def _imported_hhplab_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    source = _module_name(path)
    package = source if path.name == "__init__.py" else source.rpartition(".")[0]
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(
                alias.name
                for alias in node.names
                if alias.name == "hhplab" or alias.name.startswith("hhplab.")
            )
        elif isinstance(node, ast.ImportFrom):
            is_hhplab_import = node.module == "hhplab" or (
                node.module and node.module.startswith("hhplab.")
            )
            if node.level == 0 and node.module and is_hhplab_import:
                imported.add(node.module)
            elif node.level:
                package_parts = package.split(".")
                base_parts = package_parts[: len(package_parts) - node.level + 1]
                if node.module:
                    imported.add(".".join([*base_parts, node.module]))
                else:
                    imported.update(".".join([*base_parts, alias.name]) for alias in node.names)
    return imported


def _module_name(path: Path) -> str:
    relative = path.relative_to(REPO_ROOT).with_suffix("")
    parts = relative.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _prefix_matches(module: str, prefix: str) -> bool:
    return module == prefix or module.startswith(f"{prefix}.")


def _unexpected_files(expected_paths: set[str], added_paths: set[str]) -> set[str]:
    return {
        path
        for path in added_paths
        if path.startswith("hhplab/") and path.endswith(".py") and path not in expected_paths
    }


def _cycles_through(
    graph: dict[str, set[str]],
    scoped_modules: set[str],
    added_edges: set[tuple[str, str]],
) -> set[tuple[str, ...]]:
    """Return new strongly connected import components touching scoped modules."""
    index = 0
    indices: dict[str, int] = {}
    lowlinks: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    cycles: set[tuple[str, ...]] = set()

    def visit(node: str) -> None:
        nonlocal index
        indices[node] = index
        lowlinks[node] = index
        index += 1
        stack.append(node)
        on_stack.add(node)
        for target in graph.get(node, set()):
            if target not in indices:
                visit(target)
                lowlinks[node] = min(lowlinks[node], lowlinks[target])
            elif target in on_stack:
                lowlinks[node] = min(lowlinks[node], indices[target])
        if lowlinks[node] == indices[node]:
            component: list[str] = []
            while True:
                member = stack.pop()
                on_stack.remove(member)
                component.append(member)
                if member == node:
                    break
            has_cycle = len(component) > 1 or node in graph.get(node, set())
            has_new_edge = any(
                (source, target) in added_edges
                for source in component
                for target in graph.get(source, set())
                if target in component
            )
            if scoped_modules.intersection(component) and has_cycle and has_new_edge:
                cycles.add(tuple(sorted(component)))

    for node in graph:
        if node not in indices:
            visit(node)
    return cycles


def _expected_module_name(path: str) -> str:
    relative = PurePosixPath(path)
    parts = list(relative.with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _git_paths_at(revision: str) -> set[str]:
    completed = subprocess.run(
        ["git", "ls-tree", "-r", "--name-only", revision, "--", "hhplab"],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return {path for path in completed.stdout.splitlines() if path.endswith(".py")}


def _source_imports(source: str, path: str) -> set[str]:
    tree = ast.parse(source, filename=path)
    module = _expected_module_name(path)
    package = module if PurePosixPath(path).name == "__init__.py" else module.rpartition(".")[0]
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(
                alias.name
                for alias in node.names
                if alias.name == "hhplab" or alias.name.startswith("hhplab.")
            )
        elif isinstance(node, ast.ImportFrom):
            is_hhplab_import = node.module == "hhplab" or (
                node.module and node.module.startswith("hhplab.")
            )
            if node.level == 0 and node.module and is_hhplab_import:
                imported.add(node.module)
            elif node.level:
                package_parts = package.split(".")
                base_parts = package_parts[: len(package_parts) - node.level + 1]
                if node.module:
                    imported.add(".".join([*base_parts, node.module]))
                else:
                    imported.update(".".join([*base_parts, alias.name]) for alias in node.names)
    return imported


def _baseline_imports(path: str) -> set[str]:
    completed = subprocess.run(
        ["git", "show", f"{POLICY['baseline_commit']}:{path}"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        return set()
    return _source_imports(completed.stdout, path)


def _gate_errors(
    *,
    expectations: dict[str, dict[str, str]],
    intent: dict[str, dict[str, Any]],
    correspondence: dict[str, dict[str, Any]],
    production_paths: set[str],
    import_graph: dict[str, set[str]],
    added_import_edges: set[tuple[str, str]],
    unfinished_beads: set[str],
    final_release: bool = False,
    added_paths: set[str] | None = None,
) -> list[str]:
    errors: list[str] = []
    if set(intent) != set(expectations):
        errors.append(
            f"intent identity set differs: missing={sorted(set(expectations) - set(intent))}, "
            f"unexpected={sorted(set(intent) - set(expectations))}"
        )
    if set(correspondence) != set(expectations):
        errors.append("reconcile rows are stale, incomplete, or contain unexpected identities")

    all_expected_paths = {item["path"] for item in expectations.values()}
    if added_paths is not None:
        unexpected = _unexpected_files(all_expected_paths, added_paths)
        if unexpected:
            errors.append(f"unexpected production files: {sorted(unexpected)}")

    for intent_id, expected in expectations.items():
        element = intent.get(intent_id)
        if element is None:
            continue
        content = element.get("content", {})
        actual = {
            "path": element.get("repo_relative_path", ""),
            "kind": element.get("kind", ""),
            "parent": element.get("parent_id", ""),
            "layer": content.get("layer", ""),
            "change": content.get("change", ""),
        }
        for field, expected_value in expected.items():
            if field in {"bead", "responsibility"}:
                continue
            if actual[field] != expected_value:
                errors.append(
                    f"{intent_id} {field} drift: expected {expected_value!r}, "
                    f"found {actual[field]!r}"
                )
        if content.get("layer") not in LAYER_ORDER:
            errors.append(f"{intent_id} has an unknown architecture layer {content.get('layer')!r}")
        expected_responsibility = POLICY["responsibilities"].get(intent_id)
        if content.get("responsibility") != expected_responsibility:
            errors.append(f"{intent_id} responsibility drifted from reviewed architecture policy")
        if expected["kind"] == "semantic/module" and expected["change"] == "new":
            if content.get("dependency_allowlist") != IMPORT_PREFIXES.get(intent_id):
                errors.append(
                    f"{intent_id} dependency policy drifted from architecture_layers.toml"
                )

        parent_id = expected["parent"]
        if parent_id:
            parent = expectations.get(parent_id)
            if parent is None or not _within(expected["path"], parent["path"]):
                errors.append(f"{intent_id} is outside its declared parent {parent_id}")

        if expected["kind"] == "semantic/module":
            exists = expected["path"] in production_paths
            if not exists:
                allowed_pending = (
                    expected["change"] == "new"
                    and not final_release
                    and expected["bead"] in unfinished_beads
                )
                if not allowed_pending:
                    errors.append(f"{intent_id} is missing production module {expected['path']}")
            if exists and expected["change"] == "new":
                actual_imports = import_graph.get(_expected_module_name(expected["path"]), set())
                permitted = IMPORT_PREFIXES.get(intent_id, [])
                unexpected_imports = {
                    imported
                    for imported in actual_imports
                    if not any(_prefix_matches(imported, prefix) for prefix in permitted)
                }
                if unexpected_imports:
                    errors.append(
                        f"{intent_id} has undeclared imports: {sorted(unexpected_imports)}"
                    )
            elif exists:
                module = _expected_module_name(expected["path"])
                added_targets = {
                    target for source, target in added_import_edges if source == module
                }
                permitted = content.get("dependency_allowlist", [])
                unexpected_imports = {
                    imported
                    for imported in added_targets
                    if not any(_prefix_matches(imported, prefix) for prefix in permitted)
                }
                if unexpected_imports:
                    errors.append(
                        f"{intent_id} adds imports outside its declared dependencies: "
                        f"{sorted(unexpected_imports)}"
                    )

        if expected["kind"] == "semantic/package" and expected["path"] not in production_paths:
            if not (REPO_ROOT / expected["path"]).is_dir():
                errors.append(f"{intent_id} package directory is missing: {expected['path']}")

        row = correspondence.get(intent_id)
        if row is not None:
            if row.get("intent_revision_id") != element.get("revision_id"):
                errors.append(f"{intent_id} reconcile row has a stale intent revision")
            finding = row.get("finding")
            candidates = row.get("candidates", [])
            candidate_paths = {candidate.get("repo_relative_path") for candidate in candidates}
            if finding == "matched":
                if row.get("confidence") != "exact" or candidate_paths != {expected["path"]}:
                    errors.append(f"{intent_id} has stale, moved, or ambiguous correspondence")
            elif finding == "unknown":
                allowed_pending = (
                    expected["kind"] == "semantic/module"
                    and expected["change"] == "new"
                    and expected["path"] not in production_paths
                    and not final_release
                    and expected["bead"] in unfinished_beads
                )
                if not allowed_pending or candidates:
                    errors.append(f"{intent_id} is unexplained or ambiguous unknown correspondence")
            else:
                errors.append(f"{intent_id} has nonconforming correspondence finding {finding!r}")

    for source, forbidden_prefixes in FORBIDDEN_IMPORTS.items():
        for module, imported_modules in import_graph.items():
            if _prefix_matches(module, source):
                for imported in imported_modules:
                    if any(_prefix_matches(imported, prefix) for prefix in forbidden_prefixes):
                        errors.append(f"forbidden dependency: {module} -> {imported}")

    changed_modules = {source for source, _target in added_import_edges}
    cycles = _cycles_through(import_graph, changed_modules, added_import_edges)
    if cycles:
        errors.append(f"new scoped import cycle(s): {sorted(cycles)}")
    return errors


def _source_import_graph() -> tuple[dict[str, set[str]], set[str]]:
    graph: dict[str, set[str]] = {}
    paths: set[str] = set()
    for source in (REPO_ROOT / "hhplab").rglob("*.py"):
        if source.name == "_version.py":
            continue
        relative = source.relative_to(REPO_ROOT).as_posix()
        module = _module_name(source)
        paths.add(relative)
        graph[module] = _imported_hhplab_modules(source)
    return graph, paths


def _added_scoped_import_edges(import_graph: dict[str, set[str]]) -> set[tuple[str, str]]:
    added: set[tuple[str, str]] = set()
    for expected in ELEMENT_EXPECTATIONS.values():
        if expected["kind"] != "semantic/module":
            continue
        path = expected["path"]
        module = _expected_module_name(path)
        current = import_graph.get(module, set())
        previous = _baseline_imports(path)
        added.update((module, target) for target in current - previous)
    return added


def _bead_state() -> tuple[dict[str, str], set[str], bool]:
    statuses = _bead_statuses()
    owners = _plan_bead_owners()
    errors = []
    for intent_id, expected in ELEMENT_EXPECTATIONS.items():
        bead_id = expected["bead"]
        if not bead_id:
            continue
        if bead_id not in owners.get(intent_id, set()):
            errors.append(f"{intent_id} bead mapping disagrees with the plan: {bead_id}")
        if bead_id not in statuses:
            errors.append(f"missing bead status for {bead_id}")
    if errors:
        raise AssertionError("\n".join(errors))
    release_status = statuses.get("coclab-ff6fp.25")
    unfinished = {bead for bead, status in statuses.items() if _is_unfinished(status)}
    return statuses, unfinished, release_status in {"in_progress", "closed"}


@pytest.fixture(scope="module")
def architecture_evidence() -> dict[str, Any]:
    config = _json_command("elr", "scaffold", "--repo", ".", "--check")
    view = _json_command("elr", "intent", "view", "--working-intent", "--repo", ".")
    reconcile = _json_command(
        "elr",
        "reconcile",
        "--working-intent",
        "--repo",
        ".",
        "--input",
        POLICY["reconcile_request"],
    )
    return {"scaffold": config, "view": view, "reconcile": reconcile["report"]}


def test_results_intent_is_valid_fresh_and_fully_reconciled(
    architecture_evidence: dict[str, Any],
) -> None:
    validation = _json_command("elr", "intent", "validate")
    assert validation.get("operation") == "intent.validate"
    assert validation.get("valid") is True

    scaffold = architecture_evidence["scaffold"]
    assert scaffold["artifacts"]["observations"]["status"] == "fresh"
    source_witness = scaffold["source_snapshot"]["content_witness"]
    reconcile = architecture_evidence["reconcile"]
    batch = reconcile["batch"]
    current_view = architecture_evidence["view"]["view"]["view_witness"]
    final_pin = json.loads((REPO_ROOT / POLICY["final_pin"]).read_text(encoding="utf-8"))
    assert final_pin["pinned"] is True
    assert final_pin["view_witness"] == current_view
    assert reconcile["view_witness"] == current_view
    assert batch["view_witness"] == current_view
    assert batch["has_more"] is False
    assert batch["returned"] == len(ELEMENT_EXPECTATIONS)
    assert batch["view_conflicts"] == 0
    assert len(batch["results"]) == len(ELEMENT_EXPECTATIONS)

    result_ids = {row["intent_element_id"] for row in batch["results"]}
    assert result_ids == set(ELEMENT_EXPECTATIONS)
    for row in batch["results"]:
        observations = row.get("observation", {}).get("snapshot", {})
        if observations:
            assert observations["content_witness"] == source_witness
    bindings = reconcile["structure"]["root_bindings"]
    assert len(bindings) == 1
    assert bindings[0]["confidence"] == "exact"
    assert bindings[0]["intent_root_id"] == "intent:results-v2:hhplab"


def test_results_architecture_matches_intent_and_import_policy(
    architecture_evidence: dict[str, Any],
) -> None:
    statuses, unfinished, final_release = _bead_state()
    intent = _intent_records()
    correspondence = {
        row["intent_element_id"]: row
        for row in architecture_evidence["reconcile"]["batch"]["results"]
    }
    import_graph, production_paths = _source_import_graph()
    baseline_paths = _git_paths_at(POLICY["baseline_commit"])
    added_paths = production_paths - baseline_paths
    added_import_edges = _added_scoped_import_edges(import_graph)
    errors = _gate_errors(
        expectations=ELEMENT_EXPECTATIONS,
        intent=intent,
        correspondence=correspondence,
        production_paths=production_paths,
        import_graph=import_graph,
        added_import_edges=added_import_edges,
        unfinished_beads=unfinished,
        final_release=final_release,
        added_paths=added_paths,
    )
    assert errors == [], "\n".join(errors)

    pending = _active_pending_ids(ELEMENT_EXPECTATIONS, statuses, final_release=final_release)
    for intent_id in pending:
        assert ELEMENT_EXPECTATIONS[intent_id]["change"] == "new"


@pytest.mark.parametrize(
    ("case", "row", "added_paths", "graph", "expected_fragment"),
    [
        (
            "moved-module",
            {
                "finding": "matched",
                "confidence": "exact",
                "candidates": [{"repo_relative_path": "hhplab/recipe/moved.py"}],
            },
            set(),
            {},
            "moved",
        ),
        (
            "unexpected-production-file",
            {
                "finding": "matched",
                "confidence": "exact",
                "candidates": [{"repo_relative_path": "hhplab/analysis/sample.py"}],
            },
            {"hhplab/analysis/unplanned.py"},
            {},
            "unexpected production files",
        ),
        (
            "forbidden-import",
            {
                "finding": "matched",
                "confidence": "exact",
                "candidates": [{"repo_relative_path": "hhplab/results/bands.py"}],
            },
            set(),
            {"hhplab.analysis.sample": {"hhplab.recipe.results_plan"}},
            "forbidden dependency",
        ),
        (
            "stale-ambiguous-correspondence",
            {
                "finding": "matched",
                "confidence": "ambiguous",
                "candidates": [
                    {"repo_relative_path": "hhplab/analysis/sample.py"},
                    {"repo_relative_path": "hhplab/analysis/other.py"},
                ],
            },
            set(),
            {},
            "stale, moved, or ambiguous",
        ),
    ],
)
def test_synthetic_architecture_drift_is_rejected(
    case: str,
    row: dict[str, Any],
    added_paths: set[str],
    graph: dict[str, set[str]],
    expected_fragment: str,
) -> None:
    intent_id = "intent:results-v2:samples"
    expectations = {intent_id: ELEMENT_EXPECTATIONS[intent_id]}
    intent = {
        intent_id: {
            "intent_element_id": intent_id,
            "repo_relative_path": "hhplab/analysis/sample.py",
            "kind": "semantic/module",
            "parent_id": "intent:results-v2:analysis",
            "content": {
                "layer": "application",
                "change": "new",
                "responsibility": POLICY["responsibilities"][intent_id],
                "dependency_allowlist": ["hhplab.schema", "hhplab.analysis.contracts"],
            },
        }
    }
    correspondence = {intent_id: row}
    errors = _gate_errors(
        expectations=expectations,
        intent=intent,
        correspondence=correspondence,
        production_paths={"hhplab/analysis/sample.py"},
        import_graph=graph,
        added_import_edges=set(),
        unfinished_beads=set(),
        added_paths=added_paths,
    )
    assert any(expected_fragment in error for error in errors), (
        f"{case} unexpectedly passed: {errors}"
    )


def test_missing_new_module_is_pending_only_for_its_open_bead() -> None:
    intent_id = "intent:results-v2:samples"
    root_id = "intent:results-v2:hhplab"
    parent_id = "intent:results-v2:analysis"
    expectations = {
        root_id: ELEMENT_EXPECTATIONS[root_id],
        parent_id: ELEMENT_EXPECTATIONS[parent_id],
        intent_id: ELEMENT_EXPECTATIONS[intent_id],
    }
    intent = {
        root_id: {
            "repo_relative_path": "hhplab",
            "kind": "semantic/package",
            "revision_id": "revision:results-v2:hhplab:2",
            "content": {
                "layer": "contracts",
                "change": "retain",
                "responsibility": POLICY["responsibilities"][root_id],
            },
        },
        parent_id: {
            "repo_relative_path": "hhplab/analysis",
            "kind": "semantic/package",
            "parent_id": "intent:results-v2:hhplab",
            "revision_id": "revision:results-v2:analysis:2",
            "content": {
                "layer": "application",
                "change": "extend",
                "responsibility": POLICY["responsibilities"][parent_id],
            },
        },
        intent_id: {
            "repo_relative_path": "hhplab/analysis/sample.py",
            "kind": "semantic/module",
            "parent_id": "intent:results-v2:analysis",
            "revision_id": "revision:results-v2:samples:1",
            "content": {
                "layer": "application",
                "change": "new",
                "responsibility": POLICY["responsibilities"][intent_id],
                "dependency_allowlist": ["hhplab.schema", "hhplab.analysis.contracts"],
            },
        }
    }
    missing_row = {
        "intent_revision_id": "revision:results-v2:samples:1",
        "finding": "unknown",
        "confidence": "indeterminate",
        "candidates": [],
    }
    parent_row = {
        "intent_revision_id": "revision:results-v2:analysis:2",
        "finding": "matched",
        "confidence": "exact",
        "candidates": [{"repo_relative_path": "hhplab/analysis"}],
    }
    root_row = {
        "intent_revision_id": "revision:results-v2:hhplab:2",
        "finding": "matched",
        "confidence": "exact",
        "candidates": [{"repo_relative_path": "hhplab"}],
    }
    pending_errors = _gate_errors(
        expectations=expectations,
        intent=intent,
        correspondence={root_id: root_row, parent_id: parent_row, intent_id: missing_row},
        production_paths=set(),
        import_graph={},
        added_import_edges=set(),
        unfinished_beads={"coclab-ff6fp.8"},
    )
    assert pending_errors == []

    final_errors = _gate_errors(
        expectations=expectations,
        intent=intent,
        correspondence={root_id: root_row, parent_id: parent_row, intent_id: missing_row},
        production_paths=set(),
        import_graph={},
        added_import_edges=set(),
        unfinished_beads={"coclab-ff6fp.8"},
        final_release=True,
    )
    assert any("missing production module" in error for error in final_errors)


def test_new_scoped_import_cycle_is_rejected() -> None:
    cycle = {
        "hhplab.analysis.sample": {"hhplab.recipe.results_plan"},
        "hhplab.recipe.results_plan": {"hhplab.analysis.sample"},
    }
    assert _cycles_through(
        cycle,
        {"hhplab.analysis.sample"},
        {
            ("hhplab.analysis.sample", "hhplab.recipe.results_plan"),
            ("hhplab.recipe.results_plan", "hhplab.analysis.sample"),
        },
    ) == {("hhplab.analysis.sample", "hhplab.recipe.results_plan")}
