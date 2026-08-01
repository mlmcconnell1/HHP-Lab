"""Executable dependency-direction policy for HHP-Lab's production modules."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parents[1]
PACKAGE_ROOT = REPO_ROOT / "hhplab"
POLICY_PATH = REPO_ROOT / "architecture_layers.toml"


def _load_policy() -> dict[str, object]:
    with POLICY_PATH.open("rb") as policy_file:
        return tomllib.load(policy_file)


POLICY = _load_policy()
LAYER_ORDER = tuple(POLICY["layers"]["order"])
PACKAGE_LAYERS = POLICY["packages"]
MODULE_LAYERS = POLICY["modules"]
ALLOWED_UPWARD_EDGES = frozenset(POLICY["exceptions"]["upward_edges"])


def _module_name(path: Path) -> str:
    relative = path.relative_to(REPO_ROOT).with_suffix("")
    parts = relative.parts
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts)


def _layer_for(module: str) -> str | None:
    if module in MODULE_LAYERS:
        return MODULE_LAYERS[module]
    matches = [
        package
        for package in PACKAGE_LAYERS
        if module == package or module.startswith(f"{package}.")
    ]
    if not matches:
        return None
    return PACKAGE_LAYERS[max(matches, key=len)]


def _imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    source = _module_name(path)
    package = source if path.name == "__init__.py" else source.rpartition(".")[0]
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names if alias.name.startswith("hhplab"))
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and node.module.startswith("hhplab"):
                imported.add(node.module)
            elif node.level:
                package_parts = package.split(".")
                base_parts = package_parts[: len(package_parts) - node.level + 1]
                if node.module:
                    imported.add(".".join([*base_parts, node.module]))
                else:
                    imported.update(".".join([*base_parts, alias.name]) for alias in node.names)
    return imported


PRODUCTION_MODULES = tuple(
    sorted(_module_name(path) for path in PACKAGE_ROOT.rglob("*.py") if path.name != "_version.py")
)


@pytest.mark.parametrize("module", PRODUCTION_MODULES)
def test_every_production_module_has_a_layer(module: str) -> None:
    assert _layer_for(module) in LAYER_ORDER


def test_no_unreviewed_upward_dependencies() -> None:
    rank = {layer: index for index, layer in enumerate(LAYER_ORDER)}
    violations: set[str] = set()
    for path in PACKAGE_ROOT.rglob("*.py"):
        source = _module_name(path)
        source_layer = _layer_for(source)
        if source_layer is None:
            continue
        for target in _imports(path):
            target_layer = _layer_for(target)
            if target_layer is not None and rank[target_layer] > rank[source_layer]:
                violations.add(f"{source} -> {target}")

    assert violations == ALLOWED_UPWARD_EDGES
