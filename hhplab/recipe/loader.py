"""Recipe YAML loader with versioned schema dispatch."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import ValidationError

from hhplab.recipe.recipe_schema import RecipeV1
from hhplab.recipe.schema_results import RecipeV2

# Keep the union explicit so callers can route V2 to results workflows instead
# of accidentally handing it to the legacy build planner.
type Recipe = RecipeV1 | RecipeV2

_VERSION_REGISTRY: dict[int, type[Recipe]] = {
    1: RecipeV1,
    2: RecipeV2,
}


class RecipeLoadError(Exception):
    """Raised when a recipe file cannot be loaded or validated."""


def load_recipe(source: str | Path | dict) -> Recipe:
    """Load and validate a recipe from a YAML file path or pre-parsed dict.

    Parameters
    ----------
    source : str | Path | dict
        Path to a YAML file, or a pre-parsed dict (e.g. from tests).

    Returns
    -------
    Recipe
        Validated RecipeV1 or RecipeV2 model selected by the version field.

    Raises
    ------
    RecipeLoadError
        If the file can't be read, YAML is malformed, version is missing/unsupported,
        or schema validation fails.
    """
    data = _read_source(source)
    _check_mapping(data)
    version = _extract_version(data)
    model_cls = _resolve_version(version)
    return _validate(data, model_cls)


def _read_source(source: str | Path | dict) -> object:
    """Read YAML from a file path or return a pre-parsed dict/object."""
    if isinstance(source, dict):
        return source

    if not isinstance(source, (str, Path)):
        # Non-dict, non-path input — return as-is for _check_mapping to reject.
        return source

    path = Path(source)
    if not path.exists():
        raise RecipeLoadError(f"Recipe file not found: {path}")

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RecipeLoadError(f"Cannot read recipe file: {exc}") from exc

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise RecipeLoadError(f"Malformed YAML in {path}: {exc}") from exc

    return data


def _check_mapping(data: object) -> None:
    """Ensure the parsed YAML is a mapping (dict)."""
    if not isinstance(data, dict):
        got = type(data).__name__
        raise RecipeLoadError(f"Recipe must be a YAML mapping, got {got}.")


def _extract_version(data: dict) -> int:
    """Extract and validate the 'version' key."""
    if "version" not in data:
        raise RecipeLoadError("Recipe is missing required 'version' key.")

    version = data["version"]
    if isinstance(version, bool) or not isinstance(version, int):
        raise RecipeLoadError(f"Recipe 'version' must be an integer, got {type(version).__name__}.")

    return version


def _resolve_version(version: int) -> type[Recipe]:
    """Look up the model class for a given version number."""
    if version not in _VERSION_REGISTRY:
        supported = sorted(_VERSION_REGISTRY.keys())
        raise RecipeLoadError(
            f"Unsupported recipe version {version}. Supported versions: {supported}."
        )
    return _VERSION_REGISTRY[version]


def _validate(data: dict, model_cls: type[Recipe]) -> Recipe:
    """Validate data against the resolved model class."""
    try:
        return model_cls.model_validate(data)
    except ValidationError as exc:
        raise RecipeLoadError(f"Recipe schema validation failed:\n{exc}") from exc
