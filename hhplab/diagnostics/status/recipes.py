"""Scanner for recipe-built output namespaces."""

from __future__ import annotations

from pathlib import Path

__all__ = ["scan_recipe_outputs"]


def scan_recipe_outputs(output_root: Path) -> dict:
    recipes: list[dict] = []
    panel_count = manifest_count = diagnostics_count = map_count = 0
    if output_root.exists():
        recipe_dirs = sorted(path for path in output_root.iterdir() if path.is_dir())
        for recipe_dir in recipe_dirs:
            panel_files = sorted(p.name for p in recipe_dir.glob("panel__*.parquet"))
            manifest_files = sorted(p.name for p in recipe_dir.glob("*.manifest.json"))
            diagnostics_files = sorted(p.name for p in recipe_dir.glob("*__diagnostics.json"))
            map_files = sorted(p.name for p in recipe_dir.glob("map__*.html"))
            if not any((panel_files, manifest_files, diagnostics_files, map_files)):
                continue
            panel_count += len(panel_files)
            manifest_count += len(manifest_files)
            diagnostics_count += len(diagnostics_files)
            map_count += len(map_files)
            recipes.append(
                {
                    "name": recipe_dir.name,
                    "path": str(recipe_dir),
                    "panel_files": panel_files,
                    "manifest_files": manifest_files,
                    "diagnostics_files": diagnostics_files,
                    "map_files": map_files,
                }
            )
    return {
        "root": str(output_root),
        "count": len(recipes),
        "panel_count": panel_count,
        "manifest_count": manifest_count,
        "diagnostics_count": diagnostics_count,
        "map_count": map_count,
        "recipes": recipes,
    }
