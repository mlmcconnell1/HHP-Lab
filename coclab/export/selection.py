"""Artifact selection and inference logic for export bundles."""

from pathlib import Path

from coclab.export.types import ArtifactRecord, BundleConfig, SelectionPlan

# Standard file patterns for curated artifacts (new temporal shorthand + legacy)
PATTERNS = {
    # Panel: new panel__Y*@B*.parquet, legacy coc_panel__*.parquet
    "panel": ["data/curated/panel/panel__Y*.parquet", "data/curated/panel/coc_panel__*.parquet"],
    # Tract crosswalk: new xwalk__B*xT*.parquet, legacy coc_tract_xwalk__*.parquet
    "tract_xwalk": [
        "data/curated/xwalks/xwalk__B*xT*.parquet",
        "data/curated/xwalks/coc_tract_xwalk__*.parquet",
    ],
    # County crosswalk: new xwalk__B*xC*.parquet, legacy coc_county_xwalk__*.parquet
    "county_xwalk": [
        "data/curated/xwalks/xwalk__B*xC*.parquet",
        "data/curated/xwalks/coc_county_xwalk__*.parquet",
    ],
    # Boundaries: new boundaries__B*.parquet, legacy coc_boundaries__*.parquet
    "boundaries": [
        "data/curated/coc_boundaries/boundaries__B*.parquet",
        "data/curated/coc_boundaries/coc_boundaries__*.parquet",
    ],
    # PIT: new pit__P*.parquet, legacy pit_counts__*.parquet
    "pit": ["data/curated/pit/pit__P*.parquet", "data/curated/pit/pit_counts__*.parquet"],
    # ZORI: new zori__A*.parquet, legacy coc_zori*.parquet
    "zori": ["data/curated/zori/zori__A*.parquet", "data/curated/zori/coc_zori*.parquet"],
    # Measures: new measures__A*.parquet, legacy coc_measures__*.parquet
    "measures": [
        "data/curated/measures/measures__A*.parquet",
        "data/curated/measures/coc_measures__*.parquet",
    ],
}

# Destination paths within bundle by category
DEST_PATHS = {
    "panel": "data/panels",
    "tract_xwalk": "data/inputs/xwalks",
    "county_xwalk": "data/inputs/xwalks",
    "boundaries": "data/inputs/boundaries",
    "pit": "data/inputs/pit",
    "zori": "data/inputs/rents",
    "measures": "data/inputs/acs",
}

# Categories and which vintage flags they use
CATEGORY_VINTAGES = {
    "tract_xwalk": ["boundary_vintage", "tract_vintage"],
    "county_xwalk": ["boundary_vintage", "county_vintage"],
    "boundaries": ["boundary_vintage"],
    "pit": ["years"],
    "zori": ["boundary_vintage", "county_vintage", "acs_vintage"],
    "measures": ["boundary_vintage", "acs_vintage"],
}


def _get_vintage_from_config(config: BundleConfig, vintage_key: str) -> str | None:
    """Extract a vintage value from config by key name."""
    return getattr(config, vintage_key, None)


def _file_matches_vintage(filename: str, vintage: str | None) -> bool:
    """Check if a filename contains the specified vintage string."""
    if vintage is None:
        return True  # No filter, all files match
    return vintage in filename


def _select_latest_by_mtime(files: list[Path]) -> Path | None:
    """Select the most recently modified file from a list."""
    if not files:
        return None
    return max(files, key=lambda f: f.stat().st_mtime)


def _find_matching_files(patterns: str | list[str], base_dir: Path) -> list[Path]:
    """Find files matching glob pattern(s) relative to base_dir.

    Args:
        patterns: Single pattern string or list of patterns to match
        base_dir: Base directory for pattern resolution

    Returns:
        Sorted list of matching file paths (deduplicated)
    """
    if isinstance(patterns, str):
        patterns = [patterns]

    matches = set()
    for pattern in patterns:
        full_pattern = base_dir / pattern
        matches.update(full_pattern.parent.glob(full_pattern.name))

    return sorted(matches)


def select_panel(
    explicit_path: Path | None, config: BundleConfig, base_dir: Path
) -> tuple[Path, bool]:
    """
    Select panel file.

    Args:
        explicit_path: Explicitly provided panel path, or None for inference
        config: Bundle configuration with vintage hints
        base_dir: Base directory for file lookups

    Returns:
        Tuple of (resolved_path, was_inferred)

    Raises:
        FileNotFoundError: If no matching panel found
    """
    # If explicit path provided, use it
    if explicit_path is not None:
        resolved = base_dir / explicit_path if not explicit_path.is_absolute() else explicit_path
        if not resolved.exists():
            raise FileNotFoundError(f"Explicit panel not found: {resolved}")
        return resolved, False

    # Search for panels matching pattern
    candidates = _find_matching_files(PATTERNS["panel"], base_dir)

    # Filter by years if specified
    if config.years:
        # Try to match year range in filename, e.g., coc_panel__2011_2024__zori.parquet
        year_candidates = [f for f in candidates if config.years.replace("-", "_") in f.name]
        if year_candidates:
            candidates = year_candidates

    # Select most recent if multiple
    selected = _select_latest_by_mtime(candidates)
    if selected is None:
        raise FileNotFoundError(f"No panel files found matching {PATTERNS['panel']} in {base_dir}")

    return selected, True


def _select_files_for_category(
    category: str, config: BundleConfig, base_dir: Path
) -> list[tuple[Path, bool]]:
    """
    Select files for a given category based on vintage filters.

    Returns:
        List of (path, was_inferred) tuples
    """
    pattern = PATTERNS.get(category)
    if not pattern:
        return []

    candidates = _find_matching_files(pattern, base_dir)
    if not candidates:
        return []

    # Get relevant vintage keys for this category
    vintage_keys = CATEGORY_VINTAGES.get(category, [])

    # Apply vintage filters
    filtered = candidates
    for vintage_key in vintage_keys:
        vintage_val = _get_vintage_from_config(config, vintage_key)
        if vintage_val:
            filtered = [f for f in filtered if _file_matches_vintage(f.name, vintage_val)]

    # If filtering narrowed to nothing, fall back to most recent
    was_inferred = True
    if not filtered:
        selected = _select_latest_by_mtime(candidates)
        if selected:
            filtered = [selected]
    elif len(filtered) > 1:
        # Multiple matches - use most recent
        selected = _select_latest_by_mtime(filtered)
        if selected:
            filtered = [selected]

    return [(f, was_inferred) for f in filtered]


def select_inputs(config: BundleConfig, base_dir: Path) -> list[tuple[Path, str, bool]]:
    """
    Select input files based on vintages.

    Args:
        config: Bundle configuration with vintage specifications
        base_dir: Base directory for file lookups

    Returns:
        List of (path, category, was_inferred) tuples
    """
    results: list[tuple[Path, str, bool]] = []

    # Process each input category
    for category in ["boundaries", "tract_xwalk", "county_xwalk", "pit", "zori", "measures"]:
        selections = _select_files_for_category(category, config, base_dir)
        for path, was_inferred in selections:
            results.append((path, category, was_inferred))

    return results


def select_diagnostics(config: BundleConfig, base_dir: Path) -> list[Path]:
    """
    Select diagnostic files.

    Args:
        config: Bundle configuration
        base_dir: Base directory for file lookups

    Returns:
        List of diagnostic file paths
    """
    diagnostics_dir = base_dir / "data" / "diagnostics"
    if not diagnostics_dir.exists():
        return []

    # Collect all diagnostic artifacts (parquet, json, html)
    results: list[Path] = []
    for ext in ["*.parquet", "*.json", "*.html", "*.csv"]:
        results.extend(diagnostics_dir.glob(ext))
        # Also check subdirectories
        results.extend(diagnostics_dir.rglob(ext))

    return sorted(set(results))


def _make_artifact_record(
    path: Path, category: str, dest_base: str | None = None
) -> ArtifactRecord:
    """Create an ArtifactRecord for a given file and category."""
    # Determine role based on category
    role_map = {
        "panel": "panel",
        "tract_xwalk": "input",
        "county_xwalk": "input",
        "boundaries": "input",
        "pit": "input",
        "zori": "input",
        "measures": "input",
        "diagnostic": "diagnostic",
    }
    role = role_map.get(category, "input")

    # Determine destination path
    if dest_base:
        dest_path = f"{dest_base}/{path.name}"
    else:
        dest_dir = DEST_PATHS.get(category, "data/inputs")
        dest_path = f"{dest_dir}/{path.name}"

    return ArtifactRecord(
        role=role,  # type: ignore[arg-type]
        source_path=path,
        dest_path=dest_path,
    )


def build_selection_plan(config: BundleConfig, base_dir: Path = Path(".")) -> SelectionPlan:
    """
    Build complete selection plan based on config.

    Selects artifacts based on configuration, using inference where needed.
    Prints selections to console for transparency.

    Args:
        config: Bundle configuration specifying what to include
        base_dir: Base directory for file lookups (default: current dir)

    Returns:
        SelectionPlan with all artifacts organized by role
    """
    base_dir = base_dir.resolve()
    inferred_selections: dict[str, str] = {}

    # Initialize artifact lists
    panel_artifacts: list[ArtifactRecord] = []
    input_artifacts: list[ArtifactRecord] = []
    diagnostic_artifacts: list[ArtifactRecord] = []
    derived_artifacts: list[ArtifactRecord] = []
    codebook_artifacts: list[ArtifactRecord] = []

    # Select panel if included
    if "panel" in config.include:
        try:
            panel_path, was_inferred = select_panel(config.panel_path, config, base_dir)
            artifact = _make_artifact_record(panel_path, "panel")
            panel_artifacts.append(artifact)

            if was_inferred:
                inferred_selections["panel"] = str(panel_path)
                print(f"[inferred] panel: {panel_path.name}")
            else:
                print(f"[explicit] panel: {panel_path.name}")
        except FileNotFoundError as e:
            print(f"[warning] {e}")

    # Select inputs if included
    if "inputs" in config.include:
        inputs = select_inputs(config, base_dir)
        for path, category, was_inferred in inputs:
            artifact = _make_artifact_record(path, category)
            input_artifacts.append(artifact)

            if was_inferred:
                key = f"input:{category}"
                inferred_selections[key] = str(path)
                print(f"[inferred] {category}: {path.name}")
            else:
                print(f"[explicit] {category}: {path.name}")

    # Select diagnostics if included
    if "diagnostics" in config.include:
        diag_files = select_diagnostics(config, base_dir)
        for path in diag_files:
            artifact = ArtifactRecord(
                role="diagnostic",
                source_path=path,
                dest_path=f"diagnostics/{path.name}",
            )
            diagnostic_artifacts.append(artifact)
            print(f"[selected] diagnostic: {path.name}")

        if not diag_files:
            print("[info] No diagnostic files found in data/diagnostics/")
            print(
                "       To include diagnostics, run: coclab panel-diagnostics --panel <path> "
                "--format csv --output-dir data/diagnostics/"
            )

    # Codebook artifacts would be generated, not selected
    # This is a placeholder for integration with codebook generation
    if "codebook" in config.include:
        print("[info] Codebook will be generated during bundle creation")

    # Print summary
    total = (
        len(panel_artifacts)
        + len(input_artifacts)
        + len(diagnostic_artifacts)
        + len(derived_artifacts)
    )
    print(f"\nSelection summary: {total} artifacts selected")
    if panel_artifacts:
        print(f"  - panels: {len(panel_artifacts)}")
    if input_artifacts:
        print(f"  - inputs: {len(input_artifacts)}")
    if diagnostic_artifacts:
        print(f"  - diagnostics: {len(diagnostic_artifacts)}")
    if inferred_selections:
        print(f"  - inferred: {len(inferred_selections)} selections")

    return SelectionPlan(
        panel_artifacts=panel_artifacts,
        input_artifacts=input_artifacts,
        derived_artifacts=derived_artifacts,
        diagnostic_artifacts=diagnostic_artifacts,
        codebook_artifacts=codebook_artifacts,
        inferred_selections=inferred_selections,
    )
