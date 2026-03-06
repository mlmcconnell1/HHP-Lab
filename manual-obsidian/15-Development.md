# Development

## Running Tests

```bash
# Run all tests
pytest

# Run smoke tests only
pytest tests/test_smoke.py -v

# Run offline fixture pipeline smoke test
pytest tests/test_offline_pipeline_fixture.py -v

# Run with coverage
pytest --cov=coclab
```

## Code Quality

```bash
# Lint and check
ruff check .

# Format code
ruff format .
```

## Project Dependencies

**Core:**
- `geopandas` - Geospatial data handling
- `shapely` - Geometry operations
- `pyproj` - Coordinate transformations
- `pyarrow` - Parquet I/O
- `pandas` - Data manipulation
- `folium` - Interactive maps
- `typer` - CLI framework

**Development:**
- `pytest` - Testing
- `ruff` - Linting and formatting

## Adding a New Data Source

1. Create new ingester in `coclab/ingest/`
2. Implement the canonical schema mapping
3. Call `normalize_boundaries()` and `validate_boundaries()`
4. Register vintage using `register_vintage()`
5. Add CLI option in `cli/main.py`
6. Add tests

## Extending Validation

Add new checks in `coclab/geo/validate.py`:

```python
def _validate_custom(gdf: gpd.GeoDataFrame, result: ValidationResult) -> None:
    # Your validation logic
    if issue_found:
        result.add_warning("Description of issue", {"metadata": value})
```

---

**Previous:** [[14-Module-Reference]] | **Next:** [[16-Appendix]]
