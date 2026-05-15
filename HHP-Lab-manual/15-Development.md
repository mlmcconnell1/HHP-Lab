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
pytest --cov=hhplab
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
- `numpy` - Numerical computation
- `httpx` - HTTP client for Census API calls
- `folium` - Interactive maps
- `typer` - CLI framework
- `openpyxl` - Excel `.xlsx` reading
- `pyxlsb` - Excel `.xlsb` reading (used for PIT data)
- `ipumspy` - IPUMS API integration
- `pydantic` - Schema validation (recipe system)

**Development:**
- `pytest` - Testing
- `pytest-httpx` - HTTP mocking for tests
- `ruff` - Linting and formatting

## Adding a New Data Source

1. Create new ingester under its source-owned package, such as `hhplab/hud/` or `hhplab/census/ingest/`
2. Implement the canonical schema mapping
3. Call `normalize_boundaries()` and `validate_boundaries()`
4. Register vintage using `register_vintage()`
5. Add CLI option in `cli/main.py`
6. Add tests

## Extending Validation

Add new checks in `hhplab/geo/geo_validate.py`:

```python
def _validate_custom(gdf: gpd.GeoDataFrame, result: ValidationResult) -> None:
    # Your validation logic
    if issue_found:
        result.add_warning("Description of issue", {"metadata": value})
```

---

**Previous:** [[14-Module-Reference]] | **Next:** [[16-Appendix]]
