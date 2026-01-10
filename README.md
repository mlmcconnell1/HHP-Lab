# CoC Lab

CoC Lab is a Python-based data and geospatial infrastructure for working with Continuum of Care (CoC) boundary data. It provides tools to ingest, validate, version, and visualize CoC boundaries from HUD data sources.

## Quickstart

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/coc-pit.git
cd coc-pit

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .

# For development (includes pytest, ruff)
uv sync --extra dev
```

### Basic Usage

#### 1. Ingest CoC boundary data

Ingest boundaries from HUD Exchange GIS Tools (annual vintages):

```bash
coclab ingest-boundaries --source hud_exchange --vintage 2025
```

Or from HUD Open Data (current snapshot):

```bash
coclab ingest-boundaries --source hud_opendata --snapshot latest
```

#### 2. List available vintages

```bash
coclab list-vintages
```

Example output:
```
Available boundary vintages:

Vintage                        Source                    Features   Ingested At
-------------------------------------------------------------------------------------
2025                           hud_exchange_gis_tools    400        2025-01-15 14:30
HUDOpenData_2025-01-10         hud_opendata_arcgis       402        2025-01-10 09:15
```

#### 3. Render a CoC boundary map

Show a specific CoC (uses latest vintage by default):

```bash
coclab show --coc CO-500
```

Specify a vintage:

```bash
coclab show --coc CO-500 --vintage 2025
```

Custom output path:

```bash
coclab show --coc NY-600 --output my_map.html
```

The command generates an interactive HTML map with the CoC boundary and opens it in your browser.

### Python API

```python
from coclab.ingest import ingest_hud_exchange
from coclab.registry import latest_vintage, list_vintages
from coclab.viz import render_coc_map

# Ingest a vintage
output_path = ingest_hud_exchange("2025")

# Get the latest vintage
vintage = latest_vintage()

# List all vintages
for entry in list_vintages():
    print(f"{entry.boundary_vintage}: {entry.feature_count} features")

# Render a map
map_path = render_coc_map("CO-500", vintage="2025")
print(f"Map saved to: {map_path}")
```

## Data Sources

- **HUD Exchange GIS Tools**: Annual CoC boundary shapefiles from [hudexchange.info](https://www.hudexchange.info/programs/coc/gis-tools/)
- **HUD Open Data (ArcGIS Hub)**: Current CoC Grantee Areas from [HUD Open Data](https://hudgis-hud.opendata.arcgis.com/)

## Project Structure

```
coclab/
  cli/          # CLI commands (Typer)
  geo/          # Geometry normalization and validation
  ingest/       # Data source ingesters
  registry/     # Vintage tracking and version selection
  viz/          # Map rendering (Folium)
data/
  raw/          # Downloaded source files
  curated/      # Processed GeoParquet files
tests/          # Test suite including smoke tests
```

## Development

### Running tests

```bash
# Run all tests
pytest

# Run smoke tests only
pytest tests/test_smoke.py -v

# Run with coverage
pytest --cov=coclab
```

### Code quality

```bash
# Lint and format
ruff check .
ruff format .
```

## License

[MIT License](LICENSE)
