# Installation

## Prerequisites

- Python 3.12+
- `uv` package manager (recommended) or `pip`

## Quick Install

```bash
# Clone the repository
git clone https://github.com/mlmcconnell1/HHP-Lab.git
cd HHP-Lab

# Install with uv (recommended)
uv sync

# Or install with pip
pip install -e .

# For development (includes pytest, ruff)
uv sync --extra dev
```

## Verify Installation

```bash
# Check CLI is available
hhplab --help

# Run tests
pytest tests/test_smoke.py -v
```

## Working Directory

The CLI expects to be run from the HHP-Lab project root directory. If run from a different directory, you'll see a warning:

```
Warning: Current directory may not be the HHP-Lab project root. Missing: pyproject.toml, hhplab, data
```

The current runtime checks for `hhplab/` because that is the package directory.
Commands may still work outside the repo root, but path-oriented workflows
assume the project root as the working directory.

---

**Previous:** [[01-Overview]] | **Next:** [[03-Architecture]]
