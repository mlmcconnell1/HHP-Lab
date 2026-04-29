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
# Check CLI is available (the runtime command rename happens later)
coclab --help

# Run tests
pytest tests/test_smoke.py -v
```

## Working Directory

The CLI expects to be run from the HHP-Lab project root directory. If run from a different directory, you'll see a warning:

```
Warning: Current directory may not be the CoC Lab project root. Missing: pyproject.toml, coclab, data
```

The current runtime warning still says `CoC Lab` and checks for `coclab/`
because the package/CLI rename is staged separately. While commands may still
work, file paths assume the project root as the working directory.

---

**Previous:** [[01-Overview]] | **Next:** [[03-Architecture]]
