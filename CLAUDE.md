# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CoC Lab (Continuum of Care - Point In Time) is a Python-based data infrastructure project for managing HUD Continuum of Care boundary geometries with versioning, validation, and visualization.

## Agent Instructions

See **AGENTS.md** for workflow instructions, issue tracking commands, and session completion protocol.

## Development Setup

- **Python:** 3.12+ (see `.python-version`)
- **Package Manager:** uv
- **Virtual Environment:** `.venv/` (activate with `source .venv/bin/activate`)

## Architecture

See `background/coc_boundary_plan.md` for the detailed implementation plan covering:
- Repository layout and module structure
- Data contracts and canonical schemas
- Work packages (WP-A through WP-H)
- API interfaces and CLI commands

## Dependencies (Planned)

Core: `geopandas`, `shapely`, `pyproj`, `pyarrow`, `pandas`, `folium`, `typer`
Dev: `ruff`, `pytest`
