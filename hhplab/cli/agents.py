"""Agent-facing CLI guidance."""

import typer

AGENTS_INFO_TEXT = """# HHP-Lab Agent Quick Reference

## Automation Defaults

- Prefer machine-readable JSON output when available:
  - `hhplab status --json`
  - `hhplab build recipe-preflight --recipe <file> --json`
  - `hhplab build recipe --recipe <file> --json`
- Use `hhplab build recipe-plan --recipe <file> --json` when you need the
  resolved task graph while authoring or debugging a recipe.
- Scaffold recipe-first MSA-CoC overlap coverage workflows with:
  - `hhplab recipe init msa-coc-overlap --output <file> --json`
  - add `--overlap-basis population --acs5-population-vintage <year> --tract-vintage <year>`
    when population overlap is required.
- Run non-interactively for automation:
  - `hhplab --non-interactive ...`
  - or set `HHPLAB_NON_INTERACTIVE=1`
- Validate curated layout policy before/after writes:
  - `hhplab validate curated-layout`
- Preview curated migration changes before applying:
  - `hhplab migrate curated-layout`
  - `hhplab migrate curated-layout --apply`

## Crosswalk Rules: Geography-to-Year Matching

## Core Principle

Every dataset must be matched to the correct geographic vintage on both sides
of the crosswalk. The rules below govern which vintage to use for each source.

## Rules by Data Source

| Data Source | Geography | Crosswalk Rule |
|---|---|---|
| **PIT Counts** | CoC | Direct match; no crosswalk needed. |
| **ACS Estimates** | Census Tracts -> CoC | Use ACS tract vintage, then map to CoC boundary year. |
| **PEP Estimates** | Counties -> CoC | Use the county-to-CoC crosswalk for the PEP estimate year. |
| **ZORI (Zillow)** | Counties -> CoC | Use the county-to-CoC crosswalk for the CoC boundary year. |
| **CHAS** | Census Tracts -> CoC | Follow ACS tract-vintage rule, not CHAS release year. |

## Important Notes

- **CoC boundary reuse:** HUD does not publish new CoC boundaries every year.
  Track which boundary file is *effective* for a given program year, not when
  it was published.
- **ACS decennial transitions:** The tract vintage flips at decennial census
  boundaries with a lag. Hardcode or look up transition years rather than
  assuming the last year of the ACS range equals the tract vintage.
- **Crosswalk weights:** When using areal or population-weighted interpolation
  (tracts -> CoCs), use weights (e.g., decennial block populations) that are
  temporally consistent with the tract vintage, not the data year.
"""


def agents() -> None:
    """Display automation and crosswalk guidance for agents."""
    typer.echo(AGENTS_INFO_TEXT)
