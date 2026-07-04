"""Agent-facing CLI guidance."""

import json
from typing import Annotated

import typer

from hhplab.metro.metro_definitions import METRO_COUNT

APPROX_COC_COUNT = 380
SMALL_N_GUIDANCE = (
    f"Treat estimates from {METRO_COUNT} Glynn/Fox metros or roughly "
    f"{APPROX_COC_COUNT} CoCs as screening evidence. Report sample size, "
    "avoid overfit models, and prefer confidence intervals or robustness "
    "checks over point estimate ranking."
)

AGENTS_GUIDANCE = {
    "automation_defaults": {
        "prefer_json": [
            "hhplab status --json",
            "hhplab build recipe-preflight --recipe <file> --json",
            "hhplab build recipe --recipe <file> --json",
        ],
        "recipe_plan": "hhplab build recipe-plan --recipe <file> --json",
        "non_interactive": [
            "hhplab --non-interactive ...",
            "HHPLAB_NON_INTERACTIVE=1",
        ],
        "layout_validation": [
            "hhplab validate curated-layout",
            "hhplab migrate curated-layout",
            "hhplab migrate curated-layout --apply",
        ],
    },
    "crosswalk_rules": {
        "principle": (
            "Every dataset must be matched to the correct geographic vintage "
            "on both sides of the crosswalk."
        ),
        "sources": [
            {
                "source": "PIT Counts",
                "geography": "CoC",
                "rule": "Direct match; no crosswalk needed.",
            },
            {
                "source": "ACS Estimates",
                "geography": "Census Tracts -> CoC",
                "rule": "Use ACS tract vintage, then map to CoC boundary year.",
            },
            {
                "source": "PEP Estimates",
                "geography": "Counties -> CoC",
                "rule": "Use the county-to-CoC crosswalk for the PEP estimate year.",
            },
            {
                "source": "ZORI (Zillow)",
                "geography": "Counties -> CoC",
                "rule": "Use the county-to-CoC crosswalk for the CoC boundary year.",
            },
            {
                "source": "CHAS",
                "geography": "Census Tracts -> CoC",
                "rule": "Follow ACS tract-vintage rule, not CHAS release year.",
            },
        ],
        "notes": [
            {
                "id": "coc_boundary_reuse",
                "guidance": (
                    "HUD does not publish new CoC boundaries every year. Track "
                    "which boundary file is effective for a program year."
                ),
            },
            {
                "id": "acs_decennial_transitions",
                "guidance": (
                    "The tract vintage flips at decennial census boundaries with "
                    "a lag; do not infer it from the last ACS range year."
                ),
            },
            {
                "id": "crosswalk_weights",
                "guidance": (
                    "Use interpolation weights that are temporally consistent "
                    "with the tract vintage, not the data year."
                ),
            },
        ],
    },
    "inference_guardrails": [
        {
            "id": "small_n",
            "risk": "Small samples make noisy correlations look stronger than they are.",
            "applies_to": ["metro panels", "coc panels"],
            "guidance": SMALL_N_GUIDANCE,
        },
        {
            "id": "pit_2021_disruption",
            "risk": "The 2021 PIT count was disrupted by COVID-era enumeration changes.",
            "applies_to": ["pit", "hic", "homelessness panels"],
            "guidance": (
                "Flag 2021 separately before interpreting trend breaks or model "
                "coefficients. Consider excluding 2021, adding a 2021 indicator, "
                "or reporting sensitivity results with and without that year."
            ),
        },
        {
            "id": "spatial_autocorrelation",
            "risk": "Neighboring geographies often share unobserved housing and policy shocks.",
            "applies_to": ["county", "metro", "coc"],
            "guidance": (
                "Do not treat nearby observations as fully independent without "
                "checking spatial clustering. Prefer clustered or spatially aware "
                "standard errors when making inferential claims."
            ),
        },
        {
            "id": "acs_lag_causal_ordering",
            "risk": "ACS measures are lagged relative to PIT outcome years.",
            "applies_to": ["acs", "pit"],
            "guidance": (
                "Use the project convention that ACS vintage for PIT year Y is "
                "Y-1. Avoid causal claims where predictor timing overlaps or "
                "follows the PIT outcome."
            ),
        },
        {
            "id": "multiple_comparisons",
            "risk": "Screening many covariates inflates false-positive correlations.",
            "applies_to": ["covariate screening", "correlation analysis"],
            "guidance": (
                "Track the full set of tested covariates, distinguish exploratory "
                "from confirmatory findings, and apply a multiple-comparison "
                "correction or holdout validation before calling results robust."
            ),
        },
    ],
}

AGENTS_INFO_TEXT = f"""# HHP-Lab Agent Quick Reference

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

## Inference Guardrails

- **Small N:** Treat correlations from {METRO_COUNT} Glynn/Fox metros or roughly
  {APPROX_COC_COUNT} CoCs as screening evidence. Report sample size, avoid
  overfit models, and prefer confidence intervals or robustness checks over
  point estimate ranking.
- **2021 PIT disruption:** Flag the COVID-era 2021 PIT count separately. Consider
  excluding 2021, adding a 2021 indicator, or reporting sensitivity results with
  and without that year.
- **Spatial autocorrelation:** Neighboring geographies often share unobserved
  housing and policy shocks. Check spatial clustering before treating nearby
  observations as independent.
- **ACS lag causal ordering:** Use the convention that ACS vintage for PIT year
  Y is Y-1. Avoid causal claims where predictor timing overlaps or follows the
  PIT outcome.
- **Multiple comparisons:** Screening many covariates inflates false positives.
  Track every tested covariate, mark exploratory findings, and use correction or
  holdout validation before calling results robust.
"""


def agents(
    json_output: Annotated[
        bool,
        typer.Option("--json", help="Output machine-readable JSON."),
    ] = False,
) -> None:
    """Display automation and crosswalk guidance for agents."""
    if json_output:
        typer.echo(json.dumps(AGENTS_GUIDANCE, indent=2, sort_keys=True))
        return
    typer.echo(AGENTS_INFO_TEXT)
