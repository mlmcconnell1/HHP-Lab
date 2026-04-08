# Agent Instructions

This project uses `.beads/` for issue tracking. Two CLI tools can manage beads: **br** (beads_rust, preferred) and **bd** (beads Python). At session start, detect which is available:

```bash
command -v br && BR=br || { command -v bd && BR=bd || echo "No beads CLI found"; }
```

Use `$BR` (or just the resolved command) for all beads operations below. Prefer **br** when both are installed.

**Note:** `br` is non-invasive and never executes git commands. After `br sync --flush-only`, you must manually run `git add .beads/ && git commit`. The `bd sync` command commits and pushes automatically.

## Quick Reference

| Action | br (preferred) | bd (fallback) |
|--------|---------------|---------------|
| Onboard | `br onboard` | — |
| List issues | `br list` | `bd list --status=open` |
| Find ready work | `br ready` | `bd ready` |
| Show issue | `br show <id>` | `bd show <id>` |
| Create issue | `br create` | `bd create --title="..." --type=task --priority=2` |
| Claim work | `br update <id> --status in_progress` | `bd update <id> --status=in_progress` |
| Close issue | `br close <id>` | `bd close <id> --reason="Completed"` |
| Sync to disk | `br sync --flush-only` | `bd sync` |

## CoC-Lab Agent-Friendly CLI

Prefer these CoC-Lab runtime features when automating:

- Use machine-readable output whenever available (`--json`) to avoid parsing human text.
- Run non-interactively for CI/agents: pass `--non-interactive` or set `COCLAB_NON_INTERACTIVE=1`.
- Use `coclab agents` for built-in geography/year matching rules and operational guidance.
- Preflight environment and prerequisites with:
  - `coclab status --json`
- Validate curated naming/layout policy before and after writes:
  - `coclab validate curated-layout`
- For curated filename migrations, default to dry-run first:
  - `coclab migrate curated-layout`
  - `coclab migrate curated-layout --apply`
- For recipe workflows, validate/plan before execute:
  - `coclab build recipe-preflight --recipe <file> --json`
  - `coclab build recipe --recipe <file> --json`
  - Use `coclab build recipe-plan --recipe <file> --json` when you need the resolved task graph while authoring/debugging a recipe

## Code Style: Human and Agent Readable

All code generated for this project must be easily usable by both humans and AI agents. Apply these principles when writing code, tests, CLI commands, and data pipelines:

### Tests and fixtures
- **Declarative over implicit.** Define fixture data and expected outcomes as named constants, not as magic numbers buried in logic. An agent (or human) modifying a fixture value should not need to hand-recompute downstream expectations.
- **Parametrize over loops.** Use `@pytest.mark.parametrize` instead of for-loops inside test bodies. Failures should name the exact case in the test ID (e.g., `test_dtype[year-int64]`), not require reading assertion messages.
- **Truth tables over prose.** When a fixture has designed-in outcomes (e.g., which rows pass a threshold), document the full truth table in a visible location (module docstring or a constant), not scattered across individual test docstrings.
- **Allowlists and exceptions at module level.** If something is a known special case (e.g., columns with expected dtype normalization), declare it as a named module-level constant with a comment explaining why — not as a local variable inside a function.
- **Derive, don't duplicate.** Golden-value tests should compute expectations from the same constants that build the fixtures. Changing a fixture value should automatically update the expected outcome.

### CLI and output
- **Always provide `--json`.** Every CLI command that produces output should support a `--json` flag emitting structured, machine-parseable JSON. Agents should never need to scrape human-formatted tables or prose to extract results.
- **Actionable error messages.** Errors should state what went wrong AND what to do about it (e.g., "No ACS measures found — run `coclab aggregate acs` first"). An agent that encounters an error should be able to act on it without searching the codebase.
- **Deterministic, parseable file names.** Output artifacts should use the canonical naming from `coclab/naming.py` so agents can discover and reference them programmatically without globbing.

### Schemas and data contracts
- **Canonical column lists as code.** Output schemas (e.g., `PANEL_COLUMNS`, `ZORI_COLUMNS`) must be defined as module-level constants. When a schema changes, update the constant — never add columns silently.
- **Provenance in every artifact.** Parquet outputs must embed provenance metadata via `write_parquet_with_provenance` so downstream agents can inspect lineage without external tracking.

## Dataset Availability & Geometry Rules

When authoring recipes, selecting year ranges, or debugging missing data, use these constraints. A year outside a source's coverage window means the data **does not exist** — it is not a build failure.

### Temporal coverage

| Provider | Product | First year | Last year | Native geometry | Notes |
|----------|---------|-----------|-----------|-----------------|-------|
| hud | pit | 2007 | ongoing | coc | Annual January point-in-time count |
| census | acs5 | 2009 | ongoing | tract | 5-year estimates; vintage = end year (e.g., vintage 2023 = 2019-2023) |
| census | pep | 2010 | ongoing | county | Postcensal estimates; intercensal 2010-2020 also available |
| zillow | zori | 2015 | ongoing | county | ZORI All Homes begins Jan 2015; monthly, filter to January for PIT alignment |

- **ACS lag rule:** ACS vintage for PIT year Y is Y−1 (released ~Dec of year Y−1).
- **PEP coverage:** Postcensal vintage 2020 covers 2010-2020; vintage 2024 covers 2020-2024. Combined/intercensal fills the full 2010-2020 range.

### Census tract geometry eras

Tract-based data (ACS, crosswalks) must reference the correct decennial tract vintage. Tracts are redefined each decennial census:

| Data years | Tract vintage | Example |
|-----------|---------------|---------|
| 2000–2009 | 2000 | ACS 2009 uses 2000-era tracts |
| 2010–2019 | 2010 | ACS 2018 uses 2010-era tracts |
| 2020–2029 | 2020 | ACS 2023 uses 2020-era tracts |

**Rule:** use the most recent decennial ≤ the data year. In recipes, this drives the `segments` section of `file_set` specs — each segment maps a year range to its tract vintage.

Cross-era analysis (e.g., a 2015-2024 panel) requires a tract relationship file (2010↔2020) and separate crosswalk builds per era.

### Measure columns by data source

Different data sources produce different demographic columns. Conformance checks use `PanelRequest.measure_columns` to validate the right set:

| Source | Measure columns in panel | Notes |
|--------|-------------------------|-------|
| ACS | `total_population`, `adult_population`, `population_below_poverty`, `median_household_income`, `median_gross_rent` | Tract-level, apportioned via crosswalk |
| PEP | `population` | County-level, aggregated to target geography |

When building non-ACS panels (e.g., PEP-based metro), set `measure_columns` on `PanelRequest` so conformance checks validate the correct columns instead of defaulting to the ACS set.

## Adding Beads (Problem Noticed)

If you identify a problem in the code, even incidentally while working on something else, add a bead to make sure it is addressed later.

<!-- bv-agent-instructions-v2 -->

---

## Beads Workflow Integration

This project uses [beads_rust](https://github.com/Dicklesworthstone/beads_rust) (`br`) for issue tracking and [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) (`bv`) for graph-aware triage. Issues are stored in `.beads/` and tracked in git.

### Using bv as an AI sidecar

bv is a graph-aware triage engine for Beads projects (.beads/beads.jsonl). Instead of parsing JSONL or hallucinating graph traversal, use robot flags for deterministic, dependency-aware outputs with precomputed metrics (PageRank, betweenness, critical path, cycles, HITS, eigenvector, k-core).

**Scope boundary:** bv handles *what to work on* (triage, priority, planning). `br` handles creating, modifying, and closing beads.

**CRITICAL: Use ONLY --robot-* flags. Bare bv launches an interactive TUI that blocks your session.**

#### The Workflow: Start With Triage

**`bv --robot-triage` is your single entry point.** It returns everything you need in one call:
- `quick_ref`: at-a-glance counts + top 3 picks
- `recommendations`: ranked actionable items with scores, reasons, unblock info
- `quick_wins`: low-effort high-impact items
- `blockers_to_clear`: items that unblock the most downstream work
- `project_health`: status/type/priority distributions, graph metrics
- `commands`: copy-paste shell commands for next steps

```bash
bv --robot-triage        # THE MEGA-COMMAND: start here
bv --robot-next          # Minimal: just the single top pick + claim command

# Token-optimized output (TOON) for lower LLM context usage:
bv --robot-triage --format toon
```

#### Other bv Commands

| Command | Returns |
|---------|---------|
| `--robot-plan` | Parallel execution tracks with unblocks lists |
| `--robot-priority` | Priority misalignment detection with confidence |
| `--robot-insights` | Full metrics: PageRank, betweenness, HITS, eigenvector, critical path, cycles, k-core |
| `--robot-alerts` | Stale issues, blocking cascades, priority mismatches |
| `--robot-suggest` | Hygiene: duplicates, missing deps, label suggestions, cycle breaks |
| `--robot-diff --diff-since <ref>` | Changes since ref: new/closed/modified issues |
| `--robot-graph [--graph-format=json\|dot\|mermaid]` | Dependency graph export |

#### Scoping & Filtering

```bash
bv --robot-plan --label backend              # Scope to label's subgraph
bv --robot-insights --as-of HEAD~30          # Historical point-in-time
bv --recipe actionable --robot-plan          # Pre-filter: ready to work (no blockers)
bv --recipe high-impact --robot-triage       # Pre-filter: top PageRank scores
```

### br Commands for Issue Management

```bash
br ready              # Show issues ready to work (no blockers)
br list --status=open # All open issues
br show <id>          # Full issue details with dependencies
br create --title="..." --type=task --priority=2
br update <id> --status=in_progress
br close <id> --reason="Completed"
br close <id1> <id2>  # Close multiple issues at once
br sync --flush-only  # Export DB to JSONL
```

### Workflow Pattern

1. **Triage**: Run `bv --robot-triage` to find the highest-impact actionable work
2. **Claim**: Use `br update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `br close <id>`
5. **Sync**: Always run `br sync --flush-only` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `br ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers 0-4, not words)
- **Types**: task, bug, feature, epic, chore, docs, question
- **Blocking**: `br dep add <issue> <depends-on>` to add dependencies

### Session Protocol

```bash
git status              # Check what changed
git add <files>         # Stage code changes
br sync --flush-only    # Export beads changes to JSONL
git commit -m "..."     # Commit everything
git push                # Push to remote
```

<!-- end-bv-agent-instructions -->

<!-- archv-agent-instructions-v1 -->

---
## Architecture Workflow Integration

  This project uses [arch-viewer](https://github.com/Dicklesworthstone/arch-viewer) (`archv`) as a terminal-first architecture explorer for source repositories. It loads deterministic scaffold data from `.arch/arch.scaffold.jsonl`, overlays AI enrichment from `.arch/
  arch.enriched.jsonl`, and exposes stable machine-readable outputs for agents.

  ### Using archv as an AI sidecar

  `archv` is the architecture-context tool for this repo. Use it when you need to understand:

  - top-level structure
  - file and package relationships
  - dependency edges and reverse dependencies
  - architectural layers
  - cycles, hotspots, and graph shape
  - which tracked nodes still need enrichment

  **Scope boundary:** `archv` helps with repository structure and architecture context. It does not replace source reading, tests, or issue tracking.

  **CRITICAL: Use ONLY `--robot-*` flags in agent workflows.** Bare `archv` launches an interactive TUI and can block your session.

  ### The Workflow: Start With Orientation

  **`archv --robot-summary` is the default entry point.** Run it first to get repo stats, major components, top-level directories, and scaffold freshness status.

  ```bash
  archv --robot-summary
  archv --robot-node file:path/to/file.go
  archv --robot-graph
  archv --robot-cycles
  archv --robot-hotspots
  archv --robot-layers
  archv --robot-search "query"

  ### Common archv Commands

  # Refresh deterministic architecture facts
  archv scaffold --repo . --out .arch/arch.scaffold.jsonl

  # Find records still missing enrichment
  archv enrich pending --summary
  archv enrich pending --jsonl

  # Prepare context for enrichment work
  archv enrich request --pending
  archv enrich request --id file:path/to/file.go

  # Add or import enrichment
  archv enrich upsert --id file:path/to/file.go --summary "..." --layer domain --confidence 0.90
  archv enrich import --stdin < generated.enriched.jsonl

  ### Workflow Pattern

  1. Orient: Run archv --robot-summary
  2. Inspect: Use --robot-node, --robot-graph, --robot-cycles, --robot-hotspots, and --robot-layers
  3. Refresh facts: If the repo structure changed, regenerate the scaffold with archv scaffold
  4. Maintain enrichment: Use archv enrich pending, archv enrich request, and archv enrich upsert / archv enrich import
  5. Verify: Re-run the relevant archv --robot-* commands after updating artifacts

  ### Enrichment Maintenance Requirements

  If you add a new source file, test file, or package, you MUST update the repo's archv artifacts in the same change.

  Minimum required workflow:

  # 1. Refresh scaffold facts
  archv scaffold --repo . --out .arch/arch.scaffold.jsonl

  # 2. Check what still needs enrichment
  archv enrich pending --summary
  archv enrich pending --jsonl

  # 3. Add enrichment for the new or changed nodes
  archv enrich upsert --id file:path/to/new_file.go --summary "..." --layer ... --confidence ...
  # or generate/import a batch:
  archv enrich import --stdin < generated.enriched.jsonl

  Agent expectations:

  - If you add a new production file, add enrichment for it.
  - If you add a new test file or package and the repo tracks enrichment for those nodes, update that enrichment too.
  - Do not leave newly introduced tracked nodes in a permanently pending enrichment state without noting it.
  - Commit the updated .arch/ artifacts alongside the code change.

  ### Best Practices

  - Prefer archv output over guessing architecture from filenames or imports alone.
  - Treat scaffold data as deterministic facts and enrichment as AI-owned semantic overlay.
  - Use archv enrich pending to detect coverage gaps instead of assuming tracked artifacts are current.
  - If adjacent enrichment exists, assume archv will overlay it automatically when loading the scaffold.

<!-- end-archv-agent-instructions -->

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
   - Full test suite command: `uv run --extra dev pytest` (requires dev extras such as `pytest-httpx`)
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   # If using br:
   br sync --flush-only
   git add .beads/
   git commit -m "sync beads"
   # If using bd:
   # bd sync              # (commits and pushes automatically)
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

## Key Concepts

- **Dependencies**: Issues can block other issues. `ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
