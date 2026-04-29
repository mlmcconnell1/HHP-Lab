# HHP-Lab Rename Contract

This document is the accepted output of `coclab-i95p.1`.

It locks the target naming contract for the staged rename from
`CoC-Lab` / `coclab` to `HHP-Lab` / `hhplab` so later implementation
stages do not have to make naming decisions ad hoc.

## Scope Rule

Rename the project identity, not the homelessness domain vocabulary.

- Rename project identity strings such as `CoC-Lab`, `coc-lab`, `coclab`,
  `COCLAB_*`, `coclab.yaml`, and `coclab_panel`.
- Do not rename domain terms such as `coc`, `coc_id`, `Continuum of Care`,
  `coc_pep`, `coc_pit`, or geography names that still correctly describe the
  data model.

This distinction is mandatory. A global `coc -> hhp` replacement would be
wrong and would corrupt domain meaning across the codebase.

## Target Contract

| Surface | Current | Target | Decision |
| --- | --- | --- | --- |
| Repository display name | `CoC-Lab` | `HHP-Lab` | Rename |
| GitHub slug / remote path | `CoC-Lab` | `HHP-Lab` | Rename |
| Local checkout directory | `CoC-Lab` | `HHP-Lab` | Rename when convenient |
| Python distribution name | `coc-lab` | `hhp-lab` | Rename |
| Python package import path | `coclab` | `hhplab` | Rename |
| CLI command | `coclab` | `hhplab` | Rename |
| Repo config file | `coclab.yaml` | `hhplab.yaml` | Rename |
| User config directory | `~/.config/coclab` | `~/.config/hhplab` | Rename |
| User config file | `~/.config/coclab/config.yaml` | `~/.config/hhplab/config.yaml` | Rename |
| Environment variable prefix | `COCLAB_` | `HHPLAB_` | Rename |
| Provenance field | `coclab_version` | `hhplab_version` | Rename |
| Default CoC panel source label | `coclab_panel` | `hhplab_panel` | Rename |
| Default metro panel source label | `metro_panel` | `metro_panel` | Keep |
| Example branded metro source label | `coclab_metro_panel` | `hhplab_metro_panel` | Rename |
| Package root marker in cwd checks | `coclab/` | `hhplab/` | Rename |
| Primary manual filename | `CoC-Lab-Manual.md` | `HHP-Lab-Manual.md` | Rename |
| Beads issue prefix for new issues | `coclab` | `hhplab` | Rename |

## Explicit Non-Goals

These identifiers stay as they are unless a later bead explicitly expands
scope:

- Domain abbreviations such as `coc`, `coc_id`, and `Continuum of Care`
- Artifact names whose `coc` token describes geography rather than project
  branding
- Generic labels such as `metro_panel`

## Historical Exceptions

The rename is intended to be complete for active code and docs, but two kinds
of historical references are allowed to remain:

- Existing closed bead IDs such as `coclab-xxxx`
- Prose that explicitly discusses the former project name for historical
  context

Existing bead IDs should be treated as immutable historical identifiers.
The config should switch future issue creation to the `hhplab` prefix, but the
backlog does not need an ID rewrite.

## Compatibility Policy

No committed compatibility aliases are required.

- Do not preserve `coclab` import aliases after the final rename.
- Do not preserve `coclab` as a second CLI entrypoint after the final rename.
- Temporary working-tree glue is allowed only if needed to get from one stage
  to the next, and must be removed before the final rename closes.

This is justified because the package is not yet in production.

## Inventory Summary

The rename touches multiple categories of tracked material:

- `214` files in code and tests contain rename-sensitive project-identity
  strings.
- `23` files in README/manual/recipe/background docs contain rename-sensitive
  strings.
- `7` files in `.arch/` and `.beads/` contain rename-sensitive strings.
- A broader repo-wide scan found `259` files with one or more of:
  `CoC-Lab`, `CoC Lab`, `coc-lab`, `coclab`.

Package-scale impact from `archv --robot-summary`:

- `139` production files under the package tree
- `89` test files

## High-Risk Rename Surfaces

These files define the highest-risk contract points and must be treated as
rename anchors in later stages:

- `pyproject.toml`
  - distribution name
  - CLI script entrypoint
  - wheel package list
- `coclab/cli/main.py`
  - CLI name
  - help text
  - non-interactive env var
  - project-root marker check
- `coclab/config.py`
  - env var names
  - repo config filename
  - user config directory
- `coclab/provenance.py`
  - embedded metadata key `coclab_version`
- `coclab/panel/assemble.py`
  - default serialized source label `coclab_panel`
- `coclab/panel/finalize.py`
  - default source-label policy
- `README.md`
  - installation, CLI, config, and automation examples
- `manual-obsidian/07-Data-Model.md`
  - provenance examples and field contract
- `.beads/config.yaml`
  - future issue prefix
- `.arch/arch.scaffold.jsonl`
- `.arch/arch.enriched.jsonl`
- `.arch/arch.registry.jsonl`
- `.arch/arch.annotations.jsonl`

## File and Path Buckets

Later stages should use these buckets rather than a monolithic search/replace:

1. Repository and branding paths
   - repo slug
   - local directory name
   - `manual-obsidian/CoC-Lab-Manual.md`

2. Packaging and imports
   - `pyproject.toml`
   - package directory move `coclab/ -> hhplab/`
   - all first-party imports in production code and tests

3. CLI and configuration contract
   - command name `coclab -> hhplab`
   - `COCLAB_* -> HHPLAB_*`
   - `coclab.yaml -> hhplab.yaml`
   - `~/.config/coclab -> ~/.config/hhplab`

4. Serialized metadata
   - `coclab_version -> hhplab_version`
   - `coclab_panel -> hhplab_panel`
   - branded example labels such as `coclab_metro_panel`

5. Docs, recipes, fixtures, and tests
   - README
   - manual pages
   - recipe examples
   - fixture payloads and assertions

6. Generated architecture and issue-tracking artifacts
   - `.arch/*` regenerated after package/path moves
   - `.beads/config.yaml` updated for future issue prefix
   - existing bead IDs preserved as historical identifiers

## Stage Guidance

- Stage 2 may rename repo/docs/branding before code moves begin.
- Stage 3 is the package/import move and should not be mixed with provenance
  contract edits unless needed for a passing state.
- Stage 4 should handle the operational contract rename:
  CLI, config, env vars, provenance, and source labels.
- Stage 5 should clean the long tail:
  tests, recipes, fixtures, and `.arch/`.
- Stage 6 should remove any temporary glue and verify there are no unintended
  project-identity references left behind.

## Done Condition For Stage 1

`coclab-i95p.1` is complete when:

1. This contract exists in the repo.
2. The target naming surface is explicit and unambiguous.
3. Later stages can implement against this document without reopening naming
   questions.
