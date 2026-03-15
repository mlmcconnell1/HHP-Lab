# Bundle Layout and MANIFEST.json

`coclab build export` creates versioned bundle directories (`exports/export-N/`) for downstream analysis repositories.

## Directory Structure

```text
export-N/
  MANIFEST.json
  README.md
  data/
    panels/
    inputs/
      boundaries/
      xwalks/
      pit/
      rents/
      acs/
  diagnostics/
  codebook/
    schema.md
    variables.csv
```

Actual file population depends on `--include` and discovered artifacts.

## Command Contract

```bash
coclab build export --name my_bundle --build demo
```

Important implementation detail:
- `--build` is required in current code.
- `--name` is also required.
- `--include` defaults to `panel,manifest,codebook,diagnostics`.

## Manifest Semantics

`MANIFEST.json` records:
- bundle identity (`bundle_name`, `export_id`, `created_at_utc`)
- runtime metadata (`coclab` block)
- export parameters
- artifact inventory with hash/size/schema info where available

## Exit Codes

- `0`: success
- `2`: validation failure
- `3`: filesystem failure
- `4`: manifest generation failure

---

**Previous:** [[12-Methodology-Panel-Assembly]] | **Next:** [[14-Module-Reference]]
