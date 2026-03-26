# Bundle Layout and MANIFEST.json

`coclab build recipe-export` creates export bundles from recipe outputs for downstream analysis repositories.

## Command Contract

```bash
coclab build recipe-export --manifest data/curated/panel/<file>.manifest.json --output /tmp/bundle
```

The recipe manifest sidecar (`.manifest.json`) tracks all consumed assets, enabling reproducible exports.

---

**Previous:** [[12-Methodology-Panel-Assembly]] | **Next:** [[14-Module-Reference]]
