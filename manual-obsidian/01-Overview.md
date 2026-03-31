# Overview

CoC Lab is a data engineering and reproducibility toolkit for building **analysis-geography-centered datasets** from heterogeneous public sources.

Its core design choice is deliberate:

- **Default hub geography:** CoC boundaries by vintage (`B{year}`)
- **Alternate analysis geography:** Metro areas via researcher-defined membership rules (e.g., Glynn/Fox metros with `D{version}`)
- **Spoke datasets:** tract- and county-native inputs (ACS, ZORI, PEP) mapped into the target analysis geography via crosswalks or membership tables
- **Execution style:** declarative YAML recipes over the global curated store

## What CoC Lab Does

- Ingests boundary, census geometry, PIT, ACS, PEP, and ZORI inputs
- Builds tract↔CoC and county↔CoC crosswalks
- Aggregates source datasets into standalone CoC artifacts when needed
- Assembles geography×year panels with recipe-driven composition for CoC or metro targets
- Writes provenance metadata and recipe manifests for reproducibility
- Exports analysis bundles with a machine-readable `MANIFEST.json`

## Philosophy

### 1. Reproducibility over convenience
Aggregate commands require explicit `--years`. Recipe execution consumes curated inputs directly and emits consumed-asset manifests.

### 2. Declarative where possible
The recipe system separates:
- **Structural validation:** schema and referential integrity
- **Semantic validation:** adapter compatibility and runtime checks

### 3. Transparent temporal alignment
Vintages are explicit in file names, metadata, and docs. The system avoids hiding lag or mismatch decisions.

### 4. Analysis-geography-centered inference
The project defaults to CoC-level inference but supports metro areas as an alternate analysis geography. County-native and tract-native inputs are transformed into the target analysis frame (CoC or metro), not vice versa. See [[07-Data-Model#analysis-geography-model]] for the abstraction.

## Key Surfaces

- **CLI:** `coclab ...`
- **Recipe execution:** `coclab build recipe --recipe <file.yaml>`
- **Global curated store:** `data/curated/...`

## Panel Assembly

CoC Lab uses recipe-driven composition (`build recipe`) for multi-dataset panel construction.

---

**Next:** [[02-Installation]]
