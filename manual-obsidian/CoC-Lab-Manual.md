# CoC Lab Manual

> A comprehensive guide to the Continuum of Care (CoC) boundary data infrastructure

---

## Table of Contents

### Getting Started
- [[01-Overview|Overview]] - What is CoC Lab and what can it do?
- [[02-Installation|Installation]] - Prerequisites and setup instructions
- [[03-Architecture|Architecture]] - System design and module structure

### Reference
- [[04-CLI-Reference|CLI Reference]] - Complete command-line documentation
- [[05-Recipe-Format|Recipe Format]] - Declarative build recipe YAML schema
- [[06-Python-API|Python API]] - Programmatic access to CoC Lab functions
- [[07-Data-Model|Data Model]] - Schemas, storage locations, and provenance
- [[08-Temporal-Terminology|Temporal Terminology]] - Vintage notation and temporal concepts

### Guides
- [[09-Workflows|Workflows]] - Step-by-step processes and typical use sequences

### Methodology
- [[10-Methodology-ACS-Aggregation|ACS Aggregation]] - How ACS data is aggregated to CoC level
- [[11-Methodology-ZORI-Aggregation|ZORI Aggregation]] - How rent data is aggregated to CoC level
- [[12-Methodology-Panel-Assembly|Panel Assembly]] - How analysis panels are constructed

### Export & Integration
- [[13-Bundle-Layout|Bundle Layout]] - Export bundle structure and MANIFEST.json

### Development
- [[14-Module-Reference|Module Reference]] - Detailed module and function documentation
- [[15-Development|Development]] - Testing, code quality, and extending CoC Lab
- [[16-Appendix|Appendix]] - CoC ID format, CRS, and technical details

---

## Quick Links

**Common Tasks:**
- Build a panel from scratch: [[09-Workflows#Typical Use Sequence Building a Panel from Scratch]]
- Ingest CoC boundaries: [[04-CLI-Reference#coclab ingest boundaries]]
- Build crosswalks: [[04-CLI-Reference#coclab generate xwalks]]
- Export a bundle: [[04-CLI-Reference#coclab build export]]

**Key Concepts:**
- Recipe format and YAML schema: [[05-Recipe-Format]]
- Temporal notation and vintages: [[08-Temporal-Terminology]]
- Data sources and when to use each: [[01-Overview#Choosing a Data Source]]
- Coverage ratio interpretation: [[11-Methodology-ZORI-Aggregation#Coverage Ratio Interpretation]]
- Alignment policies: [[12-Methodology-Panel-Assembly#Alignment Policies]]

---

*Generated for CoC Lab v0*
