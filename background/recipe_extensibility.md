# Recipe Extensibility Contract

This document describes how to extend the recipe system without breaking existing v1 recipes.

## Architecture Overview

The recipe system separates concerns into two layers:

| Layer | Module | Responsibility |
|-------|--------|----------------|
| **Structural** | `coclab/recipe/recipe_schema.py` | Pydantic models — shape, types, referential integrity |
| **Semantic** | `coclab/recipe/adapters.py` | Adapter registries — geometry/dataset compatibility checks |

**Key principle:** The schema uses open string sets for geometry types and dataset providers. It never hardcodes enumerations like `"coc" | "tract" | "county"`. Runtime adapter registries validate whether a referenced type or provider actually has an implementation.

## Extending Transforms

Transforms use a discriminated union on the `type` field. To add a new transform operator:

1. Define a new model inheriting from `TransformBase`:

```python
# in recipe_schema.py

class AggregateSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    method: Literal["sum", "mean"]

class AggregateTransform(TransformBase):
    type: Literal["aggregate"] = "aggregate"
    spec: AggregateSpec = Field(..., description="Aggregate operator spec.")
```

2. Add it to the `TransformSpec` union:

```python
TransformSpec = Annotated[
    Union[CrosswalkTransform, RollupTransform, AggregateTransform],
    Field(discriminator="type"),
]
```

Existing recipes with `type: "crosswalk"` or `type: "rollup"` continue to work unchanged.

## Extending Pipeline Steps

Steps use a discriminated union on the `kind` field. To add a new step:

1. Define the step model:

```python
class FilterStep(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["filter"] = "filter"
    dataset: str
    expression: str
```

2. Add to the `StepSpec` union:

```python
StepSpec = Annotated[
    Union[MaterializeStep, ResampleStep, JoinStep, FilterStep],
    Field(discriminator="kind"),
]
```

3. If the step references datasets or transforms, add referential integrity checks in `RecipeV1._validate_references()`.

## Introducing RecipeV2

To add a new recipe version without breaking v1:

1. Define `RecipeV2` in `recipe_schema.py` (or a new file):

```python
class RecipeV2(BaseModel):
    version: Literal[2] = 2
    # ... new/changed fields ...
```

2. Register it in `coclab/recipe/loader.py`:

```python
_VERSION_REGISTRY: dict[int, type] = {
    1: RecipeV1,
    2: RecipeV2,
}
```

The loader dispatches on the `version` key in the YAML, so existing `version: 1` recipes continue to parse through `RecipeV1` with no code changes.

## Adapter Registration

Geometry and dataset validation live in adapter registries, not in the schema.

### Adding a geometry adapter

```python
from coclab.recipe.adapters import geometry_registry, ValidationDiagnostic
from coclab.recipe.recipe_schema import GeometryRef

def validate_zcta(ref: GeometryRef) -> list[ValidationDiagnostic]:
    if ref.vintage is None:
        return [ValidationDiagnostic("error", "ZCTA requires a vintage year.")]
    return []

geometry_registry.register("zcta", validate_zcta)
```

### Adding a dataset adapter

```python
from coclab.recipe.adapters import dataset_registry, ValidationDiagnostic
from coclab.recipe.recipe_schema import DatasetSpec

def validate_mit_election(spec: DatasetSpec) -> list[ValidationDiagnostic]:
    diags = []
    if spec.version != 1:
        diags.append(ValidationDiagnostic("error", f"Unsupported version {spec.version}."))
    if "office" not in spec.params:
        diags.append(ValidationDiagnostic("error", "Missing required param 'office'."))
    return diags

dataset_registry.register("mit-election", "county-returns", validate_mit_election)
```

## Backward Compatibility Rules

1. **Never remove** a transform type or step kind from the union — only add new members.
2. **Never change** the meaning of existing fields on a given version — introduce a new version instead.
3. **Schema stays structural.** Do not add fixed enums for geometry types or dataset providers. Those belong in adapter registries.
4. **Adapter registries are additive.** New adapters can be registered at any time. Missing adapters produce clear error diagnostics rather than silent failures.
