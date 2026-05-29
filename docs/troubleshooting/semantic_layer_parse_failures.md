# Troubleshooting: Semantic Layer Parse Failures

This document covers common causes of `dbt parse` failures related to semantic
models and metrics, and how to fix or improve the errors produced.

## Extra fields on YAML config objects produce vague errors

When a user adds an unrecognised field to a YAML config object (e.g. inside
`semantic_model:`, a `dimension:`, or a `metric:`), dbt's JSON Schema validator
rejects it but the default error message is unhelpful — it names the whole
object rather than the offending key:

```
Invalid models config given in models/schema.yml @ models: {...} - at path
['semantic_model']: {...} is not valid under any of the given schemas
```

**How to improve the error:** Add a `validate()` classmethod to the relevant
`Unparsed*` dataclass in `core/dbt/contracts/graph/unparsed.py`. Compare
`cls.__dataclass_fields__` against the incoming `data` dict before calling
`super().validate(data)`, and raise a `ValidationError` that names the unknown
field(s) and lists the valid ones. `UnparsedSemanticModelConfig.validate()` is
the reference implementation.

When adding such a test, use `ContractTestCase.assert_fails_validation_with_message()`
(in `tests/unit/utils/__init__.py`) to assert both that validation fails *and*
that the error message is actionable.

If you need a clear PR example, refer to PR12766.

## Union-typed fields produce even more vague errors

Several fields in `unparsed.py` use `Union[SomeConfig, bool, None]` (e.g.
`UnparsedModelUpdate.semantic_model`). When validation fails on the `SomeConfig`
branch, JSON Schema exhausts all branches of the `anyOf` and reports failure
against the union as a whole — giving no indication of which branch failed or
why:

```
at path ['semantic_model']: {'enabled': True, 'name': 'purchases', 'description':
'...'} is not valid under any of the given schemas
```

**How to improve the error:** The same `validate()` override approach works here.
By checking the sub-object's fields before `super().validate(data)` runs, the
specific error fires first and the opaque union failure is never reached.

## Standalone simple metrics must be nested under the model entry

Simple v2 metrics must be written under the model entry (`models[].metrics`),
not as a top-level `metrics:` key. A top-level `metrics:` key is valid for
derived, conversion, and cumulative metrics — but **not** for simple ones. Using
it for a simple metric raises:

```
simple metrics in v2 YAML must be attached to semantic_model
```

Move the metrics with type 'simple' to a `metrics:` list to indented under the
model entry (same level as `columns:`) to fix this:

```yaml
# Wrong — top-level metrics: key
models:
  - name: fct_revenue
    semantic_model: true
    columns: ...

metrics:
  - name: total_revenue   # fails: simple metric cannot be standalone
    type: simple
    agg: sum
    expr: revenue

# Right — metrics nested under the model entry
models:
  - name: fct_revenue
    semantic_model: true
    columns: ...
    metrics:
      - name: total_revenue
        type: simple
        agg: sum
        expr: revenue
```
