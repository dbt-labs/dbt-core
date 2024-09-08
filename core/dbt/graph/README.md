# Graph README

## Graph Selector Creation

### Selector Loading
During dbt execution, the `@requires.project` decorator creates the final selector objects used in the graph. The `SelectorConfig` class loads selectors from the project configuration, while the `selector_config_from_data` function parses these selectors.

#### Indirect Selection Default Value
In `@requires.preflight`, dbt reads CLI flags, environment variables, and the parameter's default value. It resolves these inputs based on their precedence order and stores the resolved value in global flags. When loading selectors, the [`selection_criteria_from_dict`](https://github.com/dbt-labs/dbt-core/blob/b316c5f18021fef3d7fd6ec255427054b7d2205e/core/dbt/graph/selector_spec.py#L111) function resolves the indirect selection value to the global flags value if not set. This ensures correct resolution of the indirect selection value.
