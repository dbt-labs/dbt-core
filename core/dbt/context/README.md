# Contexts and Jinja rendering

Contexts are used for Jinja rendering. They include context methods, executable macros, and various settings that are available in Jinja.

The most common entrypoint to Jinja rendering in dbt is a method named `get_rendered`, which takes two arguments: templated code (string), and a context used to render it (dictionary). 

The context is the bundle of information that is in "scope" when rendering Jinja-templated code. For instance, imagine a simple Jinja template:
```
{% set new_value = some_macro(some_variable) %}
```
Both `some_macro()` and `some_variable` must be defined in that context. Otherwise, it will raise an error when rendering.

Different contexts are used in different places because we allow access to different methods and data in different places. Executable SQL, for example, includes all available macros and the model being run. The variables and macros in scope for Jinja defined in yaml files is much more limited.

### Implementation

The context that is passed to Jinja is always in a dictionary format, not an actual class, so a `to_dict()` is executed on a context class before it is used for rendering.

Each context has a `generate_<name>_context` function to create the context. `ProviderContext` subclasses have different generate functions for parsing and for execution, so that certain functions (notably `ref`, `source`, and `config`) can return different results

### Hierarchy

All contexts inherit from the `BaseContext`, which includes "pure" methods (e.g. `tojson`), `env_var()`, and `var()` (but only CLI values, passed via `--vars`).

Methods available in parent contexts are also available in child contexts.

```
   BaseContext -- core/dbt/context/base.py
     SecretContext -- core/dbt/context/secret.py
     TargetContext -- core/dbt/context/target.py
       ConfiguredContext -- core/dbt/context/configured.py
         SchemaYamlContext -- core/dbt/context/configured.py
           DocsRuntimeContext -- core/dbt/context/configured.py
         MacroResolvingContext -- core/dbt/context/configured.py
         ManifestContext -- core/dbt/context/manifest.py
           QueryHeaderContext -- core/dbt/context/manifest.py
           ProviderContext -- core/dbt/context/provider.py
             MacroContext -- core/dbt/context/provider.py
             ModelContext -- core/dbt/context/provider.py
             TestContext -- core/dbt/context/provider.py
```

### Contexts for configuration

Contexts for rendering "special" `.yml` (configuration) files:
- `SecretContext`: Supports "secret" env vars, which are prefixed with `DBT_ENV_SECRET_`. Used for rendering in `profiles.yml` and `packages.yml` ONLY. Secrets defined elsewhere will raise explicit errors.
- `TargetContext`: The same as `Base`, plus `target` (connection profile). Used most notably in `dbt_project.yml` and `selectors.yml`.

Contexts for other `.yml` files in the project:
- `SchemaYamlContext`: Supports `vars` declared on the CLI and in `dbt_project.yml`. Does not support custom macros, beyond `var()` + `env_var()` methods. Used for all `.yml` files, to define properties and configuration.
- `DocsRuntimeContext`: Standard `.yml` file context, plus `doc()` method (with all `docs` blocks in scope). Used to resolve `description` properties.
