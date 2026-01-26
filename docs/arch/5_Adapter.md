# Adapter Framework

The adapter framework provides the abstraction layer between dbt-core and data warehouses. Each adapter (e.g., `dbt-snowflake`, `dbt-postgres`, `dbt-bigquery`) implements a common interface for database operations, allowing dbt-core to remain warehouse-agnostic while adapters handle the specifics of SQL dialect, connection management, and platform capabilities.

## Architecture Overview

The adapter framework consists of several key components:

- **Credentials**: Warehouse connection parameters parsed from `profiles.yml`
- **Adapter Plugin**: The adapter implementation loaded dynamically based on credential type
- **Connection Manager**: Thread-safe connection pooling and lifecycle management
- **Relation**: Database object abstraction (database, schema, identifier)
- **Macro Resolver**: Resolution of adapter-specific macro implementations via `adapter.dispatch()`

The base adapter classes live in the `dbt-adapters` package, with warehouse-specific implementations in their respective packages (e.g., `dbt-snowflake`).

## Initialization Flow

Adapter initialization happens during the CLI decorator chain in `core/dbt/cli/requires.py`:

### 1. Profile Loading (`@profile` decorator)

```
profiles.yml → load_profile() → Profile object with Credentials
```

The `load_profile()` function:
1. Reads `profiles.yml` from the profiles directory
2. Extracts the target configuration (e.g., `dev`, `prod`)
3. Parses the `type` field to determine which adapter to use
4. Calls `load_plugin(adapter_type)` to dynamically load the adapter package
5. Instantiates the adapter's `Credentials` class with connection parameters
6. Returns a `Profile` object containing the credentials

### 2. Runtime Config Creation (`@runtime_config` decorator)

```
Profile + Project → RuntimeConfig
```

The `RuntimeConfig` combines:
- Profile (credentials, target, threads)
- Project (dbt_project.yml settings)
- CLI flags

This config object is passed to the adapter and provides all context needed for database operations.

### 3. Adapter Registration (`@manifest` decorator)

```
RuntimeConfig → register_adapter() → get_adapter()
```

During manifest loading in `parse_manifest()`:

1. `register_adapter(runtime_config, mp_context)` registers the adapter in a global factory
2. `get_adapter(runtime_config)` retrieves the singleton adapter instance
3. `adapter.set_macro_context_generator()` configures how macro contexts are created
4. `adapter.set_macro_resolver(manifest)` provides the manifest for macro resolution
5. `adapter.connections.set_query_header()` sets up query comment headers

The adapter is now fully initialized and ready for use.

## Adapter Configuration with Manifest

After parsing completes, the adapter receives the manifest to enable macro resolution:

```python
adapter = get_adapter(runtime_config)
adapter.set_macro_resolver(manifest)
adapter.set_macro_context_generator(generate_runtime_macro_context)
```

This connection is critical because:
- Materializations and other macros use `adapter.dispatch()` to find implementations
- The adapter needs to resolve macro calls like `adapter.dispatch('create_table_as')`
- Query headers include project metadata from the manifest

## Connection Management

The adapter manages database connections through its `ConnectionManager`:

### Connection Lifecycle

```python
# Named connection context
with adapter.connection_named("master"):
    adapter.execute(sql)
    # Connection automatically released on exit
```

### Thread-Safe Pooling

- Each thread gets its own connection via `get_thread_connection()`
- Connections are named (e.g., node unique_id) for debugging and cancellation
- `cleanup_connections()` releases all connections at task end

### Connection Naming by Node

During execution, each node runs with a connection named after its unique_id:

```python
with self.adapter.connection_named(self.node.unique_id, self.node):
    # compile and execute the node
```

This enables:
- Query cancellation by node name on `KeyboardInterrupt`
- Connection tracking and debugging
- Proper isolation between concurrent node executions

## Critical Usage Points During Execution

### Before Execution (`RunTask.before_run()`)

```python
with adapter.connection_named("master"):
    self.create_schemas(adapter, required_schemas)
    self.populate_adapter_cache(adapter)
    self.safe_run_hooks(adapter, RunHookType.Start, {})
```

- Creates any missing schemas
- Populates the relation cache for fast lookups
- Runs `on-run-start` hooks

### During Node Execution

Runners interact with the adapter through the Jinja context:

```python
context = generate_runtime_model_context(model, self.config, manifest)
# context['adapter'] is a DatabaseWrapper around the real adapter
```

Key adapter methods used in materializations:
- `adapter.execute(sql)`: Run SQL, return response
- `adapter.get_columns_in_relation(relation)`: Introspect schema
- `adapter.create_schema(relation)`: Create schema if not exists
- `adapter.drop_relation(relation)`: Drop table/view
- `adapter.rename_relation(from, to)`: Atomic rename
- `adapter.dispatch(macro_name)`: Find adapter-specific macro

### Relation Cache

The adapter maintains an in-memory cache of database relations:

```python
adapter.cache_added(relation)      # Register new relation
adapter.cache_dropped(relation)    # Remove dropped relation
adapter.cache_renamed(from, to)    # Update after rename
```

Cache population happens in `populate_adapter_cache()` before execution, querying `information_schema` or equivalent to discover existing objects.

### Query Cancellation

On `KeyboardInterrupt`, the task cancels in-flight queries:

```python
adapter = get_adapter(self.config)
if adapter.is_cancelable():
    with adapter.connection_named("master"):
        for conn_name in adapter.cancel_open_connections():
            # Log cancelled connections
```

### After Execution (`RunTask.after_run()`)

```python
self.safe_run_hooks(adapter, RunHookType.End, extra_context)
adapter.cleanup_connections()
```

- Runs `on-run-end` hooks
- Cleans up all open connections

## The `adapter.dispatch()` Mechanism

`adapter.dispatch()` enables polymorphic macro invocation—the same macro call resolves to different implementations based on the active adapter.

Resolution order:
1. Current adapter type (e.g., `snowflake__create_table_as`)
2. Parent adapter types in inheritance chain
3. Default implementation (`default__create_table_as`)

This allows adapters to override specific behaviors while falling back to common implementations.

## Key Adapter Methods

| Method | Purpose |
|--------|---------|
| `execute(sql, auto_begin, fetch)` | Execute SQL statement |
| `get_columns_in_relation(relation)` | Get column metadata |
| `create_schema(relation)` | Create schema |
| `drop_relation(relation)` | Drop relation |
| `rename_relation(from, to)` | Rename relation |
| `list_schemas(database)` | List schemas in database |
| `list_relations_without_caching(schema)` | Query relations directly |
| `expand_column_types(...)` | Handle type expansion for incremental |
| `submit_python_job(model, code)` | Execute Python model (if supported) |
