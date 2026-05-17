# Execution

After parsing produces the Manifest, dbt enters the execution phase where selected nodes are compiled and run against the data warehouse. This phase is orchestrated by the `Task` framework, which provides a layered abstraction for command execution, node selection, and per-node processing.

The execution flow begins when a command's Task (e.g., `RunTask`, `BuildTask`) calls `run()`. For graph-based commands, this triggers manifest compilation into a dependency graph—including cycle detection to catch circular `ref()` dependencies—followed by node selection based on CLI arguments (`--select`, `--exclude`), and construction of a `GraphQueue` that respects topological ordering. The Task then spawns a thread pool and processes nodes concurrently, with the queue releasing downstream nodes only after their dependencies complete.

Each node is processed by a dedicated `Runner` instance, which handles the compile-then-execute lifecycle. During compilation, Jinja templates are rendered to produce `compiled_code`, with ephemeral model references resolved into CTEs. During execution, the `Runner` interacts with the adapter to run the compiled SQL or invoke materialization macros. Results are collected and used to determine whether dependent nodes should proceed or be skipped due to upstream failures.

The framework supports different execution modes: topological (default, respects dependencies) and independent (parallel execution ignoring edges, used by `dbt list`). Special handling exists for `dbt build`, which adds test edges to the graph so that tests on upstream models block downstream model execution, and coordinates unit tests to run before their associated models.

## Subsections

- [Task Framework](4.1_Task_Framework.md) - Task and Runner class hierarchies, execution orchestration
- [Graph Compilation](4.2_Graph_Compilation.md) - Building the dependency graph from the manifest
- [Node Selection](4.3_Node_Selection.md) - Selection syntax and the selector framework
- [Node Compilation](4.4_Node_Compilation.md) - Jinja rendering and CTE injection
- [Node Materialization](4.5_Node_Materialization.md) - Materialization macros and adapter interactions
