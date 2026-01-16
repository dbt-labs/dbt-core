# Commands

This section documents each dbt CLI command's implementation details, including task classes, runners, and implementation quirks.

For detailed product documentation, see: https://docs.getdbt.com/category/list-of-commands

## Command Index

| Command | Task Class | Description |
|---------|------------|-------------|
| [dbt parse](6.1_dbt_parse.md) | *(@requires.manifest decorator)* | Parse project and write manifest |
| [dbt run](6.2_dbt_run.md) | `RunTask` | Execute models against the database |
| [dbt build](6.3_dbt_build.md) | `BuildTask` | Run seeds, models, snapshots, and tests in DAG order |
| [dbt seed](6.4_dbt_seed.md) | `SeedTask` | Load CSV files into the database |
| [dbt snapshot](6.5_dbt_snapshot.md) | `SnapshotTask` | Execute SCD Type 2 snapshots |
| [dbt test](6.6_dbt_test.md) | `TestTask` | Run data tests and unit tests |
| [dbt show](6.7_dbt_show.md) | `ShowTask` | Preview query results without materializing |
| [dbt deps](6.8_dbt_deps.md) | `DepsTask` | Install package dependencies |
| [dbt docs](6.9_dbt_docs.md) | `GenerateTask` / `ServeTask` | Generate and serve documentation |
| [dbt compile](6.10_dbt_compile.md) | `CompileTask` | Generate compiled SQL without executing |
| [dbt source](6.11_dbt_source.md) | `FreshnessTask` | Check source freshness |
| [dbt run-operation](6.12_dbt_run-operation.md) | `RunOperationTask` | Execute a macro |
| [dbt init](6.13_dbt_init.md) | `InitTask` | Scaffold new project or profile |
| [dbt list](6.14_dbt_list.md) | `ListTask` | List project resources |
| [dbt retry](6.15_dbt_retry.md) | `RetryTask` | Re-execute failed nodes |
| [dbt clone](6.16_dbt_clone.md) | `CloneTask` | Create zero-copy clones from production |
| [dbt debug](6.17_dbt_debug.md) | `DebugTask` | Validate environment and connection |
| [dbt clean](6.18_dbt_clean.md) | `CleanTask` | Remove target and packages directories |

## Command Categories

### Execution Commands
Commands that execute work against the database:
- `dbt run` — Models
- `dbt build` — All resource types
- `dbt seed` — CSV data loading
- `dbt snapshot` — SCD snapshots
- `dbt test` — Data/unit tests
- `dbt clone` — Zero-copy clones
- `dbt source freshness` — Source monitoring
- `dbt retry` — Failure recovery
- `dbt run-operation` — Ad-hoc macro execution

### Compilation Commands
Commands that process SQL without executing:
- `dbt compile` — Generate compiled SQL
- `dbt show` — Preview results
- `dbt parse` — Build manifest only

### Utility Commands
Commands for project management:
- `dbt deps` — Package management
- `dbt clean` — Clear artifacts
- `dbt init` — Project scaffolding
- `dbt debug` — Warehouse connection validation
- `dbt list` — Resource listing
- `dbt docs` — Documentation generation/serving

## Common Patterns

### Context Requirements

Commands use `@requires` decorators to build their execution context:

| Decorator | Provides |
|-----------|----------|
| `@requires.preflight` | Logging, tracking setup |
| `@requires.profile` | Database credentials |
| `@requires.project` | Project configuration |
| `@requires.runtime_config` | Merged profile + project |
| `@requires.manifest` | Parsed manifest |

### Runner Pattern

Execution commands use the Runner pattern:
1. Task creates `GraphQueue` of selected nodes
2. Thread pool processes nodes in parallel
3. Each node is handled by a `Runner` subclass
4. Runner calls `compile()` → `execute()`

### Result Artifacts

Most commands write `run_results.json`. Exceptions:
- `dbt source freshness` → `sources.json`
- `dbt docs generate` → `catalog.json`
- `dbt list` → stdout only
