# CLI Architecture

## Overview

The CLI layer serves as the primary entry point for all dbt commands. It handles command-line parsing, configuration resolution, and orchestration of the execution lifecycle. The implementation is built on the [Click](https://click.palletsprojects.com/) framework, with custom extensions for dbt-specific needs like multi-source configuration and decorator-based dependency injection.

dbt can be invoked in two ways: via the command line or programmatically. Command-line invocation flows through the `cli` click group defined in `main.py`. For programmatic use, the `dbtRunner` class provides a Python API that wraps click invocation and returns structured `dbtRunnerResult` objects containing success status, results, and any exceptions.

### Command Structure (`main.py`)

Commands are defined as click-decorated functions under the root `cli` group. Each command follows a consistent pattern: it applies global flags via `@global_flags` (e.g., `--target`, `--debug`, `--fail-fast`, etc), command-specific options from the `params` module, and `@requires` decorators that build up execution context. The command body then instantiates the appropriate `Task` class for the command and calls `task.run()`. Nested command groups like `docs` and `source` contain subcommands (e.g., `docs generate`, `source freshness`).

### Parameters (`params.py`)

All CLI options are defined in `params.py` as click option decorators. Each option specifies its flags, environment variable mapping, help text, type, and default value. Custom click types in `option_types.py` handle complex inputs like YAML strings (`--vars`, `--args`) and warn-error configurations. The `MultiOption` class in `options.py` allows options like `--select` to accept multiple space-separated values.

### Flags and Configuration (`flags.py`)

The `Flags` dataclass is the primary configuration handler for running dbt. It consolidates configuration from multiple sources in priority order: CLI options take precedence over environment variables, which take precedence over project flags from `dbt_project.yml`, which take precedence over defaults. The `__init__` method walks the click context hierarchy to collect all parameter values, handling deprecated env vars and mutually exclusive options along the way.

### `@requires` Decorators (`requires.py`)

The `requires` module provides decorators that progressively build up the execution context stored in Click's `ctx.obj`. The `preflight` decorator handles initialization: creating `Flags`, setting up logging, and initializing tracking. Resource decorators—`profile`, `project`, `runtime_config`, `manifest`, and `catalogs`—each load their respective configuration or artifact and add it to context. The `postflight` decorator wraps command execution with exception handling and fires completion events.

## Execution Flow

When a command is invoked, execution flows through the decorator chain: `preflight` initializes flags and logging, then resource decorators (`profile` → `project` → `runtime_config` → `manifest`) progressively load configuration and add it to `ctx.obj`. The command body receives this fully-populated context, instantiates a `Task` with the flags, config, and manifest, then runs it. Finally, `postflight` handles any exceptions and emits completion events.

### Exception Handling

In the `postflight` decorator, the click command is invoked (i.e. `func(*args, **kwargs)`) and wrapped in a `try/except` block to handle any exceptions thrown. Any exceptions thrown from `postflight` are wrapped by custom exceptions from the `dbt.cli.exceptions` module (i.e. `ResultExit`, `ExceptionExit`) to instruct click to complete execution with a particular exit code.

Some `dbt-core` handled exceptions have an attribute named `results` which contains results from running nodes (e.g. `FailFastError`). These are wrapped in the `ResultExit` exception to represent runs that have failed in a way that `dbt-core` expects. If the invocation of the command does not throw any exceptions but does not succeed, `postflight` will still raise the `ResultExit` exception to make use of the exit code. These exceptions produce an exit code of `1`.

Exceptions wrapped with `ExceptionExit` may be thrown by `dbt-core` intentionally (i.e. an exception that inherits from `dbt.exceptions.Exception`) or unintentionally (i.e. exceptions thrown by the python runtime). In either case these are considered errors that `dbt-core` did not expect and are treated as genuine exceptions. These exceptions produce an exit code of `2`.

If no exceptions are thrown from invoking the command and the command succeeds, `postflight` will not raise any exceptions. When no exceptions are raised an exit code of `0` is produced.

### `dbtRunner`

`dbtRunner` provides a programmatic interface for our click CLI and wraps the invocation of the click commands to handle any exceptions thrown. It is a feature available for users of dbt Core, but in some instances we also use it internally for testing.

`dbtRunner.invoke` should ideally only ever return an instantiated `dbtRunnerResult` which contains the following fields:
- `success`: A boolean representing whether the command invocation was successful[
- `result`: The optional result of the command invoked. This attribute can have many types, please see [the definition of `dbtRunnerResult` for more information](https://github.com/dbt-labs/dbt-core/blob/7634345985a86b113f51b74b9b776e346b59bdbf/core/dbt/cli/main.py#L23-L37)
- `exception`: If an exception was thrown during command invocation it will be saved here, otherwise it will be `None`. Please note that the exceptions held in this attribute are not the exceptions thrown by `postflight` but instead the exceptions that `ResultExit` and `ExceptionExit` wrap

Programmatic exception handling might look like the following:
```python
from dbt.cli.main import dbtRunner, dbtRunnerResult

# initialize
dbt = dbtRunner()

# create CLI args as a list of strings
cli_args = ["run", "--select", "tag:my_tag"]

# run the command
res: dbtRunnerResult = dbt.invoke(cli_args)

# inspect the results
for r in res.result:
    print(f"{r.node.name}: {r.status}")
```

Reference: https://docs.getdbt.com/reference/programmatic-invocations

## Adding a New Command

To add a new command: (1) define the command function in `main.py` with appropriate decorators, (2) add an entry to the `Command` enum in `types.py`, and (3) add the command to the `CMD_DICT` in `flags.py`'s `command_args` function. Every command needs at minimum the `@cli.command()` decorator, `@requires.postflight`, and `@requires.preflight`.

```python
@cli.command("my-new-command")
@requires.postflight
@requires.preflight
def my_new_command(ctx, **kwargs):
    ...
```
