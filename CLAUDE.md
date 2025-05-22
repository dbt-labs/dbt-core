# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

dbt-core is the core engine of dbt (data build tool), which enables data analysts and engineers to transform data using SQL. dbt lets users write select statements that are transformed into tables and views in a data warehouse, while handling dependency management, testing, documentation, and other aspects of the data transformation process.

We are working on a REAL contribution to this open source repository. We are shooting for a production quality contribution that respects the professional maintainers at dbt Labs who will be reviewing our code.

## Development Environment Setup

### Prerequisites
- Python 3.9 or higher
- Docker and docker-compose (for testing)
- Git

### Installation

Set up a development environment:

```bash
# Create and activate a virtual environment
python3 -m venv env
source env/bin/activate

# Install development requirements and dbt-core in editable mode
make dev
# Or alternatively
pip install -r dev-requirements.txt -r editable-requirements.txt
pre-commit install
```

## Common Commands

### Building and Development

```bash
# Install dbt-core in development mode
make dev

# Clean the development environment
make clean

# Uninstall all packages in venv except build tools
make dev-uninstall
```

### Linting and Code Quality

```bash
# Run mypy for type checking
make mypy

# Run flake8 for code style checking
make flake8

# Run black for code formatting
make black

# Run all code quality checks (flake8 and mypy)
make lint
```

### Testing

```bash
# Set up a Postgres database for testing
make setup-db
# or manually
docker-compose up -d database
PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres bash test/setup_db.sh

# Run unit tests
make unit
# or
tox -e py

# Run all tests (unit tests and code checks)
make test

# Run integration tests (with Postgres)
make integration
# or with fail-fast option
make integration-fail-fast

# Running a specific test with pytest
python3 -m pytest tests/unit/test_invocation_id.py
# Run a specific unit test
python3 -m pytest tests/unit/test_invocation_id.py::TestInvocationId::test_invocation_id
# Run specific functional tests
python3 -m pytest tests/functional/sources
```

### Docker Option

Most commands can be run inside Docker by adding the USE_DOCKER=true flag:

```bash
make test USE_DOCKER=true
make integration USE_DOCKER=true
```

## Project Architecture

dbt-core is structured as follows:

- **core/dbt**: Main Python package
  - **adapters**: Base classes for database-specific functionality
  - **clients**: Interfaces with dependencies (Jinja, etc.)
  - **config**: Handles configuration from profiles, project files, and macros
  - **context**: Builds and exposes dbt-specific Jinja functionality
  - **contracts**: Defines Python dataclasses for validation
  - **events**: Logging events
  - **graph**: Produces a DAG of project resources
  - **parser**: Reads project files, validates, and constructs Python objects
  - **task**: Defines actions that dbt can perform (run, compile, test, etc.)

### Command Structure

dbt commands map to task classes. For example:
- `dbt run` => task.run.RunTask
- `dbt compile` => task.compile.CompileTask
- `dbt test` => task.test.TestTask
- `dbt docs generate` => task.docs.generate.GenerateTask

Tasks kick off "Runners" that execute in parallel, with parallelism managed via a thread pool.

## Testing Strategy

dbt-core uses multiple testing approaches:

1. **Unit Tests**: Fast Python tests that don't need a database
2. **Functional Tests**: End-to-end tests that interact with a database (primarily Postgres)

The test directory structure:
- **tests/unit/**: Unit tests for Python code
- **tests/functional/**: Functional tests for database interactions

## Debugging Tips

1. The logs for a `dbt run` have stack traces in `logs/dbt.log` in the project directory
2. Using a debugger: `pytest --pdb --pdbcls=IPython.terminal.debugger:pdb`
3. Single-thread execution: `dbt --single-threaded run`
4. Jinja debugging:
   - Print statements: `{{ log(msg, info=true) }}`
   - Debug mode: `{{ debug() }}`
5. Formatting JSON artifacts:
   ```bash
   python -m json.tool target/run_results.json > run_results.json
   ```
6. Profiling:
   ```bash
   dbt -r dbt.cprof run
   # Install and use snakeviz to view the output
   pip install snakeviz
   snakeviz dbt.cprof
   ```

## Contributing Guidelines

- **CLA Required**: All contributors must sign the [Contributor License Agreement](https://docs.getdbt.com/docs/contributor-license-agreements)
- **Adapter-specific changes**: For database adapter issues, use the adapter's repository instead of dbt-core
- **Target branch**: All pull requests should target the `main` branch
- **Testing requirements**: Add unit tests for any new code (tests/unit/ for pure Python, tests/functional/ for database interactions)
- **Code quality**: Follow code style guidelines (black, flake8, mypy)
- **Changelog**: Use `changie new` to create changelog entries - do not edit CHANGELOG.md directly
- **Review process**: PRs are labeled `ready_for_review` and assigned two reviewers who aim to respond within one week

## Changelog Management

Use [changie](https://changie.dev) for changelog entries:

```bash
# Install changie first (see changie.dev for installation instructions)
# Create a new changelog entry
changie new
# Follow the prompts to describe your changes
```

Never edit CHANGELOG.md directly - all changes go through changie to avoid merge conflicts.
