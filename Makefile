# ============================================================================
# DEPRECATED: This Makefile is maintained for backwards compatibility only.
#
# dbt-core now uses Hatch for task management and development workflows.
# Please migrate to using hatch commands directly:
#
#   make dev               →  cd core && hatch run setup
#   make unit              →  cd core && hatch run unit-tests
#   make test              →  cd core && hatch run test
#   make integration       →  cd core && hatch run integration-tests
#   make lint              →  cd core && hatch run lint
#   make code_quality      →  cd core && hatch run code-quality
#   make setup-db          →  cd core && hatch run setup-db
#   make clean             →  cd core && hatch run clean
#
# See core/pyproject.toml [tool.hatch.envs.default.scripts] for all available
# commands and CONTRIBUTING.md for detailed usage instructions.
#
# This Makefile will be removed in a future version of dbt-core.
# ============================================================================

.DEFAULT_GOAL:=help

.PHONY: dev_req
dev_req: ## Installs dbt-* packages in develop mode along with only development dependencies.
	@cd core && hatch run dev-req

.PHONY: dev
dev: ## Installs dbt-* packages in develop mode along with development dependencies and pre-commit.
	@cd core && hatch run setup

.PHONY: dev-uninstall
dev-uninstall: ## Uninstall all packages in venv except for build tools
	@pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y; \
	pip uninstall -y dbt-core

.PHONY: mypy
mypy: ## Runs mypy against staged changes for static type checking.
	@cd core && hatch run mypy

.PHONY: flake8
flake8: ## Runs flake8 against staged changes to enforce style guide.
	@cd core && hatch run flake8

.PHONY: black
black: ## Runs black against staged changes to enforce style guide.
	@cd core && hatch run black

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@cd core && hatch run lint

.PHONY: code_quality
code_quality: ## Runs all pre-commit hooks against all files.
	@cd core && hatch run code-quality

.PHONY: unit
unit: ## Runs unit tests with py
	@cd core && hatch run unit-tests

.PHONY: test
test: ## Runs unit tests with py and code checks against staged changes.
	@cd core && hatch run test

.PHONY: integration
integration: ## Runs core integration tests using postgres with py-integration
	@cd core && hatch run integration-tests

.PHONY: integration-fail-fast
integration-fail-fast: ## Runs core integration tests using postgres with py-integration in "fail fast" mode.
	@cd core && hatch run integration-tests-fail-fast

.PHONY: setup-db
setup-db: ## Setup Postgres database with docker-compose for system testing.
	@cd core && hatch run setup-db

.PHONY: clean
clean: ## Resets development environment.
	@cd core && hatch run clean

.PHONY: json_schema
json_schema: ## Update generated JSON schema using code changes.
	@cd core && hatch run json-schema

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'DEPRECATED: This Makefile is a compatibility shim.'
	@echo 'Please use "cd core && hatch run <command>" directly.'
	@echo
	@echo 'targets:'
	@grep -E '^[8+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'For more information, see CONTRIBUTING.md'
