.DEFAULT_GOAL:=help

# Optional flag to run target in a docker container.
# (example `make test USE_DOCKER=true`)
ifeq ($(USE_DOCKER),true)
	DOCKER_CMD := docker-compose run --rm test
endif

.PHONY: dev
dev: ## Installs dbt-* packages in develop mode along with development dependencies.
	pip install -r dev-requirements.txt -r editable-requirements.txt

.PHONY: mypy
mypy: .env ## Runs mypy for static type checking.
	$(DOCKER_CMD) tox -e mypy

.PHONY: flake8
flake8: .env ## Runs flake8 to enforce style guide.
	$(DOCKER_CMD) tox -e flake8

.PHONY: lint
lint: .env ## Runs all code checks in parallel.
	$(DOCKER_CMD) tox -p -e flake8,mypy

.PHONY: unit
unit: .env ## Runs unit tests with py38.
	$(DOCKER_CMD) tox -e py38

.PHONY: test
test: .env ## Runs unit tests with py38 and code checks in parallel.
	$(DOCKER_CMD) tox -p -e py38,flake8,mypy

.PHONY: integration
integration: .env integration-postgres ## Alias for integration-postgres.

.PHONY: integration-fail-fast
integration-fail-fast: .env integration-postgres-fail-fast ## Alias for integration-postgres-fail-fast.

.PHONY: integration-postgres
integration-postgres: .env ## Runs postgres integration tests with py38.
	$(DOCKER_CMD) tox -e py38-postgres -- -nauto

.PHONY: integration-postgres-fail-fast
integration-postgres-fail-fast: .env ## Runs postgres integration tests with py38 in "fail fast" mode.
	$(DOCKER_CMD) tox -e py38-postgres -- -x -nauto

.PHONY: setup-db
setup-db: ## Setup Postgres database with docker-compose for system testing.
	docker-compose up -d database
	PGHOST=localhost PGUSER=root PGPASSWORD=password PGDATABASE=postgres bash test/setup_db.sh

# This rule creates a file named .env that is used by docker-compose for passing
# the USER_ID and GROUP_ID arguments to the Docker image.
.env: ## Setup step for using using docker-compose with make target.
	@touch .env
ifneq ($(OS),Windows_NT)
ifneq ($(shell uname -s), Darwin)
	@echo USER_ID=$(shell id -u) > .env
	@echo GROUP_ID=$(shell id -g) >> .env
endif
endif

.PHONY: clean
clean: ## Resets development environment.
	rm -f .coverage
	rm -rf .eggs/
	rm -f .env
	rm -rf .tox/
	rm -rf build/
	rm -rf dbt.egg-info/
	rm -f dbt_project.yml
	rm -rf dist/
	rm -f htmlcov/*.{css,html,js,json,png}
	rm -rf logs/
	rm -rf target/
	find . -type f -name '*.pyc' -delete
	find . -type d -name '__pycache__' -depth -delete

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target] [USE_DOCKER=true]'
	@echo
	@echo 'targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'options:'
	@echo 'use USE_DOCKER=true to run target in a docker container'

