.DEFAULT_GOAL:=help

# Optional flag to run target in a docker container.
# (example `make test USE_DOCKER=true`)
ifeq ($(USE_DOCKER),true)
	DOCKER_CMD := docker-compose run --rm test
endif

LOGS_DIR := ./logs

# Optional flag to invoke tests using our CI env.
# But we always want these active for structured
# log testing.
CI_FLAGS =\
	DBT_TEST_USER_1=dbt_test_user_1\
	DBT_TEST_USER_2=dbt_test_user_2\
	DBT_TEST_USER_3=dbt_test_user_3\
	RUSTFLAGS="-D warnings"\
	LOG_DIR=./logs\
	DBT_LOG_FORMAT=json

.PHONY: dev_req
dev_req: ## Installs dbt-* packages in develop mode along with only development dependencies.
	@\
	pip install -r dev-requirements.txt -r editable-requirements.txt

.PHONY: dev
dev: dev_req ## Installs dbt-* packages in develop mode along with development dependencies and pre-commit.
	@\
	pre-commit install

.PHONY: mypy
mypy: .env ## Runs mypy against staged changes for static type checking.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual mypy-check | grep -v "INFO"

.PHONY: flake8
flake8: .env ## Runs flake8 against staged changes to enforce style guide.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual flake8-check | grep -v "INFO"

.PHONY: black
black: .env ## Runs black  against staged changes to enforce style guide.
	@\
	$(DOCKER_CMD) pre-commit run --hook-stage manual black-check -v | grep -v "INFO"

.PHONY: lint
lint: .env ## Runs flake8 and mypy code checks against staged changes.
	@\
	$(DOCKER_CMD) pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: unit
unit: .env ## Runs unit tests with py
	@\
	$(DOCKER_CMD) tox -e py

.PHONY: test
test: .env ## Runs unit tests with py and code checks against staged changes.
	@\
	$(DOCKER_CMD) tox -e py; \
	$(DOCKER_CMD) pre-commit run black-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run flake8-check --hook-stage manual | grep -v "INFO"; \
	$(DOCKER_CMD) pre-commit run mypy-check --hook-stage manual | grep -v "INFO"

.PHONY: integration
integration: .env ## Runs postgres integration tests with py-integration
	@\
	$(if $(USE_CI_FLAGS), $(CI_FLAGS)) $(DOCKER_CMD) tox -e py-integration -- -nauto

.PHONY: integration-fail-fast
integration-fail-fast: .env ## Runs postgres integration tests with py-integration in "fail fast" mode.
	@\
	$(DOCKER_CMD) tox -e py-integration -- -x -nauto

.PHONY: interop
interop: clean
	@\
	mkdir $(LOGS_DIR) && \
	$(CI_FLAGS) $(DOCKER_CMD) tox -e py-integration -- -nauto && \
	LOG_DIR=$(LOGS_DIR) cargo run --manifest-path test/interop/log_parsing/Cargo.toml

.PHONY: setup-db
setup-db: ## Setup Postgres database with docker-compose for system testing.
	@\
	docker-compose up -d database && \
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
	@echo 'cleaning repo...'
	@rm -f .coverage
	@rm -f .coverage.*
	@rm -rf .eggs/
	@rm -f .env
	@rm -rf .tox/
	@rm -rf build/
	@rm -rf dbt.egg-info/
	@rm -f dbt_project.yml
	@rm -rf dist/
	@rm -f htmlcov/*.{css,html,js,json,png}
	@rm -rf logs/
	@rm -rf target/
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -depth -delete
	@echo 'done.'


.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target] [USE_DOCKER=true]'
	@echo
	@echo 'targets:'
	@grep -E '^[8+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo 'options:'
	@echo 'use USE_DOCKER=true to run target in a docker container'
