.PHONY: install test test-unit test-integration

changed_tests := `git status --porcelain | grep '^\(M\| M\|A\| A\)' | awk '{ print $$2 }' | grep '\/test_[a-zA-Z_\-\.]\+.py'`

install:
	pip install -e .

test: .env
	@echo "Full test run starting..."
	@time docker-compose run --rm test tox

test-unit: .env
	@echo "Unit test run starting..."
	@time docker-compose run --rm test tox -e unit-py36,flake8

test-integration: .env
	@echo "Integration test run starting..."
	@time docker-compose run --rm test tox -e integration-postgres-py36,integration-redshift-py36,integration-snowflake-py36,integration-bigquery-py36

test-quick: .env
	@echo "Integration test run starting..."
	@time docker-compose run --rm test tox -e integration-postgres-py36 -- -x

# This rule creates a file named .env that is used by docker-compose for passing
# the USER_ID and GROUP_ID arguments to the Docker image.
.env:
	@touch .env
ifneq ($(OS),Windows_NT)
ifneq ($(shell uname -s), Darwin)
	@echo USER_ID=$(shell id -u) > .env
	@echo GROUP_ID=$(shell id -g) >> .env
endif
endif
	@time docker-compose build

clean:
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
