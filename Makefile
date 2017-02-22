.PHONY: install test test-unit test-integration

changed_tests := `git status --porcelain | grep '^\(M\| M\|A\| A\)' | awk '{ print $$2 }' | grep '\/test_[a-zA-Z_\-\.]\+.py'`

install:
	pip install --upgrade .

test:
	@echo "Full test run starting..."
	@time docker-compose run test tox

test-unit:
	@echo "Unit test run starting..."
	@time docker-compose run test tox -e unit-py27,unit-py35,pep8

test-integration:
	@echo "Integration test run starting..."
	@time docker-compose run test tox -e integration-postgres-py27,integration-postgres-py35,integration-snowflake-py27,integration-snowflake-py35

test-new:
	@echo "Test run starting..."
	@echo "Changed test files:"
	@echo "${changed_tests}"
	@docker-compose run test /usr/src/app/test/runner.sh ${changed_tests}
