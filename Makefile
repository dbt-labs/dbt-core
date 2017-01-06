.PHONY: test test-unit test-integration

changed_tests := `git status --porcelain | grep '^\(M\| M\|A\| A\)' | awk '{ print $$2 }' | grep '\/test_[a-zA-Z_\-\.]\+.py'`

test: test-unit test-integration

test-unit:
	@echo "Unit test run starting..."
	tox -e unit-py27,unit-py35

test-integration:
	@echo "Integration test run starting..."
	@docker-compose run test /usr/src/app/test/integration.sh

test-new:
	@echo "Test run starting..."
	@echo "Changed test files:"
	@echo "${changed_tests}"
	@docker-compose run test /usr/src/app/test/runner.sh ${changed_tests}
