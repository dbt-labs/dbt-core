.PHONY: test test-unit test-integration

changed_tests := `git status --porcelain | grep '^\(M\| M\|A\| A\)' | awk '{ print $$2 }' | grep '\/test_[a-zA-Z_\-\.]\+.py'`

test: test-unit test-integration

test-unit-quick:
	@echo "Quick unit test run starting..."
	@time docker-compose run test tox -e unit-py35

test-unit:
	@echo "Unit test run starting..."
	@time docker-compose run test tox -e unit-py27,unit-py35,pep8

test-integration:
	@echo "Integration test run starting..."
	@time docker-compose run test tox -e integration-py27,integration-py35

test-new:
	@echo "Test run starting..."
	@echo "Changed test files:"
	@echo "${changed_tests}"
	@docker-compose run test /usr/src/app/test/runner.sh ${changed_tests}
