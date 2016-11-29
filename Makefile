.PHONY: test

changed_tests := `git status --porcelain | grep '^\(M\| M\|A\| A\)' | awk '{ print $$2 }' | grep '\/test_[a-zA-Z_\-\.]\+.py'`

test:
	@echo "Test run starting..."
	@docker-compose run test /usr/src/app/test/runner.sh

test-new:
	@echo "Test run starting..."
	@echo "Changed test files:"
	@echo "${changed_tests}"
	@docker-compose run test /usr/src/app/test/runner.sh ${changed_tests}
