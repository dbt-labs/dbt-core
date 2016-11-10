.PHONY: test

test:
	@echo "Test run starting..."
	@docker-compose run test /usr/src/app/test/runner.sh
