import pytest

from dbt.tests.util import run_dbt

from tests.functional.configs.fixtures import BaseConfigProject


class TestConfigIndivTests(BaseConfigProject):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
            "vars": {
                "test": {
                    "seed_name": "seed",
                }
            },
            "tests": {"test": {"enabled": True, "severity": "WARN"}},
        }

    def test_configuring_individual_tests(
        self,
        project,
    ):
        assert len(run_dbt(["seed"])) == 1
        assert len(run_dbt(["run"])) == 2

        # all tests on (minus sleeper_agent) + WARN
        assert len(run_dbt(["test"])) == 5

        # turn off two of them directly
        assert len(run_dbt(["test", "--vars", '{"enabled_direct": False}'])) == 3

        # turn on sleeper_agent data test directly
        assert (
            len(
                run_dbt(
                    ["test", "--models", "sleeper_agent", "--vars", '{"enabled_direct": True}']
                )
            )
            == 1
        )

        # set three to ERROR directly
        results = run_dbt(
            [
                "test",
                "--models",
                "config.severity:error",
                "--vars",
                '{"enabled_direct": True, "severity_direct": "ERROR"}',
            ],
            expect_pass=False,
        )
        assert len(results) == 2
        assert results[0].status == "fail"
        assert results[1].status == "fail"
