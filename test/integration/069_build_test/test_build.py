from test.integration.base import DBTIntegrationTest, use_profile
import yaml


class TestBuild(DBTIntegrationTest):
    @property
    def schema(self):
        return "build_test_069"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["snapshots"],
            "data-paths": ["data"],
            "seeds": {
                "quote_columns": False,
            },
        }

    def build(self, expect_pass=True, extra_args=None, **kwargs):
        args = ["build"]
        if kwargs:
            args.extend(("--args", yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)

        return self.run_dbt(args, expect_pass=expect_pass)


class TestPassingBuild(TestBuild):
    @property
    def models(self):
        return "models"

    @use_profile("postgres")
    def test__postgres_build_happy_path(self):
        self.build()


class TestFailingBuild(TestBuild):
    @property
    def schema(self):
        return "build_test_069"

    @property
    def models(self):
        return "models-failing"

    @use_profile("postgres")
    def test__postgres_build_happy_path(self):
        results = self.build(expect_pass=False)
        self.assertEqual(len(results), 13)
        actual = [r.status for r in results]
        expected = ['error']*1 + ['skipped']*5 + ['pass']*2 + ['success']*5
        self.assertEqual(sorted(actual), sorted(expected))


class TestFailingTestsBuild(TestBuild):
    @property
    def schema(self):
        return "build_test_069"

    @property
    def models(self):
        return "tests-failing"

    @use_profile("postgres")
    def test__postgres_failing_test_skips_downstream(self):
        results = self.build(expect_pass=False)
        self.assertEqual(len(results), 13)
        actual = [str(r.status) for r in results]
        expected = ['fail'] + ['skipped']*6 + ['pass']*2 + ['success']*4
        self.assertEqual(sorted(actual), sorted(expected))
