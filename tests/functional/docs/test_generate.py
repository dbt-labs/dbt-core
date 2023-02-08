import os

from dbt.tests.util import run_dbt


class TestGenerate:
    def test_generate_no_manifest_on_no_compile(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        assert not os.path.exists("./target/manifest.json")
