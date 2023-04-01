import os
import json
from dbt.tests.util import run_dbt


class TestGenerate:
    def test_generate_no_manifest_on_no_compile(self, project):
        run_dbt(["docs", "generate", "--no-compile"])
        assert not os.path.exists("./target/manifest.json")

    def test_generate_empty_catalog(self, project):
        run_dbt(["docs", "generate", "--empty-catalog"])
        with open("./target/catalog.json") as file:
            catalog = json.load(file)
        assert catalog["nodes"] == {}, "nodes should be empty"
        assert catalog["sources"] == {}, "sources should be empty"
        assert catalog["errors"] is None, "errors should be null"
