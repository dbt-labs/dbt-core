import pytest

from dbt.tests.util import run_dbt

from dbt.artifacts.resources import RefArgs
from dbt.contracts.graph.manifest import Manifest
import os


def get_manifest():
    path = "./target/partial_parse.msgpack"
    if os.path.exists(path):
        with open(path, "rb") as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None


basic__schema_yml = """
version: 2

sources:
  - name: my_src
    schema: "{{ target.schema }}"
    tables:
      - name: my_tbl

models:
  - name: model_a
    columns:
      - name: fun

"""

basic__model_a_sql = """
{{ config(tags='hello', x=False) }}
{{ config(tags='world', x=True) }}

select * from {{ ref('model_b') }}
cross join {{ source('my_src', 'my_tbl') }}
where false as boop

"""

basic__model_b_sql = """
select 1 as fun
"""


class BasicExperimentalParser:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": basic__model_a_sql,
            "model_b.sql": basic__model_b_sql,
            "schema.yml": basic__schema_yml,
        }


class TestBasicExperimentalParser(BasicExperimentalParser):
    # test that the experimental parser extracts some basic ref, source, and config calls.
    def test_experimental_parser_basic(
        self,
        project,
    ):
        run_dbt(["--use-experimental-parser", "parse"])
        manifest = get_manifest()
        node = manifest.nodes["model.test.model_a"]
        assert node.refs == [RefArgs(name="model_b")]
        assert node.sources == [["my_src", "my_tbl"]]
        assert node.config._extra == {"x": True}
        assert node.config.tags == ["hello", "world"]


class TestBasicStaticParser(BasicExperimentalParser):
    # test that the static parser extracts some basic ref, source, and config calls by default
    # without the experimental flag and without rendering jinja
    def test_static_parser_basic(self, project):
        run_dbt(["--debug", "parse"])

        manifest = get_manifest()
        node = manifest.nodes["model.test.model_a"]
        assert node.refs == [RefArgs(name="model_b")]
        assert node.sources == [["my_src", "my_tbl"]]
        assert node.config._extra == {"x": True}
        assert node.config.tags == ["hello", "world"]


class TestBasicNoStaticParser(BasicExperimentalParser):
    # test that the static parser doesn't run when the flag is set
    def test_static_parser_is_disabled(self, project):
        run_dbt(["--debug", "--no-static-parser", "parse"])

        manifest = get_manifest()
        node = manifest.nodes["model.test.model_a"]
        assert node.refs == [RefArgs(name="model_b")]
        assert node.sources == [["my_src", "my_tbl"]]
        assert node.config._extra == {"x": True}
        assert node.config.tags == ["hello", "world"]
