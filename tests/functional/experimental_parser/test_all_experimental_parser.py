import pytest

from dbt.tests.util import run_dbt, run_dbt_and_capture

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

select * from {{ ref('model_a') }}
cross join {{ source('my_src', 'my_tbl') }}
where false as boop

"""


ref_macro__schema_yml = """
version: 2

"""

ref_macro__models__model_a_sql = """
select 1 as id

"""

source_macro__macros__source_sql = """
{% macro source(source_name, table_name) %}

{% endmacro %}
"""

source_macro__schema_yml = """
version: 2

"""

source_macro__models__model_a_sql = """
select 1 as id

"""

config_macro__macros__config_sql = """
{% macro config() %}

{% endmacro %}
"""

config_macro__schema_yml = """
version: 2

"""

config_macro__models__model_a_sql = """
select 1 as id

"""


class BasicExperimentalParser:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": basic__model_a_sql,
            "schema.yml": basic__schema_yml,
        }


class TestBasicExperimentalParserFlag(BasicExperimentalParser):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        os.environ["DBT_USE_EXPERIMENTAL_PARSER"] = "true"
        yield
        del os.environ["DBT_USE_EXPERIMENTAL_PARSER"]

    def test_env_use_experimental_parser(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "parse"])

        # successful stable static parsing
        assert not ("1699: " in log_output)
        # successful experimental static parsing
        assert "1698: " in log_output
        # experimental parser failed
        assert not ("1604: " in log_output)
        # static parser failed
        assert not ("1603: " in log_output)
        # jinja rendering
        assert not ("1602: " in log_output)


class TestBasicStaticParserFlag(BasicExperimentalParser):
    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        os.environ["DBT_STATIC_PARSER"] = "false"
        yield
        del os.environ["DBT_STATIC_PARSER"]

    def test_env_static_parser(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "parse"])

        print(log_output)

        # jinja rendering because of --no-static-parser
        assert "1605: " in log_output
        # successful stable static parsing
        assert not ("1699: " in log_output)
        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # experimental parser failed
        assert not ("1604: " in log_output)
        # static parser failed
        assert not ("1603: " in log_output)
        # fallback jinja rendering
        assert not ("1602: " in log_output)


class TestBasicExperimentalParser(BasicExperimentalParser):
    # test that the experimental parser extracts some basic ref, source, and config calls.
    def test_experimental_parser_basic(
        self,
        project,
    ):
        run_dbt(["--use-experimental-parser", "parse"])
        manifest = get_manifest()
        node = manifest.nodes["model.test.model_a"]
        assert node.refs == [["model_a"]]
        assert node.sources == [["my_src", "my_tbl"]]
        assert node.config._extra == {"x": True}
        assert node.config.tags == ["hello", "world"]


class TestBasicStaticParser(BasicExperimentalParser):
    # test that the static parser extracts some basic ref, source, and config calls by default
    # without the experimental flag and without rendering jinja
    def test_static_parser_basic(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "parse"])

        # successful stable static parsing
        assert "1699: " in log_output
        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # experimental parser failed
        assert not ("1604: " in log_output)
        # static parser failed
        assert not ("1603: " in log_output)
        # jinja rendering
        assert not ("1602: " in log_output)

        manifest = get_manifest()
        node = manifest.nodes["model.test.model_a"]
        assert node.refs == [["model_a"]]
        assert node.sources == [["my_src", "my_tbl"]]
        assert node.config._extra == {"x": True}
        assert node.config.tags == ["hello", "world"]


class TestBasicNoStaticParser(BasicExperimentalParser):
    # test that the static parser doesn't run when the flag is set
    def test_static_parser_is_disabled(self, project):
        _, log_output = run_dbt_and_capture(["--debug", "--no-static-parser", "parse"])

        # jinja rendering because of --no-static-parser
        assert "1605: " in log_output
        # successful stable static parsing
        assert not ("1699: " in log_output)
        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # experimental parser failed
        assert not ("1604: " in log_output)
        # static parser failed
        assert not ("1603: " in log_output)
        # fallback jinja rendering
        assert not ("1602: " in log_output)


class TestRefOverrideExperimentalParser:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": ref_macro__models__model_a_sql,
            "schema.yml": ref_macro__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "source.sql": source_macro__macros__source_sql,
        }

    # test that the experimental parser doesn't run if the ref built-in is overriden with a macro
    def test_experimental_parser_ref_override(
        self,
        project,
    ):
        _, log_output = run_dbt_and_capture(["--debug", "--use-experimental-parser", "parse"])

        print(log_output)

        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # fallback to jinja rendering
        assert "1602: " in log_output
        # experimental parser failed
        assert not ("1604: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        assert "1601: " in log_output


class TestSourceOverrideExperimentalParser:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": source_macro__models__model_a_sql,
            "schema.yml": source_macro__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "source.sql": source_macro__macros__source_sql,
        }

    # test that the experimental parser doesn't run if the source built-in is overriden with a macro
    def test_experimental_parser_source_override(
        self,
        project,
    ):
        _, log_output = run_dbt_and_capture(["--debug", "--use-experimental-parser", "parse"])

        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # fallback to jinja rendering
        assert "1602: " in log_output
        # experimental parser failed
        assert not ("1604: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        assert "1601: " in log_output


class TestConfigOverrideExperimentalParser:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": config_macro__models__model_a_sql,
            "schema.yml": config_macro__schema_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "config.sql": config_macro__macros__config_sql,
        }

    # test that the experimental parser doesn't run if the config built-in is overriden with a macro
    def test_experimental_parser_config_override(
        self,
        project,
    ):
        _, log_output = run_dbt_and_capture(["--debug", "--use-experimental-parser", "parse"])

        # successful experimental static parsing
        assert not ("1698: " in log_output)
        # fallback to jinja rendering
        assert "1602: " in log_output
        # experimental parser failed
        assert not ("1604: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        assert "1601: " in log_output
