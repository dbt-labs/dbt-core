import pytest

from dbt.tests.util import run_dbt

sources_yml = """
sources:
- name: TEST
  schema: STAGE
  tables:
  - name: TABLE
    external:
      partitions:
      - name: dl_partition
        data_type: string
        expression: split_part(METADATA$FILENAME, '/', 2)
"""

get_partitions_sql = """
{% macro get_partitions() -%}
    {% set source_nodes = graph.sources.values() if graph.sources else [] %}
    {% for node in source_nodes %}
        {% if node.external %}
            {% if node.external.partitions %}
                {{print(node.external.partitions)}}
            {% endif %}
        {% endif %}
    {% endfor %}
{%- endmacro %}
"""


class TestGraphSerialization:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "sources.yml": sources_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"get_partitions.sql": get_partitions_sql}

    def test_graph_serialization(self, project):
        manifest = run_dbt(["parse"])
        assert manifest
        assert len(manifest.sources) == 1

        run_dbt(["run-operation", "get_partitions"])
