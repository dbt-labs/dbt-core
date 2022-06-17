import pytest
from dbt.tests.util import run_dbt, write_file, get_manifest, rm_file

model_one_sql = """
select 1 as fun
"""

raw_customers_csv = """id,first_name,last_name,email
1,Michael,Perez,mperez0@chronoengine.com
2,Shawn,Mccoy,smccoy1@reddit.com
3,Kathleen,Payne,kpayne2@cargocollective.com
4,Jimmy,Cooper,jcooper3@cargocollective.com
5,Katherine,Rice,krice4@typepad.com
6,Sarah,Ryan,sryan5@gnu.org
7,Martin,Mcdonald,mmcdonald6@opera.com
8,Frank,Robinson,frobinson7@wunderground.com
9,Jennifer,Franklin,jfranklin8@mail.ru
10,Henry,Welch,hwelch9@list-manage.com
"""

my_macro_sql = """
{% macro my_macro(something) %}

    select
        '{{ something }}' as something2

{% endmacro %}

"""

customers1_md = """
{% docs customer_table %}

This table contains customer data

{% enddocs %}
"""

customers2_md = """
{% docs customer_table %}

LOTS of customer data

{% enddocs %}

"""

schema1_yml = """
version: 2

models:
    - name: model_one
      description: "{{ doc('customer_table') }}"
"""

schema2_yml = """
version: 2

models:
    - name: model_one
      description: "{{ doc('customer_table') }}"

macros:
    - name: my_macro
      description: "{{ doc('customer_table') }}"

sources:
  - name: seed_sources
    description: "{{ doc('customer_table') }}"
    schema: "{{ target.schema }}"
    tables:
      - name: raw_customers
        columns:
          - name: id
            tests:
              - not_null:
                  severity: "{{ 'error' if target.name == 'prod' else 'warn' }}"
              - unique
          - name: first_name
          - name: last_name
          - name: email

exposures:
  - name: proxy_for_dashboard
    description: "{{ doc('customer_table') }}"
    type: "dashboard"
    owner:
      name: "Dashboard Tester"
      email: "tester@dashboard.com"
    depends_on:
      - ref("model_one")
      - ref("raw_customers")
      - source("seed_sources", "raw_customers")
"""


class TestDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "raw_customers.csv": raw_customers_csv,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "my_macro.sql": my_macro_sql,
        }

    def test_pp_docs(self, project):
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1

        # Add docs file customers.md
        write_file(customers1_md, project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.docs) == 2

        # Add schema file with 'docs' description
        write_file(schema1_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.docs) == 2
        doc_id = "test.customer_table"
        assert doc_id in manifest.docs
        doc = manifest.docs[doc_id]
        doc_file_id = doc.file_id
        assert doc_file_id in manifest.files
        source_file = manifest.files[doc_file_id]
        assert len(source_file.nodes) == 1
        model_one_id = "model.test.model_one"
        assert model_one_id in source_file.nodes
        model_node = manifest.nodes[model_one_id]
        assert model_node.description == "This table contains customer data"

        # Update the doc file
        write_file(customers2_md, project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        assert len(manifest.docs) == 2
        assert model_one_id in manifest.nodes
        model_node = manifest.nodes[model_one_id]
        assert "LOTS" in model_node.description

        # Add a macro patch, source and exposure with doc
        write_file(schema2_yml, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        doc_file = manifest.files[doc_file_id]
        expected_nodes = [
            "model.test.model_one",
            "source.test.seed_sources.raw_customers",
            "macro.test.my_macro",
            "exposure.test.proxy_for_dashboard",
        ]
        assert expected_nodes == doc_file.nodes
        source_id = "source.test.seed_sources.raw_customers"
        assert manifest.sources[source_id].source_description == "LOTS of customer data"
        macro_id = "macro.test.my_macro"
        assert manifest.macros[macro_id].description == "LOTS of customer data"
        exposure_id = "exposure.test.proxy_for_dashboard"
        assert manifest.exposures[exposure_id].description == "LOTS of customer data"

        # update the doc file again
        write_file(customers1_md, project.project_root, "models", "customers.md")
        results = run_dbt(["--partial-parse", "run"])
        manifest = get_manifest(project.project_root)
        source_file = manifest.files[doc_file_id]
        assert model_one_id in source_file.nodes
        model_node = manifest.nodes[model_one_id]
        assert model_node.description == "This table contains customer data"
        assert (
            manifest.sources[source_id].source_description == "This table contains customer data"
        )
        assert manifest.macros[macro_id].description == "This table contains customer data"
        assert manifest.exposures[exposure_id].description == "This table contains customer data"

        # check that _lock is working
        with manifest._lock:
            assert manifest._lock


my_model_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
        description: "{{ doc('whatever') }}"
"""

my_model_no_description_yml = """
version: 2
models:
  - name: my_model
    columns:
      - name: id
"""

my_model_md = """
{% docs whatever %}
  cool stuff
{% enddocs %}
"""


class TestDocsRemoveReplace:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": "select 1 as id",
            "my_model.yml": my_model_yml,
            "my_model.md": my_model_md,
        }

    def test_remove_replace(self, project):
        run_dbt(["parse", "--write-manifest"])
        manifest = get_manifest(project.project_root)
        doc_id = "test.whatever"
        assert doc_id in manifest.docs
        doc = manifest.docs[doc_id]
        doc_file = manifest.files[doc.file_id]

        model_id = "model.test.my_model"
        assert model_id in manifest.nodes

        assert doc_file.nodes == [model_id]

        model = manifest.nodes[model_id]
        model_file_id = model.file_id
        assert model_file_id in manifest.files

        # remove the doc file
        rm_file(project.project_root, "models", "my_model.md")
        # remove description from schema file
        write_file(my_model_no_description_yml, project.project_root, "models", "my_model.yml")
        run_dbt(["parse", "--write-manifest"])
        manifest = get_manifest(project.project_root)
        assert doc_id not in manifest.docs
        # The bug was that the file still existed in manifest.files
        assert doc.file_id not in manifest.files

        # put back the doc file
        write_file(my_model_md, project.project_root, "models", "my_model.md")
        # put back the description in the schema file
        write_file(my_model_yml, project.project_root, "models", "my_model.yml")
        run_dbt(["parse"])
