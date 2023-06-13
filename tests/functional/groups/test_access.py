import pytest
from dbt.tests.util import run_dbt, get_manifest, write_file, rm_file
from dbt.node_types import AccessType
from dbt.exceptions import InvalidAccessTypeError, DbtReferenceError

my_model_sql = "select 1 as fun"

another_model_sql = "select 1234 as notfun"

yet_another_model_sql = "select 999 as weird"

schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: public
  - name: another_model
    description: "yet another model"
"""

ephemeral_schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: public
    config:
      materialized: ephemeral
  - name: another_model
    description: "yet another model"
"""

v2_schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: public
  - name: another_model
    description: "another model"
  - name: yet_another_model
    description: "yet another model"
    access: unsupported
"""

ref_my_model_sql = """
   select fun from {{ ref('my_model') }}
"""

groups_yml = """
groups:
  - name: analytics
    owner:
      name: analytics_owner
  - name: marts
    owner:
      name: marts_owner
"""


v3_schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: private
    group: analytics
  - name: another_model
    description: "yet another model"
  - name: ref_my_model
    description: "a model that refs my_model"
    group: analytics
"""

v4_schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: private
    group: analytics
  - name: another_model
    description: "yet another model"
  - name: ref_my_model
    description: "a model that refs my_model"
    group: marts
"""

simple_exposure_yml = """
exposures:
  - name: simple_exposure
    label: simple exposure label
    type: dashboard
    depends_on:
      - ref('my_model')
    owner:
      email: something@example.com
"""

v5_schema_yml = """
models:
  - name: my_model
    description: "my model"
    access: private
    group: analytics
  - name: another_model
    description: "yet another model"
  - name: ref_my_model
    description: "a model that refs my_model"
    group: marts
  - name: ref_my_model
    description: "a model that refs my_model"
    group: analytics
  - name: people_model
    description: "some people"
    access: private
    group: analytics
"""

people_model_sql = """
select 1 as id, 'Drew' as first_name, 'Banin' as last_name, 'yellow' as favorite_color, true as loves_dbt, 5 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Jeremy' as first_name, 'Cohen' as last_name, 'indigo' as favorite_color, true as loves_dbt, 4 as tenure, current_timestamp as created_at
union all
select 1 as id, 'Callum' as first_name, 'McCann' as last_name, 'emerald' as favorite_color, true as loves_dbt, 0 as tenure, current_timestamp as created_at
"""

people_metric_yml = """
metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: "people"
    meta:
        my_meta: 'testing'
    config:
      group: analytics
"""

v2_people_metric_yml = """
metrics:

  - name: number_of_people
    label: "Number of people"
    description: Total count of people
    type: simple
    type_params:
      measure: "people"
    meta:
        my_meta: 'testing'
    config:
      group: marts
"""


class TestAccess:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "another_model.sql": yet_another_model_sql,
            "schema.yml": schema_yml,
        }

    def test_access_attribute(self, project):
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 2

        my_model_id = "model.test.my_model"
        another_model_id = "model.test.another_model"
        assert my_model_id in manifest.nodes
        assert another_model_id in manifest.nodes

        assert manifest.nodes[my_model_id].access == AccessType.Public
        assert manifest.nodes[another_model_id].access == AccessType.Protected

        # write a file with invalid materialization for public access value
        write_file(ephemeral_schema_yml, project.project_root, "models", "schema.yml")
        with pytest.raises(InvalidAccessTypeError):
            run_dbt(["parse"])

        # write a file with an invalid access value
        write_file(yet_another_model_sql, project.project_root, "models", "yet_another_model.sql")
        write_file(v2_schema_yml, project.project_root, "models", "schema.yml")

        with pytest.raises(InvalidAccessTypeError):
            run_dbt(["parse"])

        write_file(v2_schema_yml, project.project_root, "models", "schema.yml")
        with pytest.raises(InvalidAccessTypeError):
            run_dbt(["parse"])

        # Remove invalid access files and write out model that refs my_model
        rm_file(project.project_root, "models", "yet_another_model.sql")
        write_file(schema_yml, project.project_root, "models", "schema.yml")
        write_file(ref_my_model_sql, project.project_root, "models", "ref_my_model.sql")
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 3

        # make my_model private, set same group on my_model and ref_my_model
        write_file(groups_yml, project.project_root, "models", "groups.yml")
        write_file(v3_schema_yml, project.project_root, "models", "schema.yml")
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 3
        manifest = get_manifest(project.project_root)
        ref_my_model_id = "model.test.ref_my_model"
        assert manifest.nodes[my_model_id].group == "analytics"
        assert manifest.nodes[ref_my_model_id].group == "analytics"

        # Change group on ref_my_model and it should raise
        write_file(v4_schema_yml, project.project_root, "models", "schema.yml")
        with pytest.raises(DbtReferenceError):
            run_dbt(["parse"])

        # put back group on ref_my_model, add exposure with ref to private model
        write_file(v3_schema_yml, project.project_root, "models", "schema.yml")
        # verify it works again
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 3
        # Write out exposure refing private my_model
        write_file(simple_exposure_yml, project.project_root, "models", "simple_exposure.yml")
        # Fails with reference error
        with pytest.raises(DbtReferenceError):
            run_dbt(["parse"])

        # Remove exposure and add people model and metric file
        write_file(v5_schema_yml, project.project_root, "models", "schema.yml")
        rm_file(project.project_root, "models", "simple_exposure.yml")
        write_file(people_model_sql, "models", "people_model.sql")
        write_file(people_metric_yml, "models", "people_metric.yml")
        # Should succeed
        manifest = run_dbt(["parse"])
        assert len(manifest.nodes) == 4
        manifest = get_manifest(project.project_root)
        metric_id = "metric.test.number_of_people"
        assert manifest.metrics[metric_id].group == "analytics"
