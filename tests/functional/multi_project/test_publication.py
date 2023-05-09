import pytest
import pathlib
import os

from dbt.tests.util import run_dbt, get_artifact, write_file, copy_file
from dbt.contracts.publication import PublicationArtifact, PublicModel
from dbt.exceptions import (
    PublicationConfigNotFound,
    TargetNotFoundError,
    ProjectDependencyCycleError,
)


model_one_sql = """
select 1 as fun
"""

model_two_sql = """
select fun from {{ ref('model_one') }}
"""

model_three_sql = """
select fun from {{ ref('model_two') }}
"""

models_yml = """
models:
  - name: model_one
    description: model one
    access: public
  - name: model_two
    description: non-public model
  - name: model_three
    description: model three
    access: public
"""


dependencies_yml = """
projects:
    - name: marketing
"""

marketing_pub_json = """
{
  "project_name": "marketing",
  "metadata": {
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/publication/v1.json",
    "dbt_version": "1.5.0",
    "generated_at": "2023-04-13T17:17:58.128706Z",
    "invocation_id": "56e3126f-78c7-470c-8eb0-c94af7c3eaac",
    "env": {},
    "adapter_type": "postgres",
    "quoting": {
      "database": true,
      "schema": true,
      "identifier": true
    }
  },
  "public_models": {
    "model.marketing.fct_one": {
      "name": "fct_one",
      "package_name": "marketing",
      "unique_id": "model.marketing.fct_one",
      "relation_name": '"dbt"."test_schema"."fct_one"',
      "database": "dbt",
      "schema": "test_schema",
      "identifier": "fct_one",
      "version": null,
      "latest_version": null,
      "public_node_dependencies": [],
      "generated_at": "2023-04-13T17:17:58.128706Z",
    },
    "model.marketing.fct_two": {
      "name": "fct_two",
      "package_name": "marketing",
      "unique_id": "model.marketing.fct_two",
      "relation_name": '"dbt"."test_schema"."fct_two"',
      "version": null,
      "latest_version": null,
      "public_node_dependencies": ["model.test.fct_one"],
      "generated_at": "2023-04-13T17:17:58.128706Z",
    }
  },
  "dependencies": []
}
"""

ext_node_model_sql = """
select * from {{ ref('marketing', 'fct_one') }}
"""


class TestPublicationArtifact:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_two_sql,
            "model_three.sql": model_three_sql,
            "models.yml": models_yml,
        }

    def test_publication_artifact(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        publication_dict = get_artifact(project.project_root, "target", "test_publication.json")
        publication = PublicationArtifact.from_dict(publication_dict)
        assert publication
        assert len(publication.public_models) == 2
        assert publication.public_models["model.test.model_three"].public_node_dependencies == [
            "model.test.model_one"
        ]


class TestPublicationArtifacts:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_two_sql,
            "model_three.sql": model_three_sql,
            "models.yml": models_yml,
        }

    def test_pub_artifacts(self, project):
        write_file(dependencies_yml, "dependencies.yml")

        # Dependencies lists "marketing" project, but no publication file found
        with pytest.raises(PublicationConfigNotFound):
            run_dbt(["parse"])

        # Write out publication file and try again
        m_pub_json = marketing_pub_json.replace("test_schema", project.test_schema)
        (pathlib.Path(project.project_root) / "publications").mkdir(parents=True, exist_ok=True)
        write_file(m_pub_json, project.project_root, "publications", "marketing_publication.json")

        manifest = run_dbt(["parse"])
        assert manifest.publications
        assert "marketing" in manifest.publications
        assert "model.marketing.fct_one" in manifest.publications["marketing"].public_node_ids

        # Check dependencies in publication_artifact
        publication_dict = get_artifact(project.project_root, "target", "test_publication.json")
        publication = PublicationArtifact.from_dict(publication_dict)
        assert publication.dependencies == ["marketing"]

        # source_node, target_model_name, target_model_package, target_model_version, current_project, node_package
        resolved_node = manifest.resolve_ref(None, "fct_one", "marketing", None, "test", "test")
        assert resolved_node
        assert isinstance(resolved_node, PublicModel)
        assert resolved_node.unique_id == "model.marketing.fct_one"

        # add new model that references external_node and parse
        write_file(ext_node_model_sql, project.project_root, "models", "test_model_one.sql")
        manifest = run_dbt(["parse"])

        model_id = "model.test.test_model_one"
        public_model_id = "model.marketing.fct_one"
        model = manifest.nodes[model_id]
        assert model.depends_on.public_nodes == [public_model_id]
        assert public_model_id in manifest.parent_map
        assert manifest.parent_map[model_id] == [public_model_id]
        # check that publication configs contain correct list of public model unique_ids
        assert manifest.publications["marketing"].public_node_ids == [
            "model.marketing.fct_one",
            "model.marketing.fct_two",
        ]
        assert len(manifest.public_nodes) == 2

        # Create the relation for the public node (fct_one)
        project.run_sql(f'create table "{project.test_schema}"."fct_one" (id integer)')
        project.run_sql(f'insert into "{project.test_schema}"."fct_one" values (1), (2)')
        results = run_dbt(["run"])
        assert len(results) == 4

        # Test for only publication artifact has changed, no partial parsing
        # Change public node name from fct_one to fct_three
        m_pub_json = m_pub_json.replace("fct_one", "fct_three")
        # Change generated_at field
        m_pub_json = m_pub_json.replace("04-13", "04-24")
        write_file(m_pub_json, project.project_root, "publications", "marketing_publication.json")
        # test_model_one references a missing public model
        with pytest.raises(TargetNotFoundError):
            manifest = run_dbt(["parse"])

        # Add another public reference
        m_pub_json = m_pub_json.replace("fct_three", "fct_one")
        m_pub_json = m_pub_json.replace("04-13", "04-25")
        write_file(m_pub_json, project.project_root, "publications", "marketing_publication.json")
        write_file(ext_node_model_sql, project.project_root, "models", "test_model_two.sql")
        results = run_dbt(["run"])
        assert len(results) == 5


dependencies_alt_yml = """
projects:
    - name: test_alt
"""

model_alt_yml = """
models:
  - name: model_alt
    description: model alt
    access: public
"""

model_alt_ref_sql = """
select * from {{ ref('test_alt', 'model_alt') }}
"""


# This test case uses the conftest.py in this test directory to allow
# creating a minimal second project (project_alt) so that we can have two projects in
# the same test.
class TestMultiProjects:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_two_sql,
            "model_three.sql": model_three_sql,
            "model_alt_ref.sql": model_alt_ref_sql,
            "models.yml": models_yml,
        }

    @pytest.fixture(scope="class")
    def models_alt(self):
        return {
            "model_alt.sql": "select 1 as fun",
            "model_alt.yml": model_alt_yml,
        }

    def test_multi_projects(self, project, project_alt):
        # run the alternate project by using the alternate project root
        # (There is currently a bug where project-dir requires a chdir to work.)
        os.chdir(project_alt.project_root)
        results = run_dbt(["run", "--project-dir", str(project_alt.project_root)])
        assert len(results) == 1

        # Check publication artifact
        publication_dict = get_artifact(
            project_alt.project_root, "target", "test_alt_publication.json"
        )
        publication = PublicationArtifact.from_dict(publication_dict)
        assert len(publication.public_models) == 1

        # copy the publication artifact from test_alt to test project
        (pathlib.Path(project.project_root) / "publications").mkdir(parents=True, exist_ok=True)
        target_path = os.path.join(project_alt.project_root, "target")
        copy_file(
            target_path,
            "test_alt_publication.json",
            project.project_root,
            ["publications", "test_alt_publication.json"],
        )

        # run the base project
        os.chdir(project.project_root)
        write_file(dependencies_alt_yml, project.project_root, "dependencies.yml")
        results = run_dbt(["run", "--project-dir", str(project.project_root)])
        assert len(results) == 4


class TestProjectCycles:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
        }

    def test_project_cycles(self, project):
        write_file(dependencies_yml, "dependencies.yml")
        # Create a project dependency that's the same as the current project
        m_pub_json = marketing_pub_json.replace('"dependencies": []', '"dependencies": ["test"]')
        (pathlib.Path(project.project_root) / "publications").mkdir(parents=True, exist_ok=True)
        write_file(m_pub_json, project.project_root, "publications", "marketing_publication.json")

        with pytest.raises(ProjectDependencyCycleError):
            run_dbt(["parse"])
