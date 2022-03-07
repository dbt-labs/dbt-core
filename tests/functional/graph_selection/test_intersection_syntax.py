import pytest

from dbt.tests.util import run_dbt
from tests.functional.graph_selection.fixtures import SelectionFixtures


selectors_yml = """
            selectors:
            - name: same_intersection
              definition:
                intersection:
                  - fqn: users
                  - fqn: users
            - name: tags_intersection
              definition:
                intersection:
                  - tag: bi
                  - tag: users
            - name: triple_descending
              definition:
                intersection:
                  - fqn: "*"
                  - tag: bi
                  - tag: users
            - name: triple_ascending
              definition:
                intersection:
                  - tag: users
                  - tag: bi
                  - fqn: "*"
            - name: intersection_with_exclusion
              definition:
                intersection:
                  - method: fqn
                    value: users_rollup_dependency
                    parents: true
                  - method: fqn
                    value: users
                    children: true
                  - exclude:
                    - users_rollup_dependency
            - name: intersection_exclude_intersection
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - tag:bi
                        - method: fqn
                          value: users_rollup
                          children: true
            - name: intersection_exclude_intersection_lack
              definition:
                intersection:
                  - tag:bi
                  - "@users"
                  - exclude:
                      - intersection:
                        - method: fqn
                          value: emails
                          children_parents: true
                        - method: fqn
                          value: emails_alt
                          children_parents: true
        """


def verify_selected_users(project, results):
    # users
    assert len(results) == 1

    created_models = project.get_tables_in_schema()
    assert "users" in created_models
    assert "users_rollup" not in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


def verify_selected_users_and_rollup(project, results):
    # users, users_rollup
    assert len(results) == 2

    created_models = project.get_tables_in_schema()
    assert "users" in created_models
    assert "users_rollup" in created_models
    assert "emails_alt" not in created_models
    assert "subdir" not in created_models
    assert "nested_users" not in created_models


@pytest.fixture
def run_seed(project):
    run_dbt(["seed"])


# The project and run_seed fixtures will be executed for each test method
@pytest.mark.usefixtures("project", "run_seed")
class TestIntersectionSyncs(SelectionFixtures):
    @pytest.fixture(scope="class")
    def selectors(self):
        return selectors_yml

    def test_same_model_intersection(self, project):
        run_dbt(["seed"])

        results = run_dbt(["run", "--models", "users,users"])
        verify_selected_users(project, results)

    def test_same_model_intersection_selectors(self, project):

        results = run_dbt(["run", "--selector", "same_intersection"])
        verify_selected_users(project, results)

    def test_tags_intersection(self, project):

        results = run_dbt(["run", "--models", "tag:bi,tag:users"])
        verify_selected_users(project, results)

    def test_tags_intersection_selectors(self, project):

        results = run_dbt(["run", "--selector", "tags_intersection"])
        verify_selected_users(project, results)

    def test_intersection_triple_descending(self, project):

        results = run_dbt(["run", "--models", "*,tag:bi,tag:users"])
        verify_selected_users(project, results)

    def test_intersection_triple_descending_schema(self, project):

        results = run_dbt(["run", "--models", "*,tag:bi,tag:users"])
        verify_selected_users(project, results)

    def test_intersection_triple_descending_schema_selectors(self, project):

        results = run_dbt(["run", "--selector", "triple_descending"])
        verify_selected_users(project, results)

    def test_intersection_triple_ascending(self, project):

        results = run_dbt(["run", "--models", "tag:users,tag:bi,*"])
        verify_selected_users(project, results)

    def test_intersection_triple_ascending_schema_selectors(self, project):

        results = run_dbt(["run", "--selector", "triple_ascending"])
        verify_selected_users(project, results)

    def test_intersection_with_exclusion(self, project):

        results = run_dbt(
            [
                "run",
                "--models",
                "+users_rollup_dependency,users+",
                "--exclude",
                "users_rollup_dependency",
            ]
        )
        verify_selected_users_and_rollup(project, results)

    def test_intersection_with_exclusion_selectors(self, project):

        results = run_dbt(["run", "--selector", "intersection_with_exclusion"])
        verify_selected_users_and_rollup(project, results)

    def test_intersection_exclude_intersection(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "--exclude", "tag:bi,users_rollup+"]
        )
        verify_selected_users(project, results)

    def test_intersection_exclude_intersection_selectors(self, project):

        results = run_dbt(["run", "--selector", "intersection_exclude_intersection"])
        verify_selected_users(project, results)

    def test_intersection_exclude_intersection_lack(self, project):

        results = run_dbt(["run", "--models", "tag:bi,@users", "--exclude", "@emails,@emails_alt"])
        verify_selected_users_and_rollup(project, results)

    def test_intersection_exclude_intersection_lack_selector(self, project):

        results = run_dbt(["run", "--selector", "intersection_exclude_intersection_lack"])
        verify_selected_users_and_rollup(project, results)

    def test_intersection_exclude_triple_intersection(self, project):

        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "--exclude", "*,tag:bi,users_rollup"]
        )
        verify_selected_users(project, results)

    def test_intersection_concat(self, project):

        results = run_dbt(["run", "--models", "tag:bi,@users", "emails_alt"])
        # users, users_rollup, emails_alt
        assert len(results) == 3

        created_models = project.get_tables_in_schema()
        assert "users" in created_models
        assert "users_rollup" in created_models
        assert "emails_alt" in created_models
        assert "subdir" not in created_models
        assert "nested_users" not in created_models

    def test_intersection_concat_intersection(self, project):

        run_dbt(["seed"])
        results = run_dbt(["run", "--models", "tag:bi,@users", "@emails_alt,emails_alt"])
        # users, users_rollup, emails_alt
        assert len(results) == 3

        created_models = project.get_tables_in_schema()
        assert "users" in created_models
        assert "users_rollup" in created_models
        assert "emails_alt" in created_models
        assert "subdir" not in created_models
        assert "nested_users" not in created_models

    def test_intersection_concat_exclude(self, project):

        run_dbt(["seed"])
        results = run_dbt(
            ["run", "--models", "tag:bi,@users", "emails_alt", "--exclude", "users_rollup"]
        )
        # users, emails_alt
        assert len(results) == 2

        created_models = project.get_tables_in_schema()
        assert "users" in created_models
        assert "emails_alt" in created_models
        assert "users_rollup" not in created_models
        assert "subdir" not in created_models
        assert "nested_users" not in created_models

    def test_intersection_concat_exclude_concat(self, project):

        run_dbt(["seed"])
        results = run_dbt(
            [
                "run",
                "--models",
                "tag:bi,@users",
                "emails_alt,@users",
                "--exclude",
                "users_rollup_dependency",
                "users_rollup",
            ]
        )
        # users, emails_alt
        assert len(results) == 2

        created_models = project.get_tables_in_schema()
        assert "users" in created_models
        assert "emails_alt" in created_models
        assert "users_rollup" not in created_models
        assert "subdir" not in created_models
        assert "nested_users" not in created_models

    def test_intersection_concat_exclude_intersection_concat(self, project):

        run_dbt(["seed"])
        results = run_dbt(
            [
                "run",
                "--models",
                "tag:bi,@users",
                "emails_alt,@users",
                "--exclude",
                "@users,users_rollup_dependency",
                "@users,users_rollup",
            ]
        )
        # users, emails_alt
        assert len(results) == 2

        created_models = project.get_tables_in_schema()
        assert "users" in created_models
        assert "emails_alt" in created_models
        assert "users_rollup" not in created_models
        assert "subdir" not in created_models
        assert "nested_users" not in created_models
