import pytest

from dbt.tests.util import run_dbt

model_sql = """
{{
    config(
        materialized='table'
    )
}}
select 1 as id
"""

another_schema_model_sql = """
{{
    config(
        materialized='table',
        schema='another_schema'
    )
}}
select 1 as id
"""


class BaseCachingTest:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            'config-version': 2,
            'quoting': {
                'identifier': False,
                'schema': False,
            }
        }

    def run_and_inspect_cache(self, project, run_args=None):
        if not run_args:
            run_args = ['run']
        run_dbt(run_args)
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        relation = next(iter(adapter.cache.relations.values()))
        assert relation.inner.schema == project.test_schema
        assert relation.schema == project.test_schema.lower()

        run_dbt(run_args)
        adapter = project.adapter
        assert len(adapter.cache.relations) == 1
        second_relation = next(iter(adapter.cache.relations.values()))
        assert relation.inner == second_relation.inner

    def test_cache(self, project):
        self.run_and_inspect_cache(project)


class BaseCachingLowercaseModel(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
        }


class BaseCachingUppercaseModel(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "MODEL.sql": model_sql,
        }


class BaseCachingSelectedSchemaOnly(BaseCachingTest):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
            "another_schema_model.sql": another_schema_model_sql,
        }

    def test_cache(self, project):
        # this should only cache the schema containing the selected model
        run_args = ['--cache-selected-only', 'run', '--select', 'model']
        self.run_and_inspect_cache(project, run_args)


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingUppercaseModel(BaseCachingUppercaseModel):
    pass


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    pass
