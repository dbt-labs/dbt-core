from test.integration.base import DBTIntegrationTest,  use_profile
import os


class BaseTestCustomMaterialization(DBTIntegrationTest):
    @property
    def schema(self):
        return 'dbt_custom_materializations_053'

    @staticmethod
    def dir(value):
        return os.path.normpath(value)

    @property
    def models(self):
        return "models"


class TestOverrideAdapterDependency(BaseTestCustomMaterialization):
    # make sure that if there's a dependency with an adapter-specific
    # materialization, we honor that materialization
    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'local': 'override-view-adapter-dep'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_adapter_dependency(self):
        self.run_dbt(['deps'])
        # this should error because the override is buggy
        self.run_dbt(['run'], expect_pass=False)


class TestOverrideDefaultDependency(BaseTestCustomMaterialization):
    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'local': 'override-view-default-dep'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_default_dependency(self):
        self.run_dbt(['deps'])
        # this should error because the override is buggy
        self.run_dbt(['run'], expect_pass=False)


class TestOverrideAdapterDependencyPassing(BaseTestCustomMaterialization):
    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'local': 'override-view-adapter-pass-dep'
                }
            ]
        }

    @use_profile('postgres')
    def test_postgres_default_dependency(self):
        self.run_dbt(['deps'])
        # this should pass because the override is ok
        self.run_dbt(['run'])


class TestOverrideAdapterLocal(BaseTestCustomMaterialization):
    # make sure that the local default wins over the dependency
    # adapter-specific

    @property
    def packages_config(self):
        return {
            'packages': [
                {
                    'local': 'override-view-adapter-pass-dep'
                }
            ]
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['override-view-adapter-macros']
        }

    @use_profile('postgres')
    def test_postgres_default_dependency(self):
        self.run_dbt(['deps'])
        # this should error because the override is buggy
        self.run_dbt(['run'], expect_pass=False)


class TestOverrideDefaultReturn(BaseTestCustomMaterialization):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['override-view-return-no-relation']
        }

    @use_profile('postgres')
    def test_postgres_default_dependency(self):
        self.run_dbt(['deps'])
        results = self.run_dbt(['run'], expect_pass=False)
        assert 'did not explicitly return a list of relations' in results[0].message
