from tests.integration.base import DBTIntegrationTest, use_profile


class TestDispatchMacroUseParent(DBTIntegrationTest):
    @property
    def schema(self):
        return "test_macros"

    @property
    def models(self):
        return "dispatch-inheritance-models"

    @use_profile('redshift')
    def test_redshift_inherited_macro(self):
        self.run_dbt(['run'])
