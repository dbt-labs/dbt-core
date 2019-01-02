from test.integration.base import DBTIntegrationTest, use_profile

class TestExternalReference(DBTIntegrationTest):
    @property
    def schema(self):
        return "external_reference_037"

    @property
    def models(self):
        return "test/integration/037_external_reference_test/models"

    def setUp(self):
        super(TestExternalReference, self).setUp()
        self.use_default_project()
        self.external_schema = self.unique_schema()+'z'
        self.run_sql(
            'create schema "{}"'.format(self.external_schema)
        )
        self.run_sql(
            'create table "{}"."external" (id integer)'
            .format(self.external_schema)
        )
        self.run_sql(
            'insert into "{}"."external" values (1), (2)'
            .format(self.external_schema)
        )

    def tearDown(self):
        # This has to happen before we drop the external schema, because
        # otherwise postgres hangs forever.
        self._drop_schemas()
        self.adapter.drop_schema(self.default_database, self.external_schema,
                                 model_name='__test')
        super(TestExternalReference, self).tearDown()

    @use_profile('postgres')
    def test__postgres__external_reference(self):
        self.assertEquals(len(self.run_dbt()), 1)
        # running it again should succeed
        self.assertEquals(len(self.run_dbt()), 1)

# The opposite of the test above -- check that external relations that
# depend on a dbt model do not create issues with caching
class TestExternalDependency(DBTIntegrationTest):
    @property
    def schema(self):
        return "external_dependency_037"

    @property
    def models(self):
        return "test/integration/037_external_reference_test/standalone_models"

    def tearDown(self):
        # This has to happen before we drop the external schema, because
        # otherwise postgres hangs forever.
        self._drop_schemas()
        self.adapter.drop_schema(self.default_database, self.external_schema,
                                 model_name='__test')
        super(TestExternalDependency, self).tearDown()

    @use_profile('postgres')
    def test__postgres__external_reference(self):
        self.assertEquals(len(self.run_dbt()), 1)

        # create a view outside of the dbt schema that depends on this model
        self.external_schema = self.unique_schema()+'zz'
        self.run_sql(
            'create schema "{}"'.format(self.external_schema)
        )
        self.run_sql(
            'create view "{}"."external" as (select * from {}.my_model)'
            .format(self.external_schema, self.unique_schema())
        )

        # running it again should succeed
        self.assertEquals(len(self.run_dbt()), 1)

