import os
from datetime import datetime, timedelta
from test.integration.base import DBTIntegrationTest, use_profile
from dbt.exceptions import CompilationException


class TestSourceOverrides(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self._id = 101

    @property
    def schema(self):
        return "source_overrides_059"

    @property
    def models(self):
        return 'models'

    @property
    def packages_config(self):
        return {
            'packages': [
                {'local': 'local_dependency'},
            ],
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'localdep': {
                    'enabled': False,
                    'keep': {
                        'enabled': True,
                    }
                },
                'quote_columns': False,
            },
            'sources': {
                'localdep': {
                    'my_other_source': {
                        'enabled': False,
                    }
                }
            }
        }

    def _set_updated_at_to(self, delta):
        insert_time = datetime.utcnow() + delta
        timestr = insert_time.strftime("%Y-%m-%d %H:%M:%S")
        # favorite_color,id,first_name,email,ip_address,updated_at
        insert_id = self._id
        self._id += 1
        raw_sql = """INSERT INTO {schema}.{source}
            ({quoted_columns})
        VALUES (
            'blue',{id},'Jake','abc@example.com','192.168.1.1','{time}'
        )"""
        quoted_columns = ','.join(
            self.adapter.quote(c) for c in
            ('favorite_color', 'id', 'first_name', 'email', 'ip_address', 'updated_at')
        )
        self.run_sql(
            raw_sql,
            kwargs={
                'schema': self.unique_schema(),
                'time': timestr,
                'id': insert_id,
                'source': self.adapter.quote('snapshot_freshness_base'),
                'quoted_columns': quoted_columns,
            }
        )

    @use_profile('postgres')
    def test_postgres_source_overrides(self):
        self.run_dbt(['deps'])
        seed_results = self.run_dbt(['seed'])
        assert len(seed_results) == 5

        # There should be 7, as we disabled 1 test of the original 8
        test_results = self.run_dbt(['test'])
        assert len(test_results) == 7

        results = self.run_dbt(['run'])
        assert len(results) == 1

        self.assertTablesEqual('expected_result', 'my_model')

        # set the updated_at field of this seed to last week
        self._set_updated_at_to(timedelta(days=-7))
        # if snapshot-freshness fails, freshness just didn't happen!
        results = self.run_dbt(
            ['source', 'snapshot-freshness'], expect_pass=False
        )
        # we disabled my_other_source, so we only run the one freshness check
        # in
        self.assertEqual(len(results), 1)
        # If snapshot-freshness passes, that means error_after was
        # applied from the source override but not the source table override
        self._set_updated_at_to(timedelta(days=-2))
        results = self.run_dbt(
            ['source', 'snapshot-freshness'], expect_pass=False,
        )
        self.assertEqual(len(results), 1)

        self._set_updated_at_to(timedelta(hours=-12))
        results = self.run_dbt(
            ['source', 'snapshot-freshness'], expect_pass=True
        )
        self.assertEqual(len(results), 1)

        self.use_default_project({
            'sources': {
                'localdep': {
                    'my_other_source': {
                        'enabled': True,
                    }
                }
            }
        })
        # enable my_other_source, snapshot freshness should fail due to the new
        # not-fresh source
        results = self.run_dbt(
            ['source', 'snapshot-freshness'], expect_pass=False
        )
        self.assertEqual(len(results), 2)


class TestSourceDuplicateOverrides(DBTIntegrationTest):
    def setUp(self):
        super().setUp()
        self._id = 101

    @property
    def schema(self):
        return "source_overrides_059"

    @property
    def models(self):
        return 'dupe-models'

    @property
    def packages_config(self):
        return {
            'packages': [
                {'local': 'local_dependency'},
            ],
        }

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seeds': {
                'localdep': {
                    'enabled': False,
                    'keep': {
                        'enabled': True,
                    }
                },
                'quote_columns': False,
            },
            'sources': {
                'localdep': {
                    'my_other_source': {
                        'enabled': False,
                    }
                }
            }
        }

    @use_profile('postgres')
    def test_postgres_source_duplicate_overrides(self):
        self.run_dbt(['deps'])
        with self.assertRaises(CompilationException) as exc:
            self.run_dbt(['compile'])

        self.assertIn('dbt found two schema.yml entries for the same source named', str(exc.exception))
        self.assertIn('one of these files', str(exc.exception))
        schema1_path = os.path.join('dupe-models', 'schema1.yml')
        schema2_path = os.path.join('dupe-models', 'schema2.yml')
        self.assertIn(schema1_path, str(exc.exception))
        self.assertIn(schema2_path, str(exc.exception))
