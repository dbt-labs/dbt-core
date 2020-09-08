from test.integration.base import DBTIntegrationTest, use_profile
import os

import json


class BasePersistDocsTest(DBTIntegrationTest):
    @property
    def schema(self):
        return "persist_docs_060"

    @property
    def models(self):
        return "models"

    def _assert_common_comments(self, *comments):
        for comment in comments:
            assert '"with double quotes"' in comment
            assert """'''abc123'''""" in comment
            assert '\n' in comment
            assert 'Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting' in comment
            assert '/* comment */' in comment
            if os.name == 'nt':
                assert '--\r\n' in comment or '--\n' in comment
            else:
                assert '--\n' in comment

    def _assert_has_table_comments(self, table_node):
        table_comment = table_node['metadata']['comment']
        assert table_comment.startswith('Table model description')

        table_id_comment = table_node['columns']['id']['comment']
        assert table_id_comment.startswith('id Column description')

        table_name_comment = table_node['columns']['name']['comment']
        assert table_name_comment.startswith(
            'Some stuff here and then a call to')

        self._assert_common_comments(
            table_comment, table_id_comment, table_name_comment
        )

    def _assert_has_view_comments(self, view_node, has_node_comments=True,
                                  has_column_comments=True):
        view_comment = view_node['metadata']['comment']
        if has_node_comments:
            assert view_comment.startswith('View model description')
            self._assert_common_comments(view_comment)
        else:
            assert view_comment is None

        view_id_comment = view_node['columns']['id']['comment']
        if has_column_comments:
            assert view_id_comment.startswith('id Column description')
            self._assert_common_comments(view_id_comment)
        else:
            assert view_id_comment is None

        view_name_comment = view_node['columns']['name']['comment']
        assert view_name_comment is None


class TestPersistDocs(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def run_has_comments_pglike(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 3
        table_node = catalog_data['nodes']['model.test.table_model']
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data['nodes']['model.test.view_model']
        self._assert_has_view_comments(view_node)

        no_docs_node = catalog_data['nodes']['model.test.no_docs_model']
        self._assert_has_view_comments(no_docs_node, False, False)

    @use_profile('postgres')
    def test_postgres_comments(self):
        self.run_has_comments_pglike()

    @use_profile('redshift')
    def test_redshift_comments(self):
        self.run_has_comments_pglike()

    @use_profile('snowflake')
    def test_snowflake_comments(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 3
        table_node = catalog_data['nodes']['model.test.table_model']
        table_comment = table_node['metadata']['comment']
        assert table_comment.startswith('Table model description')

        table_id_comment = table_node['columns']['ID']['comment']
        assert table_id_comment.startswith('id Column description')

        table_name_comment = table_node['columns']['NAME']['comment']
        assert table_name_comment.startswith(
            'Some stuff here and then a call to')



class TestPersistDocsLateBinding(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                    'view_model': {
                        'bind': False,
                    }
                }
            }
        }

    @use_profile('redshift')
    def test_redshift_late_binding_view(self):
        self.run_dbt()
        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 3
        table_node = catalog_data['nodes']['model.test.table_model']
        view_node = self._assert_has_table_comments(table_node)

        view_node = catalog_data['nodes']['model.test.view_model']
        self._assert_has_view_comments(view_node, False, False)

        no_docs_node = catalog_data['nodes']['model.test.no_docs_model']
        self._assert_has_view_comments(no_docs_node, False, False)


class TestPersistDocsSimple(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
            'seeds': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }

    @use_profile('snowflake')
    def test_snowflake_persist_docs(self):
        self.run_dbt()

    @use_profile('bigquery')
    def test_bigquery_persist_docs(self):
        self.run_dbt(['seed'])
        self.run_dbt()
        desc_map = {
            'seed': 'Seed model description',
            'table_model': 'Table model description',
            'view_model': 'View model description',
        }
        for node_id in ['seed', 'table_model', 'view_model']:
            with self.adapter.connection_named('_test'):
                client = self.adapter.connections \
                    .get_thread_connection().handle

                table_id = "{}.{}.{}".format(
                    self.default_database,
                    self.unique_schema(),
                    node_id
                )
                bq_table = client.get_table(table_id)

                bq_schema = bq_table.schema

                assert bq_table.description.startswith(desc_map[node_id])
                assert bq_schema[0].description.startswith('id Column description ')
                if not node_id.startswith('view'):
                    assert bq_schema[1].description.startswith('Some stuff here and then a call to')


class TestPersistDocsNested(BasePersistDocsTest):
    @property
    def project_config(self):
        return {
            'config-version': 2,
            'models': {
                'test': {
                    '+persist_docs': {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @property
    def models(self):
        return 'models-bigquery-nested'

    @use_profile('bigquery')
    def test_bigquery_persist_docs(self):
        """
        run dbt and use the bigquery client from the adapter to check if the
        colunmn descriptions are persisted on the test model table and view.

        Next, generate the catalog and check if the comments are also included.
        """
        self.run_dbt(['seed'])
        self.run_dbt()

        self.run_dbt(['docs', 'generate'])
        with open('target/catalog.json') as fp:
            catalog_data = json.load(fp)
        assert 'nodes' in catalog_data
        assert len(catalog_data['nodes']) == 3  # seed, table, and view model

        for node_id in ['table_model_nested', 'view_model_nested']:
            # check the descriptions using the api
            with self.adapter.connection_named('_test'):
                client = self.adapter.connections \
                    .get_thread_connection().handle

                table_id = "{}.{}.{}".format(
                    self.default_database,
                    self.unique_schema(),
                    node_id
                )
                bq_schema = client.get_table(table_id).schema

                level_1_field = bq_schema[0]
                assert level_1_field.description == \
                       "level_1 column description"

                level_2_field = level_1_field.fields[0]
                assert level_2_field.description == \
                       "level_2 column description"

                level_3_field = level_2_field.fields[0]
                assert level_3_field.description == \
                       "level_3 column description"

            # check the descriptions in the catalog
            node = catalog_data['nodes']['model.test.{}'.format(node_id)]

            level_1_column = node['columns']['level_1']
            assert level_1_column['comment'] == "level_1 column description"

            level_2_column = node['columns']['level_1.level_2']
            assert level_2_column['comment'] == "level_2 column description"

            level_3_column = node['columns']['level_1.level_2.level_3_a']
            assert level_3_column['comment'] == "level_3 column description"
