from mock import MagicMock, patch
import unittest

import dbt.flags
import dbt.node_runners
from dbt.adapters.factory import get_adapter

from collections import OrderedDict


class TestRunner(unittest.TestCase):

    def setUp(self):
        dbt.flags.STRICT_MODE = True
        dbt.flags.NON_DESTRUCTIVE = True

        self.profile = {
            'type': 'postgres',
            'dbname': 'postgres',
            'user': 'root',
            'host': 'database',
            'pass': 'password123',
            'port': 5432,
            'schema': 'public'
        }

        self.model_config = {
            'enabled': True,
            'materialized': 'view',
            'post-hook': [],
            'pre-hook': [],
            'vars': {},
        }

        self.model = {
            'name': 'view',
            'resource_type': 'model',
            'unique_id': 'model.root.view',
            'fqn': ['root_project', 'view'],
            'empty': False,
            'package_name': 'root',
            'root_path': '/usr/src/app',
            'depends_on': [
                'model.root.ephemeral'
            ],
            'config': self.model_config,
            'path': 'view.sql',
            'raw_sql': 'select * from {{ref("ephemeral")}}',
            'compiled': True,
            'extra_ctes_injected': False,
            'extra_ctes': OrderedDict([('model.root.ephemeral', None)]),
            'compiled_sql': 'select * from __dbt__CTE__ephemeral',
            'injected_sql': ('with __dbt__CTE__ephemeral as ('
                             'select * from "public"."ephemeral"',
                             ')'
                             'select * from __dbt__CTE__ephemeral'),
            'wrapped_sql':  ('create view "public"."view" as ('
                             'with __dbt__CTE__ephemeral as ('
                             'select * from "public"."ephemeral"'
                             ')'
                             'select * from __dbt__CTE__ephemeral'
                             '))')
        }

        self.existing = {}

        def fake_drop(profile, relation, relation_type, model_name):
            del self.existing[relation]

        def fake_query_for_existing(profile, schema, model_name):
            return self.existing

        self._drop = dbt.adapters.postgres.PostgresAdapter.drop
        self._query_for_existing = \
            dbt.adapters.postgres.PostgresAdapter.query_for_existing

        dbt.adapters.postgres.PostgresAdapter.drop = MagicMock(
            side_effect=fake_drop)

        dbt.adapters.postgres.PostgresAdapter.query_for_existing = MagicMock(
            side_effect=fake_query_for_existing)

    def tearDown(self):
        dbt.adapters.postgres.PostgresAdapter.drop = self._drop
        dbt.adapters.postgres.PostgresAdapter.query_for_existing = \
            self._query_for_existing


    def get_model_runner(self, model):
        project = MagicMock()
        adapter = get_adapter(self.profile)
        index, total = 0, 0
        runner = dbt.node_runners.ModelRunner(project, adapter, model, index, total)
        return runner

    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=None)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__view(self,
                                  mock_adapter_truncate,
                                  mock_adapter_rename,
                                  mock_adapter_execute_model):
        model = self.model.copy()
        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_called_once()
        mock_adapter_execute_model.assert_called_once()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__view__existing(self,
                                            mock_adapter_truncate,
                                            mock_adapter_rename,
                                            mock_adapter_execute_model):
        self.existing = {'view': 'view'}

        model = self.model.copy()

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_not_called()
        mock_adapter_execute_model.assert_not_called()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__table(self,
                                   mock_adapter_truncate,
                                   mock_adapter_rename,
                                   mock_adapter_execute_model):
        model = self.model.copy()
        model['config']['materialized'] = 'table'

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_not_called()
        mock_adapter_execute_model.assert_called_once()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__table__existing(self,
                                             mock_adapter_truncate,
                                             mock_adapter_rename,
                                             mock_adapter_execute_model):
        self.existing = {'view': 'table'}

        model = self.model.copy()
        model['config']['materialized'] = 'table'

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_called_once()
        mock_adapter_rename.assert_not_called()
        mock_adapter_execute_model.assert_called_once()



    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=None)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__view__destructive(self,
                                               mock_adapter_truncate,
                                               mock_adapter_rename,
                                               mock_adapter_execute_model):
        dbt.flags.NON_DESTRUCTIVE = False

        model = self.model.copy()

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_called_once()
        mock_adapter_execute_model.assert_called_once()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__view__existing__destructive(self,
                                                         mock_adapter_truncate,
                                                         mock_adapter_rename,
                                                         mock_adapter_execute_model):
        dbt.flags.NON_DESTRUCTIVE = False

        self.existing = {'view': 'view'}
        model = self.model.copy()

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_called_once()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_called_once()
        mock_adapter_execute_model.assert_called_once()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__table__destructive(self,
                                                mock_adapter_truncate,
                                                mock_adapter_rename,
                                                mock_adapter_execute_model):
        dbt.flags.NON_DESTRUCTIVE = False

        model = self.model.copy()
        model['config']['materialized'] = 'table'

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_not_called()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_called_once()
        mock_adapter_execute_model.assert_called_once()


    @patch('dbt.adapters.postgres.PostgresAdapter.execute_model', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.rename', return_value=1)
    @patch('dbt.adapters.postgres.PostgresAdapter.truncate', return_value=None)
    def test__execute_model__table__existing__destructive(self,
                                                          mock_adapter_truncate,
                                                          mock_adapter_rename,
                                                          mock_adapter_execute_model):
        dbt.flags.NON_DESTRUCTIVE = False

        self.existing = {'view': 'table'}

        model = self.model.copy()
        model['config']['materialized'] = 'table'

        runner = self.get_model_runner(model)
        runner.execute(model, existing=self.existing)

        dbt.adapters.postgres.PostgresAdapter.drop.assert_called_once()

        mock_adapter_truncate.assert_not_called()
        mock_adapter_rename.assert_called_once()
        mock_adapter_execute_model.assert_called_once()
