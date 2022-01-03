from test.integration.base import DBTIntegrationTest, use_profile
import os
import random
import shutil
import string

import pytest

from dbt.exceptions import CompilationException, IncompatibleSchemaException


class TestModifiedState(DBTIntegrationTest):
    @property
    def schema(self):
        return "modified_state_062"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
            'seeds': {
                'test': {
                    'quote_columns': True,
                }
            }
        }

    def _symlink_test_folders(self):
        # dbt's normal symlink behavior breaks this test. Copy the files
        # so we can freely modify them.
        for entry in os.listdir(self.test_original_source_path):
            src = os.path.join(self.test_original_source_path, entry)
            tst = os.path.join(self.test_root_dir, entry)
            if entry in {'models', 'seeds', 'macros', 'previous_state'}:
                shutil.copytree(src, tst)
            elif os.path.isdir(entry) or entry.endswith('.sql'):
                os.symlink(src, tst)

    def copy_state(self):
        assert not os.path.exists('state')
        os.makedirs('state')
        shutil.copyfile('target/manifest.json', 'state/manifest.json')

    def setUp(self):
        super().setUp()
        self.run_dbt(['seed'])
        self.run_dbt(['run'])
        self.copy_state()

    @use_profile('postgres')
    def test_postgres_changed_seed_contents_state(self):
        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'], expect_pass=True)
        assert len(results) == 0
        with open('seeds/seed.csv') as fp:
            fp.readline()
            newline = fp.newlines
        with open('seeds/seed.csv', 'a') as fp:
            fp.write(f'3,carl{newline}')

        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'state:modified+', '--state', './state'])
        assert len(results) == 7
        assert set(results) == {'test.seed', 'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}

        shutil.rmtree('./state')
        self.copy_state()

        with open('seeds/seed.csv', 'a') as fp:
            # assume each line is ~2 bytes + len(name)
            target_size = 1*1024*1024
            line_size = 64

            num_lines = target_size // line_size

            maxlines = num_lines + 4

            for idx in range(4, maxlines):
                value = ''.join(random.choices(string.ascii_letters, k=62))
                fp.write(f'{idx},{value}{newline}')

        # now if we run again, we should get a warning
        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        with pytest.raises(CompilationException) as exc:
            self.run_dbt(['--warn-error', 'ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'])
        assert '>1MB' in str(exc.value)

        shutil.rmtree('./state')
        self.copy_state()

        # once it's in path mode, we don't mark it as modified if it changes
        with open('seeds/seed.csv', 'a') as fp:
            fp.write(f'{random},test{newline}')

        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'], expect_pass=True)
        assert len(results) == 0

    @use_profile('postgres')
    def test_postgres_changed_seed_config(self):
        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'], expect_pass=True)
        assert len(results) == 0

        self.use_default_project({'seeds': {'test': {'quote_columns': False}}})

        # quoting change -> seed changed
        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

    @use_profile('postgres')
    def test_postgres_unrendered_config_same(self):
        results = self.run_dbt(['ls', '--resource-type', 'model', '--select', 'state:modified', '--state', './state'], expect_pass=True)
        assert len(results) == 0

        # although this is the default value, dbt will recognize it as a change
        # for previously-unconfigured models, because it's been explicitly set
        self.use_default_project({'models': {'test': {'materialized': 'view'}}})
        results = self.run_dbt(['ls', '--resource-type', 'model', '--select', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.view_model'

    @use_profile('postgres')
    def test_postgres_changed_model_contents(self):
        results = self.run_dbt(['run', '--models', 'state:modified', '--state', './state'])
        assert len(results) == 0

        with open('models/table_model.sql') as fp:
            fp.readline()
            newline = fp.newlines

        with open('models/table_model.sql', 'w') as fp:
            fp.write("{{ config(materialized='table') }}")
            fp.write(newline)
            fp.write("select * from {{ ref('seed') }}")
            fp.write(newline)

        results = self.run_dbt(['run', '--models', 'state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0].node.name == 'table_model'

    @use_profile('postgres')
    def test_postgres_new_macro(self):
        with open('macros/macros.sql') as fp:
            fp.readline()
            newline = fp.newlines

        new_macro = '{% macro my_other_macro() %}{% endmacro %}' + newline

        # add a new macro to a new file
        with open('macros/second_macro.sql', 'w') as fp:
            fp.write(new_macro)

        results, stdout = self.run_dbt_and_capture(['run', '--models', 'state:modified', '--state', './state'])
        assert len(results) == 0

        os.remove('macros/second_macro.sql')
        # add a new macro to the existing file
        with open('macros/macros.sql', 'a') as fp:
            fp.write(new_macro)

        results, stdout = self.run_dbt_and_capture(['run', '--models', 'state:modified', '--state', './state'])
        assert len(results) == 0

    @use_profile('postgres')
    def test_postgres_changed_macro_contents(self):
        with open('macros/macros.sql') as fp:
            fp.readline()
            newline = fp.newlines

        # modify an existing macro
        with open('macros/macros.sql', 'w') as fp:
            fp.write("{% macro my_macro() %}")
            fp.write(newline)
            fp.write("    {% do log('in a macro', info=True) %}")
            fp.write(newline)
            fp.write('{% endmacro %}')
            fp.write(newline)

        # table_model calls this macro
        results, stdout = self.run_dbt_and_capture(['run', '--models', 'state:modified', '--state', './state'])
        assert len(results) == 1

    @use_profile('postgres')
    def test_postgres_changed_exposure(self):
        with open('models/exposures.yml', 'a') as fp:
            fp.write('      name: John Doe\n')

        results, stdout = self.run_dbt_and_capture(['run', '--models', '+state:modified', '--state', './state'])
        assert len(results) == 1
        assert results[0].node.name == 'view_model'

    @use_profile('postgres')
    def test_postgres_previous_version_manifest(self):
        # This tests that a different schema version in the file throws an error
        with self.assertRaises(IncompatibleSchemaException) as exc:
            results = self.run_dbt(['ls', '-s',  'state:modified',  '--state',  './previous_state'])
            self.assertEqual(exc.CODE, 10014)
