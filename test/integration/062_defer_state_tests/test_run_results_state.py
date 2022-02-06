from test.integration.base import DBTIntegrationTest, use_profile
import os
import random
import shutil
import string

import pytest

from dbt.exceptions import CompilationException


class TestRunResultsState(DBTIntegrationTest):
    @property
    def schema(self):
        return "run_results_state_062"

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
            if entry in {'models', 'seeds', 'macros'}:
                shutil.copytree(src, tst)
            elif os.path.isdir(entry) or entry.endswith('.sql'):
                os.symlink(src, tst)

    def copy_state(self):
        assert not os.path.exists('state')
        os.makedirs('state')
        shutil.copyfile('target/manifest.json', 'state/manifest.json')
        shutil.copyfile('target/run_results.json', 'state/run_results.json')

    def setUp(self):
        super().setUp()
        self.run_dbt(['build'])
        self.copy_state()
    
    def rebuild_run_dbt(self, expect_pass=True):
        shutil.rmtree('./state')
        self.run_dbt(['build'], expect_pass=expect_pass)
        self.copy_state()

    @use_profile('postgres')
    def test_postgres_seed_run_results_state(self):
        shutil.rmtree('./state')
        self.run_dbt(['seed'])
        self.copy_state()
        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'result:success', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:success', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:success+', '--state', './state'])
        assert len(results) == 7
        assert set(results) == {'test.seed', 'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}

        with open('seeds/seed.csv') as fp:
            fp.readline()
            newline = fp.newlines
        with open('seeds/seed.csv', 'a') as fp:
            fp.write(f'\"\'\'3,carl{newline}')
        shutil.rmtree('./state')
        self.run_dbt(['seed'], expect_pass=False)
        self.copy_state()

        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'result:error', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:error', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:error+', '--state', './state'])
        assert len(results) == 7
        assert set(results) == {'test.seed', 'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}


        with open('seeds/seed.csv') as fp:
            fp.readline()
            newline = fp.newlines
        with open('seeds/seed.csv', 'a') as fp:
            # assume each line is ~2 bytes + len(name)
            target_size = 1*1024*1024
            line_size = 64

            num_lines = target_size // line_size

            maxlines = num_lines + 4

            for idx in range(4, maxlines):
                value = ''.join(random.choices(string.ascii_letters, k=62))
                fp.write(f'{idx},{value}{newline}')
        shutil.rmtree('./state')
        self.run_dbt(['seed'], expect_pass=False)
        self.copy_state()

        results = self.run_dbt(['ls', '--resource-type', 'seed', '--select', 'result:error', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:error', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.seed'

        results = self.run_dbt(['ls', '--select', 'result:error+', '--state', './state'])
        assert len(results) == 7
        assert set(results) == {'test.seed', 'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}

    @use_profile('postgres')
    def test_postgres_build_run_results_state(self):
        results = self.run_dbt(['build', '--select', 'result:error', '--state', './state'])
        assert len(results) == 0

        with open('models/view_model.sql') as fp:
            fp.readline()
            newline = fp.newlines

        with open('models/view_model.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        self.rebuild_run_dbt(expect_pass=False)

        results = self.run_dbt(['build', '--select', 'result:error', '--state', './state'], expect_pass=False)
        assert len(results) == 3
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'view_model', 'not_null_view_model_id','unique_view_model_id'}

        results = self.run_dbt(['ls', '--select', 'result:error', '--state', './state'])
        assert len(results) == 3
        assert set(results) == {'test.view_model', 'test.not_null_view_model_id', 'test.unique_view_model_id'}

        results = self.run_dbt(['build', '--select', 'result:error+', '--state', './state'], expect_pass=False)
        assert len(results) == 4
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'table_model','view_model', 'not_null_view_model_id','unique_view_model_id'}

        results = self.run_dbt(['ls', '--select', 'result:error+', '--state', './state'])
        assert len(results) == 6 # includes exposure
        assert set(results) == {'test.table_model', 'test.view_model', 'test.ephemeral_model', 'test.not_null_view_model_id', 'test.unique_view_model_id', 'exposure:test.my_exposure'}

        # test failure on build tests
        # fail the unique test
        with open('models/view_model.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select 1 as id union all select 1 as id")
            fp.write(newline)
        
        self.rebuild_run_dbt(expect_pass=False)

        results = self.run_dbt(['build', '--select', 'result:fail', '--state', './state'], expect_pass=False)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'

        results = self.run_dbt(['ls', '--select', 'result:fail', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.unique_view_model_id'

        results = self.run_dbt(['build', '--select', 'result:fail+', '--state', './state'], expect_pass=False)
        assert len(results) == 2
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'table_model', 'unique_view_model_id'}

        results = self.run_dbt(['ls', '--select', 'result:fail+', '--state', './state'])
        assert len(results) == 1
        assert set(results) == {'test.unique_view_model_id'}

        # change the unique test severity from error to warn and reuse the same view_model.sql changes above
        f = open('models/schema.yml', 'r')
        filedata = f.read()
        f.close()
        newdata = filedata.replace('error','warn')
        f = open('models/schema.yml', 'w')
        f.write(newdata)
        f.close()

        self.rebuild_run_dbt(expect_pass=True)

        results = self.run_dbt(['build', '--select', 'result:warn', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'

        results = self.run_dbt(['ls', '--select', 'result:warn', '--state', './state'])
        assert len(results) == 1
        assert results[0] == 'test.unique_view_model_id'

        results = self.run_dbt(['build', '--select', 'result:warn+', '--state', './state'], expect_pass=True)
        assert len(results) == 2 # includes table_model to be run
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'table_model', 'unique_view_model_id'}

        results = self.run_dbt(['ls', '--select', 'result:warn+', '--state', './state'])
        assert len(results) == 1
        assert set(results) == {'test.unique_view_model_id'}

    @use_profile('postgres')
    def test_postgres_run_run_results_state(self):
        results = self.run_dbt(['run', '--select', 'result:success', '--state', './state'], expect_pass=True)
        assert len(results) == 2
        assert results[0].node.name == 'view_model'
        assert results[1].node.name == 'table_model'
        
        # clear state and rerun upstream view model to test + operator
        shutil.rmtree('./state')
        self.run_dbt(['run', '--select', 'view_model'], expect_pass=True)
        self.copy_state()
        results = self.run_dbt(['run', '--select', 'result:success+', '--state', './state'], expect_pass=True)
        assert len(results) == 2
        assert results[0].node.name == 'view_model'
        assert results[1].node.name == 'table_model'

        # check we are starting from a place with 0 errors
        results = self.run_dbt(['run', '--select', 'result:error', '--state', './state'])
        assert len(results) == 0
        
        # force an error in the view model to test error and skipped states
        with open('models/view_model.sql') as fp:
            fp.readline()
            newline = fp.newlines

        with open('models/view_model.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        shutil.rmtree('./state')
        self.run_dbt(['run'], expect_pass=False)
        self.copy_state()

        # test single result selector on error
        results = self.run_dbt(['run', '--select', 'result:error', '--state', './state'], expect_pass=False)
        assert len(results) == 1
        assert results[0].node.name == 'view_model'
        
        # test + operator selection on error
        results = self.run_dbt(['run', '--select', 'result:error+', '--state', './state'], expect_pass=False)
        assert len(results) == 2
        assert results[0].node.name == 'view_model'
        assert results[1].node.name == 'table_model'

        # single result selector on skipped. Expect this to pass becase underlying view already defined above
        results = self.run_dbt(['run', '--select', 'result:skipped', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0].node.name == 'table_model'

        # add a downstream model that depends on table_model for skipped+ selector
        with open('models/table_model_downstream.sql', 'w') as fp:
            fp.write("select * from {{ref('table_model')}}")
        
        shutil.rmtree('./state')
        self.run_dbt(['run'], expect_pass=False)
        self.copy_state()

        results = self.run_dbt(['run', '--select', 'result:skipped+', '--state', './state'], expect_pass=True)
        assert len(results) == 2
        assert results[0].node.name == 'table_model'
        assert results[1].node.name == 'table_model_downstream'
    
    
    @use_profile('postgres')
    def test_postgres_test_run_results_state(self):
        # run passed nodes
        results = self.run_dbt(['test', '--select', 'result:pass', '--state', './state'], expect_pass=True)
        assert len(results) == 2
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'unique_view_model_id', 'not_null_view_model_id'}
        
        # run passed nodes with + operator
        results = self.run_dbt(['test', '--select', 'result:pass+', '--state', './state'], expect_pass=True)
        assert len(results) == 2
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'unique_view_model_id', 'not_null_view_model_id'}

        # update view model to generate a failure case
        os.remove('./models/view_model.sql')
        with open('models/view_model.sql', 'w') as fp:
            fp.write("select 1 as id union all select 1 as id")
        
        self.rebuild_run_dbt(expect_pass=False)

        # test with failure selector
        results = self.run_dbt(['test', '--select', 'result:fail', '--state', './state'], expect_pass=False)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'

        # test with failure selector and + operator
        results = self.run_dbt(['test', '--select', 'result:fail+', '--state', './state'], expect_pass=False)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'

        # change the unique test severity from error to warn and reuse the same view_model.sql changes above
        with open('models/schema.yml', 'r+') as f:
            filedata = f.read()
            newdata = filedata.replace('error','warn')
            f.seek(0)
            f.write(newdata)
            f.truncate()
        
        # rebuild - expect_pass = True because we changed the error to a warning this time around
        self.rebuild_run_dbt(expect_pass=True)

        # test with warn selector
        results = self.run_dbt(['test', '--select', 'result:warn', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'

        # test with warn selector and + operator
        results = self.run_dbt(['test', '--select', 'result:warn+', '--state', './state'], expect_pass=True)
        assert len(results) == 1
        assert results[0].node.name == 'unique_view_model_id'


    @use_profile('postgres')
    def test_postgres_concurrent_selectors_run_run_results_state(self):
        results = self.run_dbt(['run', '--select', 'state:modified+', 'result:error+', '--state', './state'])
        assert len(results) == 0

        # force an error on a dbt model
        with open('models/view_model.sql') as fp:
            fp.readline()
            newline = fp.newlines

        with open('models/view_model.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        shutil.rmtree('./state')
        self.run_dbt(['run'], expect_pass=False)
        self.copy_state()

        # modify another dbt model
        with open('models/table_model_modified_example.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        results = self.run_dbt(['run', '--select', 'state:modified+', 'result:error+', '--state', './state'], expect_pass=False)
        assert len(results) == 3
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'view_model', 'table_model_modified_example', 'table_model'}
    

    @use_profile('postgres')
    def test_postgres_concurrent_selectors_test_run_results_state(self):
        # create failure test case for result:fail selector
        os.remove('./models/view_model.sql')
        with open('./models/view_model.sql', 'w') as f:
            f.write('select 1 as id union all select 1 as id union all select null as id')

        # run dbt build again to trigger test errors
        self.rebuild_run_dbt(expect_pass=False)
        
        # get the failures from 
        results = self.run_dbt(['test', '--select', 'result:fail', '--exclude', 'not_null_view_model_id', '--state', './state'], expect_pass=False)
        assert len(results) == 1
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'unique_view_model_id'}
        
        
    @use_profile('postgres')
    def test_postgres_concurrent_selectors_build_run_results_state(self):
        results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', '--state', './state'])
        assert len(results) == 0

        # force an error on a dbt model
        with open('models/view_model.sql') as fp:
            fp.readline()
            newline = fp.newlines

        with open('models/view_model.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        self.rebuild_run_dbt(expect_pass=False)

        # modify another dbt model
        with open('models/table_model_modified_example.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_error")
            fp.write(newline)
        
        results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', '--state', './state'], expect_pass=False)
        assert len(results) == 5
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'table_model_modified_example', 'view_model', 'table_model', 'not_null_view_model_id', 'unique_view_model_id'}
        
        # create failure test case for result:fail selector
        os.remove('./models/view_model.sql')
        with open('./models/view_model.sql', 'w') as f:
            f.write('select 1 as id union all select 1 as id')

        # create error model case for result:error selector
        with open('./models/error_model.sql', 'w') as f:
            f.write('select 1 as id from not_exists')
        
        # create something downstream from the error model to rerun
        with open('./models/downstream_of_error_model.sql', 'w') as f:
            f.write('select * from {{ ref("error_model") }} )')
        
        # regenerate build state
        self.rebuild_run_dbt(expect_pass=False)

        # modify model again to trigger the state:modified selector 
        with open('models/table_model_modified_example.sql', 'w') as fp:
            fp.write(newline)
            fp.write("select * from forced_another_error")
            fp.write(newline)
        
        results = self.run_dbt(['build', '--select', 'state:modified+', 'result:error+', 'result:fail+', '--state', './state'], expect_pass=False)
        assert len(results) == 5
        nodes = set([elem.node.name for elem in results])
        assert nodes == {'error_model', 'downstream_of_error_model', 'table_model_modified_example', 'table_model', 'unique_view_model_id'}
