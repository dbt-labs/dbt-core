from dbt.exceptions import CompilationException, ParsingException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
from dbt.contracts.results import TestStatus
from dbt.parser.partial import special_override_macros
from test.integration.base import DBTIntegrationTest, use_profile, normalize, get_manifest
import shutil
import os


# Note: every test case needs to have separate directories, otherwise
# they will interfere with each other when tests are multi-threaded

class BasePPTest(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068A"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'seed-paths': ['seeds'],
            'test-paths': ['tests'],
            'macro-paths': ['macros'],
            'analysis-paths': ['analyses'],
            'snapshot-paths': ['snapshots'],
            'seeds': {
                'quote_columns': False,
            },
        }

    def setup_directories(self):
        # Create the directories for the test in the `self.test_root_dir`
        # directory after everything else is symlinked. We can copy to and
        # delete files in this directory without tests interfering with each other.
        os.mkdir(os.path.join(self.test_root_dir, 'models'))
        os.mkdir(os.path.join(self.test_root_dir, 'tests'))
        os.mkdir(os.path.join(self.test_root_dir, 'tests', 'generic'))
        os.mkdir(os.path.join(self.test_root_dir, 'seeds'))
        os.mkdir(os.path.join(self.test_root_dir, 'macros'))
        os.mkdir(os.path.join(self.test_root_dir, 'analyses'))
        os.mkdir(os.path.join(self.test_root_dir, 'snapshots'))
        os.environ['DBT_PP_TEST'] = 'true'



class ModelTest(BasePPTest):

    @use_profile('postgres')
    def test_postgres_pp_models(self):
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # add a model file
        self.copy_file('test-files/model_two.sql', 'models/model_two.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # add a schema file
        self.copy_file('test-files/models-schema1.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        self.assertIn('model.test.model_one', manifest.nodes)
        model_one_node = manifest.nodes['model.test.model_one']
        self.assertEqual(model_one_node.description, 'The first model')
        self.assertEqual(model_one_node.patch_path, 'test://' + normalize('models/schema.yml'))

        # add a model and a schema file (with a test) at the same time
        self.copy_file('test-files/models-schema2.yml', 'models/schema.yml')
        self.copy_file('test-files/model_three.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "test"], expect_pass=False)
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        project_files = [f for f in manifest.files if f.startswith('test://')]
        self.assertEqual(len(project_files), 4)
        model_3_file_id = 'test://' + normalize('models/model_three.sql')
        self.assertIn(model_3_file_id, manifest.files)
        model_three_file = manifest.files[model_3_file_id]
        self.assertEqual(model_three_file.parse_file_type, ParseFileType.Model)
        self.assertEqual(type(model_three_file).__name__, 'SourceFile')
        model_three_node = manifest.nodes[model_three_file.nodes[0]]
        schema_file_id = 'test://' + normalize('models/schema.yml')
        self.assertEqual(model_three_node.patch_path, schema_file_id)
        self.assertEqual(model_three_node.description, 'The third model')
        schema_file = manifest.files[schema_file_id]
        self.assertEqual(type(schema_file).__name__, 'SchemaSourceFile')
        self.assertEqual(len(schema_file.tests), 1)
        tests = schema_file.get_all_test_ids()
        self.assertEqual(tests, ['test.test.unique_model_three_id.6776ac8160'])
        unique_test_id = tests[0]
        self.assertIn(unique_test_id, manifest.nodes)

        # modify model sql file, ensure description still there
        self.copy_file('test-files/model_three_modified.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        model_id = 'model.test.model_three'
        self.assertIn(model_id, manifest.nodes)
        model_three_node = manifest.nodes[model_id]
        self.assertEqual(model_three_node.description, 'The third model')

        # Change the model 3 test from unique to not_null
        self.copy_file('test-files/models-schema2b.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "test"], expect_pass=False)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models/schema.yml')
        schema_file = manifest.files[schema_file_id]
        tests = schema_file.get_all_test_ids()
        self.assertEqual(tests, ['test.test.not_null_model_three_id.3162ce0a6f'])
        not_null_test_id = tests[0]
        self.assertIn(not_null_test_id, manifest.nodes.keys())
        self.assertNotIn(unique_test_id, manifest.nodes.keys())
        self.assertEqual(len(results), 1)

        # go back to previous version of schema file, removing patch, test, and model for model three
        self.copy_file('test-files/models-schema1.yml', 'models/schema.yml')
        self.rm_file(normalize('models/model_three.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # remove schema file, still have 3 models
        self.copy_file('test-files/model_three.sql', 'models/model_three.sql')
        self.rm_file(normalize('models/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models/schema.yml')
        self.assertNotIn(schema_file_id, manifest.files)
        project_files = [f for f in manifest.files if f.startswith('test://')]
        self.assertEqual(len(project_files), 3)

        # Put schema file back and remove a model
        # referred to in schema file
        self.copy_file('test-files/models-schema2.yml', 'models/schema.yml')
        self.rm_file('models/model_three.sql')
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put model back again
        self.copy_file('test-files/model_three.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add model four refing model three
        self.copy_file('test-files/model_four1.sql', 'models/model_four.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 4)

        # Remove model_three and change model_four to ref model_one
        # and change schema file to remove model_three
        self.rm_file('models/model_three.sql')
        self.copy_file('test-files/model_four2.sql', 'models/model_four.sql')
        self.copy_file('test-files/models-schema1.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Remove model four, put back model three, put back schema file
        self.copy_file('test-files/model_three.sql', 'models/model_three.sql')
        self.copy_file('test-files/models-schema2.yml', 'models/schema.yml')
        self.rm_file('models/model_four.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add a macro
        self.copy_file('test-files/my_macro.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        macro_id = 'macro.test.do_something'
        self.assertIn(macro_id, manifest.macros)

        # Modify the macro
        self.copy_file('test-files/my_macro2.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add a macro patch
        self.copy_file('test-files/models-schema3.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Remove the macro
        self.rm_file('macros/my_macro.sql')
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # put back macro file, got back to schema file with no macro
        # add separate macro patch schema file
        self.copy_file('test-files/models-schema2.yml', 'models/schema.yml')
        self.copy_file('test-files/my_macro.sql', 'macros/my_macro.sql')
        self.copy_file('test-files/macros.yml', 'macros/macros.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # delete macro and schema file
        self.rm_file('macros/my_macro.sql')
        self.rm_file('macros/macros.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add an empty schema file
        self.copy_file('test-files/empty_schema.yml', 'models/eschema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add version to empty schema file
        self.copy_file('test-files/empty_schema_with_version.yml', 'models/eschema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Disable model_three
        self.copy_file('test-files/model_three_disabled.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        model_id = 'model.test.model_three'
        self.assertIn(model_id, manifest.disabled)
        self.assertNotIn(model_id, manifest.nodes)

        # Edit disabled model three
        self.copy_file('test-files/model_three_disabled2.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        model_id = 'model.test.model_three'
        self.assertIn(model_id, manifest.disabled)
        self.assertNotIn(model_id, manifest.nodes)

        # Remove disabled from model three
        self.copy_file('test-files/model_three.sql', 'models/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        model_id = 'model.test.model_three'
        self.assertIn(model_id, manifest.nodes)
        self.assertNotIn(model_id, manifest.disabled)


class TestSources(BasePPTest):

    @use_profile('postgres')
    def test_postgres_pp_sources(self):
        self.setup_directories()
        # initial run
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        self.run_dbt(['clean'])
        self.copy_file('test-files/raw_customers.csv', 'seeds/raw_customers.csv')
        self.copy_file('test-files/sources-tests1.sql', 'macros/tests.sql')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # Partial parse running 'seed'
        self.run_dbt(['--partial-parse', 'seed'])
        manifest = get_manifest()
        seed_file_id = 'test://' + normalize('seeds/raw_customers.csv')
        self.assertIn(seed_file_id, manifest.files)

        # Add another seed file
        self.copy_file('test-files/raw_customers.csv', 'seeds/more_customers.csv')
        self.run_dbt(['--partial-parse', 'run'])
        seed_file_id = 'test://' + normalize('seeds/more_customers.csv')
        manifest = get_manifest()
        self.assertIn(seed_file_id, manifest.files)
        seed_id = 'seed.test.more_customers'
        self.assertIn(seed_id, manifest.nodes)

        # Remove seed file and add a schema files with a source referring to raw_customers
        self.rm_file(normalize('seeds/more_customers.csv'))
        self.copy_file('test-files/schema-sources1.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)
        file_id = 'test://' + normalize('models/sources.yml')
        self.assertIn(file_id, manifest.files)

        # add a model referring to raw_customers source
        self.copy_file('test-files/customers.sql', 'models/customers.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # remove sources schema file
        self.rm_file(normalize('models/sources.yml'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # put back sources and add an exposures file
        self.copy_file('test-files/schema-sources2.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # remove seed referenced in exposures file
        self.rm_file(normalize('seeds/raw_customers.csv'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # put back seed and remove depends_on from exposure
        self.copy_file('test-files/raw_customers.csv', 'seeds/raw_customers.csv')
        self.copy_file('test-files/schema-sources3.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Add seed config with test to schema.yml, remove exposure
        self.copy_file('test-files/schema-sources4.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Change seed name to wrong name
        self.copy_file('test-files/schema-sources5.yml', 'models/sources.yml')
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put back seed name to right name
        self.copy_file('test-files/schema-sources4.yml', 'models/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Add docs file customers.md
        self.copy_file('test-files/customers1.md', 'models/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])

        # Change docs file customers.md
        self.copy_file('test-files/customers2.md', 'models/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])

        # Delete docs file
        self.rm_file(normalize('models/customers.md'))
        results = self.run_dbt(["--partial-parse", "run"])

        # Add a data test
        self.copy_file('test-files/test-macro.sql', 'macros/test-macro.sql')
        self.copy_file('test-files/my_test.sql', 'tests/my_test.sql')
        results = self.run_dbt(["--partial-parse", "test"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 9)
        test_id = 'test.test.my_test'
        self.assertIn(test_id, manifest.nodes)

        # Change macro that data test depends on
        self.copy_file('test-files/test-macro2.sql', 'macros/test-macro.sql')
        results = self.run_dbt(["--partial-parse", "test"])
        manifest = get_manifest()

        # Add an analysis
        self.copy_file('test-files/my_analysis.sql', 'analyses/my_analysis.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()

        # Remove data test
        self.rm_file(normalize('tests/my_test.sql'))
        results = self.run_dbt(["--partial-parse", "test"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 9)

        # Remove analysis
        self.rm_file(normalize('analyses/my_analysis.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 8)

        # Change source test
        self.copy_file('test-files/sources-tests2.sql', 'macros/tests.sql')
        results = self.run_dbt(["--partial-parse", "run"])


class TestPartialParsingDependency(BasePPTest):

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }

    @use_profile("postgres")
    def test_postgres_parsing_with_dependency(self):
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        self.run_dbt(["clean"])
        self.run_dbt(["deps"])
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

        # Add a source override
        self.copy_file('test-files/schema-models-c.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)
        source_id = 'source.local_dep.seed_source.seed'
        self.assertIn(source_id, manifest.sources)
        # We have 1 root model, 1 local_dep model, 1 local_dep seed, 1 local_dep source test, 2 root source tests
        self.assertEqual(len(manifest.nodes), 5)
        test_id = 'test.local_dep.source_unique_seed_source_seed_id.afa94935ed'
        test_node = manifest.nodes[test_id]


        # Remove a source override
        self.rm_file(normalize('models/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)


class TestMacros(BasePPTest):

    @use_profile('postgres')
    def test_postgres_nested_macros(self):
        self.setup_directories()
        self.copy_file('test-files/model_a.sql', 'models/model_a.sql')
        self.copy_file('test-files/model_b.sql', 'models/model_b.sql')
        self.copy_file('test-files/macros-schema.yml', 'models/schema.yml')
        self.copy_file('test-files/custom_schema_tests1.sql', 'macros/custom_schema_tests.sql')
        results = self.run_dbt()
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        macro_child_map = manifest.build_macro_child_map()
        macro_unique_id = 'macro.test.test_type_two'

        results = self.run_dbt(['test'], expect_pass=False)
        results = sorted(results, key=lambda r: r.node.name)
        self.assertEqual(len(results), 2)
        # type_one_model_a_
        self.assertEqual(results[0].status, TestStatus.Fail)
        self.assertRegex(results[0].node.compiled_sql, r'union all')
        # type_two_model_a_
        self.assertEqual(results[1].status, TestStatus.Warn)
        self.assertEqual(results[1].node.config.severity, 'WARN')

        self.copy_file('test-files/custom_schema_tests2.sql', 'macros/custom_schema_tests.sql')
        results = self.run_dbt(["--partial-parse", "test"], expect_pass=False)
        manifest = get_manifest()
        test_node_id = 'test.test.type_two_model_a_.842bc6c2a7'
        self.assertIn(test_node_id, manifest.nodes)
        results = sorted(results, key=lambda r: r.node.name)
        self.assertEqual(len(results), 2)
        # type_two_model_a_
        self.assertEqual(results[1].status, TestStatus.Fail)
        self.assertEqual(results[1].node.config.severity, 'ERROR')

    @use_profile('postgres')
    def test_postgres_skip_macros(self):
        expected_special_override_macros = [
            'ref', 'source', 'config', 'generate_schema_name',
            'generate_database_name', 'generate_alias_name'
        ]
        self.assertEqual(special_override_macros, expected_special_override_macros)

        # initial run so we have a msgpack file
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        results = self.run_dbt()

        # add a new ref override macro
        self.copy_file('test-files/ref_override.sql', 'macros/ref_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # modify a ref override macro
        self.copy_file('test-files/ref_override2.sql', 'macros/ref_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # remove a ref override macro
        self.rm_file(normalize('macros/ref_override.sql'))
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # custom generate_schema_name macro
        self.copy_file('test-files/gsm_override.sql', 'macros/gsm_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # change generate_schema_name macro
        self.copy_file('test-files/gsm_override2.sql', 'macros/gsm_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)


class TestSnapshots(BasePPTest):

    @use_profile('postgres')
    def test_postgres_pp_snapshots(self):

        # initial run 
        self.setup_directories()
        self.copy_file('test-files/orders.sql', 'models/orders.sql')
        results = self.run_dbt() 
        self.assertEqual(len(results), 1)

        # add snapshot
        self.copy_file('test-files/snapshot.sql', 'snapshots/snapshot.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        snapshot_id = 'snapshot.test.orders_snapshot'
        self.assertIn(snapshot_id, manifest.nodes)
        snapshot2_id = 'snapshot.test.orders2_snapshot'
        self.assertIn(snapshot2_id, manifest.nodes)

        # run snapshot
        results = self.run_dbt(["--partial-parse", "snapshot"])
        self.assertEqual(len(results), 2)

        # modify snapshot
        self.copy_file('test-files/snapshot2.sql', 'snapshots/snapshot.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)

        # delete snapshot
        self.rm_file(normalize('snapshots/snapshot.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)


class TestTests(BasePPTest):

    @use_profile('postgres')
    def test_postgres_pp_generic_tests(self):

        # initial run 
        self.setup_directories()
        self.copy_file('test-files/orders.sql', 'models/orders.sql')
        self.copy_file('test-files/generic_schema.yml', 'models/schema.yml')
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        expected_nodes = ['model.test.orders', 'test.test.unique_orders_id.1360ecc70e']
        self.assertCountEqual(expected_nodes, list(manifest.nodes.keys()))

        # add generic test in test-path
        self.copy_file('test-files/generic_test.sql', 'tests/generic/generic_test.sql')
        self.copy_file('test-files/generic_test_schema.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        test_id = 'test.test.is_odd_orders_id.82834fdc5b'
        self.assertIn(test_id, manifest.nodes)
        expected_nodes = ['model.test.orders', 'test.test.unique_orders_id.1360ecc70e', 'test.test.is_odd_orders_id.82834fdc5b']
        self.assertCountEqual(expected_nodes, list(manifest.nodes.keys()))

        # edit generic test in test-path
        self.copy_file('test-files/generic_test_edited.sql', 'tests/generic/generic_test.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        test_id = 'test.test.is_odd_orders_id.82834fdc5b'
        self.assertIn(test_id, manifest.nodes)
        expected_nodes = ['model.test.orders', 'test.test.unique_orders_id.1360ecc70e', 'test.test.is_odd_orders_id.82834fdc5b']
        self.assertCountEqual(expected_nodes, list(manifest.nodes.keys()))
