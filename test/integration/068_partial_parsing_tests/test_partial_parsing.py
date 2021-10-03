from dbt.exceptions import CompilationException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
from dbt.contracts.results import TestStatus
from dbt.parser.partial import special_override_macros
from test.integration.base import DBTIntegrationTest, use_profile, normalize
import shutil
import os


# Note: every test case needs to have separate directories, otherwise
# they will interfere with each other when tests are multi-threaded

def get_manifest():
    path = './target/partial_parse.msgpack'
    if os.path.exists(path):
        with open(path, 'rb') as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None

class TestModels(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068A"

    @property
    def models(self):
        return "models-a"


    @use_profile('postgres')
    def test_postgres_pp_models(self):
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # add a model file
        shutil.copyfile('extra-files/model_two.sql', 'models-a/model_two.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # add a schema file
        shutil.copyfile('extra-files/models-schema1.yml', 'models-a/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)
        manifest = get_manifest()
        self.assertIn('model.test.model_one', manifest.nodes)
        model_one_node = manifest.nodes['model.test.model_one']
        self.assertEqual(model_one_node.description, 'The first model')
        self.assertEqual(model_one_node.patch_path, 'test://' + normalize('models-a/schema.yml'))

        # add a model and a schema file (with a test) at the same time
        shutil.copyfile('extra-files/models-schema2.yml', 'models-a/schema.yml')
        shutil.copyfile('extra-files/model_three.sql', 'models-a/model_three.sql')
        results = self.run_dbt(["--partial-parse", "test"], expect_pass=False)
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        project_files = [f for f in manifest.files if f.startswith('test://')]
        self.assertEqual(len(project_files), 4)
        model_3_file_id = 'test://' + normalize('models-a/model_three.sql')
        self.assertIn(model_3_file_id, manifest.files)
        model_three_file = manifest.files[model_3_file_id]
        self.assertEqual(model_three_file.parse_file_type, ParseFileType.Model)
        self.assertEqual(type(model_three_file).__name__, 'SourceFile')
        model_three_node = manifest.nodes[model_three_file.nodes[0]]
        schema_file_id = 'test://' + normalize('models-a/schema.yml')
        self.assertEqual(model_three_node.patch_path, schema_file_id)
        self.assertEqual(model_three_node.description, 'The third model')
        schema_file = manifest.files[schema_file_id]
        self.assertEqual(type(schema_file).__name__, 'SchemaSourceFile')
        self.assertEqual(len(schema_file.tests), 1)
        tests = schema_file.get_all_test_ids()
        self.assertEqual(tests, ['test.test.unique_model_three_id.6776ac8160'])
        unique_test_id = tests[0]
        self.assertIn(unique_test_id, manifest.nodes)

        # Change the model 3 test from unique to not_null
        shutil.copyfile('extra-files/models-schema2b.yml', 'models-a/schema.yml')
        results = self.run_dbt(["--partial-parse", "test"], expect_pass=False)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models-a/schema.yml')
        schema_file = manifest.files[schema_file_id]
        tests = schema_file.get_all_test_ids()
        self.assertEqual(tests, ['test.test.not_null_model_three_id.3162ce0a6f'])
        not_null_test_id = tests[0]
        self.assertIn(not_null_test_id, manifest.nodes.keys())
        self.assertNotIn(unique_test_id, manifest.nodes.keys())
        self.assertEqual(len(results), 1)

        # go back to previous version of schema file, removing patch, test, and model for model three
        shutil.copyfile('extra-files/models-schema1.yml', 'models-a/schema.yml')
        os.remove(normalize('models-a/model_three.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # remove schema file, still have 3 models
        shutil.copyfile('extra-files/model_three.sql', 'models-a/model_three.sql')
        os.remove(normalize('models-a/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models-a/schema.yml')
        self.assertNotIn(schema_file_id, manifest.files)
        project_files = [f for f in manifest.files if f.startswith('test://')]
        self.assertEqual(len(project_files), 3)

        # Put schema file back and remove a model
        # referred to in schema file
        shutil.copyfile('extra-files/models-schema2.yml', 'models-a/schema.yml')
        os.remove(normalize('models-a/model_three.sql'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put model back again
        shutil.copyfile('extra-files/model_three.sql', 'models-a/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add model four refing model three
        shutil.copyfile('extra-files/model_four1.sql', 'models-a/model_four.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 4)

        # Remove model_three and change model_four to ref model_one
        # and change schema file to remove model_three
        os.remove(normalize('models-a/model_three.sql'))
        shutil.copyfile('extra-files/model_four2.sql', 'models-a/model_four.sql')
        shutil.copyfile('extra-files/models-schema1.yml', 'models-a/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Remove model four, put back model three, put back schema file
        shutil.copyfile('extra-files/model_three.sql', 'models-a/model_three.sql')
        shutil.copyfile('extra-files/models-schema2.yml', 'models-a/schema.yml')
        os.remove(normalize('models-a/model_four.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add a macro
        shutil.copyfile('extra-files/my_macro.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        macro_id = 'macro.test.do_something'
        self.assertIn(macro_id, manifest.macros)

        # Modify the macro
        shutil.copyfile('extra-files/my_macro2.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add a macro patch
        shutil.copyfile('extra-files/models-schema3.yml', 'models-a/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Remove the macro
        os.remove(normalize('macros/my_macro.sql'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # put back macro file, got back to schema file with no macro
        # add separate macro patch schema file
        shutil.copyfile('extra-files/models-schema2.yml', 'models-a/schema.yml')
        shutil.copyfile('extra-files/my_macro.sql', 'macros/my_macro.sql')
        shutil.copyfile('extra-files/macros.yml', 'macros/macros.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # delete macro and schema file
        print(f"\n\n*** remove macro and macro_patch\n\n")
        os.remove(normalize('macros/my_macro.sql'))
        os.remove(normalize('macros/macros.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add an empty schema file
        shutil.copyfile('extra-files/empty_schema.yml', 'models-a/eschema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add version to empty schema file
        shutil.copyfile('extra-files/empty_schema_with_version.yml', 'models-a/eschema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

    def tearDown(self):
        if os.path.exists(normalize('models-a/model_two.sql')):
            os.remove(normalize('models-a/model_two.sql'))
        if os.path.exists(normalize('models-a/model_three.sql')):
            os.remove(normalize('models-a/model_three.sql'))
        if os.path.exists(normalize('models-a/model_four.sql')):
            os.remove(normalize('models-a/model_four.sql'))
        if os.path.exists(normalize('models-a/schema.yml')):
            os.remove(normalize('models-a/schema.yml'))
        if os.path.exists(normalize('target/partial_parse.msgpack')):
            os.remove(normalize('target/partial_parse.msgpack'))
        if os.path.exists(normalize('macros/my_macro.sql')):
            os.remove(normalize('macros/my_macro.sql'))
        if os.path.exists(normalize('models-a/eschema.yml')):
            os.remove(normalize('models-a/eschema.yml'))
        if os.path.exists(normalize('macros/macros.yml')):
            os.remove(normalize('macros/macros.yml'))


class TestSources(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068B"

    @property
    def models(self):
        return "models-b"

    @property
    def project_config(self):
        cfg = {
            'config-version': 2,
            'data-paths': ['seed'],
            'test-paths': ['tests'],
            'macro-paths': ['macros-b'],
            'analysis-paths': ['analysis'],
            'seeds': {
                'quote_columns': False,
            },
        }
        return cfg

    def tearDown(self):
        if os.path.exists(normalize('models-b/sources.yml')):
            os.remove(normalize('models-b/sources.yml'))
        if os.path.exists(normalize('seed/raw_customers.csv')):
            os.remove(normalize('seed/raw_customers.csv'))
        if os.path.exists(normalize('seed/more_customers.csv')):
            os.remove(normalize('seed/more_customers.csv'))
        if os.path.exists(normalize('models-b/customers.sql')):
            os.remove(normalize('models-b/customers.sql'))
        if os.path.exists(normalize('models-b/exposures.yml')):
            os.remove(normalize('models-b/exposures.yml'))
        if os.path.exists(normalize('models-b/customers.md')):
            os.remove(normalize('models-b/customers.md'))
        if os.path.exists(normalize('target/partial_parse.msgpack')):
            os.remove(normalize('target/partial_parse.msgpack'))
        if os.path.exists(normalize('tests/my_test.sql')):
            os.remove(normalize('tests/my_test.sql'))
        if os.path.exists(normalize('analysis/my_analysis.sql')):
            os.remove(normalize('analysis/my_analysis.sql'))
        if os.path.exists(normalize('macros-b/tests.sql')):
            os.remove(normalize('macros-b/tests.sql'))


    @use_profile('postgres')
    def test_postgres_pp_sources(self):
        # initial run
        self.run_dbt(['clean'])
        shutil.copyfile('extra-files/raw_customers.csv', 'seed/raw_customers.csv')
        shutil.copyfile('extra-files/sources-tests1.sql', 'macros-b/tests.sql')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # Partial parse running 'seed'
        self.run_dbt(['--partial-parse', 'seed'])
        manifest = get_manifest()
        seed_file_id = 'test://' + normalize('seed/raw_customers.csv')
        self.assertIn(seed_file_id, manifest.files)

        # Add another seed file
        shutil.copyfile('extra-files/raw_customers.csv', 'seed/more_customers.csv')
        self.run_dbt(['--partial-parse', 'run'])
        seed_file_id = 'test://' + normalize('seed/more_customers.csv')
        manifest = get_manifest()
        self.assertIn(seed_file_id, manifest.files)
        seed_id = 'seed.test.more_customers'
        self.assertIn(seed_id, manifest.nodes)

        # Remove seed file and add a schema files with a source referring to raw_customers
        os.remove(normalize('seed/more_customers.csv'))
        shutil.copyfile('extra-files/schema-sources1.yml', 'models-b/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)
        file_id = 'test://' + normalize('models-b/sources.yml')
        self.assertIn(file_id, manifest.files)

        # add a model referring to raw_customers source
        shutil.copyfile('extra-files/customers.sql', 'models-b/customers.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 2)

        # remove sources schema file
        os.remove(normalize('models-b/sources.yml'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # put back sources and add an exposures file
        shutil.copyfile('extra-files/schema-sources2.yml', 'models-b/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # remove seed referenced in exposures file
        os.remove(normalize('seed/raw_customers.csv'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # put back seed and remove depends_on from exposure
        shutil.copyfile('extra-files/raw_customers.csv', 'seed/raw_customers.csv')
        shutil.copyfile('extra-files/schema-sources3.yml', 'models-b/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Add seed config with test to schema.yml, remove exposure
        shutil.copyfile('extra-files/schema-sources4.yml', 'models-b/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Change seed name to wrong name
        shutil.copyfile('extra-files/schema-sources5.yml', 'models-b/sources.yml')
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "--warn-error", "run"])

        # Put back seed name to right name
        shutil.copyfile('extra-files/schema-sources4.yml', 'models-b/sources.yml')
        results = self.run_dbt(["--partial-parse", "run"])

        # Add docs file customers.md
        shutil.copyfile('extra-files/customers1.md', 'models-b/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])

        # Change docs file customers.md
        shutil.copyfile('extra-files/customers2.md', 'models-b/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])

        # Delete docs file
        os.remove(normalize('models-b/customers.md'))
        results = self.run_dbt(["--partial-parse", "run"])

        # Add a data test
        shutil.copyfile('extra-files/my_test.sql', 'tests/my_test.sql')
        results = self.run_dbt(["--partial-parse", "test"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 9)
        test_id = 'test.test.my_test'
        self.assertIn(test_id, manifest.nodes)

        # Add an analysis
        shutil.copyfile('extra-files/my_analysis.sql', 'analysis/my_analysis.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()

        # Remove data test
        os.remove(normalize('tests/my_test.sql'))
        results = self.run_dbt(["--partial-parse", "test"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 9)

        # Remove analysis
        os.remove(normalize('analysis/my_analysis.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 8)

        # Change source test
        shutil.copyfile('extra-files/sources-tests2.sql', 'macros-b/tests.sql')
        results = self.run_dbt(["--partial-parse", "run"])


class TestPartialParsingDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068C"

    @property
    def models(self):
        return "models-c"

    @property
    def packages_config(self):
        return {
            "packages": [
                {
                    'local': 'local_dependency'
                }
            ]
        }

    def tearDown(self):
        if os.path.exists(normalize('models-c/schema.yml')):
            os.remove(normalize('models-c/schema.yml'))

    @use_profile("postgres")
    def test_postgres_parsing_with_dependency(self):
        self.run_dbt(["clean"])
        self.run_dbt(["deps"])
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

        # Add a source override
        shutil.copyfile('extra-files/schema-models-c.yml', 'models-c/schema.yml')
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
        os.remove(normalize('models-c/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)


class TestMacros(DBTIntegrationTest):
    @property
    def schema(self):
        return "068-macros"

    @property
    def models(self):
        return "macros-models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "macro-paths": ["macros-macros"],
        }

    def tearDown(self):
        if os.path.exists(normalize('macros-macros/custom_schema_tests.sql')):
            os.remove(normalize('macros-macros/custom_schema_tests.sql'))
        if os.path.exists(normalize('macros-macros/ref_override.sql')):
            os.remove(normalize('macros-macros/ref_override.sql'))
        if os.path.exists(normalize('macros-macros/gsm_override.sql')):
            os.remove(normalize('macros-macros/gsm_override.sql'))

    @use_profile('postgres')
    def test_postgres_nested_macros(self):

        shutil.copyfile('extra-files/custom_schema_tests1.sql', 'macros-macros/custom_schema_tests.sql')
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

        shutil.copyfile('extra-files/custom_schema_tests2.sql', 'macros-macros/custom_schema_tests.sql')
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
        results = self.run_dbt()

        # add a new ref override macro
        shutil.copyfile('extra-files/ref_override.sql', 'macros-macros/ref_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # modify a ref override macro
        shutil.copyfile('extra-files/ref_override2.sql', 'macros-macros/ref_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # remove a ref override macro
        os.remove(normalize('macros-macros/ref_override.sql'))
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # custom generate_schema_name macro
        shutil.copyfile('extra-files/gsm_override.sql', 'macros-macros/gsm_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)

        # change generate_schema_name macro
        shutil.copyfile('extra-files/gsm_override2.sql', 'macros-macros/gsm_override.sql')
        results, log_output = self.run_dbt_and_capture(['--partial-parse', 'run'])
        self.assertTrue('Starting full parse.' in log_output)
