from dbt.exceptions import CompilationException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
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
        return "test_067A"

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
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        self.assertEqual(len(manifest.files), 33)
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

        # go back to previous version of schema file, removing patch and test for model three
        shutil.copyfile('extra-files/models-schema1.yml', 'models-a/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # remove schema file, still have 3 models
        os.remove(normalize('models-a/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)
        manifest = get_manifest()
        schema_file_id = 'test://' + normalize('models-a/schema.yml')
        self.assertNotIn(schema_file_id, manifest.files)
        self.assertEqual(len(manifest.files), 32)

        # Put schema file back and remove a model
        # referred to in schema file
        shutil.copyfile('extra-files/models-schema2.yml', 'models-a/schema.yml')
        os.remove(normalize('models-a/model_three.sql'))
        with self.assertRaises(CompilationException):
            results = self.run_dbt(["--partial-parse", "run"])

        # Put model back again
        shutil.copyfile('extra-files/model_three.sql', 'models-a/model_three.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Add a macro
        shutil.copyfile('extra-files/my_macro.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Modify the macro
        shutil.copyfile('extra-files/my_macro2.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

        # Remove the macro
        os.remove(normalize('macros/my_macro.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 3)

    def tearDown(self):
        if os.path.exists(normalize('models-a/model_two.sql')):
            os.remove(normalize('models-a/model_two.sql'))
        if os.path.exists(normalize('models-a/model_three.sql')):
            os.remove(normalize('models-a/model_three.sql'))
        if os.path.exists(normalize('models-a/schema.yml')):
            os.remove(normalize('models-a/schema.yml'))
        if os.path.exists(normalize('target/partial_parse.msgpack')):
            os.remove(normalize('target/partial_parse.msgpack'))
        if os.path.exists(normalize('macros/my_macro.sql')):
            os.remove(normalize('macros/my_macro.sql'))


class TestSources(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_067B"

    @property
    def models(self):
        return "models-b"

    @property
    def project_config(self):
        cfg = {
            'config-version': 2,
            'data-paths': ['seed'],
            'test-paths': ['tests'],
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


    @use_profile('postgres')
    def test_postgres_pp_sources(self):
        # initial run
        self.run_dbt(['clean'])
        shutil.copyfile('extra-files/raw_customers.csv', 'seed/raw_customers.csv')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # create a seed file, parse and run it
        self.run_dbt(['seed'])
        manifest = get_manifest()
        seed_file_id = 'test://' + normalize('seed/raw_customers.csv')
        self.assertIn(seed_file_id, manifest.files)

        # add a schema files with a source referring to raw_customers
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
            results = self.run_dbt(["--partial-parse", "run"])

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
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 8)
        test_id = 'test.test.my_test'
        self.assertIn(test_id, manifest.nodes)

        # Add an analysis
        shutil.copyfile('extra-files/my_analysis.sql', 'analysis/my_analysis.sql')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()

        # Remove data test
        os.remove(normalize('tests/my_test.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 8)

        # Remove analysis
        os.remove(normalize('analysis/my_analysis.sql'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.nodes), 7)


class TestPartialParsingDependency(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_067C"

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
        test_id = 'test.local_dep.source_unique_seed_source_seed_id.c37cdbabae'
        test_node = manifest.nodes[test_id]


        # Remove a source override
        os.remove(normalize('models-c/schema.yml'))
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.sources), 1)

