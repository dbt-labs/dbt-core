from dbt.exceptions import CompilationException
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.files import ParseFileType
from test.integration.base import DBTIntegrationTest, use_profile, normalize, get_manifest
import shutil
import os


class TestDocs(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068docs"

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
        os.mkdir(os.path.join(self.test_root_dir, 'models'))
        os.mkdir(os.path.join(self.test_root_dir, 'tests'))
        os.mkdir(os.path.join(self.test_root_dir, 'seeds'))
        os.mkdir(os.path.join(self.test_root_dir, 'macros'))
        os.mkdir(os.path.join(self.test_root_dir, 'analyses'))
        os.mkdir(os.path.join(self.test_root_dir, 'snapshots'))
        os.environ['DBT_PP_TEST'] = 'true'


    @use_profile('postgres')
    def test_postgres_pp_docs(self):
        # initial run
        self.setup_directories()
        self.copy_file('test-files/model_one.sql', 'models/model_one.sql')
        self.copy_file('test-files/raw_customers.csv', 'seeds/raw_customers.csv')
        self.copy_file('test-files/my_macro-docs.sql', 'macros/my_macro.sql')
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # Add docs file customers.md
        self.copy_file('test-files/customers1.md', 'models/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.docs), 2)
        model_one_node = manifest.nodes['model.test.model_one']

        # Add schema file with 'docs' description
        self.copy_file('test-files/schema-docs.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.docs), 2)
        doc_id = 'test.customer_table'
        self.assertIn(doc_id, manifest.docs)
        doc = manifest.docs[doc_id]
        doc_file_id = doc.file_id
        self.assertIn(doc_file_id, manifest.files)
        source_file = manifest.files[doc_file_id]
        self.assertEqual(len(source_file.nodes), 1)
        model_one_id = 'model.test.model_one'
        self.assertIn(model_one_id, source_file.nodes)
        model_node = manifest.nodes[model_one_id]
        self.assertEqual(model_node.description, 'This table contains customer data')

        # Update the doc file
        self.copy_file('test-files/customers2.md', 'models/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.docs), 2)
        doc_node = manifest.docs[doc_id]
        model_one_id = 'model.test.model_one'
        self.assertIn(model_one_id, manifest.nodes)
        model_node = manifest.nodes[model_one_id]
        self.assertRegex(model_node.description, r'LOTS')

        # Add a macro patch, source and exposure with doc
        self.copy_file('test-files/schema-docs2.yml', 'models/schema.yml')
        results = self.run_dbt(["--partial-parse", "run"])
        self.assertEqual(len(results), 1)
        manifest = get_manifest()
        doc_file = manifest.files[doc_file_id]
        expected_nodes = ['model.test.model_one', 'source.test.seed_sources.raw_customers', 'macro.test.my_macro', 'exposure.test.proxy_for_dashboard']
        self.assertEqual(expected_nodes, doc_file.nodes)
        source_id = 'source.test.seed_sources.raw_customers'
        self.assertEqual(manifest.sources[source_id].source_description, 'LOTS of customer data')
        macro_id = 'macro.test.my_macro'
        self.assertEqual(manifest.macros[macro_id].description, 'LOTS of customer data')
        exposure_id = 'exposure.test.proxy_for_dashboard'
        self.assertEqual(manifest.exposures[exposure_id].description, 'LOTS of customer data')


        # update the doc file again
        self.copy_file('test-files/customers1.md', 'models/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        source_file = manifest.files[doc_file_id]
        model_one_id = 'model.test.model_one'
        self.assertIn(model_one_id, source_file.nodes)
        model_node = manifest.nodes[model_one_id]
        self.assertEqual(model_node.description, 'This table contains customer data')
        self.assertEqual(manifest.sources[source_id].source_description, 'This table contains customer data')
        self.assertEqual(manifest.macros[macro_id].description, 'This table contains customer data')
        self.assertEqual(manifest.exposures[exposure_id].description, 'This table contains customer data')

        # check that _lock is working
        with manifest._lock:
            self.assertIsNotNone(manifest._lock)


