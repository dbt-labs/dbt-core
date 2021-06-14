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

class TestDocs(DBTIntegrationTest):

    @property
    def schema(self):
        return "test_068docs"

    @property
    def models(self):
        return "models-docs"

    @property
    def project_config(self):
        cfg = {
            'config-version': 2,
            'data-paths': ['seed-docs'],
            'seeds': {
                'quote_columns': False,
            },
            'macro-paths': ['macros-docs'],
        }
        return cfg

    def tearDown(self):
        if os.path.exists(normalize('models-docs/customers.md')):
            os.remove(normalize('models-docs/customers.md'))
        if os.path.exists(normalize('models-docs/schema.yml')):
            os.remove(normalize('models-docs/schema.yml'))


    @use_profile('postgres')
    def test_postgres_pp_docs(self):
        # initial run
        self.run_dbt(['clean'])
        results = self.run_dbt(["run"])
        self.assertEqual(len(results), 1)

        # Add docs file customers.md
        shutil.copyfile('extra-files/customers1.md', 'models-docs/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.docs), 2)
        model_one_node = manifest.nodes['model.test.model_one']

        # Add schema file with 'docs' description
        shutil.copyfile('extra-files/schema-docs.yml', 'models-docs/schema.yml')
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
        shutil.copyfile('extra-files/customers2.md', 'models-docs/customers.md')
        results = self.run_dbt(["--partial-parse", "run"])
        manifest = get_manifest()
        self.assertEqual(len(manifest.docs), 2)
        doc_node = manifest.docs[doc_id]
        model_one_id = 'model.test.model_one'
        self.assertIn(model_one_id, manifest.nodes)
        model_node = manifest.nodes[model_one_id]
        self.assertRegex(model_node.description, r'LOTS')

        # Add a macro patch, source and exposure with doc
        shutil.copyfile('extra-files/schema-docs2.yml', 'models-docs/schema.yml')
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
        shutil.copyfile('extra-files/customers1.md', 'models-docs/customers.md')
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


