from dbt.contracts.graph.manifest import Manifest
import os
from test.integration.base import DBTIntegrationTest, use_profile


def get_manifest():
    path = './target/partial_parse.msgpack'
    if os.path.exists(path):
        with open(path, 'rb') as fp:
            manifest_mp = fp.read()
        manifest: Manifest = Manifest.from_msgpack(manifest_mp)
        return manifest
    else:
        return None


class TestBasicExperimentalParser(DBTIntegrationTest):
    @property
    def schema(self):
        return "072_basic"

    @property
    def models(self):
        return "basic"

    # test that the experimental parser extracts some basic ref, source, and config calls.
    @use_profile('postgres')
    def test_postgres_experimental_parser_basic(self):
        results = self.run_dbt(['--use-experimental-parser', 'parse'])
        manifest = get_manifest()
        node = manifest.nodes['model.test.model_a']
        self.assertEqual(node.refs, [['model_a']])
        self.assertEqual(node.sources, [['my_src', 'my_tbl']])
        self.assertEqual(node.config._extra, {'x': True})
        self.assertEqual(node.config.tags, ['hello', 'world'])

class TestRefOverrideExperimentalParser(DBTIntegrationTest):
    @property
    def schema(self):
        return "072_ref_macro"

    @property
    def models(self):
        return "ref_macro/models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['source_macro', 'macros'],
        }

    # test that the experimental parser doesn't run if the ref built-in is overriden with a macro
    @use_profile('postgres')
    def test_postgres_experimental_parser_ref_override(self):
        _, log_output = self.run_dbt_and_capture(['--debug', '--use-experimental-parser', 'parse'])
        
        print(log_output)

        # successful static parsing
        self.assertFalse("1699: " in log_output)
        # ran static parser but failed
        self.assertFalse("1602: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        self.assertTrue("1601: " in log_output)

class TestSourceOverrideExperimentalParser(DBTIntegrationTest):
    @property
    def schema(self):
        return "072_source_macro"

    @property
    def models(self):
        return "source_macro/models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['source_macro', 'macros'],
        }

    # test that the experimental parser doesn't run if the source built-in is overriden with a macro
    @use_profile('postgres')
    def test_postgres_experimental_parser_source_override(self):
        _, log_output = self.run_dbt_and_capture(['--debug', '--use-experimental-parser', 'parse'])
        
        print(log_output)

        # successful static parsing
        self.assertFalse("1699: " in log_output)
        # ran static parser but failed
        self.assertFalse("1602: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        self.assertTrue("1601: " in log_output)

class TestConfigOverrideExperimentalParser(DBTIntegrationTest):
    @property
    def schema(self):
        return "072_config_macro"

    @property
    def models(self):
        return "config_macro/models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['config_macro', 'macros'],
        }

    # test that the experimental parser doesn't run if the config built-in is overriden with a macro
    @use_profile('postgres')
    def test_postgres_experimental_parser_config_override(self):
        _, log_output = self.run_dbt_and_capture(['--debug', '--use-experimental-parser', 'parse'])
        
        print(log_output)

        # successful static parsing
        self.assertFalse("1699: " in log_output)
        # ran static parser but failed
        self.assertFalse("1602: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        self.assertTrue("1601: " in log_output)
