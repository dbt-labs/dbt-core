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

    @use_profile('postgres')
    def test_postgres_env_use_experimental_parser(self):
        def cleanup():
            del os.environ['DBT_USE_EXPERIMENTAL_PARSER']
            
        self.addCleanup(cleanup)
        os.environ['DBT_USE_EXPERIMENTAL_PARSER'] = 'true'
        _, log_output = self.run_dbt_and_capture(['--debug', 'parse'])

        # successful stable static parsing
        self.assertFalse("1699: " in log_output)
        # successful experimental static parsing
        self.assertTrue("1698: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
        # static parser failed
        self.assertFalse("1603: " in log_output)
        # jinja rendering
        self.assertFalse("1602: " in log_output)

    @use_profile('postgres')
    def test_postgres_env_static_parser(self):
        def cleanup():
            del os.environ['DBT_STATIC_PARSER']
            
        self.addCleanup(cleanup)
        os.environ['DBT_STATIC_PARSER'] = 'false'
        _, log_output = self.run_dbt_and_capture(['--debug', 'parse'])

        print(log_output)

        # jinja rendering because of --no-static-parser
        self.assertTrue("1605: " in log_output)
        # successful stable static parsing
        self.assertFalse("1699: " in log_output)
        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
        # static parser failed
        self.assertFalse("1603: " in log_output)
        # fallback jinja rendering
        self.assertFalse("1602: " in log_output)

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

    # test that the static parser extracts some basic ref, source, and config calls by default
    # without the experimental flag and without rendering jinja
    @use_profile('postgres')
    def test_postgres_static_parser_basic(self):
        _, log_output = self.run_dbt_and_capture(['--debug', 'parse'])

        # successful stable static parsing
        self.assertTrue("1699: " in log_output)
        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
        # static parser failed
        self.assertFalse("1603: " in log_output)
        # jinja rendering
        self.assertFalse("1602: " in log_output)

        manifest = get_manifest()
        node = manifest.nodes['model.test.model_a']
        self.assertEqual(node.refs, [['model_a']])
        self.assertEqual(node.sources, [['my_src', 'my_tbl']])
        self.assertEqual(node.config._extra, {'x': True})
        self.assertEqual(node.config.tags, ['hello', 'world'])

    # test that the static parser doesn't run when the flag is set
    @use_profile('postgres')
    def test_postgres_static_parser_is_disabled(self):
        _, log_output = self.run_dbt_and_capture(['--debug', '--no-static-parser', 'parse'])

        # jinja rendering because of --no-static-parser
        self.assertTrue("1605: " in log_output)
        # successful stable static parsing
        self.assertFalse("1699: " in log_output)
        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
        # static parser failed
        self.assertFalse("1603: " in log_output)
        # fallback jinja rendering
        self.assertFalse("1602: " in log_output)


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

        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # fallback to jinja rendering
        self.assertTrue("1602: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
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

        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # fallback to jinja rendering
        self.assertTrue("1602: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
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

        # successful experimental static parsing
        self.assertFalse("1698: " in log_output)
        # fallback to jinja rendering
        self.assertTrue("1602: " in log_output)
        # experimental parser failed
        self.assertFalse("1604: " in log_output)
        # didn't run static parser because dbt detected a built-in macro override
        self.assertTrue("1601: " in log_output)
