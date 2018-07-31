import json
import os

from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest, use_profile

import dbt.exceptions

class TestGoodDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/035_docs_blocks', path)
        )

    @property
    def models(self):
        return self.dir("models")


    @use_profile('postgres')
    def test_valid_doc_ref(self):
        self.assertEqual(len(self.run_dbt()), 1)

        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        model_data = manifest['nodes']['model.test.model']
        self.assertEqual(
            model_data['description'],
            'My model is just a copy of the seed'
        )
        self.assertIn(
            {
                'name': 'id',
                'description': 'The user ID number'
            },
            model_data['columns']
        )
        self.assertIn(
            {
                'name': 'first_name',
                'description': "The user's first name",
            },
            model_data['columns']
        )

        self.assertIn(
            {
                'name': 'last_name',
                'description': "The user's last name",
            },
            model_data['columns']
        )
        self.assertEqual(len(model_data['columns']), 3)


class TestMissingDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/035_docs_blocks', path)
        )

    @property
    def models(self):
        return self.dir("missing_docs_models")

    @use_profile('postgres')
    def test_missing_doc_ref(self):
        # The run should fail since we could not find the docs reference.
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(expect_pass=False)

class TestBadDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(
            os.path.join('test/integration/035_docs_blocks', path)
        )

    @property
    def models(self):
        return self.dir("invalid_name_models")

    @use_profile('postgres')
    def test_invalid_doc_ref(self):
        # The run should fail since we could not find the docs reference.
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(expect_pass=False)
