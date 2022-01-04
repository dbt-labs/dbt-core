import json
import os

from test.integration.base import DBTIntegrationTest, use_profile

import dbt.exceptions

class TestGoodDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("models")

    @use_profile('postgres')
    def test_postgres_valid_doc_ref(self):
        self.assertEqual(len(self.run_dbt()), 1)

        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        model_data = manifest['nodes']['model.test.model']
        self.assertEqual(
            model_data['description'],
            'My model is just a copy of the seed'
        )
        self.assertEqual(
            {
                'name': 'id',
                'description': 'The user ID number',
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['id']
        )
        self.assertEqual(
            {
                'name': 'first_name',
                'description': "The user's first name",
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['first_name']
        )

        self.assertEqual(
            {
                'name': 'last_name',
                'description': "The user's last name",
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['last_name']
        )
        self.assertEqual(len(model_data['columns']), 3)

    @use_profile('postgres')
    def test_postgres_alternative_docs_path(self):
        self.use_default_project({"docs-paths": [self.dir("docs")]})
        self.assertEqual(len(self.run_dbt()), 1)

        self.assertTrue(os.path.exists('./target/manifest.json'))

        with open('./target/manifest.json') as fp:
            manifest = json.load(fp)

        model_data = manifest['nodes']['model.test.model']
        self.assertEqual(
            model_data['description'],
            'Alt text about the model'
        )
        self.assertEqual(
            {
                'name': 'id',
                'description': 'The user ID number with alternative text',
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['id']
        )
        self.assertEqual(
            {
                'name': 'first_name',
                'description': "The user's first name",
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['first_name']
        )

        self.assertEqual(
            {
                'name': 'last_name',
                'description': "The user's last name in this other file",
                'data_type': None,
                'meta': {},
                'quote': None,
                'tags': [],
            },
            model_data['columns']['last_name']
        )
        self.assertEqual(len(model_data['columns']), 3)

    @use_profile('postgres')
    def test_postgres_alternative_docs_path_missing(self):
        self.use_default_project({"docs-paths": [self.dir("not-docs")]})
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt()


class TestMissingDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("missing_docs_models")

    @use_profile('postgres')
    def test_postgres_missing_doc_ref(self):
        # The run should fail since we could not find the docs reference.
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt()


class TestBadDocsBlocks(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("invalid_name_models")

    @use_profile('postgres')
    def test_postgres_invalid_doc_ref(self):
        # The run should fail since we could not find the docs reference.
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(expect_pass=False)

class TestDuplicateDocsBlock(DBTIntegrationTest):
    @property
    def schema(self):
        return 'docs_blocks_035'

    @staticmethod
    def dir(path):
        return os.path.normpath(path)

    @property
    def models(self):
        return self.dir("duplicate_docs")

    @use_profile('postgres')
    def test_postgres_duplicate_doc_ref(self):
        with self.assertRaises(dbt.exceptions.CompilationException):
            self.run_dbt(expect_pass=False)
