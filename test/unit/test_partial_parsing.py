import unittest
from unittest import mock
import time

import dbt.exceptions
from dbt.parser.partial import PartialParsing
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.contracts.files import ParseFileType, SourceFile, SchemaSourceFile, FilePath, FileHash
from dbt.node_types import NodeType
from .utils import normalize


class TestPartialParsing(unittest.TestCase):

    def setUp(self):

        project_name = 'my_test'
        project_root = '/users/root'
        model_file = SourceFile(
            path=FilePath(project_root=project_root, searched_path='models', relative_path='my_model.sql', modification_time=time.time()),
            checksum=FileHash.from_contents('abcdef'),
            project_name=project_name,
            parse_file_type=ParseFileType.Model,
            nodes=['model.my_test.my_model'],
        )
        schema_file = SchemaSourceFile(
            path=FilePath(project_root=project_root, searched_path='models', relative_path='schema.yml', modification_time=time.time()),
            checksum=FileHash.from_contents('ghijkl'),
            project_name=project_name,
            parse_file_type=ParseFileType.Schema,
            dfy={'version': 2, 'models': [{'name': 'my_model', 'description': 'Test model'}]},
            ndp=['model.my_test.my_model'],
        )
        self.saved_files = {model_file.file_id: model_file, schema_file.file_id: schema_file}
        model_node = self.get_model('my_model')
        nodes = { model_node.unique_id: model_node }
        self.saved_manifest = Manifest(files=self.saved_files, nodes=nodes)
        self.new_files = {
            model_file.file_id: SourceFile.from_dict(model_file.to_dict()),
            schema_file.file_id: SchemaSourceFile.from_dict(schema_file.to_dict()),
        }

        self.partial_parsing = PartialParsing(self.saved_manifest, self.new_files)

    def get_model(self, name):
        return ParsedModelNode(
            package_name='my_test',
            root_path='/users/root/',
            path=f'{name}.sql',
            original_file_path=f'models/{name}.sql',
            raw_sql='select * from wherever',
            name=name,
            resource_type=NodeType.Model,
            unique_id=f'model.my_test.{name}',
            fqn=['my_test', 'models', name],
            database='test_db',
            schema='test_schema',
            alias='bar',
            checksum=FileHash.from_contents(''),
            patch_path='my_test://' + normalize('models/schema.yml'),
        )

    def test_simple(self):

        # Nothing has changed
        self.assertIsNotNone(self.partial_parsing)
        self.assertTrue(self.partial_parsing.skip_parsing())

        # Change a model file
        model_file_id = 'my_test://' + normalize('models/my_model.sql')
        self.partial_parsing.new_files[model_file_id].checksum = FileHash.from_contents('xyzabc')
        self.partial_parsing.build_file_diff()
        self.assertFalse(self.partial_parsing.skip_parsing())
        pp_files = self.partial_parsing.get_parsing_files()
        # models has 'patch_path' so we expect to see a SchemaParser file listed
        schema_file_id = 'my_test://' + normalize('models/schema.yml')
        expected_pp_files = {'my_test': {'ModelParser': [model_file_id], 'SchemaParser': [schema_file_id]}}
        self.assertEqual(pp_files, expected_pp_files)
        expected_pp_dict = {'version': 2, 'models': [{'name': 'my_model', 'description': 'Test model'}]}
        schema_file = self.saved_files[schema_file_id]
        self.assertEqual(schema_file.pp_dict, expected_pp_dict)
