import unittest
import time

from dbt.parser.partial import PartialParsing
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.files import ParseFileType, SourceFile, SchemaSourceFile, FilePath, FileHash
from dbt.node_types import NodeType
from .utils import normalize


class TestPartialParsing(unittest.TestCase):
    def setUp(self):

        project_name = "my_test"
        project_root = "/users/root"
        sql_model_file = SourceFile(
            path=FilePath(
                project_root=project_root,
                searched_path="models",
                relative_path="my_model.sql",
                modification_time=time.time(),
            ),
            checksum=FileHash.from_contents("abcdef"),
            project_name=project_name,
            parse_file_type=ParseFileType.Model,
            nodes=["model.my_test.my_model"],
            env_vars=[],
        )
        sql_model_file_untouched = SourceFile(
            path=FilePath(
                project_root=project_root,
                searched_path="models",
                relative_path="my_model_untouched.sql",
                modification_time=time.time(),
            ),
            checksum=FileHash.from_contents("abcdef"),
            project_name=project_name,
            parse_file_type=ParseFileType.Model,
            nodes=["model.my_test.my_model_untouched"],
            env_vars=[],
        )

        python_model_file = SourceFile(
            path=FilePath(
                project_root=project_root,
                searched_path="models",
                relative_path="python_model.py",
                modification_time=time.time(),
            ),
            checksum=FileHash.from_contents("lalala"),
            project_name=project_name,
            parse_file_type=ParseFileType.Model,
            nodes=["model.my_test.python_model"],
            env_vars=[],
        )
        python_model_file_untouched = SourceFile(
            path=FilePath(
                project_root=project_root,
                searched_path="models",
                relative_path="python_model_untouched.py",
                modification_time=time.time(),
            ),
            checksum=FileHash.from_contents("lalala"),
            project_name=project_name,
            parse_file_type=ParseFileType.Model,
            nodes=["model.my_test.python_model_untouched"],
            env_vars=[],
        )
        schema_file = SchemaSourceFile(
            path=FilePath(
                project_root=project_root,
                searched_path="models",
                relative_path="schema.yml",
                modification_time=time.time(),
            ),
            checksum=FileHash.from_contents("ghijkl"),
            project_name=project_name,
            parse_file_type=ParseFileType.Schema,
            dfy={
                "version": 2,
                "models": [
                    {"name": "my_model", "description": "Test model"},
                    {"name": "python_model", "description": "python"},
                ],
            },
            ndp=["model.my_test.my_model"],
            env_vars={},
        )
        self.saved_files = {
            schema_file.file_id: schema_file,
            sql_model_file.file_id: sql_model_file,
            python_model_file.file_id: python_model_file,
            sql_model_file_untouched.file_id: sql_model_file_untouched,
            python_model_file_untouched.file_id: python_model_file_untouched,
        }
        sql_model_node = self.get_model("my_model")
        sql_model_node_untouched = self.get_model("my_model_untouched")
        python_model_node = self.get_python_model("python_model")
        python_model_node_untouched = self.get_python_model("python_model_untouched")
        nodes = {
            sql_model_node.unique_id: sql_model_node,
            python_model_node.unique_id: python_model_node,
            sql_model_node_untouched.unique_id: sql_model_node_untouched,
            python_model_node_untouched.unique_id: python_model_node_untouched,
        }
        self.saved_manifest = Manifest(files=self.saved_files, nodes=nodes)
        self.new_files = {
            sql_model_file.file_id: SourceFile.from_dict(sql_model_file.to_dict()),
            python_model_file.file_id: SourceFile.from_dict(python_model_file.to_dict()),
            sql_model_file_untouched.file_id: SourceFile.from_dict(
                sql_model_file_untouched.to_dict()
            ),
            python_model_file_untouched.file_id: SourceFile.from_dict(
                python_model_file_untouched.to_dict()
            ),
            schema_file.file_id: SchemaSourceFile.from_dict(schema_file.to_dict()),
        }

        self.partial_parsing = PartialParsing(self.saved_manifest, self.new_files)

    def get_model(self, name):
        return ModelNode(
            package_name="my_test",
            path=f"{name}.sql",
            original_file_path=f"models/{name}.sql",
            language="sql",
            raw_code="select * from wherever",
            name=name,
            resource_type=NodeType.Model,
            unique_id=f"model.my_test.{name}",
            fqn=["my_test", "models", name],
            database="test_db",
            schema="test_schema",
            alias="bar",
            checksum=FileHash.from_contents(""),
            patch_path="my_test://" + normalize("models/schema.yml"),
        )

    def get_python_model(self, name):
        return ModelNode(
            package_name="my_test",
            path=f"{name}.py",
            original_file_path=f"models/{name}.py",
            raw_code="import something",
            language="python",
            name=name,
            resource_type=NodeType.Model,
            unique_id=f"model.my_test.{name}",
            fqn=["my_test", "models", name],
            database="test_db",
            schema="test_schema",
            alias="bar",
            checksum=FileHash.from_contents(""),
            patch_path="my_test://" + normalize("models/schema.yml"),
        )

    def test_simple(self):

        # Nothing has changed
        self.assertIsNotNone(self.partial_parsing)
        self.assertTrue(self.partial_parsing.skip_parsing())

        # Change a model file
        sql_model_file_id = "my_test://" + normalize("models/my_model.sql")
        self.partial_parsing.new_files[sql_model_file_id].checksum = FileHash.from_contents(
            "xyzabc"
        )

        python_model_file_id = "my_test://" + normalize("models/python_model.py")
        self.partial_parsing.new_files[python_model_file_id].checksum = FileHash.from_contents(
            "ohohoh"
        )

        self.partial_parsing.build_file_diff()
        self.assertFalse(self.partial_parsing.skip_parsing())
        pp_files = self.partial_parsing.get_parsing_files()
        pp_files["my_test"]["ModelParser"] = set(pp_files["my_test"]["ModelParser"])
        # models has 'patch_path' so we expect to see a SchemaParser file listed
        schema_file_id = "my_test://" + normalize("models/schema.yml")
        expected_pp_files = {
            "my_test": {
                "ModelParser": set([sql_model_file_id, python_model_file_id]),
                "SchemaParser": [schema_file_id],
            }
        }
        self.assertEqual(pp_files, expected_pp_files)
        schema_file = self.saved_files[schema_file_id]
        schema_file_model_names = set([model["name"] for model in schema_file.pp_dict["models"]])
        expected_model_names = set(["python_model", "my_model"])
        self.assertEqual(schema_file_model_names, expected_model_names)
        schema_file_model_descriptions = set(
            [model["description"] for model in schema_file.pp_dict["models"]]
        )
        expected_model_descriptions = set(["Test model", "python"])
        self.assertEqual(schema_file_model_descriptions, expected_model_descriptions)
