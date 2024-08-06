import time
from copy import deepcopy
from typing import Dict, List

import pytest

from dbt.contracts.files import (
    BaseSourceFile,
    FileHash,
    FilePath,
    ParseFileType,
    SchemaSourceFile,
    SourceFile,
)
from dbt.node_types import NodeType
from dbt.parser.partial import PartialParsing
from dbt.tests.util import safe_set_invocation_context
from tests.unit.utils import normalize
from tests.unit.utils.manifest import make_generic_test, make_model

PROJECT_NAME = "my_test"


@pytest.fixture
def files() -> Dict[str, BaseSourceFile]:
    project_root = "/users/root"
    sql_model_file = SourceFile(
        path=FilePath(
            project_root=project_root,
            searched_path="models",
            relative_path="my_model.sql",
            modification_time=time.time(),
        ),
        checksum=FileHash.from_contents("abcdef"),
        project_name=PROJECT_NAME,
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
        project_name=PROJECT_NAME,
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
        project_name=PROJECT_NAME,
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
        project_name=PROJECT_NAME,
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
        project_name=PROJECT_NAME,
        parse_file_type=ParseFileType.Schema,
        dfy={
            "version": 2,
            "models": [
                {"name": "my_model", "description": "Test model"},
                {"name": "python_model", "description": "python"},
                {"name": "not_null", "model": "test.my_test.test_my_model"},
            ],
        },
        ndp=["model.my_test.my_model"],
        env_vars={},
        data_tests={"models": {"not_null": {"test.my_test.test_my_model": []}}},
    )
    return {
        schema_file.file_id: schema_file,
        sql_model_file.file_id: sql_model_file,
        sql_model_file_untouched.file_id: sql_model_file_untouched,
        python_model_file.file_id: python_model_file,
        python_model_file_untouched.file_id: python_model_file_untouched,
    }


@pytest.fixture
def nodes() -> List[NodeType]:
    patch_path = "my_test://" + normalize("models/schema.yml")
    my_model = make_model(PROJECT_NAME, "my_model", "", patch_path=patch_path)
    return [
        my_model,
        make_model(PROJECT_NAME, "my_model_untouched", "", patch_path=patch_path),
        make_model(PROJECT_NAME, "python_model", "", language="python", patch_path=patch_path),
        make_model(
            PROJECT_NAME, "python_model_untouched", "", language="python", patch_path=patch_path
        ),
        make_generic_test(PROJECT_NAME, "test", my_model, {}),
    ]


@pytest.fixture
def partial_parsing(manifest, files):
    safe_set_invocation_context()
    return PartialParsing(manifest, deepcopy(files))


def test_simple(partial_parsing, files, nodes):
    # Nothing has changed
    assert partial_parsing is not None
    assert partial_parsing.skip_parsing() is True

    # Change a model file
    sql_model_file_id = "my_test://" + normalize("models/my_model.sql")
    partial_parsing.new_files[sql_model_file_id].checksum = FileHash.from_contents("xyzabc")

    python_model_file_id = "my_test://" + normalize("models/python_model.py")
    partial_parsing.new_files[python_model_file_id].checksum = FileHash.from_contents("ohohoh")

    partial_parsing.build_file_diff()
    assert partial_parsing.skip_parsing() is False
    pp_files = partial_parsing.get_parsing_files()
    pp_files["my_test"]["ModelParser"] = set(pp_files["my_test"]["ModelParser"])
    # models has 'patch_path' so we expect to see a SchemaParser file listed
    schema_file_id = "my_test://" + normalize("models/schema.yml")
    expected_pp_files = {
        "my_test": {
            "ModelParser": set([sql_model_file_id, python_model_file_id]),
            "SchemaParser": [schema_file_id],
        }
    }
    assert pp_files == expected_pp_files
    schema_file = files[schema_file_id]
    schema_file_model_names = set([model["name"] for model in schema_file.pp_dict["models"]])
    expected_model_names = set(["python_model", "my_model"])
    assert schema_file_model_names == expected_model_names
    schema_file_model_descriptions = set(
        [model["description"] for model in schema_file.pp_dict["models"]]
    )
    expected_model_descriptions = set(["Test model", "python"])
    assert schema_file_model_descriptions == expected_model_descriptions


def test_schedule_nodes_for_parsing_basic(partial_parsing, nodes):
    assert partial_parsing.file_diff["deleted"] == []
    assert partial_parsing.project_parser_files == {}
    partial_parsing.schedule_nodes_for_parsing([nodes[0].unique_id])
    assert partial_parsing.project_parser_files == {
        "my_test": {
            "ModelParser": ["my_test://models/my_model.sql"],
            "SchemaParser": ["my_test://models/schema.yml"],
        }
    }


def test_schedule_macro_nodes_for_parsing_basic(partial_parsing):
    # XXX it seems kind of confusing what exactly this function does.
    # Whoever Changes this function please add more comment.

    # this rely on the dfy and data_tests fields in schema node to add schema file to reparse
    partial_parsing.schedule_macro_nodes_for_parsing(["test.my_test.test_my_model"])
    assert partial_parsing.project_parser_files == {
        "my_test": {"SchemaParser": ["my_test://models/schema.yml"]}
    }


class TestFileDiff:
    @pytest.fixture
    def partial_parsing(self, manifest, files):
        safe_set_invocation_context()
        saved_files = deepcopy(files)
        saved_files["my_test://models/python_model_untouched.py"].checksum = (
            FileHash.from_contents("something new")
        )
        return PartialParsing(manifest, saved_files)

    def test_build_file_diff_basic(self, partial_parsing):
        partial_parsing.build_file_diff()
        assert set(partial_parsing.file_diff["unchanged"]) == {
            "my_test://models/my_model_untouched.sql",
            "my_test://models/my_model.sql",
            "my_test://models/schema.yml",
            "my_test://models/python_model.py",
        }
        assert partial_parsing.file_diff["changed"] == [
            "my_test://models/python_model_untouched.py"
        ]
