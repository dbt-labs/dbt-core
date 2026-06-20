from types import SimpleNamespace

import pytest

from dbt.artifacts.resources.base import FileHash
from dbt.contracts.files import FilePath, ParseFileType, SchemaSourceFile, SourceFile
from dbt.parser.read_files import FileDiff, InputFile, ReadFilesFromDiff, normalize_file_contents


@pytest.mark.parametrize(
    "file_contents,expected_normalized_file_contents",
    [
        ("", ""),
        (" ", ""),
        ("  ", ""),
        ("\n", ""),
        ("a b", "a b"),
        ("a  b", "a b"),
        ("a\nb", "a b"),
        ("a\n b", "a b"),
        ("a b ", "a b"),
        ("  a b  ", "a b"),
        ("\na b\n", "a b"),
        ("\n\na b\n\n", "a b"),
        # Windows (CRLF) and legacy-Mac (CR) line endings must normalize
        # identically to Unix (LF) so checksums match across platforms (#11473).
        ("a\r\nb", "a b"),
        ("a\rb", "a b"),
        ("a\r\n\r\nb", "a b"),
        ("select 1\r\nfrom t\r\n", "select 1 from t"),
        ("select 1\nfrom t\n", "select 1 from t"),
    ],
)
def test_normalize_file_contents(file_contents: str, expected_normalized_file_contents: str):
    assert normalize_file_contents(file_contents) == expected_normalized_file_contents


def _saved_source_file(project_name, input_path, source_cls, parse_file_type):
    """Build a SourceFile/SchemaSourceFile standing in for an entry already in
    the saved manifest. Its checksum is a placeholder; ReadFilesFromDiff
    recomputes it from the incoming diff content."""
    parts = input_path.split("/")
    file_path = FilePath(
        searched_path=parts[0],
        relative_path="/".join(parts[1:]),
        modification_time=0.0,
        project_root="/project",
    )
    return source_cls(
        path=file_path,
        checksum=FileHash.from_contents("stale"),
        project_name=project_name,
        parse_file_type=parse_file_type,
        contents="stale",
    )


@pytest.mark.parametrize(
    "input_path,parse_file_type,source_cls,lf_content",
    [
        (
            "models/my_model.sql",
            ParseFileType.Model,
            SourceFile,
            "select 1 as id\nfrom my_table\n",
        ),
        # A non-SQL project file (schema YAML) flows through the same checksum
        # line in ReadFilesFromDiff, so it must be normalized too.
        (
            "models/_schema.yml",
            ParseFileType.Schema,
            SchemaSourceFile,
            "version: 2\nmodels:\n  - name: my_model\n",
        ),
    ],
)
def test_read_files_from_diff_normalizes_line_endings(
    input_path, parse_file_type, source_cls, lf_content
):
    """ReadFilesFromDiff (the ``--partial-parse-file-diff`` / dbt Cloud path)
    must canonicalize line endings the same way on-disk reads do (#11473), so a
    file delivered with CRLF content hashes identically to its LF equivalent and
    to the normalized on-disk baseline. Without normalization the FileDiff path
    produces a CRLF-sensitive checksum and falsely flags ``state:modified``."""
    project_name = "test"
    file_id = f"{project_name}://{input_path}"
    crlf_content = lf_content.replace("\n", "\r\n")

    saved_files = {
        file_id: _saved_source_file(project_name, input_path, source_cls, parse_file_type)
    }
    file_diff = FileDiff(
        deleted=[],
        changed=[InputFile(path=input_path, content=crlf_content)],
        added=[],
    )
    reader = ReadFilesFromDiff(
        root_project_name=project_name,
        all_projects={},
        file_diff=file_diff,
        saved_files=saved_files,
    )
    reader.read_files()

    actual = reader.files[file_id].checksum
    # CRLF content hashes the same as the normalized LF baseline (no false-modified).
    assert actual == FileHash.from_contents(normalize_file_contents(crlf_content))
    assert actual == FileHash.from_contents(normalize_file_contents(lf_content))
    # And specifically NOT the raw CRLF hash (the pre-fix, CRLF-sensitive behavior).
    assert actual != FileHash.from_contents(crlf_content)


def _stub_project(project_root="/project"):
    """Minimal stand-in for a dbt Project exposing only the path attributes
    ReadFilesFromDiff.get_project_file_types() reads when resolving an *added*
    file's parse type. Only ``model_paths`` contains ``models`` so a
    ``models/*.sql`` file resolves unambiguously to a model."""
    return SimpleNamespace(
        project_root=project_root,
        macro_paths=["macros"],
        model_paths=["models"],
        snapshot_paths=["snapshots"],
        analysis_paths=["analyses"],
        test_paths=["tests"],
        generic_test_paths=["tests/generic"],
        seed_paths=["seeds"],
        docs_paths=["docs"],
        all_source_paths=["models-properties"],
        fixture_paths=["fixtures"],
        function_paths=["functions"],
    )


def test_read_files_from_diff_added_normalizes_line_endings():
    """The *added* branch of ReadFilesFromDiff hashes brand-new files at a
    second site (read_files.py, the ``self.file_diff.added`` loop) that is
    distinct from the *changed* branch covered above. A model added via the file
    diff with CRLF line endings must hash the same as its normalized LF
    equivalent so it is not falsely flagged ``state:modified`` (#11473). This
    guards that site without needing the heavier functional harness."""
    lf_content = "select 1 as id\nfrom my_table\n"
    crlf_content = lf_content.replace("\n", "\r\n")

    reader = ReadFilesFromDiff(
        root_project_name="test",
        all_projects={"test": _stub_project()},
        file_diff=FileDiff(
            deleted=[],
            changed=[],
            added=[InputFile(path="models/crlf_added.sql", content=crlf_content)],
        ),
        saved_files={},
    )
    reader.read_files()

    # Exactly one file is added; read its checksum from the created entry rather
    # than reconstructing the file_id (which is built with the OS path separator).
    added_files = list(reader.files.values())
    assert len(added_files) == 1
    assert added_files[0].parse_file_type == ParseFileType.Model
    actual = added_files[0].checksum
    # CRLF content hashes the same as the normalized LF baseline (no false-modified).
    assert actual == FileHash.from_contents(normalize_file_contents(crlf_content))
    assert actual == FileHash.from_contents(normalize_file_contents(lf_content))
    # And specifically NOT the raw CRLF hash (the pre-fix, CRLF-sensitive behavior).
    assert actual != FileHash.from_contents(crlf_content)
