import pytest

from dbt.parser.read_files import normalize_file_contents


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
    ],
)
def test_normalize_file_contents(file_contents: str, expected_normalized_file_contents: str):
    assert normalize_file_contents(file_contents) == expected_normalized_file_contents
