import pytest

from tests.openlineage_utils import _escape_special_regex_chars


@pytest.mark.parametrize(
    "template_string, escaped_string",
    [
        ("{{ .* }}_openlineage_project", ".*_openlineage_project"),
        ("{{ [a-z]{0, 2} }}_openlineage_project", "[a-z]{0, 2}_openlineage_project"),
        (r"{{ [a-z]{0, 2} }} select *", r"[a-z]{0, 2}\ select\ \*"),
    ],
    ids=["without_special_chars", "with_special_chars", "with_escaped_special_chars"],
)
def test_escape_special_regex_chars(template_string, escaped_string):
    actual_escaped_string = _escape_special_regex_chars(template_string)
    assert actual_escaped_string == escaped_string
