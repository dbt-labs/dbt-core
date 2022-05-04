import dbt.exceptions
from typing import Any, Dict, Optional
import yaml

# the C version is faster, but it doesn't always exist
try:
    from yaml import CLoader as Loader, CSafeLoader as SafeLoader, CDumper as Dumper
except ImportError:
    from yaml import Loader, SafeLoader, Dumper  # type: ignore  # noqa: F401

from dbt.ui import warning_tag

YAML_ERROR_MESSAGE = """
Syntax error near line {line_number}
------------------------------
{nice_error}

Raw Error:
------------------------------
{raw_error}
""".strip()


class UniqueKeyLoader(SafeLoader):
    """A subclass that checks for unique yaml mapping nodes.

    This class extends `SafeLoader` from the `yaml` library to check for
    unique top level keys (mapping nodes). See issue (https://github.com/yaml/pyyaml/issues/165)
    and solution (https://gist.github.com/pypt/94d747fe5180851196eb?permalink_comment_id=4015118).
    """

    def construct_mapping(self, node, deep=False):
        mapping = set()
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if key in mapping:
                raise dbt.exceptions.DuplicateYamlKeyException(
                    f"Duplicate {key!r} key found in yaml file"
                )
            mapping.add(key)
        return super().construct_mapping(node, deep)


def line_no(i, line, width=3):
    line_number = str(i).ljust(width)
    return "{}| {}".format(line_number, line)


def prefix_with_line_numbers(string, no_start, no_end):
    line_list = string.split("\n")

    numbers = range(no_start, no_end)
    relevant_lines = line_list[no_start:no_end]

    return "\n".join([line_no(i + 1, line) for (i, line) in zip(numbers, relevant_lines)])


def contextualized_yaml_error(raw_contents, error):
    mark = error.problem_mark

    min_line = max(mark.line - 3, 0)
    max_line = mark.line + 4

    nice_error = prefix_with_line_numbers(raw_contents, min_line, max_line)

    return YAML_ERROR_MESSAGE.format(
        line_number=mark.line + 1, nice_error=nice_error, raw_error=error
    )


def safe_load(contents) -> Optional[Dict[str, Any]]:
    return yaml.load(contents, Loader=UniqueKeyLoader)


def load_yaml_text(contents, path=None):
    try:
        return safe_load(contents)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if hasattr(e, "problem_mark"):
            error = contextualized_yaml_error(contents, e)
        else:
            error = str(e)

        raise dbt.exceptions.ValidationException(error)
    except dbt.exceptions.DuplicateYamlKeyException as e:
        # TODO: We may want to raise an exception instead of a warning in the future.
        e.msg = f"{e} {path.searched_path}/{path.relative_path}."
        dbt.exceptions.warn_or_raise(e, log_fmt=warning_tag("{}"))
