import collections
import dataclasses
from typing import Any, Dict, List, Optional, Tuple

import yaml

import dbt_common.exceptions
import dbt_common.exceptions.base
from dbt import deprecations

# the C version is faster, but it doesn't always exist
try:
    from yaml import CDumper as Dumper
    from yaml import CLoader as Loader
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import Dumper, Loader, SafeLoader  # type: ignore  # noqa: F401


YAML_ERROR_MESSAGE = """
Syntax error near line {line_number}
------------------------------
{nice_error}

Raw Error:
------------------------------
{raw_error}
""".strip()


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
    return yaml.load(contents, Loader=SafeLoader)


def load_yaml_text(contents, path=None, loader=SafeLoader) -> Optional[Dict[str, Any]]:
    try:
        return yaml.load(contents, loader)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if hasattr(e, "problem_mark"):
            error = contextualized_yaml_error(contents, e)
        else:
            error = str(e)

        raise dbt_common.exceptions.base.DbtValidationError(error)


@dataclasses.dataclass
class YamlCheckFailure:
    failure_type: str
    message: str


def checked_load(contents) -> Tuple[Optional[Dict[str, Any]], List[YamlCheckFailure]]:
    # A hacky (but sadly justified) method for modifying a bit of PyYAML. We create
    # a new local subclass of SafeLoader, since we need to associate state with
    # the static class, but static classes do not have non-static state. This allows
    # us to be sure we have exclusive access to the class.
    class CheckedLoader(SafeLoader):
        check_failures: List[YamlCheckFailure] = []

        def construct_mapping(self, node, deep=False):
            if not isinstance(node, yaml.MappingNode):
                raise yaml.constructor.ConstructorError(
                    None, None, "expected a mapping node, but found %s" % node.id, node.start_mark
                )
            mapping = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if not isinstance(key, collections.abc.Hashable):
                    raise yaml.constructor.ConstructorError(
                        "while constructing a mapping",
                        node.start_mark,
                        "found unhashable key",
                        key_node.start_mark,
                    )
                value = self.construct_object(value_node, deep=deep)

                if key in mapping:
                    self.check_failures.append(
                        YamlCheckFailure(
                            "duplicate_key", f"Duplicate key '{key}' at {key_node.start_mark}"
                        )
                    )

                mapping[key] = value
            return mapping

    CheckedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, CheckedLoader.construct_mapping
    )

    dct = load_yaml_text(contents, loader=CheckedLoader)
    check_failures = CheckedLoader.check_failures

    return (dct, check_failures)


def issue_deprecation_warnings_for_failures(failures: List[YamlCheckFailure], file: str):
    for failure in failures:
        if failure.failure_type == "duplicate_key":
            deprecations.warn(
                "duplicate-yaml-keys-deprecation",
                duplicate_description=failure.message,
                file=file,
            )
