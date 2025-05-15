import collections
import dataclasses
from typing import Any, Dict, List, Optional, Tuple

import yaml

from dbt import deprecations
from dbt.clients.yaml_helper import load_yaml_text

# the C version is faster, but it doesn't always exist
try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore  # noqa: F401


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
            is_override = (
                len(node.value) > 0
                and len(node.value[0]) > 0
                and getattr(node.value[0][0], "value") == "<<"
            )
            self.flatten_mapping(node)
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

                if not is_override and key in mapping:
                    start_mark = str(key_node.start_mark)
                    if start_mark.startswith("  in"):  # this means it was at the top level
                        message = f"Duplicate key '{key}' {start_mark.lstrip()}"
                    else:
                        message = f"Duplicate key '{key}' at {key_node.start_mark}"

                    self.check_failures.append(YamlCheckFailure("duplicate_key", message))

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
