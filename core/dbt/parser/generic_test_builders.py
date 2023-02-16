import re
from copy import deepcopy
from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
    Dict,
    Any,
    Tuple,
    Optional,
    List,
)

from dbt.clients.jinja import get_rendered, GENERIC_TEST_KWARGS_NAME
from dbt.contracts.graph.nodes import UnpatchedSourceDefinition
from dbt.contracts.graph.unparsed import (
    TestDef,
    UnparsedAnalysisUpdate,
    UnparsedMacroUpdate,
    UnparsedNodeUpdate,
    UnparsedExposure,
)
from dbt.exceptions import (
    CustomMacroPopulatingConfigValueError,
    SameKeyNestedError,
    TagNotStringError,
    TagsNotListOfStringsError,
    TestArgIncludesModelError,
    TestArgsNotDictError,
    TestDefinitionDictLengthError,
    TestTypeError,
    TestNameNotStringError,
    UnexpectedTestNamePatternError,
    UndefinedMacroError,
)
from dbt.parser.search import FileBlock
from dbt.utils import md5


def synthesize_generic_test_names(
    test_type: str, test_name: str, args: Dict[str, Any]
) -> Tuple[str, str]:
    # Using the type, name, and arguments to this generic test, synthesize a (hopefully) unique name
    # Will not be unique if multiple tests have same name + arguments, and only configs differ
    # Returns a shorter version (hashed/truncated, for the compiled file)
    # as well as the full name (for the unique_id + FQN)
    flat_args = []
    for arg_name in sorted(args):
        # the model is already embedded in the name, so skip it
        if arg_name == "model":
            continue
        arg_val = args[arg_name]

        if isinstance(arg_val, dict):
            parts = list(arg_val.values())
        elif isinstance(arg_val, (list, tuple)):
            parts = list(arg_val)
        else:
            parts = [arg_val]

        flat_args.extend([str(part) for part in parts])

    clean_flat_args = [re.sub("[^0-9a-zA-Z_]+", "_", arg) for arg in flat_args]
    unique = "__".join(clean_flat_args)

    # for the file path + alias, the name must be <64 characters
    # if the full name is too long, include the first 30 identifying chars plus
    # a 32-character hash of the full contents

    test_identifier = "{}_{}".format(test_type, test_name)
    full_name = "{}_{}".format(test_identifier, unique)

    if len(full_name) >= 64:
        test_trunc_identifier = test_identifier[:30]
        label = md5(full_name)
        short_name = "{}_{}".format(test_trunc_identifier, label)
    else:
        short_name = full_name

    return short_name, full_name


@dataclass
class YamlBlock(FileBlock):
    data: Dict[str, Any]

    @classmethod
    def from_file_block(cls, src: FileBlock, data: Dict[str, Any]):
        return cls(
            file=src.file,
            data=data,
        )


Testable = TypeVar("Testable", UnparsedNodeUpdate, UnpatchedSourceDefinition)

ColumnTarget = TypeVar(
    "ColumnTarget",
    UnparsedNodeUpdate,
    UnparsedAnalysisUpdate,
    UnpatchedSourceDefinition,
)

Target = TypeVar(
    "Target",
    UnparsedNodeUpdate,
    UnparsedMacroUpdate,
    UnparsedAnalysisUpdate,
    UnpatchedSourceDefinition,
    UnparsedExposure,
)


@dataclass
class TargetBlock(YamlBlock, Generic[Target]):
    target: Target

    @property
    def name(self):
        return self.target.name

    @property
    def columns(self):
        return []

    @property
    def tests(self) -> List[TestDef]:
        return []

    @classmethod
    def from_yaml_block(cls, src: YamlBlock, target: Target) -> "TargetBlock[Target]":
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class TargetColumnsBlock(TargetBlock[ColumnTarget], Generic[ColumnTarget]):
    @property
    def columns(self):
        if self.target.columns is None:
            return []
        else:
            return self.target.columns


@dataclass
class TestBlock(TargetColumnsBlock[Testable], Generic[Testable]):
    @property
    def tests(self) -> List[TestDef]:
        if self.target.tests is None:
            return []
        else:
            return self.target.tests

    @property
    def quote_columns(self) -> Optional[bool]:
        return self.target.quote_columns

    @classmethod
    def from_yaml_block(cls, src: YamlBlock, target: Testable) -> "TestBlock[Testable]":
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class GenericTestBlock(TestBlock[Testable], Generic[Testable]):
    test: Dict[str, Any]
    column_name: Optional[str]
    tags: List[str]

    @classmethod
    def from_test_block(
        cls,
        src: TestBlock,
        test: Dict[str, Any],
        column_name: Optional[str],
        tags: List[str],
    ) -> "GenericTestBlock":
        return cls(
            file=src.file,
            data=src.data,
            target=src.target,
            test=test,
            column_name=column_name,
            tags=tags,
        )


class TestBuilder(Generic[Testable]):
    """An object to hold assorted test settings and perform basic parsing

    Test names have the following pattern:
        - the test name itself may be namespaced (package.test)
        - or it may not be namespaced (test)

    """

    # The 'test_name' is used to find the 'macro' that implements the test
    TEST_NAME_PATTERN = re.compile(
        r"((?P<test_namespace>([a-zA-Z_][0-9a-zA-Z_]*))\.)?"
        r"(?P<test_name>([a-zA-Z_][0-9a-zA-Z_]*))"
    )
    # args in the test entry representing test configs
    CONFIG_ARGS = (
        "severity",
        "tags",
        "enabled",
        "where",
        "limit",
        "warn_if",
        "error_if",
        "fail_calc",
        "store_failures",
        "meta",
        "database",
        "schema",
        "alias",
    )

    def __init__(
        self,
        test: Dict[str, Any],
        target: Testable,
        package_name: str,
        render_ctx: Dict[str, Any],
        column_name: str = None,
    ) -> None:
        test_name, test_args = self.extract_test_args(test, column_name)
        self.args: Dict[str, Any] = test_args
        if "model" in self.args:
            raise TestArgIncludesModelError()
        self.package_name: str = package_name
        self.target: Testable = target

        self.args["model"] = self.build_model_str()

        match = self.TEST_NAME_PATTERN.match(test_name)
        if match is None:
            raise UnexpectedTestNamePatternError(test_name)

        groups = match.groupdict()
        self.name: str = groups["test_name"]
        self.namespace: str = groups["test_namespace"]
        self.config: Dict[str, Any] = {}

        # This code removes keys identified as config args from the test entry
        # dictionary. The keys remaining in the 'args' dictionary will be
        # "kwargs", or keyword args that are passed to the test macro.
        # The "kwargs" are not rendered into strings until compilation time.
        # The "configs" are rendered here (since they were not rendered back
        # in the 'get_key_dicts' methods in the schema parsers).
        for key in self.CONFIG_ARGS:
            value = self.args.pop(key, None)
            # 'modifier' config could be either top level arg or in config
            if value and "config" in self.args and key in self.args["config"]:
                raise SameKeyNestedError()
            if not value and "config" in self.args:
                value = self.args["config"].pop(key, None)
            if isinstance(value, str):

                try:
                    value = get_rendered(value, render_ctx, native=True)
                except UndefinedMacroError as e:
                    raise CustomMacroPopulatingConfigValueError(
                        target_name=self.target.name,
                        column_name=column_name,
                        name=self.name,
                        key=key,
                        err_msg=e.msg,
                    )

            if value is not None:
                self.config[key] = value

        if "config" in self.args:
            del self.args["config"]

        if self.namespace is not None:
            self.package_name = self.namespace

        # If the user has provided a custom name for this generic test, use it
        # Then delete the "name" argument to avoid passing it into the test macro
        # Otherwise, use an auto-generated name synthesized from test inputs
        self.compiled_name: str = ""
        self.fqn_name: str = ""

        if "name" in self.args:
            # Assign the user-defined name here, which will be checked for uniqueness later
            # we will raise an error if two tests have same name for same model + column combo
            self.compiled_name = self.args["name"]
            self.fqn_name = self.args["name"]
            del self.args["name"]
        else:
            short_name, full_name = self.get_synthetic_test_names()
            self.compiled_name = short_name
            self.fqn_name = full_name
            # use hashed name as alias if full name is too long
            if short_name != full_name and "alias" not in self.config:
                self.config["alias"] = short_name

    def _bad_type(self) -> TypeError:
        return TypeError('invalid target type "{}"'.format(type(self.target)))

    @staticmethod
    def extract_test_args(test, name=None) -> Tuple[str, Dict[str, Any]]:
        if not isinstance(test, dict):
            raise TestTypeError(test)

        # If the test is a dictionary with top-level keys, the test name is "test_name"
        # and the rest are arguments
        # {'name': 'my_favorite_test', 'test_name': 'unique', 'config': {'where': '1=1'}}
        if "test_name" in test.keys():
            test_name = test.pop("test_name")
            test_args = test
        # If the test is a nested dictionary with one top-level key, the test name
        # is the dict name, and nested keys are arguments
        # {'unique': {'name': 'my_favorite_test', 'config': {'where': '1=1'}}}
        else:
            test = list(test.items())
            if len(test) != 1:
                raise TestDefinitionDictLengthError(test)
            test_name, test_args = test[0]

        if not isinstance(test_args, dict):
            raise TestArgsNotDictError(test_args)
        if not isinstance(test_name, str):
            raise TestNameNotStringError(test_name)
        test_args = deepcopy(test_args)
        if name is not None:
            test_args["column_name"] = name
        return test_name, test_args

    @property
    def enabled(self) -> Optional[bool]:
        return self.config.get("enabled")

    @property
    def alias(self) -> Optional[str]:
        return self.config.get("alias")

    @property
    def severity(self) -> Optional[str]:
        sev = self.config.get("severity")
        if sev:
            return sev.upper()
        else:
            return None

    @property
    def store_failures(self) -> Optional[bool]:
        return self.config.get("store_failures")

    @property
    def where(self) -> Optional[str]:
        return self.config.get("where")

    @property
    def limit(self) -> Optional[int]:
        return self.config.get("limit")

    @property
    def warn_if(self) -> Optional[str]:
        return self.config.get("warn_if")

    @property
    def error_if(self) -> Optional[str]:
        return self.config.get("error_if")

    @property
    def fail_calc(self) -> Optional[str]:
        return self.config.get("fail_calc")

    @property
    def meta(self) -> Optional[dict]:
        return self.config.get("meta")

    @property
    def database(self) -> Optional[str]:
        return self.config.get("database")

    @property
    def schema(self) -> Optional[str]:
        return self.config.get("schema")

    def get_static_config(self):
        config = {}
        if self.alias is not None:
            config["alias"] = self.alias
        if self.severity is not None:
            config["severity"] = self.severity
        if self.enabled is not None:
            config["enabled"] = self.enabled
        if self.where is not None:
            config["where"] = self.where
        if self.limit is not None:
            config["limit"] = self.limit
        if self.warn_if is not None:
            config["warn_if"] = self.warn_if
        if self.error_if is not None:
            config["error_if"] = self.error_if
        if self.fail_calc is not None:
            config["fail_calc"] = self.fail_calc
        if self.store_failures is not None:
            config["store_failures"] = self.store_failures
        if self.meta is not None:
            config["meta"] = self.meta
        if self.database is not None:
            config["database"] = self.database
        if self.schema is not None:
            config["schema"] = self.schema
        return config

    def tags(self) -> List[str]:
        tags = self.config.get("tags", [])
        if isinstance(tags, str):
            tags = [tags]
        if not isinstance(tags, list):
            raise TagsNotListOfStringsError(tags)
        for tag in tags:
            if not isinstance(tag, str):
                raise TagNotStringError(tag)
        return tags[:]

    def macro_name(self) -> str:
        macro_name = "test_{}".format(self.name)
        if self.namespace is not None:
            macro_name = "{}.{}".format(self.namespace, macro_name)
        return macro_name

    def get_synthetic_test_names(self) -> Tuple[str, str]:
        # Returns two names: shorter (for the compiled file), full (for the unique_id + FQN)
        if isinstance(self.target, UnparsedNodeUpdate):
            name = self.name
        elif isinstance(self.target, UnpatchedSourceDefinition):
            name = "source_" + self.name
        else:
            raise self._bad_type()
        if self.namespace is not None:
            name = "{}_{}".format(self.namespace, name)
        return synthesize_generic_test_names(name, self.target.name, self.args)

    def construct_config(self) -> str:
        configs = ",".join(
            [
                f"{key}="
                + (
                    ('"' + value.replace('"', '\\"') + '"')
                    if isinstance(value, str)
                    else str(value)
                )
                for key, value in self.config.items()
            ]
        )
        if configs:
            return f"{{{{ config({configs}) }}}}"
        else:
            return ""

    # this is the 'raw_code' that's used in 'render_update' and execution
    # of the test macro
    def build_raw_code(self) -> str:
        return ("{{{{ {macro}(**{kwargs_name}) }}}}{config}").format(
            macro=self.macro_name(),
            config=self.construct_config(),
            kwargs_name=GENERIC_TEST_KWARGS_NAME,
        )

    def build_model_str(self):
        targ = self.target
        if isinstance(self.target, UnparsedNodeUpdate):
            target_str = f"ref('{targ.name}')"
        elif isinstance(self.target, UnpatchedSourceDefinition):
            target_str = f"source('{targ.source.name}', '{targ.table.name}')"
        return f"{{{{ get_where_subquery({target_str}) }}}}"
