import hashlib
import re
from copy import deepcopy
from dataclasses import dataclass
from typing import (
    Generic, TypeVar, Dict, Any, Tuple, Optional, List,
)

from dbt.clients.jinja import get_rendered, SCHEMA_TEST_KWARGS_NAME
from dbt.contracts.graph.parsed import UnpatchedSourceDefinition
from dbt.contracts.graph.unparsed import (
    TestDef,
    UnparsedAnalysisUpdate,
    UnparsedMacroUpdate,
    UnparsedNodeUpdate,
    UnparsedExposure,
)
from dbt.exceptions import raise_compiler_error
from dbt.parser.search import FileBlock


def get_nice_schema_test_name(
    test_type: str, test_name: str, args: Dict[str, Any]
) -> Tuple[str, str]:
    flat_args = []
    for arg_name in sorted(args):
        # the model is already embedded in the name, so skip it
        if arg_name == 'model':
            continue
        arg_val = args[arg_name]

        if isinstance(arg_val, dict):
            parts = list(arg_val.values())
        elif isinstance(arg_val, (list, tuple)):
            parts = list(arg_val)
        else:
            parts = [arg_val]

        flat_args.extend([str(part) for part in parts])

    clean_flat_args = [re.sub('[^0-9a-zA-Z_]+', '_', arg) for arg in flat_args]
    unique = "__".join(clean_flat_args)

    # for the file path + alias, the name must be <64 characters
    # if the full name is too long, include the first 30 identifying chars plus
    # a 32-character hash of the full contents

    test_identifier = '{}_{}'.format(test_type, test_name)
    full_name = '{}_{}'.format(test_identifier, unique)

    if len(full_name) >= 64:
        test_trunc_identifier = test_identifier[:30]
        label = hashlib.md5(full_name.encode('utf-8')).hexdigest()
        short_name = '{}_{}'.format(test_trunc_identifier, label)
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


Testable = TypeVar(
    'Testable', UnparsedNodeUpdate, UnpatchedSourceDefinition
)

ColumnTarget = TypeVar(
    'ColumnTarget',
    UnparsedNodeUpdate,
    UnparsedAnalysisUpdate,
    UnpatchedSourceDefinition,
)

Target = TypeVar(
    'Target',
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
    def from_yaml_block(
        cls, src: YamlBlock, target: Target
    ) -> 'TargetBlock[Target]':
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
    def from_yaml_block(
        cls, src: YamlBlock, target: Testable
    ) -> 'TestBlock[Testable]':
        return cls(
            file=src.file,
            data=src.data,
            target=target,
        )


@dataclass
class SchemaTestBlock(TestBlock[Testable], Generic[Testable]):
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
    ) -> 'SchemaTestBlock':
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
        r'((?P<test_namespace>([a-zA-Z_][0-9a-zA-Z_]*))\.)?'
        r'(?P<test_name>([a-zA-Z_][0-9a-zA-Z_]*))'
    )
    # kwargs representing test configs
    MODIFIER_ARGS = (
        'severity', 'tags', 'enabled', 'where', 'limit', 'warn_if', 'error_if',
        'fail_calc', 'store_failures'
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
        if 'model' in self.args:
            raise_compiler_error(
                'Test arguments include "model", which is a reserved argument',
            )
        self.package_name: str = package_name
        self.target: Testable = target

        self.args['model'] = self.build_model_str()

        match = self.TEST_NAME_PATTERN.match(test_name)
        if match is None:
            raise_compiler_error(
                'Test name string did not match expected pattern: {}'
                .format(test_name)
            )

        groups = match.groupdict()
        self.name: str = groups['test_name']
        self.namespace: str = groups['test_namespace']
        self.modifiers: Dict[str, Any] = {}
        for key in self.MODIFIER_ARGS:
            value = self.args.pop(key, None)
            if isinstance(value, str):
                value = get_rendered(value, render_ctx, native=True)
            if value is not None:
                self.modifiers[key] = value

        if self.namespace is not None:
            self.package_name = self.namespace

        compiled_name, fqn_name = self.get_test_name()
        self.compiled_name: str = compiled_name
        self.fqn_name: str = fqn_name

        # use hashed name as alias if too long
        if compiled_name != fqn_name:
            self.modifiers['alias'] = compiled_name

    def _bad_type(self) -> TypeError:
        return TypeError('invalid target type "{}"'.format(type(self.target)))

    @staticmethod
    def extract_test_args(test, name=None) -> Tuple[str, Dict[str, Any]]:
        if not isinstance(test, dict):
            raise_compiler_error(
                'test must be dict or str, got {} (value {})'.format(
                    type(test), test
                )
            )

        test = list(test.items())
        if len(test) != 1:
            raise_compiler_error(
                'test definition dictionary must have exactly one key, got'
                ' {} instead ({} keys)'.format(test, len(test))
            )
        test_name, test_args = test[0]

        if not isinstance(test_args, dict):
            raise_compiler_error(
                'test arguments must be dict, got {} (value {})'.format(
                    type(test_args), test_args
                )
            )
        if not isinstance(test_name, str):
            raise_compiler_error(
                'test name must be a str, got {} (value {})'.format(
                    type(test_name), test_name
                )
            )
        test_args = deepcopy(test_args)
        if name is not None:
            test_args['column_name'] = name
        return test_name, test_args

    @property
    def enabled(self) -> Optional[bool]:
        return self.modifiers.get('enabled')

    @property
    def alias(self) -> Optional[str]:
        return self.modifiers.get('alias')

    @property
    def severity(self) -> Optional[str]:
        sev = self.modifiers.get('severity')
        if sev:
            return sev.upper()
        else:
            return None

    @property
    def store_failures(self) -> Optional[bool]:
        return self.modifiers.get('store_failures')

    @property
    def where(self) -> Optional[str]:
        return self.modifiers.get('where')

    @property
    def limit(self) -> Optional[int]:
        return self.modifiers.get('limit')

    @property
    def warn_if(self) -> Optional[str]:
        return self.modifiers.get('warn_if')

    @property
    def error_if(self) -> Optional[str]:
        return self.modifiers.get('error_if')

    @property
    def fail_calc(self) -> Optional[str]:
        return self.modifiers.get('fail_calc')

    def tags(self) -> List[str]:
        tags = self.modifiers.get('tags', [])
        if isinstance(tags, str):
            tags = [tags]
        if not isinstance(tags, list):
            raise_compiler_error(
                f'got {tags} ({type(tags)}) for tags, expected a list of '
                f'strings'
            )
        for tag in tags:
            if not isinstance(tag, str):
                raise_compiler_error(
                    f'got {tag} ({type(tag)}) for tag, expected a str'
                )
        return tags[:]

    def macro_name(self) -> str:
        macro_name = 'test_{}'.format(self.name)
        if self.namespace is not None:
            macro_name = "{}.{}".format(self.namespace, macro_name)
        return macro_name

    def get_test_name(self) -> Tuple[str, str]:
        if isinstance(self.target, UnparsedNodeUpdate):
            name = self.name
        elif isinstance(self.target, UnpatchedSourceDefinition):
            name = 'source_' + self.name
        else:
            raise self._bad_type()
        if self.namespace is not None:
            name = '{}_{}'.format(self.namespace, name)
        return get_nice_schema_test_name(name, self.target.name, self.args)

    def construct_config(self) -> str:
        configs = ",".join([
            f"{key}=" + (
                ("\"" + value.replace('\"', '\\\"') + "\"") if isinstance(value, str)
                else str(value)
            )
            for key, value
            in self.modifiers.items()
        ])
        if configs:
            return f"{{{{ config({configs}) }}}}"
        else:
            return ""

    # this is the 'raw_sql' that's used in 'render_update' and execution
    # of the test macro
    def build_raw_sql(self) -> str:
        return (
            "{{{{ {macro}(**{kwargs_name}) }}}}{config}"
        ).format(
            macro=self.macro_name(),
            config=self.construct_config(),
            kwargs_name=SCHEMA_TEST_KWARGS_NAME,
        )

    def build_model_str(self):
        targ = self.target
        cfg_where = "config.get('where')"
        if isinstance(self.target, UnparsedNodeUpdate):
            identifier = self.target.name
            target_str = f"{{{{ ref('{targ.name}') }}}}"
        elif isinstance(self.target, UnpatchedSourceDefinition):
            identifier = self.target.table.name
            target_str = f"{{{{ source('{targ.source.name}', '{targ.table.name}') }}}}"
        filtered = f"(select * from {target_str} where {{{{{cfg_where}}}}}) {identifier}"
        return f"{{% if {cfg_where} %}}{filtered}{{% else %}}{target_str}{{% endif %}}"
