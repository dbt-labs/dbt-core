import hashlib
import re
from dataclasses import dataclass
from typing import Generic, TypeVar, Dict, Any, Tuple, Optional, List, Union

from dbt.clients.jinja import get_rendered
from dbt.contracts.graph.unparsed import (
    UnparsedNodeUpdate, UnparsedSourceDefinition,
    UnparsedSourceTableDefinition, NamedTested
)
from dbt.exceptions import raise_compiler_error
from dbt.parser.search import FileBlock


def get_nice_schema_test_name(
    test_type: str, test_name: str, args: Dict[str, Any]
) -> Tuple[str, str]:
    flat_args = []
    for arg_name in sorted(args):
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

    cutoff = 32
    if len(unique) <= cutoff:
        label = unique
    else:
        label = hashlib.md5(unique.encode('utf-8')).hexdigest()

    filename = '{}_{}_{}'.format(test_type, test_name, label)
    name = '{}_{}_{}'.format(test_type, test_name, unique)

    return filename, name


def as_kwarg(key: str, value: Any) -> str:
    test_value = str(value)
    is_function = re.match(r'^\s*(env_var|ref|var|source|doc)\s*\(.+\)\s*$',
                           test_value)

    # if the value is a function, don't wrap it in quotes!
    if is_function:
        formatted_value = value
    else:
        formatted_value = value.__repr__()

    return "{key}={value}".format(key=key, value=formatted_value)


@dataclass
class YamlBlock(FileBlock):
    data: Dict[str, Any]

    @classmethod
    def from_file_block(cls, src: FileBlock, data: Dict[str, Any]):
        return cls(
            file=src.file,
            data=data,
        )


@dataclass
class SourceTarget:
    source: UnparsedSourceDefinition
    table: UnparsedSourceTableDefinition

    @property
    def name(self) -> str:
        return '{0.name}_{1.name}'.format(self.source, self.table)

    @property
    def columns(self) -> List[NamedTested]:
        if self.table.columns is None:
            return []
        else:
            return self.table.columns

    @property
    def tests(self) -> List[Union[Dict[str, Any], str]]:
        if self.table.tests is None:
            return []
        else:
            return self.table.tests


NodeTarget = UnparsedNodeUpdate


Target = TypeVar('Target', NodeTarget, SourceTarget)


@dataclass
class TargetBlock(YamlBlock, Generic[Target]):
    target: Target

    @property
    def name(self):
        return self.target.name

    @property
    def columns(self):
        if self.target.columns is None:
            return []
        else:
            return self.target.columns

    @property
    def tests(self) -> List[Union[Dict[str, Any], str]]:
        if self.target.tests is None:
            return []
        else:
            return self.target.tests

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
class SchemaTestBlock(TargetBlock):
    test: Dict[str, Any]
    column_name: Optional[str]

    @classmethod
    def from_target_block(
        cls, src: TargetBlock, test: Dict[str, Any], column_name: Optional[str]
    ) -> 'SchemaTestBlock':
        return cls(
            file=src.file,
            data=src.data,
            target=src.target,
            test=test,
            column_name=column_name
        )


class TestBuilder(Generic[Target]):
    """An object to hold assorted test settings and perform basic parsing

    Test names have the following pattern:
        - the test name itself may be namespaced (package.test)
        - or it may not be namespaced (test)
        - the test may have arguments embedded in the name (, severity=WARN)
        - or it may not have arguments.

    """
    TEST_NAME_PATTERN = re.compile(
        r'((?P<test_namespace>([a-zA-Z_][0-9a-zA-Z_]*))\.)?'
        r'(?P<test_name>([a-zA-Z_][0-9a-zA-Z_]*))'
    )
    # map magic keys to default values
    MODIFIER_ARGS = {'severity': 'ERROR'}

    def __init__(
        self,
        test: Dict[str, Any],
        target: Target,
        package_name: str,
        render_ctx: Dict[str, Any],
        column_name: str = None,
    ) -> None:
        test_name, test_args = self.extract_test_args(test, column_name)
        self.args: Dict[str, Any] = test_args
        self.package_name: str = package_name
        self.target: Target = target

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
        for key, default in self.MODIFIER_ARGS.items():
            value = self.args.pop(key, default)
            if isinstance(value, str):
                value = get_rendered(value, render_ctx)
            self.modifiers[key] = value

        if self.namespace is not None:
            self.package_name = self.namespace

        compiled_name, fqn_name = self.get_test_name()
        self.compiled_name: str = compiled_name
        self.fqn_name: str = fqn_name

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
        if name is not None:
            test_args['column_name'] = name
        return test_name, test_args

    def severity(self) -> str:
        return self.modifiers.get('severity', 'ERROR').upper()

    def test_kwargs_str(self) -> str:
        # sort the dict so the keys are rendered deterministically (for tests)
        return ', '.join((
            as_kwarg(key, self.args[key])
            for key in sorted(self.args)
        ))

    def macro_name(self) -> str:
        macro_name = 'test_{}'.format(self.name)
        if self.namespace is not None:
            macro_name = "{}.{}".format(self.namespace, macro_name)
        return macro_name

    def describe_test_target(self) -> str:
        if isinstance(self.target, NodeTarget):
            fmt = "model('{0}')"
        elif isinstance(self.target, SourceTarget):
            fmt = "source('{0.source}', '{0.table}')"
        else:
            raise self._bad_type()
        return fmt.format(self.target)

        raise NotImplementedError('describe_test_target not implemented!')

    def get_test_name(self) -> Tuple[str, str]:
        if isinstance(self.target, NodeTarget):
            name = self.name
        elif isinstance(self.target, SourceTarget):
            name = 'source_' + self.name
        else:
            raise self._bad_type()
        if self.namespace is not None:
            name = '{}_{}'.format(self.namespace, name)
        return get_nice_schema_test_name(name, self.target.name, self.args)

    def build_raw_sql(self) -> str:
        return (
            "{{{{ config(severity='{severity}') }}}}"
            "{{{{ {macro}(model={model}, {kwargs}) }}}}"
        ).format(
            model=self.build_model_str(),
            macro=self.macro_name(),
            kwargs=self.test_kwargs_str(),
            severity=self.severity()
        )

    def build_model_str(self):
        if isinstance(self.target, NodeTarget):
            fmt = "ref('{0.name}')"
        elif isinstance(self.target, SourceTarget):
            fmt = "source('{0.source.name}', '{0.table.name}')"
        else:
            raise self._bad_type()
        return fmt.format(self.target)
