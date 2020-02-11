import itertools
import os

from abc import abstractmethod
from typing import (
    Iterable, Dict, Any, Union, List, Optional, Generic, TypeVar, Type
)

from hologram import ValidationError

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import get_rendered
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config import RuntimeConfig, ConfigRenderer
from dbt.context.docs import generate_parser_docs
from dbt.context.target import generate_target_context
from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.parsed import (
    ParsedNodePatch,
    ParsedSourceDefinition,
    ColumnInfo,
    ParsedTestNode,
    ParsedMacroPatch,
)
from dbt.contracts.graph.unparsed import (
    UnparsedSourceDefinition, UnparsedNodeUpdate, UnparsedColumn,
    UnparsedMacroUpdate, UnparsedAnalysisUpdate,
    UnparsedSourceTableDefinition, FreshnessThreshold,
)
from dbt.exceptions import (
    validator_error_message, JSONValidationException,
    raise_invalid_schema_yml_version, ValidationException, CompilationException
)
from dbt.node_types import NodeType
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock, FilesystemSearcher
from dbt.parser.schema_test_builders import (
    TestBuilder, SourceTarget, Target, SchemaTestBlock, TargetBlock, YamlBlock,
    TestBlock,
)
from dbt.utils import get_pseudo_test_path, coerce_dict_str


UnparsedSchemaYaml = Union[
    UnparsedSourceDefinition,
    UnparsedNodeUpdate,
    UnparsedAnalysisUpdate,
    UnparsedMacroUpdate,
]

TestDef = Union[str, Dict[str, Any]]


def error_context(
    path: str,
    key: str,
    data: Any,
    cause: Union[str, ValidationException, JSONValidationException]
) -> str:
    """Provide contextual information about an error while parsing
    """
    if isinstance(cause, str):
        reason = cause
    elif isinstance(cause, ValidationError):
        reason = validator_error_message(cause)
    else:
        reason = cause.msg
    return (
        'Invalid {key} config given in {path} @ {key}: {data} - {reason}'
        .format(key=key, path=path, data=data, reason=reason)
    )


class ParserRef:
    """A helper object to hold parse-time references."""
    def __init__(self):
        self.column_info: Dict[str, ColumnInfo] = {}

    def add(self, column: UnparsedColumn, description, data_type, meta):
        self.column_info[column.name] = ColumnInfo(
            name=column.name,
            description=description,
            data_type=data_type,
            meta=meta,
            tags=column.tags,
        )


def column_info(
    config: RuntimeConfig,
    target: UnparsedSchemaYaml,
    *descriptions: str,
) -> None:
    context = generate_parser_docs(config, target)
    for description in descriptions:
        get_rendered(description, context)


def _trimmed(inp: str) -> str:
    if len(inp) < 50:
        return inp
    return inp[:44] + '...' + inp[-3:]


class SchemaParser(SimpleParser[SchemaTestBlock, ParsedTestNode]):
    """
    The schema parser is really big because schemas are really complicated!

    There are basically three phases to the schema parser:
        - read_yaml_{models,sources}: read in yaml as a dictionary, then
            validate it against the basic structures required so we can start
            parsing (NodeTarget, SourceTarget)
            - these return potentially many Targets per yaml block, since each
              source can have multiple tables
        - parse_target_{model,source}: Read in the underlying target, parse and
            return a list of all its tests (model and column tests), collect
            any refs/descriptions, and return a parsed entity with the
            appropriate information.
    """
    @classmethod
    def get_compiled_path(cls, block: FileBlock) -> str:
        # should this raise an error?
        return block.path.relative_path

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Test

    def get_paths(self):
        return FilesystemSearcher(
            self.project, self.project.all_source_paths, '.yml'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedTestNode:
        return ParsedTestNode.from_dict(dct, validate=validate)

    def _parse_format_version(
        self, yaml: YamlBlock
    ) -> None:
        path = yaml.path.relative_path
        if 'version' not in yaml.data:
            raise_invalid_schema_yml_version(path, 'no version is specified')

        version = yaml.data['version']
        # if it's not an integer, the version is malformed, or not
        # set. Either way, only 'version: 2' is supported.
        if not isinstance(version, int):
            raise_invalid_schema_yml_version(
                path, 'the version is not an integer'
            )
        if version != 2:
            raise_invalid_schema_yml_version(
                path, 'version {} is not supported'.format(version)
            )

    def _yaml_from_file(
        self, source_file: SourceFile
    ) -> Optional[Dict[str, Any]]:
        """If loading the yaml fails, raise an exception.
        """
        path: str = source_file.path.relative_path
        try:
            return load_yaml_text(source_file.contents)
        except ValidationException as e:
            reason = validator_error_message(e)
            raise CompilationException(
                'Error reading {}: {} - {}'
                .format(self.project.project_name, path, reason)
            )
        return None

    def parse_column_tests(
        self, block: TestBlock, column: UnparsedColumn
    ) -> None:
        if not column.tests:
            return

        for test in column.tests:
            self.parse_test(block, test, column)

    def parse_node(self, block: SchemaTestBlock) -> ParsedTestNode:
        """In schema parsing, we rewrite most of the part of parse_node that
        builds the initial node to be parsed, but rendering is basically the
        same
        """
        render_ctx = generate_target_context(
            self.root_project, self.root_project.cli_vars
        )
        builder = TestBuilder[Target](
            test=block.test,
            target=block.target,
            column_name=block.column_name,
            package_name=self.project.project_name,
            render_ctx=render_ctx,
        )

        original_name = os.path.basename(block.path.original_file_path)
        compiled_path = get_pseudo_test_path(
            builder.compiled_name, original_name, 'schema_test',
        )
        fqn_path = get_pseudo_test_path(
            builder.fqn_name, original_name, 'schema_test',
        )
        # the fqn for tests actually happens in the test target's name, which
        # is not necessarily this package's name
        fqn = self.get_fqn(fqn_path, builder.fqn_name)

        config = self.initial_config(fqn)

        metadata = {
            'namespace': builder.namespace,
            'name': builder.name,
            'kwargs': builder.args,
        }

        # copy - we don't want to mutate the tags!
        tags = block.tags[:]
        tags.extend(builder.tags())
        if 'schema' not in tags:
            tags.append('schema')

        node = self._create_parsetime_node(
            block=block,
            path=compiled_path,
            config=config,
            tags=tags,
            name=builder.fqn_name,
            raw_sql=builder.build_raw_sql(),
            column_name=block.column_name,
            test_metadata=metadata,
        )
        self.render_update(node, config)
        self.add_result_node(block, node)
        return node

    def parse_test(
        self,
        target_block: TestBlock,
        test: TestDef,
        column: Optional[UnparsedColumn],
    ) -> None:
        if isinstance(test, str):
            test = {test: {}}

        if column is None:
            column_name: Optional[str] = None
            column_tags: List[str] = []
        else:
            column_name = column.name
            should_quote = (
                column.quote or
                (column.quote is None and target_block.quote_columns)
            )
            if should_quote:
                column_name = get_adapter(self.root_project).quote(column_name)
            column_tags = column.tags

        block = SchemaTestBlock.from_test_block(
            src=target_block,
            test=test,
            column_name=column_name,
            tags=column_tags,
        )
        try:
            self.parse_node(block)
        except CompilationException as exc:
            context = _trimmed(str(block.target))
            msg = (
                'Invalid test config given in {}:'
                '\n\t{}\n\t@: {}'
                .format(block.path.original_file_path, exc.msg, context)
            )
            raise CompilationException(msg) from exc

    def parse_tests(self, block: TestBlock) -> None:
        for column in block.columns:
            self.parse_column_tests(block, column)

        for test in block.tests:
            self.parse_test(block, test, None)

    def parse_file(self, block: FileBlock) -> None:
        dct = self._yaml_from_file(block.file)
        # mark the file as seen, even if there are no macros in it
        self.results.get_file(block.file)
        if dct:
            yaml_block = YamlBlock.from_file_block(block, dct)

            self._parse_format_version(yaml_block)

            parser: YamlDocsReader
            for key in NodeType.documentable():
                plural = key.pluralize()
                if key == NodeType.Source:
                    parser = SourceParser(self, yaml_block, plural)
                elif key == NodeType.Macro:
                    parser = MacroPatchParser(self, yaml_block, plural)
                elif key == NodeType.Analysis:
                    parser = AnalysisPatchParser(self, yaml_block, plural)
                else:
                    parser = TestablePatchParser(self, yaml_block, plural)
                for test_block in parser.parse():
                    self.parse_tests(test_block)


Parsed = TypeVar(
    'Parsed',
    ParsedSourceDefinition, ParsedNodePatch, ParsedMacroPatch
)
NodeTarget = TypeVar(
    'NodeTarget',
    UnparsedNodeUpdate, UnparsedAnalysisUpdate
)
NonSourceTarget = TypeVar(
    'NonSourceTarget',
    UnparsedNodeUpdate, UnparsedAnalysisUpdate, UnparsedMacroUpdate
)


class YamlDocsReader(Generic[Target, Parsed]):
    def __init__(
        self, schema_parser: SchemaParser, yaml: YamlBlock, key: str
    ) -> None:
        self.schema_parser = schema_parser
        self.key = key
        self.yaml = yaml

    @property
    def results(self):
        return self.schema_parser.results

    @property
    def project(self):
        return self.schema_parser.project

    @property
    def default_database(self):
        return self.schema_parser.default_database

    @property
    def root_project(self):
        return self.schema_parser.root_project

    def get_key_dicts(self) -> Iterable[Dict[str, Any]]:
        data = self.yaml.data.get(self.key, [])
        if not isinstance(data, list):
            raise CompilationException(
                '{} must be a list, got {} instead: ({})'
                .format(self.key, type(data), _trimmed(str(data)))
            )
        path = self.yaml.path.original_file_path

        for entry in data:
            if coerce_dict_str(entry) is not None:
                yield entry
            else:
                msg = error_context(
                    path, self.key, data, 'expected a dict with string keys'
                )
                raise CompilationException(msg)

    def parse_docs(self, block: TargetBlock) -> ParserRef:
        refs = ParserRef()
        for column in block.columns:
            description = column.description
            data_type = column.data_type
            meta = column.meta
            column_info(
                self.root_project,
                block.target,
                description,
            )

            refs.add(column, description, data_type, meta)
        return refs

    @abstractmethod
    def get_unparsed_target(self) -> Iterable[Target]:
        raise NotImplementedError('get_unparsed_target is abstract')

    @abstractmethod
    def get_block(self, node: Target) -> TargetBlock:
        raise NotImplementedError('get_block is abstract')

    @abstractmethod
    def parse_patch(
        self, block: TargetBlock[Target], refs: ParserRef
    ) -> None:
        raise NotImplementedError('parse_patch is abstract')

    def parse(self) -> List[TestBlock]:
        node: Target
        test_blocks: List[TestBlock] = []
        for node in self.get_unparsed_target():
            node_block = self.get_block(node)
            if isinstance(node_block, TestBlock):
                test_blocks.append(node_block)
            refs = self.parse_docs(node_block)
            self.parse_patch(node_block, refs)
        return test_blocks


class YamlParser(Generic[Target, Parsed]):
    def __init__(
        self, schema_parser: SchemaParser, yaml: YamlBlock, key: str
    ) -> None:
        self.schema_parser = schema_parser
        self.key = key
        self.yaml = yaml

    @property
    def results(self):
        return self.schema_parser.results

    @property
    def project(self):
        return self.schema_parser.project

    @property
    def default_database(self):
        return self.schema_parser.default_database

    @property
    def root_project(self):
        return self.schema_parser.root_project

    def get_key_dicts(self) -> Iterable[Dict[str, Any]]:
        data = self.yaml.data.get(self.key, [])
        if not isinstance(data, list):
            raise CompilationException(
                '{} must be a list, got {} instead: ({})'
                .format(self.key, type(data), _trimmed(str(data)))
            )
        path = self.yaml.path.original_file_path

        for entry in data:
            if coerce_dict_str(entry) is not None:
                yield entry
            else:
                msg = error_context(
                    path, self.key, data, 'expected a dict with string keys'
                )
                raise CompilationException(msg)

    def parse_docs(self, block: TargetBlock) -> ParserRef:
        refs = ParserRef()
        for column in block.columns:
            description = column.description
            data_type = column.data_type
            meta = column.meta
            column_info(
                self.root_project, block.target, description
            )

            refs.add(column, description, data_type, meta)
        return refs

    def parse(self):
        node: Target
        for node in self.get_unparsed_target():
            node_block = TargetBlock.from_yaml_block(self.yaml, node)
            refs = self.parse_docs(node_block)
            self.parse_tests(node_block)
            self.parse_patch(node_block, refs)

    def parse_tests(self, target: TargetBlock[Target]) -> None:
        # some yaml parsers just don't have tests (macros, analyses)
        pass

    @abstractmethod
    def get_unparsed_target(self) -> Iterable[Target]:
        raise NotImplementedError('get_unparsed_target is abstract')

    @abstractmethod
    def parse_patch(
        self, block: TargetBlock[Target], refs: ParserRef
    ) -> None:
        raise NotImplementedError('parse_patch is abstract')


class SourceParser(YamlDocsReader[SourceTarget, ParsedSourceDefinition]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._renderer = ConfigRenderer(
            generate_target_context(
                self.root_project, self.root_project.cli_vars
            )
        )

    def get_block(self, node: SourceTarget) -> TestBlock:
        return TestBlock.from_yaml_block(self.yaml, node)

    def get_unparsed_target(self) -> Iterable[SourceTarget]:
        path = self.yaml.path.original_file_path

        for data in self.get_key_dicts():
            try:
                data = self._renderer.render_schema_source(data)
                source = UnparsedSourceDefinition.from_dict(data)
            except (ValidationError, JSONValidationException) as exc:
                msg = error_context(path, self.key, data, exc)
                raise CompilationException(msg) from exc
            else:
                for table in source.tables:
                    yield SourceTarget(source, table)

    def _calculate_freshness(
        self,
        source: UnparsedSourceDefinition,
        table: UnparsedSourceTableDefinition,
    ) -> Optional[FreshnessThreshold]:
        # if both are non-none, merge them. If both are None, the freshness is
        # None. If just table.freshness is None, the user disabled freshness
        # for the table.
        # the result should be None as the user explicitly disabled freshness.
        if source.freshness is not None and table.freshness is not None:
            return source.freshness.merged(table.freshness)
        elif source.freshness is None and table.freshness is not None:
            return table.freshness
        else:
            return None

    def parse_patch(
        self, block: TargetBlock[SourceTarget], refs: ParserRef
    ) -> None:
        source = block.target.source
        table = block.target.table
        unique_id = '.'.join([
            NodeType.Source, self.project.project_name, source.name, table.name
        ])
        description = table.description or ''
        meta = table.meta or {}
        source_description = source.description or ''
        column_info(
            self.root_project, source, description, source_description
        )

        loaded_at_field = table.loaded_at_field or source.loaded_at_field

        freshness = self._calculate_freshness(source, table)
        quoting = source.quoting.merged(table.quoting)
        path = block.path.original_file_path
        source_meta = source.meta or {}

        # make sure we don't do duplicate tags from source + table
        tags = sorted(set(itertools.chain(source.tags, table.tags)))

        result = ParsedSourceDefinition(
            package_name=self.project.project_name,
            database=(source.database or self.default_database),
            schema=(source.schema or source.name),
            identifier=(table.identifier or table.name),
            root_path=self.project.project_root,
            path=path,
            original_file_path=path,
            columns=refs.column_info,
            unique_id=unique_id,
            name=table.name,
            description=description,
            external=table.external,
            source_name=source.name,
            source_description=source_description,
            source_meta=source_meta,
            meta=meta,
            loader=source.loader,
            loaded_at_field=loaded_at_field,
            freshness=freshness,
            quoting=quoting,
            resource_type=NodeType.Source,
            fqn=[self.project.project_name, source.name, table.name],
            tags=tags,
        )
        self.results.add_source(self.yaml.file, result)


class NonSourceParser(
    YamlDocsReader[NonSourceTarget, Parsed], Generic[NonSourceTarget, Parsed]
):
    def collect_column_info(
        self, block: TargetBlock[NonSourceTarget]
    ) -> str:
        description = block.target.description
        column_info(
            self.root_project, block.target, description
        )
        return description

    @abstractmethod
    def _target_type(self) -> Type[NonSourceTarget]:
        raise NotImplementedError('_unsafe_from_dict not implemented')

    def get_unparsed_target(self) -> Iterable[NonSourceTarget]:
        path = self.yaml.path.original_file_path

        for data in self.get_key_dicts():
            data.update({
                'original_file_path': path,
                'yaml_key': self.key,
                'package_name': self.project.project_name,
            })
            try:
                model = self._target_type().from_dict(data)
            except (ValidationError, JSONValidationException) as exc:
                msg = error_context(path, self.key, data, exc)
                raise CompilationException(msg) from exc
            else:
                yield model


class NodePatchParser(
    NonSourceParser[NodeTarget, ParsedNodePatch],
    Generic[NodeTarget]
):
    def parse_patch(
        self, block: TargetBlock[NodeTarget], refs: ParserRef
    ) -> None:
        description = self.collect_column_info(block)
        result = ParsedNodePatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            description=description,
            columns=refs.column_info,
            meta=block.target.meta,
            docs=block.target.docs,
        )
        self.results.add_patch(self.yaml.file, result)


class TestablePatchParser(NodePatchParser[UnparsedNodeUpdate]):
    def get_block(self, node: UnparsedNodeUpdate) -> TestBlock:
        return TestBlock.from_yaml_block(self.yaml, node)

    def _target_type(self) -> Type[UnparsedNodeUpdate]:
        return UnparsedNodeUpdate


class AnalysisPatchParser(NodePatchParser[UnparsedAnalysisUpdate]):
    def get_block(self, node: UnparsedAnalysisUpdate) -> TargetBlock:
        return TargetBlock.from_yaml_block(self.yaml, node)

    def _target_type(self) -> Type[UnparsedAnalysisUpdate]:
        return UnparsedAnalysisUpdate


class MacroPatchParser(NonSourceParser[UnparsedMacroUpdate, ParsedMacroPatch]):
    def collect_column_info(
        self, block: TargetBlock[UnparsedMacroUpdate]
    ) -> str:
        description = block.target.description
        arg_docs = [arg.description for arg in block.target.arguments]
        column_info(
            self.root_project, block.target, description, *arg_docs
        )
        return description

    def get_block(self, node: UnparsedMacroUpdate) -> TargetBlock:
        return TargetBlock.from_yaml_block(self.yaml, node)

    def _target_type(self) -> Type[UnparsedMacroUpdate]:
        return UnparsedMacroUpdate

    def parse_patch(
        self, block: TargetBlock[UnparsedMacroUpdate], refs: ParserRef
    ) -> None:
        description = self.collect_column_info(block)

        result = ParsedMacroPatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            arguments=block.target.arguments,
            description=description,
            meta=block.target.meta,
            docs=block.target.docs,
        )
        self.results.add_macro_patch(self.yaml.file, result)
