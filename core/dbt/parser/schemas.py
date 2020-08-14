import itertools
import os

from abc import ABCMeta, abstractmethod
from typing import (
    Iterable, Dict, Any, Union, List, Optional, Generic, TypeVar, Type
)

from hologram import ValidationError, JsonSchemaMixin

from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import get_rendered, add_rendered_test_kwargs
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SchemaYamlRenderer
from dbt.context.context_config import (
    ContextConfigType,
    ContextConfigGenerator,
)
from dbt.context.configured import generate_schema_yml
from dbt.context.target import generate_target_context
from dbt.contracts.files import FileHash
from dbt.contracts.graph.manifest import SourceFile
from dbt.contracts.graph.model_config import SourceConfig
from dbt.contracts.graph.parsed import (
    ParsedNodePatch,
    ParsedSourceDefinition,
    ColumnInfo,
    ParsedSchemaTestNode,
    ParsedMacroPatch,
    UnpatchedSourceDefinition,
)
from dbt.contracts.graph.unparsed import (
    UnparsedSourceDefinition, UnparsedNodeUpdate, UnparsedColumn,
    UnparsedMacroUpdate, UnparsedAnalysisUpdate, SourcePatch,
    HasDocs, HasColumnDocs, HasColumnTests, FreshnessThreshold,
)
from dbt.exceptions import (
    validator_error_message, JSONValidationException,
    raise_invalid_schema_yml_version, ValidationException,
    CompilationException, warn_or_error, InternalException
)
from dbt.node_types import NodeType
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock, FilesystemSearcher
from dbt.parser.schema_test_builders import (
    TestBuilder, SchemaTestBlock, TargetBlock, YamlBlock,
    TestBlock, Testable
)
from dbt.utils import (
    get_pseudo_test_path, coerce_dict_str
)


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

    def add(
        self,
        column: Union[HasDocs, UnparsedColumn],
        description: str,
        data_type: Optional[str],
        meta: Dict[str, Any],
    ):
        tags: List[str] = []
        tags.extend(getattr(column, 'tags', ()))
        self.column_info[column.name] = ColumnInfo(
            name=column.name,
            description=description,
            data_type=data_type,
            meta=meta,
            tags=tags,
            _extra=column.extra
        )

    @classmethod
    def from_target(
        cls, target: Union[HasColumnDocs, HasColumnTests]
    ) -> 'ParserRef':
        refs = cls()
        for column in target.columns:
            description = column.description
            data_type = column.data_type
            meta = column.meta
            refs.add(column, description, data_type, meta)
        return refs


def _trimmed(inp: str) -> str:
    if len(inp) < 50:
        return inp
    return inp[:44] + '...' + inp[-3:]


def merge_freshness(
    base: Optional[FreshnessThreshold], update: Optional[FreshnessThreshold]
) -> Optional[FreshnessThreshold]:
    if base is not None and update is not None:
        return base.merged(update)
    elif base is None and update is not None:
        return update
    else:
        return None


class SchemaParser(SimpleParser[SchemaTestBlock, ParsedSchemaTestNode]):
    def __init__(
        self, results, project, root_project, macro_manifest,
    ) -> None:
        super().__init__(results, project, root_project, macro_manifest)
        all_v_2 = (
            self.root_project.config_version == 2 and
            self.project.config_version == 2
        )
        if all_v_2:
            ctx = generate_schema_yml(
                self.root_project, self.project.project_name
            )
        else:
            ctx = generate_target_context(
                self.root_project, self.root_project.cli_vars
            )

        self.raw_renderer = SchemaYamlRenderer(ctx)
        self.config_generator = ContextConfigGenerator(self.root_project)

    @classmethod
    def get_compiled_path(cls, block: FileBlock) -> str:
        # should this raise an error?
        return block.path.relative_path

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Test

    def get_paths(self):
        # TODO: In order to support this, make FilesystemSearcher accept a list
        # of file patterns. eg: ['.yml', '.yaml']
        yaml_files = list(FilesystemSearcher(
            self.project, self.project.all_source_paths, '.yaml'
        ))
        if yaml_files:
            warn_or_error(
                'A future version of dbt will parse files with both'
                ' .yml and .yaml file extensions. dbt found'
                f' {len(yaml_files)} files with .yaml extensions in'
                ' your dbt project. To avoid errors when upgrading'
                ' to a future release, either remove these files from'
                ' your dbt project, or change their extensions.'
            )
        return FilesystemSearcher(
            self.project, self.project.all_source_paths, '.yml'
        )

    def parse_from_dict(self, dct, validate=True) -> ParsedSchemaTestNode:
        return ParsedSchemaTestNode.from_dict(dct, validate=validate)

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

    def parse_source(
        self, target: UnpatchedSourceDefinition
    ) -> ParsedSourceDefinition:
        source = target.source
        table = target.table
        refs = ParserRef.from_target(table)
        unique_id = target.unique_id
        description = table.description or ''
        meta = table.meta or {}
        source_description = source.description or ''
        loaded_at_field = table.loaded_at_field or source.loaded_at_field

        freshness = merge_freshness(source.freshness, table.freshness)
        quoting = source.quoting.merged(table.quoting)
        # path = block.path.original_file_path
        source_meta = source.meta or {}

        # make sure we don't do duplicate tags from source + table
        tags = sorted(set(itertools.chain(source.tags, table.tags)))

        config = self.config_generator.calculate_node_config(
            config_calls=[],
            fqn=target.fqn,
            resource_type=NodeType.Source,
            project_name=self.project.project_name,
            base=False,
        )
        if not isinstance(config, SourceConfig):
            raise InternalException(
                f'Calculated a {type(config)} for a source, but expected '
                f'a SourceConfig'
            )

        default_database = self.root_project.credentials.database

        return ParsedSourceDefinition(
            package_name=target.package_name,
            database=(source.database or default_database),
            schema=(source.schema or source.name),
            identifier=(table.identifier or table.name),
            root_path=target.root_path,
            path=target.path,
            original_file_path=target.original_file_path,
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
            fqn=target.fqn,
            tags=tags,
            config=config,
        )

    def create_test_node(
        self,
        target: Union[UnpatchedSourceDefinition, UnparsedNodeUpdate],
        path: str,
        config: ContextConfigType,
        tags: List[str],
        fqn: List[str],
        name: str,
        raw_sql: str,
        test_metadata: Dict[str, Any],
        column_name: Optional[str],
    ) -> ParsedSchemaTestNode:

        dct = {
            'alias': name,
            'schema': self.default_schema,
            'database': self.default_database,
            'fqn': fqn,
            'name': name,
            'root_path': self.project.project_root,
            'resource_type': self.resource_type,
            'tags': tags,
            'path': path,
            'original_file_path': target.original_file_path,
            'package_name': self.project.project_name,
            'raw_sql': raw_sql,
            'unique_id': self.generate_unique_id(name),
            'config': self.config_dict(config),
            'test_metadata': test_metadata,
            'column_name': column_name,
            'checksum': FileHash.empty().to_dict(),
        }
        try:
            return self.parse_from_dict(dct)
        except ValidationError as exc:
            msg = validator_error_message(exc)
            # this is a bit silly, but build an UnparsedNode just for error
            # message reasons
            node = self._create_error_node(
                name=target.name,
                path=path,
                original_file_path=target.original_file_path,
                raw_sql=raw_sql,
            )
            raise CompilationException(msg, node=node) from exc

    def _parse_generic_test(
        self,
        target: Testable,
        test: Dict[str, Any],
        tags: List[str],
        column_name: Optional[str],
    ) -> ParsedSchemaTestNode:

        render_ctx = generate_target_context(
            self.root_project, self.root_project.cli_vars
        )
        try:
            builder = TestBuilder(
                test=test,
                target=target,
                column_name=column_name,
                package_name=target.package_name,
                render_ctx=render_ctx,
            )
        except CompilationException as exc:
            context = _trimmed(str(target))
            msg = (
                'Invalid test config given in {}:'
                '\n\t{}\n\t@: {}'
                .format(target.original_file_path, exc.msg, context)
            )
            raise CompilationException(msg) from exc
        original_name = os.path.basename(target.original_file_path)
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
        tags = sorted(set(itertools.chain(tags, builder.tags())))
        if 'schema' not in tags:
            tags.append('schema')

        node = self.create_test_node(
            target=target,
            path=compiled_path,
            config=config,
            fqn=fqn,
            tags=tags,
            name=builder.fqn_name,
            raw_sql=builder.build_raw_sql(),
            column_name=column_name,
            test_metadata=metadata,
        )
        self.render_update(node, config)
        return node

    def parse_source_test(
        self,
        target: UnpatchedSourceDefinition,
        test: Dict[str, Any],
        column: Optional[UnparsedColumn],
    ) -> ParsedSchemaTestNode:
        column_name: Optional[str]
        if column is None:
            column_name = None
        else:
            column_name = column.name
            should_quote = (
                column.quote or
                (column.quote is None and target.quote_columns)
            )
            if should_quote:
                column_name = get_adapter(self.root_project).quote(column_name)

        tags_sources = [target.source.tags, target.table.tags]
        if column is not None:
            tags_sources.append(column.tags)
        tags = list(itertools.chain.from_iterable(tags_sources))

        node = self._parse_generic_test(
            target=target,
            test=test,
            tags=tags,
            column_name=column_name
        )
        # we can't go through result.add_node - no file... instead!
        if node.config.enabled:
            self.results.add_node_nofile(node)
        else:
            self.results.add_disabled_nofile(node)
        return node

    def parse_node(self, block: SchemaTestBlock) -> ParsedSchemaTestNode:
        """In schema parsing, we rewrite most of the part of parse_node that
        builds the initial node to be parsed, but rendering is basically the
        same
        """
        node = self._parse_generic_test(
            target=block.target,
            test=block.test,
            tags=block.tags,
            column_name=block.column_name,
        )
        self.add_result_node(block, node)
        return node

    def render_with_context(
        self, node: ParsedSchemaTestNode, config: ContextConfigType,
    ) -> None:
        """Given the parsed node and a ContextConfigType to use during
        parsing, collect all the refs that might be squirreled away in the test
        arguments. This includes the implicit "model" argument.
        """
        # make a base context that doesn't have the magic kwargs field
        context = self._context_for(node, config)
        # update it with the rendered test kwargs (which collects any refs)
        add_rendered_test_kwargs(context, node, capture_macros=True)

        # the parsed node is not rendered in the native context.
        get_rendered(
            node.raw_sql, context, node, capture_macros=True
        )

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
        self.parse_node(block)

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
            try:
                dct = self.raw_renderer.render_data(dct)
            except CompilationException as exc:
                raise CompilationException(
                    f'Failed to render {block.path.original_file_path} from '
                    f'project {self.project.project_name}: {exc}'
                ) from exc

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
    UnpatchedSourceDefinition, ParsedNodePatch, ParsedMacroPatch
)
NodeTarget = TypeVar(
    'NodeTarget',
    UnparsedNodeUpdate, UnparsedAnalysisUpdate
)
NonSourceTarget = TypeVar(
    'NonSourceTarget',
    UnparsedNodeUpdate, UnparsedAnalysisUpdate, UnparsedMacroUpdate
)


class YamlDocsReader(metaclass=ABCMeta):
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

    @abstractmethod
    def parse(self) -> List[TestBlock]:
        raise NotImplementedError('parse is abstract')


T = TypeVar('T', bound=JsonSchemaMixin)


class SourceParser(YamlDocsReader):
    def _target_from_dict(self, cls: Type[T], data: Dict[str, Any]) -> T:
        path = self.yaml.path.original_file_path
        try:
            return cls.from_dict(data)
        except (ValidationError, JSONValidationException) as exc:
            msg = error_context(path, self.key, data, exc)
            raise CompilationException(msg) from exc

    def parse(self) -> List[TestBlock]:
        for data in self.get_key_dicts():
            data = self.project.credentials.translate_aliases(
                data, recurse=True
            )

            is_override = 'overrides' in data
            if is_override:
                data['path'] = self.yaml.path.original_file_path
                patch = self._target_from_dict(SourcePatch, data)
                self.results.add_source_patch(self.yaml.file, patch)
            else:
                source = self._target_from_dict(UnparsedSourceDefinition, data)
                self.add_source_definitions(source)
        return []

    def add_source_definitions(self, source: UnparsedSourceDefinition) -> None:
        original_file_path = self.yaml.path.original_file_path
        fqn_path = self.yaml.path.relative_path
        for table in source.tables:
            unique_id = '.'.join([
                NodeType.Source, self.project.project_name,
                source.name, table.name
            ])

            # the FQN is project name / path elements /source_name /table_name
            fqn = self.schema_parser.get_fqn_prefix(fqn_path)
            fqn.extend([source.name, table.name])

            result = UnpatchedSourceDefinition(
                source=source,
                table=table,
                path=original_file_path,
                original_file_path=original_file_path,
                root_path=self.project.project_root,
                package_name=self.project.project_name,
                unique_id=unique_id,
                resource_type=NodeType.Source,
                fqn=fqn,
            )
            self.results.add_source(self.yaml.file, result)


class NonSourceParser(YamlDocsReader, Generic[NonSourceTarget, Parsed]):
    @abstractmethod
    def _target_type(self) -> Type[NonSourceTarget]:
        raise NotImplementedError('_unsafe_from_dict not implemented')

    @abstractmethod
    def get_block(self, node: NonSourceTarget) -> TargetBlock:
        raise NotImplementedError('get_block is abstract')

    @abstractmethod
    def parse_patch(
        self, block: TargetBlock[NonSourceTarget], refs: ParserRef
    ) -> None:
        raise NotImplementedError('parse_patch is abstract')

    def parse(self) -> List[TestBlock]:
        node: NonSourceTarget
        test_blocks: List[TestBlock] = []
        for node in self.get_unparsed_target():
            node_block = self.get_block(node)
            if isinstance(node_block, TestBlock):
                test_blocks.append(node_block)
            if isinstance(node, (HasColumnDocs, HasColumnTests)):
                refs: ParserRef = ParserRef.from_target(node)
            else:
                refs = ParserRef()
            self.parse_patch(node_block, refs)
        return test_blocks

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
        result = ParsedNodePatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            description=block.target.description,
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
    def get_block(self, node: UnparsedMacroUpdate) -> TargetBlock:
        return TargetBlock.from_yaml_block(self.yaml, node)

    def _target_type(self) -> Type[UnparsedMacroUpdate]:
        return UnparsedMacroUpdate

    def parse_patch(
        self, block: TargetBlock[UnparsedMacroUpdate], refs: ParserRef
    ) -> None:
        result = ParsedMacroPatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            arguments=block.target.arguments,
            description=block.target.description,
            meta=block.target.meta,
            docs=block.target.docs,
        )
        self.results.add_macro_patch(self.yaml.file, result)
