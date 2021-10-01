import itertools
import os
import pathlib

from abc import ABCMeta, abstractmethod
from hashlib import md5
from typing import (
    Iterable, Dict, Any, Union, List, Optional, Generic, TypeVar, Type
)

from dbt.dataclass_schema import ValidationError, dbtClassMixin

from dbt.adapters.factory import get_adapter, get_adapter_package_names
from dbt.clients.jinja import get_rendered, add_rendered_test_kwargs
from dbt.clients.yaml_helper import load_yaml_text
from dbt.config.renderer import SchemaYamlRenderer
from dbt.context.context_config import (
    ContextConfig,
)
from dbt.context.configured import generate_schema_yml
from dbt.context.target import generate_target_context
from dbt.context.providers import (
    generate_parse_exposure, generate_test_context
)
from dbt.context.macro_resolver import MacroResolver
from dbt.contracts.files import FileHash, SchemaSourceFile
from dbt.contracts.graph.parsed import (
    ParsedNodePatch,
    ColumnInfo,
    ParsedGenericTestNode,
    ParsedMacroPatch,
    UnpatchedSourceDefinition,
    ParsedExposure,
)
from dbt.contracts.graph.unparsed import (
    HasColumnDocs,
    HasColumnTests,
    HasDocs,
    SourcePatch,
    UnparsedAnalysisUpdate,
    UnparsedColumn,
    UnparsedMacroUpdate,
    UnparsedNodeUpdate,
    UnparsedExposure,
    UnparsedSourceDefinition,
)
from dbt.exceptions import (
    warn_invalid_patch, validator_error_message, JSONValidationException,
    raise_invalid_schema_yml_version, ValidationException,
    CompilationException, raise_duplicate_patch_name,
    raise_duplicate_macro_patch_name, InternalException,
    raise_duplicate_source_patch_name, warn_or_error,
)
from dbt.node_types import NodeType
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock
from dbt.parser.generic_test_builders import (
    TestBuilder, GenericTestBlock, TargetBlock, YamlBlock,
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

schema_file_keys = (
    'models', 'seeds', 'snapshots', 'sources',
    'macros', 'analyses', 'exposures',
)


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


def yaml_from_file(
    source_file: SchemaSourceFile
) -> Dict[str, Any]:
    """If loading the yaml fails, raise an exception.
    """
    path = source_file.path.relative_path
    try:
        return load_yaml_text(source_file.contents)
    except ValidationException as e:
        reason = validator_error_message(e)
        raise CompilationException(
            'Error reading {}: {} - {}'
            .format(source_file.project_name, path, reason)
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
        quote: Optional[bool]
        if isinstance(column, UnparsedColumn):
            quote = column.quote
        else:
            quote = None
        self.column_info[column.name] = ColumnInfo(
            name=column.name,
            description=description,
            data_type=data_type,
            meta=meta,
            tags=tags,
            quote=quote,
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


class SchemaParser(SimpleParser[GenericTestBlock, ParsedGenericTestNode]):
    def __init__(
        self, project, manifest, root_project,
    ) -> None:
        super().__init__(project, manifest, root_project)
        all_v_2 = (
            self.root_project.config_version == 2 and
            self.project.config_version == 2
        )
        if all_v_2:
            self.render_ctx = generate_schema_yml(
                self.root_project, self.project.project_name
            )
        else:
            self.render_ctx = generate_target_context(
                self.root_project, self.root_project.cli_vars
            )

        self.raw_renderer = SchemaYamlRenderer(self.render_ctx)

        internal_package_names = get_adapter_package_names(
            self.root_project.credentials.type
        )
        self.macro_resolver = MacroResolver(
            self.manifest.macros,
            self.root_project.project_name,
            internal_package_names
        )

    @classmethod
    def get_compiled_path(cls, block: FileBlock) -> str:
        # should this raise an error?
        return block.path.relative_path

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Test

    def parse_from_dict(self, dct, validate=True) -> ParsedGenericTestNode:
        if validate:
            ParsedGenericTestNode.validate(dct)
        return ParsedGenericTestNode.from_dict(dct)

    def parse_column_tests(
        self, block: TestBlock, column: UnparsedColumn
    ) -> None:
        if not column.tests:
            return

        for test in column.tests:
            self.parse_test(block, test, column)

    def create_test_node(
        self,
        target: Union[UnpatchedSourceDefinition, UnparsedNodeUpdate],
        path: str,
        config: ContextConfig,
        tags: List[str],
        fqn: List[str],
        name: str,
        raw_sql: str,
        test_metadata: Dict[str, Any],
        column_name: Optional[str],
    ) -> ParsedGenericTestNode:

        HASH_LENGTH = 10

        # N.B: This function builds a hashable string from any given test_metadata dict.
        #   it's a bit fragile for general use (only supports str, int, float, List, Dict)
        #   but it gets the job done here without the overhead of complete ser(de).
        def get_hashable_md(
            data: Union[str, int, float, List, Dict]
        ) -> Union[str, List, Dict]:
            if type(data) == dict:
                return {k: get_hashable_md(data[k]) for k in sorted(data.keys())}  # type: ignore
            elif type(data) == list:
                return [get_hashable_md(val) for val in data]  # type: ignore
            else:
                return str(data)

        hashable_metadata = repr(get_hashable_md(test_metadata))
        hash_string = ''.join([name, hashable_metadata]).encode('utf-8')
        test_hash = md5(hash_string).hexdigest()[-HASH_LENGTH:]

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
            'unique_id': self.generate_unique_id(name, test_hash),
            'config': self.config_dict(config),
            'test_metadata': test_metadata,
            'column_name': column_name,
            'checksum': FileHash.empty().to_dict(omit_none=True),
        }
        try:
            ParsedGenericTestNode.validate(dct)
            return ParsedGenericTestNode.from_dict(dct)
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

    # lots of time spent in this method
    def _parse_generic_test(
        self,
        target: Testable,
        test: Dict[str, Any],
        tags: List[str],
        column_name: Optional[str],
    ) -> ParsedGenericTestNode:
        try:
            builder = TestBuilder(
                test=test,
                target=target,
                column_name=column_name,
                package_name=target.package_name,
                render_ctx=self.render_ctx,
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
        compiled_path = get_pseudo_test_path(builder.compiled_name, original_name)

        # fqn is the relative path of the yaml file where this generic test is defined,
        # minus the project-level directory and the file name itself
        # TODO pass a consistent path object from both UnparsedNode and UnpatchedSourceDefinition
        path = pathlib.Path(target.original_file_path)
        relative_path = str(path.relative_to(*path.parts[:1]))
        fqn = self.get_fqn(relative_path, builder.fqn_name)

        # this is the ContextConfig that is used in render_update
        config: ContextConfig = self.initial_config(fqn)

        metadata = {
            'namespace': builder.namespace,
            'name': builder.name,
            'kwargs': builder.args,
        }
        tags = sorted(set(itertools.chain(tags, builder.tags())))

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
        self.render_test_update(node, config, builder)

        return node

    # This does special shortcut processing for the two
    # most common internal macros, not_null and unique,
    # which avoids the jinja rendering to resolve config
    # and variables, etc, which might be in the macro.
    # In the future we will look at generalizing this
    # more to handle additional macros or to use static
    # parsing to avoid jinja overhead.
    def render_test_update(self, node, config, builder):
        macro_unique_id = self.macro_resolver.get_macro_id(
            node.package_name, 'test_' + builder.name)
        # Add the depends_on here so we can limit the macros added
        # to the context in rendering processing
        node.depends_on.add_macro(macro_unique_id)
        if (macro_unique_id in
                ['macro.dbt.test_not_null', 'macro.dbt.test_unique']):
            config_call_dict = builder.get_static_config()
            config._config_call_dict = config_call_dict
            # This sets the config from dbt_project
            self.update_parsed_node_config(node, config)
            # source node tests are processed at patch_source time
            if isinstance(builder.target, UnpatchedSourceDefinition):
                sources = [builder.target.fqn[-2], builder.target.fqn[-1]]
                node.sources.append(sources)
            else:  # all other nodes
                node.refs.append([builder.target.name])
        else:
            try:
                # make a base context that doesn't have the magic kwargs field
                context = generate_test_context(
                    node, self.root_project, self.manifest, config,
                    self.macro_resolver,
                )
                # update with rendered test kwargs (which collects any refs)
                add_rendered_test_kwargs(context, node, capture_macros=True)
                # the parsed node is not rendered in the native context.
                get_rendered(
                    node.raw_sql, context, node, capture_macros=True
                )
                self.update_parsed_node_config(node, config)
            except ValidationError as exc:
                # we got a ValidationError - probably bad types in config()
                msg = validator_error_message(exc)
                raise CompilationException(msg, node=node) from exc

    def parse_node(self, block: GenericTestBlock) -> ParsedGenericTestNode:
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
        self.add_test_node(block, node)
        return node

    def add_test_node(self, block: GenericTestBlock, node: ParsedGenericTestNode):
        test_from = {"key": block.target.yaml_key, "name": block.target.name}
        if node.config.enabled:
            self.manifest.add_node(block.file, node, test_from)
        else:
            self.manifest.add_disabled(block.file, node, test_from)

    def render_with_context(
        self, node: ParsedGenericTestNode, config: ContextConfig,
    ) -> None:
        """Given the parsed node and a ContextConfig to use during
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

        block = GenericTestBlock.from_test_block(
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

    def parse_file(self, block: FileBlock, dct: Dict = None) -> None:
        assert isinstance(block.file, SchemaSourceFile)
        if not dct:
            dct = yaml_from_file(block.file)

        if dct:
            try:
                # This does a deep_map which will fail if there are circular references
                dct = self.raw_renderer.render_data(dct)
            except CompilationException as exc:
                raise CompilationException(
                    f'Failed to render {block.path.original_file_path} from '
                    f'project {self.project.project_name}: {exc}'
                ) from exc

            # contains the FileBlock and the data (dictionary)
            yaml_block = YamlBlock.from_file_block(block, dct)

            parser: YamlDocsReader

            # There are 7 kinds of parsers:
            # Model, Seed, Snapshot, Source, Macro, Analysis, Exposures

            # NonSourceParser.parse(), TestablePatchParser is a variety of
            # NodePatchParser
            if 'models' in dct:
                parser = TestablePatchParser(self, yaml_block, 'models')
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # NonSourceParser.parse()
            if 'seeds' in dct:
                parser = TestablePatchParser(self, yaml_block, 'seeds')
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # NonSourceParser.parse()
            if 'snapshots' in dct:
                parser = TestablePatchParser(self, yaml_block, 'snapshots')
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # This parser uses SourceParser.parse() which doesn't return
            # any test blocks. Source tests are handled at a later point
            # in the process.
            if 'sources' in dct:
                parser = SourceParser(self, yaml_block, 'sources')
                parser.parse()

            # NonSourceParser.parse() (but never test_blocks)
            if 'macros' in dct:
                parser = MacroPatchParser(self, yaml_block, 'macros')
                parser.parse()

            # NonSourceParser.parse() (but never test_blocks)
            if 'analyses' in dct:
                parser = AnalysisPatchParser(self, yaml_block, 'analyses')
                parser.parse()

            # parse exposures
            if 'exposures' in dct:
                exp_parser = ExposureParser(self, yaml_block)
                for node in exp_parser.parse():
                    self.manifest.add_exposure(yaml_block.file, node)


def check_format_version(
    file_path, yaml_dct
) -> None:
    if 'version' not in yaml_dct:
        raise_invalid_schema_yml_version(file_path, 'no version is specified')

    version = yaml_dct['version']
    # if it's not an integer, the version is malformed, or not
    # set. Either way, only 'version: 2' is supported.
    if not isinstance(version, int):
        raise_invalid_schema_yml_version(
            file_path, 'the version is not an integer'
        )
    if version != 2:
        raise_invalid_schema_yml_version(
            file_path, 'version {} is not supported'.format(version)
        )


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


# abstract base class (ABCMeta)
class YamlReader(metaclass=ABCMeta):
    def __init__(
        self, schema_parser: SchemaParser, yaml: YamlBlock, key: str
    ) -> None:
        self.schema_parser = schema_parser
        # key: models, seeds, snapshots, sources, macros,
        # analyses, exposures
        self.key = key
        self.yaml = yaml

    @property
    def manifest(self):
        return self.schema_parser.manifest

    @property
    def project(self):
        return self.schema_parser.project

    @property
    def default_database(self):
        return self.schema_parser.default_database

    @property
    def root_project(self):
        return self.schema_parser.root_project

    # for the different schema subparsers ('models', 'source', etc)
    # get the list of dicts pointed to by the key in the yaml config,
    # ensure that the dicts have string keys
    def get_key_dicts(self) -> Iterable[Dict[str, Any]]:
        data = self.yaml.data.get(self.key, [])
        if not isinstance(data, list):
            raise CompilationException(
                '{} must be a list, got {} instead: ({})'
                .format(self.key, type(data), _trimmed(str(data)))
            )
        path = self.yaml.path.original_file_path

        # for each dict in the data (which is a list of dicts)
        for entry in data:
            # check that entry is a dict and that all dict values
            # are strings
            if coerce_dict_str(entry) is not None:
                yield entry
            else:
                msg = error_context(
                    path, self.key, data, 'expected a dict with string keys'
                )
                raise CompilationException(msg)


class YamlDocsReader(YamlReader):
    @abstractmethod
    def parse(self) -> List[TestBlock]:
        raise NotImplementedError('parse is abstract')


T = TypeVar('T', bound=dbtClassMixin)


# This parses the 'sources' keys in yaml files.
class SourceParser(YamlDocsReader):
    def _target_from_dict(self, cls: Type[T], data: Dict[str, Any]) -> T:
        path = self.yaml.path.original_file_path
        try:
            cls.validate(data)
            return cls.from_dict(data)
        except (ValidationError, JSONValidationException) as exc:
            msg = error_context(path, self.key, data, exc)
            raise CompilationException(msg) from exc

    # The other parse method returns TestBlocks. This one doesn't.
    # This takes the yaml dictionaries in 'sources' keys and uses them
    # to create UnparsedSourceDefinition objects. They are then turned
    # into UnpatchedSourceDefinition objects in 'add_source_definitions'
    # or SourcePatch objects in 'add_source_patch'
    def parse(self) -> List[TestBlock]:
        # get a verified list of dicts for the key handled by this parser
        for data in self.get_key_dicts():
            data = self.project.credentials.translate_aliases(
                data, recurse=True
            )

            is_override = 'overrides' in data
            if is_override:
                data['path'] = self.yaml.path.original_file_path
                patch = self._target_from_dict(SourcePatch, data)
                assert isinstance(self.yaml.file, SchemaSourceFile)
                source_file = self.yaml.file
                # source patches must be unique
                key = (patch.overrides, patch.name)
                if key in self.manifest.source_patches:
                    raise_duplicate_source_patch_name(patch, self.manifest.source_patches[key])
                self.manifest.source_patches[key] = patch
                source_file.source_patches.append(key)
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

            source_def = UnpatchedSourceDefinition(
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
            self.manifest.add_source(self.yaml.file, source_def)


# This class has three main subclasses: TestablePatchParser (models,
# seeds, snapshots), MacroPatchParser, and AnalysisPatchParser
class NonSourceParser(YamlDocsReader, Generic[NonSourceTarget, Parsed]):
    @abstractmethod
    def _target_type(self) -> Type[NonSourceTarget]:
        raise NotImplementedError('_target_type not implemented')

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
        # get list of 'node' objects
        # UnparsedNodeUpdate (TestablePatchParser, models, seeds, snapshots)
        #      = HasColumnTests, HasTests
        # UnparsedAnalysisUpdate (UnparsedAnalysisParser, analyses)
        #      = HasColumnDocs, HasDocs
        # UnparsedMacroUpdate (MacroPatchParser, 'macros')
        #      = HasDocs
        # correspond to this parser's 'key'
        for node in self.get_unparsed_target():
            # node_block is a TargetBlock (Macro or Analysis)
            # or a TestBlock (all of the others)
            node_block = self.get_block(node)
            if isinstance(node_block, TestBlock):
                # TestablePatchParser = models, seeds, snapshots
                test_blocks.append(node_block)
            if isinstance(node, (HasColumnDocs, HasColumnTests)):
                # UnparsedNodeUpdate and UnparsedAnalysisUpdate
                refs: ParserRef = ParserRef.from_target(node)
            else:
                refs = ParserRef()
            # This adds the node_block to self.manifest
            # as a ParsedNodePatch or ParsedMacroPatch
            self.parse_patch(node_block, refs)
        # This will always be empty if the node a macro or analysis
        return test_blocks

    def get_unparsed_target(self) -> Iterable[NonSourceTarget]:
        path = self.yaml.path.original_file_path

        # get verified list of dicts for the 'key' that this
        # parser handles
        key_dicts = self.get_key_dicts()
        for data in key_dicts:
            # add extra data to each dict. This updates the dicts
            # in the parser yaml
            data.update({
                'original_file_path': path,
                'yaml_key': self.key,
                'package_name': self.project.project_name,
            })
            try:
                # target_type: UnparsedNodeUpdate, UnparsedAnalysisUpdate,
                # or UnparsedMacroUpdate
                self._target_type().validate(data)
                if self.key != 'macros':
                    # macros don't have the 'config' key support yet
                    self.normalize_meta_attribute(data, path)
                node = self._target_type().from_dict(data)
            except (ValidationError, JSONValidationException) as exc:
                msg = error_context(path, self.key, data, exc)
                raise CompilationException(msg) from exc
            else:
                yield node

    # We want to raise an error if 'meta' is in two places, and move 'meta'
    # from toplevel to config if necessary
    def normalize_meta_attribute(self, data, path):
        if 'meta' in data:
            if 'config' in data and 'meta' in data['config']:
                raise CompilationException(f"""
                    In {path}: found meta dictionary in 'config' dictionary and as top-level key.
                    Remove the top-level key and define it under 'config' dictionary only.
                """.strip())
            else:
                if 'config' not in data:
                    data['config'] = {}
                data['config']['meta'] = data.pop('meta')

    def patch_node_config(self, node, patch):
        # Get the ContextConfig that's used in calculating the config
        # This must match the model resource_type that's being patched
        config = ContextConfig(
            self.schema_parser.root_project,
            node.fqn,
            node.resource_type,
            self.schema_parser.project.project_name,
        )
        # We need to re-apply the config_call_dict after the patch config
        config._config_call_dict = node.config_call_dict
        self.schema_parser.update_parsed_node_config(node, config, patch_config_dict=patch.config)


class NodePatchParser(
    NonSourceParser[NodeTarget, ParsedNodePatch],
    Generic[NodeTarget]
):
    def parse_patch(
        self, block: TargetBlock[NodeTarget], refs: ParserRef
    ) -> None:
        # We're not passing the ParsedNodePatch around anymore, so we
        # could possibly skip creating one. Leaving here for now for
        # code consistency.
        patch = ParsedNodePatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            description=block.target.description,
            columns=refs.column_info,
            meta=block.target.meta,
            docs=block.target.docs,
            config=block.target.config,
        )
        assert isinstance(self.yaml.file, SchemaSourceFile)
        source_file: SchemaSourceFile = self.yaml.file
        if patch.yaml_key in ['models', 'seeds', 'snapshots']:
            unique_id = self.manifest.ref_lookup.get_unique_id(patch.name, None)
            if unique_id:
                resource_type = NodeType(unique_id.split('.')[0])
                if resource_type.pluralize() != patch.yaml_key:
                    warn_invalid_patch(patch, resource_type)
                    return

        elif patch.yaml_key == 'analyses':
            unique_id = self.manifest.analysis_lookup.get_unique_id(patch.name, None)
        else:
            raise InternalException(
                f'Unexpected yaml_key {patch.yaml_key} for patch in '
                f'file {source_file.path.original_file_path}'
            )
        if unique_id is None:
            # This will usually happen when a node is disabled
            return

        # patches can't be overwritten
        node = self.manifest.nodes.get(unique_id)
        if node:
            if node.patch_path:
                package_name, existing_file_path = node.patch_path.split('://')
                raise_duplicate_patch_name(patch, existing_file_path)
            source_file.append_patch(patch.yaml_key, unique_id)
            # If this patch has config changes, re-calculate the node config
            # with the patch config
            if patch.config:
                self.patch_node_config(node, patch)
            node.patch(patch)


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
        patch = ParsedMacroPatch(
            name=block.target.name,
            original_file_path=block.target.original_file_path,
            yaml_key=block.target.yaml_key,
            package_name=block.target.package_name,
            arguments=block.target.arguments,
            description=block.target.description,
            meta=block.target.meta,
            docs=block.target.docs,
            config=block.target.config,
        )
        assert isinstance(self.yaml.file, SchemaSourceFile)
        source_file = self.yaml.file
        # macros are fully namespaced
        unique_id = f'macro.{patch.package_name}.{patch.name}'
        macro = self.manifest.macros.get(unique_id)
        if not macro:
            warn_or_error(
                f'WARNING: Found patch for macro "{patch.name}" '
                f'which was not found'
            )
            return
        if macro.patch_path:
            package_name, existing_file_path = macro.patch_path.split('://')
            raise_duplicate_macro_patch_name(patch, existing_file_path)
        source_file.macro_patches[patch.name] = unique_id
        macro.patch(patch)


class ExposureParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Exposure.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_exposure(self, unparsed: UnparsedExposure) -> ParsedExposure:
        package_name = self.project.project_name
        unique_id = f'{NodeType.Exposure}.{package_name}.{unparsed.name}'
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        parsed = ParsedExposure(
            package_name=package_name,
            root_path=self.project.project_root,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            fqn=fqn,
            name=unparsed.name,
            type=unparsed.type,
            url=unparsed.url,
            meta=unparsed.meta,
            tags=unparsed.tags,
            description=unparsed.description,
            owner=unparsed.owner,
            maturity=unparsed.maturity,
        )
        ctx = generate_parse_exposure(
            parsed,
            self.root_project,
            self.schema_parser.manifest,
            package_name,
        )
        depends_on_jinja = '\n'.join(
            '{{ ' + line + '}}' for line in unparsed.depends_on
        )
        get_rendered(
            depends_on_jinja, ctx, parsed, capture_macros=True
        )
        # parsed now has a populated refs/sources
        return parsed

    def parse(self) -> Iterable[ParsedExposure]:
        for data in self.get_key_dicts():
            try:
                UnparsedExposure.validate(data)
                unparsed = UnparsedExposure.from_dict(data)
            except (ValidationError, JSONValidationException) as exc:
                msg = error_context(self.yaml.path, self.key, data, exc)
                raise CompilationException(msg) from exc
            parsed = self.parse_exposure(unparsed)
            yield parsed
