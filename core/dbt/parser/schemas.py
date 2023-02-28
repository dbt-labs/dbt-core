import itertools
import os
import pathlib

from abc import ABCMeta, abstractmethod
from typing import Iterable, Dict, Any, Union, List, Optional, Generic, TypeVar, Type

from dbt.dataclass_schema import ValidationError, dbtClassMixin

from dbt.adapters.factory import get_adapter, get_adapter_package_names
from dbt.clients.jinja import get_rendered, add_rendered_test_kwargs
from dbt.clients.yaml_helper import load_yaml_text
from dbt.parser.schema_renderer import SchemaYamlRenderer
from dbt.context.context_config import (
    ContextConfig,
    BaseContextConfigGenerator,
    ContextConfigGenerator,
    UnrenderedConfigGenerator,
)
from dbt.context.configured import generate_schema_yml_context, SchemaYamlVars
from dbt.context.providers import (
    generate_parse_exposure,
    generate_parse_metrics,
    generate_test_context,
)
from dbt.context.macro_resolver import MacroResolver
from dbt.contracts.files import FileHash, SchemaSourceFile
from dbt.contracts.graph.model_config import MetricConfig, ExposureConfig
from dbt.contracts.graph.nodes import (
    ParsedNodePatch,
    ColumnInfo,
    GenericTestNode,
    ParsedMacroPatch,
    UnpatchedSourceDefinition,
    Exposure,
    Metric,
    Group,
    ManifestNode,
    GraphMemberNode,
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
    UnparsedMetric,
    UnparsedSourceDefinition,
    UnparsedGroup,
)
from dbt.exceptions import (
    CompilationError,
    DuplicateMacroPatchNameError,
    DuplicatePatchPathError,
    DuplicateSourcePatchNameError,
    JSONValidationError,
    DbtInternalError,
    SchemaConfigError,
    TestConfigError,
    ParsingError,
    PropertyYMLInvalidTagError,
    PropertyYMLMissingVersionError,
    PropertyYMLVersionNotIntError,
    DbtValidationError,
    YamlLoadError,
    YamlParseDictError,
    YamlParseListError,
)
from dbt.events.functions import warn_or_error
from dbt.events.types import WrongResourceSchemaFile, NoNodeForYamlKey, MacroNotFoundForPatch
from dbt.node_types import NodeType
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock
from dbt.parser.generic_test_builders import (
    TestBuilder,
    GenericTestBlock,
    TargetBlock,
    YamlBlock,
    TestBlock,
    Testable,
)
from dbt.utils import get_pseudo_test_path, coerce_dict_str, md5


TestDef = Union[str, Dict[str, Any]]

schema_file_keys = (
    "models",
    "seeds",
    "snapshots",
    "sources",
    "macros",
    "analyses",
    "exposures",
    "metrics",
)


def yaml_from_file(source_file: SchemaSourceFile) -> Dict[str, Any]:
    """If loading the yaml fails, raise an exception."""
    try:
        # source_file.contents can sometimes be None
        return load_yaml_text(source_file.contents or "", source_file.path)
    except DbtValidationError as e:
        raise YamlLoadError(
            project_name=source_file.project_name, path=source_file.path.relative_path, exc=e
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
        constraints: Optional[List[str]],
        constraints_check: Optional[str],
        meta: Dict[str, Any],
    ):
        tags: List[str] = []
        tags.extend(getattr(column, "tags", ()))
        quote: Optional[bool]
        if isinstance(column, UnparsedColumn):
            quote = column.quote
        else:
            quote = None
        self.column_info[column.name] = ColumnInfo(
            name=column.name,
            description=description,
            data_type=data_type,
            constraints=constraints,
            constraints_check=constraints_check,
            meta=meta,
            tags=tags,
            quote=quote,
            _extra=column.extra,
        )

    @classmethod
    def from_target(cls, target: Union[HasColumnDocs, HasColumnTests]) -> "ParserRef":
        refs = cls()
        for column in target.columns:
            description = column.description
            data_type = column.data_type
            constraints = column.constraints
            constraints_check = column.constraints_check
            meta = column.meta
            refs.add(column, description, data_type, constraints, constraints_check, meta)
        return refs


def _trimmed(inp: str) -> str:
    if len(inp) < 50:
        return inp
    return inp[:44] + "..." + inp[-3:]


class SchemaParser(SimpleParser[GenericTestBlock, GenericTestNode]):
    def __init__(
        self,
        project,
        manifest,
        root_project,
    ) -> None:
        super().__init__(project, manifest, root_project)

        self.schema_yaml_vars = SchemaYamlVars()
        self.render_ctx = generate_schema_yml_context(
            self.root_project, self.project.project_name, self.schema_yaml_vars
        )
        internal_package_names = get_adapter_package_names(self.root_project.credentials.type)
        self.macro_resolver = MacroResolver(
            self.manifest.macros, self.root_project.project_name, internal_package_names
        )

    @classmethod
    def get_compiled_path(cls, block: FileBlock) -> str:
        # should this raise an error?
        return block.path.relative_path

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Test

    def parse_from_dict(self, dct, validate=True) -> GenericTestNode:
        if validate:
            GenericTestNode.validate(dct)
        return GenericTestNode.from_dict(dct)

    def parse_column_tests(self, block: TestBlock, column: UnparsedColumn) -> None:
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
        raw_code: str,
        test_metadata: Dict[str, Any],
        file_key_name: str,
        column_name: Optional[str],
    ) -> GenericTestNode:

        HASH_LENGTH = 10

        # N.B: This function builds a hashable string from any given test_metadata dict.
        #   it's a bit fragile for general use (only supports str, int, float, List, Dict)
        #   but it gets the job done here without the overhead of complete ser(de).
        def get_hashable_md(data: Union[str, int, float, List, Dict]) -> Union[str, List, Dict]:
            if type(data) == dict:
                return {k: get_hashable_md(data[k]) for k in sorted(data.keys())}  # type: ignore
            elif type(data) == list:
                return [get_hashable_md(val) for val in data]  # type: ignore
            else:
                return str(data)

        hashable_metadata = repr(get_hashable_md(test_metadata))
        hash_string = "".join([name, hashable_metadata])
        test_hash = md5(hash_string)[-HASH_LENGTH:]

        dct = {
            "alias": name,
            "schema": self.default_schema,
            "database": self.default_database,
            "fqn": fqn,
            "name": name,
            "resource_type": self.resource_type,
            "tags": tags,
            "path": path,
            "original_file_path": target.original_file_path,
            "package_name": self.project.project_name,
            "raw_code": raw_code,
            "language": "sql",
            "unique_id": self.generate_unique_id(name, test_hash),
            "config": self.config_dict(config),
            "test_metadata": test_metadata,
            "column_name": column_name,
            "checksum": FileHash.empty().to_dict(omit_none=True),
            "file_key_name": file_key_name,
        }
        try:
            GenericTestNode.validate(dct)
            return GenericTestNode.from_dict(dct)
        except ValidationError as exc:
            # this is a bit silly, but build an UnparsedNode just for error
            # message reasons
            node = self._create_error_node(
                name=target.name,
                path=path,
                original_file_path=target.original_file_path,
                raw_code=raw_code,
            )
            raise TestConfigError(exc, node)

    # lots of time spent in this method
    def _parse_generic_test(
        self,
        target: Testable,
        test: Dict[str, Any],
        tags: List[str],
        column_name: Optional[str],
        schema_file_id: str,
    ) -> GenericTestNode:
        try:
            builder = TestBuilder(
                test=test,
                target=target,
                column_name=column_name,
                package_name=target.package_name,
                render_ctx=self.render_ctx,
            )
            if self.schema_yaml_vars.env_vars:
                self.store_env_vars(target, schema_file_id, self.schema_yaml_vars.env_vars)
                self.schema_yaml_vars.env_vars = {}

        except ParsingError as exc:
            context = _trimmed(str(target))
            msg = "Invalid test config given in {}:\n\t{}\n\t@: {}".format(
                target.original_file_path, exc.msg, context
            )
            raise ParsingError(msg) from exc

        except CompilationError as exc:
            context = _trimmed(str(target))
            msg = (
                "Invalid generic test configuration given in "
                f"{target.original_file_path}: \n{exc.msg}\n\t@: {context}"
            )
            raise CompilationError(msg) from exc

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

        # builder.args contains keyword args for the test macro,
        # not configs which have been separated out in the builder.
        # The keyword args are not completely rendered until compilation.
        metadata = {
            "namespace": builder.namespace,
            "name": builder.name,
            "kwargs": builder.args,
        }
        tags = sorted(set(itertools.chain(tags, builder.tags())))

        if isinstance(target, UnpatchedSourceDefinition):
            file_key_name = f"{target.source.yaml_key}.{target.source.name}"
        else:
            file_key_name = f"{target.yaml_key}.{target.name}"

        node = self.create_test_node(
            target=target,
            path=compiled_path,
            config=config,
            fqn=fqn,
            tags=tags,
            name=builder.fqn_name,
            raw_code=builder.build_raw_code(),
            column_name=column_name,
            test_metadata=metadata,
            file_key_name=file_key_name,
        )
        self.render_test_update(node, config, builder, schema_file_id)

        return node

    def _lookup_attached_node(
        self, target: Testable
    ) -> Optional[Union[ManifestNode, GraphMemberNode]]:
        """Look up attached node for Testable target nodes other than sources. Can be None if generic test attached to SQL node with no corresponding .sql file."""
        attached_node = None  # type: Optional[Union[ManifestNode, GraphMemberNode]]
        if not isinstance(target, UnpatchedSourceDefinition):
            attached_node_unique_id = self.manifest.ref_lookup.get_unique_id(target.name, None)
            if attached_node_unique_id:
                attached_node = self.manifest.nodes[attached_node_unique_id]
            else:
                disabled_node = self.manifest.disabled_lookup.find(
                    target.name, None
                ) or self.manifest.disabled_lookup.find(target.name.upper(), None)
                if disabled_node:
                    attached_node = self.manifest.disabled[disabled_node[0].unique_id][0]
        return attached_node

    def store_env_vars(self, target, schema_file_id, env_vars):
        self.manifest.env_vars.update(env_vars)
        if schema_file_id in self.manifest.files:
            schema_file = self.manifest.files[schema_file_id]
            if isinstance(target, UnpatchedSourceDefinition):
                search_name = target.source.name
                yaml_key = target.source.yaml_key
                if "." in search_name:  # source file definitions
                    (search_name, _) = search_name.split(".")
            else:
                search_name = target.name
                yaml_key = target.yaml_key
            for var in env_vars.keys():
                schema_file.add_env_var(var, yaml_key, search_name)

    # This does special shortcut processing for the two
    # most common internal macros, not_null and unique,
    # which avoids the jinja rendering to resolve config
    # and variables, etc, which might be in the macro.
    # In the future we will look at generalizing this
    # more to handle additional macros or to use static
    # parsing to avoid jinja overhead.
    def render_test_update(self, node, config, builder, schema_file_id):
        macro_unique_id = self.macro_resolver.get_macro_id(
            node.package_name, "test_" + builder.name
        )
        # Add the depends_on here so we can limit the macros added
        # to the context in rendering processing
        node.depends_on.add_macro(macro_unique_id)
        if macro_unique_id in ["macro.dbt.test_not_null", "macro.dbt.test_unique"]:
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
                    node,
                    self.root_project,
                    self.manifest,
                    config,
                    self.macro_resolver,
                )
                # update with rendered test kwargs (which collects any refs)
                # Note: This does not actually update the kwargs with the rendered
                # values. That happens in compilation.
                add_rendered_test_kwargs(context, node, capture_macros=True)
                # the parsed node is not rendered in the native context.
                get_rendered(node.raw_code, context, node, capture_macros=True)
                self.update_parsed_node_config(node, config)
                # env_vars should have been updated in the context env_var method
            except ValidationError as exc:
                # we got a ValidationError - probably bad types in config()
                raise SchemaConfigError(exc, node=node) from exc

        # Set attached_node for generic test nodes, if available.
        # Generic test node inherits attached node's group config value.
        attached_node = self._lookup_attached_node(builder.target)
        if attached_node:
            node.attached_node = attached_node.unique_id
            node.group, node.group = attached_node.group, attached_node.group

    def parse_node(self, block: GenericTestBlock) -> GenericTestNode:
        """In schema parsing, we rewrite most of the part of parse_node that
        builds the initial node to be parsed, but rendering is basically the
        same
        """
        node = self._parse_generic_test(
            target=block.target,
            test=block.test,
            tags=block.tags,
            column_name=block.column_name,
            schema_file_id=block.file.file_id,
        )
        self.add_test_node(block, node)
        return node

    def add_test_node(self, block: GenericTestBlock, node: GenericTestNode):
        test_from = {"key": block.target.yaml_key, "name": block.target.name}
        if node.config.enabled:
            self.manifest.add_node(block.file, node, test_from)
        else:
            self.manifest.add_disabled(block.file, node, test_from)

    def render_with_context(
        self,
        node: GenericTestNode,
        config: ContextConfig,
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
        get_rendered(node.raw_code, context, node, capture_macros=True)

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
            should_quote = column.quote or (column.quote is None and target_block.quote_columns)
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
            # contains the FileBlock and the data (dictionary)
            yaml_block = YamlBlock.from_file_block(block, dct)

            parser: YamlDocsReader

            # There are 7 kinds of parsers:
            # Model, Seed, Snapshot, Source, Macro, Analysis, Exposures

            # NonSourceParser.parse(), TestablePatchParser is a variety of
            # NodePatchParser
            if "models" in dct:
                # the models are already in the manifest as nodes when we reach this code,
                # even if they are disabled in the schema file
                parser = TestablePatchParser(self, yaml_block, "models")
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # NonSourceParser.parse()
            if "seeds" in dct:
                parser = TestablePatchParser(self, yaml_block, "seeds")
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # NonSourceParser.parse()
            if "snapshots" in dct:
                parser = TestablePatchParser(self, yaml_block, "snapshots")
                for test_block in parser.parse():
                    self.parse_tests(test_block)

            # This parser uses SourceParser.parse() which doesn't return
            # any test blocks. Source tests are handled at a later point
            # in the process.
            if "sources" in dct:
                parser = SourceParser(self, yaml_block, "sources")
                parser.parse()

            # NonSourceParser.parse() (but never test_blocks)
            if "macros" in dct:
                parser = MacroPatchParser(self, yaml_block, "macros")
                parser.parse()

            # NonSourceParser.parse() (but never test_blocks)
            if "analyses" in dct:
                parser = AnalysisPatchParser(self, yaml_block, "analyses")
                parser.parse()

            # parse exposures
            if "exposures" in dct:
                exp_parser = ExposureParser(self, yaml_block)
                exp_parser.parse()

            # parse metrics
            if "metrics" in dct:
                metric_parser = MetricParser(self, yaml_block)
                metric_parser.parse()

            # parse groups
            if "groups" in dct:
                group_parser = GroupParser(self, yaml_block)
                group_parser.parse()


def check_format_version(file_path, yaml_dct) -> None:
    if "version" not in yaml_dct:
        raise PropertyYMLMissingVersionError(file_path)

    version = yaml_dct["version"]
    # if it's not an integer, the version is malformed, or not
    # set. Either way, only 'version: 2' is supported.
    if not isinstance(version, int):
        raise PropertyYMLVersionNotIntError(file_path, version)

    if version != 2:
        raise PropertyYMLInvalidTagError(file_path, version)


Parsed = TypeVar("Parsed", UnpatchedSourceDefinition, ParsedNodePatch, ParsedMacroPatch)
NodeTarget = TypeVar("NodeTarget", UnparsedNodeUpdate, UnparsedAnalysisUpdate)
NonSourceTarget = TypeVar(
    "NonSourceTarget", UnparsedNodeUpdate, UnparsedAnalysisUpdate, UnparsedMacroUpdate
)


# abstract base class (ABCMeta)
class YamlReader(metaclass=ABCMeta):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock, key: str) -> None:
        self.schema_parser = schema_parser
        # key: models, seeds, snapshots, sources, macros,
        # analyses, exposures
        self.key = key
        self.yaml = yaml
        self.schema_yaml_vars = SchemaYamlVars()
        self.render_ctx = generate_schema_yml_context(
            self.schema_parser.root_project,
            self.schema_parser.project.project_name,
            self.schema_yaml_vars,
        )
        self.renderer = SchemaYamlRenderer(self.render_ctx, self.key)

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
            raise ParsingError(
                "{} must be a list, got {} instead: ({})".format(
                    self.key, type(data), _trimmed(str(data))
                )
            )
        path = self.yaml.path.original_file_path

        # for each dict in the data (which is a list of dicts)
        for entry in data:

            # check that entry is a dict and that all dict values
            # are strings
            if coerce_dict_str(entry) is None:
                raise YamlParseListError(path, self.key, data, "expected a dict with string keys")

            if "name" not in entry:
                raise ParsingError("Entry did not contain a name")

            # Render the data (except for tests and descriptions).
            # See the SchemaYamlRenderer
            entry = self.render_entry(entry)
            if self.schema_yaml_vars.env_vars:
                self.schema_parser.manifest.env_vars.update(self.schema_yaml_vars.env_vars)
                schema_file = self.yaml.file
                assert isinstance(schema_file, SchemaSourceFile)
                for var in self.schema_yaml_vars.env_vars.keys():
                    schema_file.add_env_var(var, self.key, entry["name"])
                self.schema_yaml_vars.env_vars = {}

            yield entry

    def render_entry(self, dct):
        try:
            # This does a deep_map which will fail if there are circular references
            dct = self.renderer.render_data(dct)
        except ParsingError as exc:
            raise ParsingError(
                f"Failed to render {self.yaml.file.path.original_file_path} from "
                f"project {self.project.project_name}: {exc}"
            ) from exc
        return dct


class YamlDocsReader(YamlReader):
    @abstractmethod
    def parse(self) -> List[TestBlock]:
        raise NotImplementedError("parse is abstract")


T = TypeVar("T", bound=dbtClassMixin)


# This parses the 'sources' keys in yaml files.
class SourceParser(YamlDocsReader):
    def _target_from_dict(self, cls: Type[T], data: Dict[str, Any]) -> T:
        path = self.yaml.path.original_file_path
        try:
            cls.validate(data)
            return cls.from_dict(data)
        except (ValidationError, JSONValidationError) as exc:
            raise YamlParseDictError(path, self.key, data, exc)

    # The other parse method returns TestBlocks. This one doesn't.
    # This takes the yaml dictionaries in 'sources' keys and uses them
    # to create UnparsedSourceDefinition objects. They are then turned
    # into UnpatchedSourceDefinition objects in 'add_source_definitions'
    # or SourcePatch objects in 'add_source_patch'
    def parse(self) -> List[TestBlock]:
        # get a verified list of dicts for the key handled by this parser
        for data in self.get_key_dicts():
            data = self.project.credentials.translate_aliases(data, recurse=True)

            is_override = "overrides" in data
            if is_override:
                data["path"] = self.yaml.path.original_file_path
                patch = self._target_from_dict(SourcePatch, data)
                assert isinstance(self.yaml.file, SchemaSourceFile)
                source_file = self.yaml.file
                # source patches must be unique
                key = (patch.overrides, patch.name)
                if key in self.manifest.source_patches:
                    raise DuplicateSourcePatchNameError(patch, self.manifest.source_patches[key])
                self.manifest.source_patches[key] = patch
                source_file.source_patches.append(key)
            else:
                source = self._target_from_dict(UnparsedSourceDefinition, data)
                self.add_source_definitions(source)
        return []

    def add_source_definitions(self, source: UnparsedSourceDefinition) -> None:
        package_name = self.project.project_name
        original_file_path = self.yaml.path.original_file_path
        fqn_path = self.yaml.path.relative_path
        for table in source.tables:
            unique_id = ".".join([NodeType.Source, package_name, source.name, table.name])

            # the FQN is project name / path elements /source_name /table_name
            fqn = self.schema_parser.get_fqn_prefix(fqn_path)
            fqn.extend([source.name, table.name])

            source_def = UnpatchedSourceDefinition(
                source=source,
                table=table,
                path=original_file_path,
                original_file_path=original_file_path,
                package_name=package_name,
                unique_id=unique_id,
                resource_type=NodeType.Source,
                fqn=fqn,
                name=f"{source.name}_{table.name}",
            )
            self.manifest.add_source(self.yaml.file, source_def)


# This class has three main subclasses: TestablePatchParser (models,
# seeds, snapshots), MacroPatchParser, and AnalysisPatchParser
class NonSourceParser(YamlDocsReader, Generic[NonSourceTarget, Parsed]):
    @abstractmethod
    def _target_type(self) -> Type[NonSourceTarget]:
        raise NotImplementedError("_target_type not implemented")

    @abstractmethod
    def get_block(self, node: NonSourceTarget) -> TargetBlock:
        raise NotImplementedError("get_block is abstract")

    @abstractmethod
    def parse_patch(self, block: TargetBlock[NonSourceTarget], refs: ParserRef) -> None:
        raise NotImplementedError("parse_patch is abstract")

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

            # There's no unique_id on the node yet so cannot add to disabled dict
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
            data.update(
                {
                    "original_file_path": path,
                    "yaml_key": self.key,
                    "package_name": self.project.project_name,
                }
            )
            try:
                # target_type: UnparsedNodeUpdate, UnparsedAnalysisUpdate,
                # or UnparsedMacroUpdate
                self._target_type().validate(data)
                if self.key != "macros":
                    # macros don't have the 'config' key support yet
                    self.normalize_meta_attribute(data, path)
                    self.normalize_docs_attribute(data, path)
                    self.normalize_group_attribute(data, path)
                node = self._target_type().from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(path, self.key, data, exc)
            else:
                yield node

    # We want to raise an error if some attributes are in two places, and move them
    # from toplevel to config if necessary
    def normalize_attribute(self, data, path, attribute):
        if attribute in data:
            if "config" in data and attribute in data["config"]:
                raise ParsingError(
                    f"""
                    In {path}: found {attribute} dictionary in 'config' dictionary and as top-level key.
                    Remove the top-level key and define it under 'config' dictionary only.
                """.strip()
                )
            else:
                if "config" not in data:
                    data["config"] = {}
                data["config"][attribute] = data.pop(attribute)

    def normalize_meta_attribute(self, data, path):
        return self.normalize_attribute(data, path, "meta")

    def normalize_docs_attribute(self, data, path):
        return self.normalize_attribute(data, path, "docs")

    def normalize_group_attribute(self, data, path):
        return self.normalize_attribute(data, path, "group")

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


class NodePatchParser(NonSourceParser[NodeTarget, ParsedNodePatch], Generic[NodeTarget]):
    def parse_patch(self, block: TargetBlock[NodeTarget], refs: ParserRef) -> None:
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
            access=block.target.access,
        )
        assert isinstance(self.yaml.file, SchemaSourceFile)
        source_file: SchemaSourceFile = self.yaml.file
        if patch.yaml_key in ["models", "seeds", "snapshots"]:
            unique_id = self.manifest.ref_lookup.get_unique_id(patch.name, None)
            if unique_id:
                resource_type = NodeType(unique_id.split(".")[0])
                if resource_type.pluralize() != patch.yaml_key:
                    warn_or_error(
                        WrongResourceSchemaFile(
                            patch_name=patch.name,
                            resource_type=resource_type,
                            plural_resource_type=resource_type.pluralize(),
                            yaml_key=patch.yaml_key,
                            file_path=patch.original_file_path,
                        )
                    )
                    return

        elif patch.yaml_key == "analyses":
            unique_id = self.manifest.analysis_lookup.get_unique_id(patch.name, None)
        else:
            raise DbtInternalError(
                f"Unexpected yaml_key {patch.yaml_key} for patch in "
                f"file {source_file.path.original_file_path}"
            )
        # handle disabled nodes
        if unique_id is None:
            # Node might be disabled. Following call returns list of matching disabled nodes
            found_nodes = self.manifest.disabled_lookup.find(patch.name, patch.package_name)
            if found_nodes:
                if len(found_nodes) > 1 and patch.config.get("enabled"):
                    # There are multiple disabled nodes for this model and the schema file wants to enable one.
                    # We have no way to know which one to enable.
                    resource_type = found_nodes[0].unique_id.split(".")[0]
                    msg = (
                        f"Found {len(found_nodes)} matching disabled nodes for "
                        f"{resource_type} '{patch.name}'. Multiple nodes for the same "
                        "unique id cannot be enabled in the schema file. They must be enabled "
                        "in `dbt_project.yml` or in the sql files."
                    )
                    raise ParsingError(msg)

                # all nodes in the disabled dict have the same unique_id so just grab the first one
                # to append with the uniqe id
                source_file.append_patch(patch.yaml_key, found_nodes[0].unique_id)
                for node in found_nodes:
                    node.patch_path = source_file.file_id
                    # re-calculate the node config with the patch config.  Always do this
                    # for the case when no config is set to ensure the default of true gets captured
                    if patch.config:
                        self.patch_node_config(node, patch)

                    node.patch(patch)
            else:
                warn_or_error(
                    NoNodeForYamlKey(
                        patch_name=patch.name,
                        yaml_key=patch.yaml_key,
                        file_path=source_file.path.original_file_path,
                    )
                )
                return

        # patches can't be overwritten
        node = self.manifest.nodes.get(unique_id)
        if node:
            if node.patch_path:
                package_name, existing_file_path = node.patch_path.split("://")
                raise DuplicatePatchPathError(patch, existing_file_path)

            source_file.append_patch(patch.yaml_key, node.unique_id)
            # re-calculate the node config with the patch config.  Always do this
            # for the case when no config is set to ensure the default of true gets captured
            if patch.config:
                self.patch_node_config(node, patch)

            node.patch(patch)
            self.validate_constraints(node)

    def validate_constraints(self, patched_node):
        error_messages = []
        if patched_node.resource_type == "model" and patched_node.config.contract is True:
            validators = [
                self.constraints_schema_validator(patched_node),
                self.constraints_materialization_validator(patched_node),
                self.constraints_language_validator(patched_node),
                self.constraints_data_type_validator(patched_node),
            ]
            error_messages = [validator for validator in validators if validator != "None"]

        if error_messages:
            original_file_path = patched_node.original_file_path
            raise ParsingError(
                f"Original File Path: ({original_file_path})\nConstraints must be defined in a `yml` schema configuration file like `schema.yml`.\nOnly the SQL table materialization is supported for constraints. \n`data_type` values must be defined for all columns and NOT be null or blank.{self.convert_errors_to_string(error_messages)}"
            )

    def convert_errors_to_string(self, error_messages: List[str]):
        n = len(error_messages)
        if not n:
            return ""
        if n == 1:
            return error_messages[0]
        error_messages_string = "".join(error_messages[:-1]) + f"{error_messages[-1]}"
        return error_messages_string

    def constraints_schema_validator(self, patched_node):
        schema_error = False
        if patched_node.columns == {}:
            schema_error = True
        schema_error_msg = "\n    Schema Error: `yml` configuration does NOT exist"
        schema_error_msg_payload = f"{schema_error_msg if schema_error else None}"
        return schema_error_msg_payload

    def constraints_materialization_validator(self, patched_node):
        materialization_error = {}
        if patched_node.config.materialized != "table":
            materialization_error = {"materialization": patched_node.config.materialized}
        materialization_error_msg = f"\n    Materialization Error: {materialization_error}"
        materialization_error_msg_payload = (
            f"{materialization_error_msg if materialization_error else None}"
        )
        return materialization_error_msg_payload

    def constraints_language_validator(self, patched_node):
        language_error = {}
        language = str(patched_node.language)
        if language != "sql":
            language_error = {"language": language}
        language_error_msg = f"\n    Language Error: {language_error}"
        language_error_msg_payload = f"{language_error_msg if language_error else None}"
        return language_error_msg_payload

    def constraints_data_type_validator(self, patched_node):
        data_type_errors = set()
        for column, column_info in patched_node.columns.items():
            if column_info.data_type is None:
                data_type_error = {column}
                data_type_errors.update(data_type_error)
        data_type_errors_msg = (
            f"\n    Columns with `data_type` Blank/Null Errors: {data_type_errors}"
        )
        data_type_errors_msg_payload = f"{data_type_errors_msg if data_type_errors else None}"
        return data_type_errors_msg_payload


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

    def parse_patch(self, block: TargetBlock[UnparsedMacroUpdate], refs: ParserRef) -> None:
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
        unique_id = f"macro.{patch.package_name}.{patch.name}"
        macro = self.manifest.macros.get(unique_id)
        if not macro:
            warn_or_error(MacroNotFoundForPatch(patch_name=patch.name))
            return
        if macro.patch_path:
            package_name, existing_file_path = macro.patch_path.split("://")
            raise DuplicateMacroPatchNameError(patch, existing_file_path)
        source_file.macro_patches[patch.name] = unique_id
        macro.patch(patch)


class ExposureParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Exposure.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_exposure(self, unparsed: UnparsedExposure):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Exposure}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        config = self._generate_exposure_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=True,
        )

        config = config.finalize_and_validate()

        unrendered_config = self._generate_exposure_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=False,
        )

        if not isinstance(config, ExposureConfig):
            raise DbtInternalError(
                f"Calculated a {type(config)} for an exposure, but expected an ExposureConfig"
            )

        parsed = Exposure(
            resource_type=NodeType.Exposure,
            package_name=package_name,
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
            label=unparsed.label,
            owner=unparsed.owner,
            maturity=unparsed.maturity,
            config=config,
            unrendered_config=unrendered_config,
        )
        ctx = generate_parse_exposure(
            parsed,
            self.root_project,
            self.schema_parser.manifest,
            package_name,
        )
        depends_on_jinja = "\n".join("{{ " + line + "}}" for line in unparsed.depends_on)
        get_rendered(depends_on_jinja, ctx, parsed, capture_macros=True)
        # parsed now has a populated refs/sources/metrics

        if parsed.config.enabled:
            self.manifest.add_exposure(self.yaml.file, parsed)
        else:
            self.manifest.add_disabled(self.yaml.file, parsed)

    def _generate_exposure_config(
        self, target: UnparsedExposure, fqn: List[str], package_name: str, rendered: bool
    ):
        generator: BaseContextConfigGenerator
        if rendered:
            generator = ContextConfigGenerator(self.root_project)
        else:
            generator = UnrenderedConfigGenerator(self.root_project)

        # configs with precendence set
        precedence_configs = dict()
        # apply exposure configs
        precedence_configs.update(target.config)

        return generator.calculate_node_config(
            config_call_dict={},
            fqn=fqn,
            resource_type=NodeType.Exposure,
            project_name=package_name,
            base=False,
            patch_config_dict=precedence_configs,
        )

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedExposure.validate(data)
                unparsed = UnparsedExposure.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_exposure(unparsed)


class MetricParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Metric.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_metric(self, unparsed: UnparsedMetric):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Metric}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        fqn = self.schema_parser.get_fqn_prefix(path)
        fqn.append(unparsed.name)

        config = self._generate_metric_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=True,
        )

        config = config.finalize_and_validate()

        unrendered_config = self._generate_metric_config(
            target=unparsed,
            fqn=fqn,
            package_name=package_name,
            rendered=False,
        )

        if not isinstance(config, MetricConfig):
            raise DbtInternalError(
                f"Calculated a {type(config)} for a metric, but expected a MetricConfig"
            )

        parsed = Metric(
            resource_type=NodeType.Metric,
            package_name=package_name,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            fqn=fqn,
            model=unparsed.model,
            name=unparsed.name,
            description=unparsed.description,
            label=unparsed.label,
            calculation_method=unparsed.calculation_method,
            expression=str(unparsed.expression),
            timestamp=unparsed.timestamp,
            dimensions=unparsed.dimensions,
            window=unparsed.window,
            time_grains=unparsed.time_grains,
            filters=unparsed.filters,
            meta=unparsed.meta,
            tags=unparsed.tags,
            config=config,
            unrendered_config=unrendered_config,
            group=config.group,
        )

        ctx = generate_parse_metrics(
            parsed,
            self.root_project,
            self.schema_parser.manifest,
            package_name,
        )

        if parsed.model is not None:
            model_ref = "{{ " + parsed.model + " }}"
            get_rendered(model_ref, ctx, parsed)

        parsed.expression = get_rendered(
            parsed.expression,
            ctx,
            node=parsed,
        )

        # if the metric is disabled we do not want it included in the manifest, only in the disabled dict
        if parsed.config.enabled:
            self.manifest.add_metric(self.yaml.file, parsed)
        else:
            self.manifest.add_disabled(self.yaml.file, parsed)

    def _generate_metric_config(
        self, target: UnparsedMetric, fqn: List[str], package_name: str, rendered: bool
    ):
        generator: BaseContextConfigGenerator
        if rendered:
            generator = ContextConfigGenerator(self.root_project)
        else:
            generator = UnrenderedConfigGenerator(self.root_project)

        # configs with precendence set
        precedence_configs = dict()
        # first apply metric configs
        precedence_configs.update(target.config)

        config = generator.calculate_node_config(
            config_call_dict={},
            fqn=fqn,
            resource_type=NodeType.Metric,
            project_name=package_name,
            base=False,
            patch_config_dict=precedence_configs,
        )
        return config

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedMetric.validate(data)
                unparsed = UnparsedMetric.from_dict(data)

            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)
            self.parse_metric(unparsed)


class GroupParser(YamlReader):
    def __init__(self, schema_parser: SchemaParser, yaml: YamlBlock):
        super().__init__(schema_parser, yaml, NodeType.Group.pluralize())
        self.schema_parser = schema_parser
        self.yaml = yaml

    def parse_group(self, unparsed: UnparsedGroup):
        package_name = self.project.project_name
        unique_id = f"{NodeType.Group}.{package_name}.{unparsed.name}"
        path = self.yaml.path.relative_path

        parsed = Group(
            resource_type=NodeType.Group,
            package_name=package_name,
            path=path,
            original_file_path=self.yaml.path.original_file_path,
            unique_id=unique_id,
            name=unparsed.name,
            owner=unparsed.owner,
        )

        self.manifest.add_group(self.yaml.file, parsed)

    def parse(self):
        for data in self.get_key_dicts():
            try:
                UnparsedGroup.validate(data)
                unparsed = UnparsedGroup.from_dict(data)
            except (ValidationError, JSONValidationError) as exc:
                raise YamlParseDictError(self.yaml.path, self.key, data, exc)

            self.parse_group(unparsed)
