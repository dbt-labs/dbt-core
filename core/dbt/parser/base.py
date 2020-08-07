import abc
import itertools
import os
from typing import (
    List, Dict, Any, Iterable, Generic, TypeVar
)

from hologram import ValidationError

from dbt import utils
from dbt.clients.jinja import MacroGenerator
from dbt.clients.system import load_file_contents
from dbt.context.providers import (
    generate_parser_model,
    generate_generate_component_name_macro,
)
from dbt.adapters.factory import get_adapter
from dbt.clients.jinja import get_rendered
from dbt.config import Project, RuntimeConfig
from dbt.context.context_config import (
    LegacyContextConfig, ContextConfig, ContextConfigType
)
from dbt.contracts.files import (
    SourceFile, FilePath, FileHash
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import HasUniqueID
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.exceptions import (
    CompilationException, validator_error_message, InternalException
)
from dbt import hooks
from dbt.node_types import NodeType
from dbt.parser.results import ParseResult, ManifestNodes
from dbt.parser.search import FileBlock

# internally, the parser may store a less-restrictive type that will be
# transformed into the final type. But it will have to be derived from
# ParsedNode to be operable.
FinalValue = TypeVar('FinalValue', bound=HasUniqueID)
IntermediateValue = TypeVar('IntermediateValue', bound=HasUniqueID)

IntermediateNode = TypeVar('IntermediateNode', bound=Any)
FinalNode = TypeVar('FinalNode', bound=ManifestNodes)


ConfiguredBlockType = TypeVar('ConfiguredBlockType', bound=FileBlock)


class BaseParser(Generic[FinalValue]):
    def __init__(self, results: ParseResult, project: Project) -> None:
        self.results = results
        self.project = project
        # this should be a superset of [x.path for x in self.results.files]
        # because we fill it via search()
        self.searched: List[FilePath] = []

    @abc.abstractmethod
    def get_paths(self) -> Iterable[FilePath]:
        pass

    def search(self) -> List[FilePath]:
        self.searched = list(self.get_paths())
        return self.searched

    @abc.abstractmethod
    def parse_file(self, block: FileBlock) -> None:
        pass

    @abc.abstractproperty
    def resource_type(self) -> NodeType:
        pass

    def generate_unique_id(self, resource_name: str) -> str:
        """Returns a unique identifier for a resource"""
        return "{}.{}.{}".format(self.resource_type,
                                 self.project.project_name,
                                 resource_name)

    def load_file(
        self,
        path: FilePath,
        *,
        set_contents: bool = True,
    ) -> SourceFile:
        file_contents = load_file_contents(path.absolute_path, strip=False)
        checksum = FileHash.from_contents(file_contents)
        source_file = SourceFile(path=path, checksum=checksum)
        if set_contents:
            source_file.contents = file_contents.strip()
        else:
            source_file.contents = ''
        return source_file


class Parser(BaseParser[FinalValue], Generic[FinalValue]):
    def __init__(
        self,
        results: ParseResult,
        project: Project,
        root_project: RuntimeConfig,
        macro_manifest: Manifest,
    ) -> None:
        super().__init__(results, project)
        self.root_project = root_project
        self.macro_manifest = macro_manifest


class RelationUpdate:
    def __init__(
        self, config: RuntimeConfig, manifest: Manifest, component: str
    ) -> None:
        macro = manifest.find_generate_macro_by_name(
            component=component,
            root_project_name=config.project_name,
        )
        if macro is None:
            raise InternalException(
                f'No macro with name generate_{component}_name found'
            )

        root_context = generate_generate_component_name_macro(
            macro, config, manifest
        )
        self.updater = MacroGenerator(macro, root_context)
        self.component = component

    def __call__(
        self, parsed_node: Any, config_dict: Dict[str, Any]
    ) -> None:
        override = config_dict.get(self.component)
        new_value = self.updater(override, parsed_node)
        if isinstance(new_value, str):
            new_value = new_value.strip()
        setattr(parsed_node, self.component, new_value)


class ConfiguredParser(
    Parser[FinalNode],
    Generic[ConfiguredBlockType, IntermediateNode, FinalNode],
):
    def __init__(
        self,
        results: ParseResult,
        project: Project,
        root_project: RuntimeConfig,
        macro_manifest: Manifest,
    ) -> None:
        super().__init__(results, project, root_project, macro_manifest)

        self._update_node_database = RelationUpdate(
            manifest=macro_manifest, config=root_project, component='database'
        )
        self._update_node_schema = RelationUpdate(
            manifest=macro_manifest, config=root_project, component='schema'
        )
        self._update_node_alias = RelationUpdate(
            manifest=macro_manifest, config=root_project, component='alias'
        )

    @abc.abstractclassmethod
    def get_compiled_path(cls, block: ConfiguredBlockType) -> str:
        pass

    @abc.abstractmethod
    def parse_from_dict(self, dict, validate=True) -> IntermediateNode:
        pass

    @abc.abstractproperty
    def resource_type(self) -> NodeType:
        pass

    @property
    def default_schema(self):
        return self.root_project.credentials.schema

    @property
    def default_database(self):
        return self.root_project.credentials.database

    def get_fqn_prefix(self, path: str) -> List[str]:
        no_ext = os.path.splitext(path)[0]
        fqn = [self.project.project_name]
        fqn.extend(utils.split_path(no_ext)[:-1])
        return fqn

    def get_fqn(self, path: str, name: str) -> List[str]:
        """Get the FQN for the node. This impacts node selection and config
        application.
        """
        fqn = self.get_fqn_prefix(path)
        fqn.append(name)
        return fqn

    def _mangle_hooks(self, config):
        """Given a config dict that may have `pre-hook`/`post-hook` keys,
        convert it from the yucky maybe-a-string, maybe-a-dict to a dict.
        """
        # Like most of parsing, this is a horrible hack :(
        for key in hooks.ModelHookType:
            if key in config:
                config[key] = [hooks.get_hook_dict(h) for h in config[key]]

    def _create_error_node(
        self, name: str, path: str, original_file_path: str, raw_sql: str,
    ) -> UnparsedNode:
        """If we hit an error before we've actually parsed a node, provide some
        level of useful information by attaching this to the exception.
        """
        # this is a bit silly, but build an UnparsedNode just for error
        # message reasons
        return UnparsedNode(
            name=name,
            resource_type=self.resource_type,
            path=path,
            original_file_path=original_file_path,
            root_path=self.project.project_root,
            package_name=self.project.project_name,
            raw_sql=raw_sql,
        )

    def _create_parsetime_node(
        self,
        block: ConfiguredBlockType,
        path: str,
        config: ContextConfigType,
        fqn: List[str],
        name=None,
        **kwargs,
    ) -> IntermediateNode:
        """Create the node that will be passed in to the parser context for
        "rendering". Some information may be partial, as it'll be updated by
        config() and any ref()/source() calls discovered during rendering.
        """
        if name is None:
            name = block.name
        dct = {
            'alias': name,
            'schema': self.default_schema,
            'database': self.default_database,
            'fqn': fqn,
            'name': name,
            'root_path': self.project.project_root,
            'resource_type': self.resource_type,
            'path': path,
            'original_file_path': block.path.original_file_path,
            'package_name': self.project.project_name,
            'raw_sql': block.contents,
            'unique_id': self.generate_unique_id(name),
            'config': self.config_dict(config),
            'checksum': block.file.checksum.to_dict(),
        }
        dct.update(kwargs)
        try:
            return self.parse_from_dict(dct)
        except ValidationError as exc:
            msg = validator_error_message(exc)
            # this is a bit silly, but build an UnparsedNode just for error
            # message reasons
            node = self._create_error_node(
                name=block.name,
                path=path,
                original_file_path=block.path.original_file_path,
                raw_sql=block.contents,
            )
            raise CompilationException(msg, node=node)

    def _context_for(
        self, parsed_node: IntermediateNode, config: ContextConfigType
    ) -> Dict[str, Any]:
        return generate_parser_model(
            parsed_node, self.root_project, self.macro_manifest, config
        )

    def render_with_context(
        self, parsed_node: IntermediateNode, config: ContextConfigType
    ) -> None:
        """Given the parsed node and a ContextConfigType to use during parsing,
        render the node's sql wtih macro capture enabled.

        Note: this mutates the config object when config() calls are rendered.
        """
        # during parsing, we don't have a connection, but we might need one, so
        # we have to acquire it.
        with get_adapter(self.root_project).connection_for(parsed_node):
            context = self._context_for(parsed_node, config)

            get_rendered(
                parsed_node.raw_sql, context, parsed_node, capture_macros=True
            )

    def update_parsed_node_config(
        self, parsed_node: IntermediateNode, config_dict: Dict[str, Any]
    ) -> None:
        # Overwrite node config
        final_config_dict = parsed_node.config.to_dict()
        final_config_dict.update(config_dict)
        # re-mangle hooks, in case we got new ones
        self._mangle_hooks(final_config_dict)
        parsed_node.config = parsed_node.config.from_dict(final_config_dict)

    def update_parsed_node_name(
        self, parsed_node: IntermediateNode, config_dict: Dict[str, Any]
    ) -> None:
        self._update_node_database(parsed_node, config_dict)
        self._update_node_schema(parsed_node, config_dict)
        self._update_node_alias(parsed_node, config_dict)

    def update_parsed_node(
        self, parsed_node: IntermediateNode, config: ContextConfigType
    ) -> None:
        """Given the ContextConfigType used for parsing and the parsed node,
        generate and set the true values to use, overriding the temporary parse
        values set in _build_intermediate_parsed_node.
        """
        config_dict = config.build_config_dict()

        # Set tags on node provided in config blocks
        model_tags = config_dict.get('tags', [])
        parsed_node.tags.extend(model_tags)

        # do this once before we parse the node database/schema/alias, so
        # parsed_node.config is what it would be if they did nothing
        self.update_parsed_node_config(parsed_node, config_dict)
        self.update_parsed_node_name(parsed_node, config_dict)

        # at this point, we've collected our hooks. Use the node context to
        # render each hook and collect refs/sources
        hooks = list(itertools.chain(parsed_node.config.pre_hook,
                                     parsed_node.config.post_hook))
        # skip context rebuilding if there aren't any hooks
        if not hooks:
            return
        # we could cache the original context from parsing this node. Is that
        # worth the cost in memory/complexity?
        context = self._context_for(parsed_node, config)
        for hook in hooks:
            get_rendered(hook.sql, context, parsed_node, capture_macros=True)

    def initial_config(self, fqn: List[str]) -> ContextConfigType:
        config_version = min(
            [self.project.config_version, self.root_project.config_version]
        )
        # grab a list of the existing project names. This is for var conversion
        all_projects = self.root_project.load_dependencies()
        if config_version == 1:
            return LegacyContextConfig(
                self.root_project.as_v1(all_projects),
                self.project.as_v1(all_projects),
                fqn,
                self.resource_type,
            )
        elif config_version == 2:
            return ContextConfig(
                self.root_project,
                fqn,
                self.resource_type,
                self.project.project_name,
            )
        else:
            raise InternalException(
                f'Got an unexpected project version={config_version}, '
                f'expected 1 or 2'
            )

    def config_dict(
        self, config: ContextConfigType,
    ) -> Dict[str, Any]:
        config_dict = config.build_config_dict(base=True)
        self._mangle_hooks(config_dict)
        return config_dict

    def render_update(
        self, node: IntermediateNode, config: ContextConfigType
    ) -> None:
        try:
            self.render_with_context(node, config)
            self.update_parsed_node(node, config)
        except ValidationError as exc:
            # we got a ValidationError - probably bad types in config()
            msg = validator_error_message(exc)
            raise CompilationException(msg, node=node) from exc

    def add_result_node(self, block: FileBlock, node: ManifestNodes):
        if node.config.enabled:
            self.results.add_node(block.file, node)
        else:
            self.results.add_disabled(block.file, node)

    def parse_node(self, block: ConfiguredBlockType) -> FinalNode:
        compiled_path: str = self.get_compiled_path(block)
        fqn = self.get_fqn(compiled_path, block.name)

        config: ContextConfigType = self.initial_config(fqn)

        node = self._create_parsetime_node(
            block=block,
            path=compiled_path,
            config=config,
            fqn=fqn,
        )
        self.render_update(node, config)
        result = self.transform(node)
        self.add_result_node(block, result)
        return result

    @abc.abstractmethod
    def parse_file(self, file_block: FileBlock) -> None:
        pass

    @abc.abstractmethod
    def transform(self, node: IntermediateNode) -> FinalNode:
        pass


class SimpleParser(
    ConfiguredParser[ConfiguredBlockType, FinalNode, FinalNode],
    Generic[ConfiguredBlockType, FinalNode]
):
    def transform(self, node):
        return node


class SQLParser(
    ConfiguredParser[FileBlock, IntermediateNode, FinalNode],
    Generic[IntermediateNode, FinalNode]
):
    def parse_file(self, file_block: FileBlock) -> None:
        self.parse_node(file_block)


class SimpleSQLParser(
    SQLParser[FinalNode, FinalNode]
):
    def transform(self, node):
        return node
