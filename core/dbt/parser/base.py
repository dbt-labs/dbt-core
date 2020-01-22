import abc
import itertools
import os
from typing import (
    List, Dict, Any, Callable, Iterable, Optional, Generic, TypeVar
)

from hologram import ValidationError

import dbt.context.parser
import dbt.flags
from dbt import deprecations
from dbt import hooks
from dbt.clients.jinja import get_rendered
from dbt.config import Project, RuntimeConfig
from dbt.contracts.graph.manifest import (
    Manifest, SourceFile, FilePath, FileHash
)
from dbt.contracts.graph.parsed import HasUniqueID
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.exceptions import (
    CompilationException, validator_error_message
)
from dbt.include.global_project import PROJECT_NAME as GLOBAL_PROJECT_NAME
from dbt.node_types import NodeType
from dbt.source_config import SourceConfig
from dbt.parser.results import ParseResult, ManifestNodes
from dbt.parser.search import FileBlock
from dbt.clients.system import load_file_contents

# internally, the parser may store a less-restrictive type that will be
# transformed into the final type. But it will have to be derived from
# ParsedNode to be operable.
FinalValue = TypeVar('FinalValue', bound=HasUniqueID)
IntermediateValue = TypeVar('IntermediateValue', bound=HasUniqueID)

IntermediateNode = TypeVar('IntermediateNode', bound=Any)
FinalNode = TypeVar('FinalNode', bound=ManifestNodes)


RelationUpdate = Callable[[Optional[str], IntermediateNode], str]
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

    def load_file(self, path: FilePath) -> SourceFile:
        file_contents = load_file_contents(path.absolute_path, strip=False)
        checksum = FileHash.from_contents(file_contents)
        source_file = SourceFile(path=path, checksum=checksum)
        source_file.contents = file_contents.strip()
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
        self._get_schema_func: Optional[RelationUpdate] = None
        self._get_alias_func: Optional[RelationUpdate] = None

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

    def get_schema_func(self) -> RelationUpdate:
        """The get_schema function is set by a few different things:
            - if there is a 'generate_schema_name' macro in the root project,
                it will be used.
            - if that does not exist but there is a 'generate_schema_name'
                macro in the 'dbt' internal project, that will be used
            - if neither of those exist (unit tests?), a function that returns
                the 'default schema' as set in the root project's 'credentials'
                is used
        """
        if self._get_schema_func is not None:
            return self._get_schema_func

        get_schema_macro = self.macro_manifest.find_macro_by_name(
            'generate_schema_name',
            self.root_project.project_name
        )
        if get_schema_macro is None:
            get_schema_macro = self.macro_manifest.find_macro_by_name(
                'generate_schema_name',
                GLOBAL_PROJECT_NAME
            )
        # this is only true in tests!
        if get_schema_macro is None:
            def get_schema(custom_schema_name=None, node=None):
                return self.default_schema
        else:
            root_context = dbt.context.parser.generate_macro(
                get_schema_macro, self.root_project,
                self.macro_manifest
            )
            get_schema = get_schema_macro.generator(root_context)

        self._get_schema_func = get_schema
        return self._get_schema_func

    def get_alias_func(self) -> RelationUpdate:
        """The get_alias function is set by a few different things:
            - if there is a 'generate_alias_name' macro in the root project,
                it will be used.
            - if that does not exist but there is a 'generate_alias_name'
                macro in the 'dbt' internal project, that will be used
            - if neither of those exist (unit tests?), a function that returns
                the 'default alias' as set in the model's filename or alias
                configuration.
        """
        if self._get_alias_func is not None:
            return self._get_alias_func

        get_alias_macro = self.macro_manifest.find_macro_by_name(
            'generate_alias_name',
            self.root_project.project_name
        )
        if get_alias_macro is None:
            get_alias_macro = self.macro_manifest.find_macro_by_name(
                'generate_alias_name',
                GLOBAL_PROJECT_NAME
            )

        # the generate_alias_name macro might not exist
        if get_alias_macro is None:
            def get_alias(custom_alias_name, node):
                if custom_alias_name is None:
                    return node.name
                else:
                    return custom_alias_name
        else:
            root_context = dbt.context.parser.generate_macro(
                get_alias_macro, self.root_project,
                self.macro_manifest
            )
            get_alias = get_alias_macro.generator(root_context)

        self._get_alias_func = get_alias
        return self._get_alias_func

    def get_fqn(self, path: str, name: str) -> List[str]:
        """Get the FQN for the node. This impacts node selection and config
        application.
        """
        no_ext = os.path.splitext(path)[0]
        fqn = [self.project.project_name]
        fqn.extend(dbt.utils.split_path(no_ext)[:-1])
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
        config: SourceConfig,
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
            'fqn': config.fqn,
            'name': name,
            'root_path': self.project.project_root,
            'resource_type': self.resource_type,
            'path': path,
            'original_file_path': block.path.original_file_path,
            'package_name': self.project.project_name,
            'raw_sql': block.contents,
            'unique_id': self.generate_unique_id(name),
            'config': self.config_dict(config),
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
        self, parsed_node: IntermediateNode, config: SourceConfig
    ) -> Dict[str, Any]:
        return dbt.context.parser.generate(
            parsed_node, self.root_project, self.macro_manifest, config
        )

    def render_with_context(
        self, parsed_node: IntermediateNode, config: SourceConfig
    ) -> None:
        """Given the parsed node and a SourceConfig to use during parsing,
        render the node's sql wtih macro capture enabled.

        Note: this mutates the config object when config() calls are rendered.
        """
        context = self._context_for(parsed_node, config)

        get_rendered(parsed_node.raw_sql, context, parsed_node,
                     capture_macros=True)

    def update_parsed_node_schema(
        self, parsed_node: IntermediateNode, config_dict: Dict[str, Any]
    ) -> None:
        # Special macro defined in the global project. Use the root project's
        # definition, not the current package
        schema_override = config_dict.get('schema')
        get_schema = self.get_schema_func()
        try:
            schema = get_schema(schema_override, parsed_node)
        except dbt.exceptions.CompilationException as exc:
            too_many_args = (
                "macro 'dbt_macro__generate_schema_name' takes not more than "
                "1 argument(s)"
            )
            if too_many_args not in str(exc):
                raise
            deprecations.warn('generate-schema-name-single-arg')
            schema = get_schema(schema_override)  # type: ignore
        parsed_node.schema = schema.strip()

    def update_parsed_node_alias(
        self, parsed_node: IntermediateNode, config_dict: Dict[str, Any]
    ) -> None:
        alias_override = config_dict.get('alias')
        get_alias = self.get_alias_func()
        parsed_node.alias = get_alias(alias_override, parsed_node).strip()

    def update_parsed_node_config(
        self, parsed_node: IntermediateNode, config_dict: Dict[str, Any]
    ) -> None:
        # Overwrite node config
        final_config_dict = parsed_node.config.to_dict()
        final_config_dict.update(config_dict)
        # re-mangle hooks, in case we got new ones
        self._mangle_hooks(final_config_dict)
        parsed_node.config = parsed_node.config.from_dict(final_config_dict)

    def update_parsed_node(
        self, parsed_node: IntermediateNode, config: SourceConfig
    ) -> None:
        """Given the SourceConfig used for parsing and the parsed node,
        generate and set the true values to use, overriding the temporary parse
        values set in _build_intermediate_parsed_node.
        """
        config_dict = config.config

        # Set tags on node provided in config blocks
        model_tags = config_dict.get('tags', [])
        parsed_node.tags.extend(model_tags)

        # do this once before we parse the node schema/alias, so
        # parsed_node.config is what it would be if they did nothing
        self.update_parsed_node_config(parsed_node, config_dict)

        parsed_node.database = config_dict.get(
            'database', self.default_database
        ).strip()
        self.update_parsed_node_schema(parsed_node, config_dict)
        self.update_parsed_node_alias(parsed_node, config_dict)

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

    def initial_config(self, fqn: List[str]) -> SourceConfig:
        return SourceConfig(self.root_project, self.project, fqn,
                            self.resource_type)

    def config_dict(self, config: SourceConfig) -> Dict[str, Any]:
        config_dict = config.config
        self._mangle_hooks(config_dict)
        return config_dict

    def render_update(
        self, node: IntermediateNode, config: SourceConfig
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

        config: SourceConfig = self.initial_config(fqn)

        node = self._create_parsetime_node(
            block=block,
            path=compiled_path,
            config=config
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
