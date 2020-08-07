import os
import pickle
from datetime import datetime
from typing import (
    Dict, Optional, Mapping, Callable, Any, List, Type, Union, MutableMapping
)

import dbt.exceptions
import dbt.flags as flags

from dbt import deprecations
from dbt.adapters.factory import (
    get_relation_class_by_name,
)
from dbt.helper_types import PathSet
from dbt.logger import GLOBAL_LOGGER as logger, DbtProcessState
from dbt.node_types import NodeType
from dbt.clients.jinja import get_rendered
from dbt.clients.system import make_directory
from dbt.config import Project, RuntimeConfig
from dbt.context.docs import generate_runtime_docs
from dbt.contracts.files import FilePath, FileHash
from dbt.contracts.graph.compiled import NonSourceNode
from dbt.contracts.graph.manifest import Manifest, Disabled
from dbt.contracts.graph.parsed import (
    ParsedSourceDefinition, ParsedNode, ParsedMacro, ColumnInfo,
)
from dbt.exceptions import (
    ref_target_not_found,
    get_target_not_found_or_disabled_msg,
    source_target_not_found,
    get_source_not_found_or_disabled_msg,
    warn_or_error,
)
from dbt.parser.base import BaseParser, Parser
from dbt.parser.analysis import AnalysisParser
from dbt.parser.data_test import DataTestParser
from dbt.parser.docs import DocumentationParser
from dbt.parser.hooks import HookParser
from dbt.parser.macros import MacroParser
from dbt.parser.models import ModelParser
from dbt.parser.results import ParseResult
from dbt.parser.schemas import SchemaParser
from dbt.parser.search import FileBlock
from dbt.parser.seeds import SeedParser
from dbt.parser.snapshots import SnapshotParser
from dbt.parser.sources import patch_sources
from dbt.ui import warning_tag
from dbt.version import __version__


PARTIAL_PARSE_FILE_NAME = 'partial_parse.pickle'
PARSING_STATE = DbtProcessState('parsing')
DEFAULT_PARTIAL_PARSE = False


_parser_types: List[Type[Parser]] = [
    ModelParser,
    SnapshotParser,
    AnalysisParser,
    DataTestParser,
    HookParser,
    SeedParser,
    DocumentationParser,
    SchemaParser,
]


# TODO: this should be calculated per-file based on the vars() calls made in
# parsing, so changing one var doesn't invalidate everything. also there should
# be something like that for env_var - currently changing env_vars in way that
# impact graph selection or configs will result in weird test failures.
# finally, we should hash the actual profile used, not just root project +
# profiles.yml + relevant args. While sufficient, it is definitely overkill.
def make_parse_result(
    config: RuntimeConfig, all_projects: Mapping[str, Project]
) -> ParseResult:
    """Make a ParseResult from the project configuration and the profile."""
    # if any of these change, we need to reject the parser
    vars_hash = FileHash.from_contents(
        '\x00'.join([
            getattr(config.args, 'vars', '{}') or '{}',
            getattr(config.args, 'profile', '') or '',
            getattr(config.args, 'target', '') or '',
            __version__
        ])
    )
    profile_path = os.path.join(config.args.profiles_dir, 'profiles.yml')
    with open(profile_path) as fp:
        profile_hash = FileHash.from_contents(fp.read())

    project_hashes = {}
    for name, project in all_projects.items():
        path = os.path.join(project.project_root, 'dbt_project.yml')
        with open(path) as fp:
            project_hashes[name] = FileHash.from_contents(fp.read())

    return ParseResult(
        vars_hash=vars_hash,
        profile_hash=profile_hash,
        project_hashes=project_hashes,
    )


class ManifestLoader:
    def __init__(
        self,
        root_project: RuntimeConfig,
        all_projects: Mapping[str, Project],
        macro_hook: Optional[Callable[[Manifest], Any]] = None,
    ) -> None:
        self.root_project: RuntimeConfig = root_project
        self.all_projects: Mapping[str, Project] = all_projects
        self.macro_hook: Callable[[Manifest], Any]
        if macro_hook is None:
            self.macro_hook = lambda m: None
        else:
            self.macro_hook = macro_hook

        self.results: ParseResult = make_parse_result(
            root_project, all_projects,
        )
        self._loaded_file_cache: Dict[str, FileBlock] = {}

    def parse_with_cache(
        self,
        path: FilePath,
        parser: BaseParser,
        old_results: Optional[ParseResult],
    ) -> None:
        block = self._get_file(path, parser)
        if not self._get_cached(block, old_results, parser):
            parser.parse_file(block)

    def _get_cached(
        self,
        block: FileBlock,
        old_results: Optional[ParseResult],
        parser: BaseParser,
    ) -> bool:
        # TODO: handle multiple parsers w/ same files, by
        # tracking parser type vs node type? Or tracking actual
        # parser type during parsing?
        if old_results is None:
            return False
        if old_results.has_file(block.file):
            return self.results.sanitized_update(
                block.file, old_results, parser.resource_type
            )
        return False

    def _get_file(self, path: FilePath, parser: BaseParser) -> FileBlock:
        if path.search_key in self._loaded_file_cache:
            block = self._loaded_file_cache[path.search_key]
        else:
            block = FileBlock(file=parser.load_file(path))
            self._loaded_file_cache[path.search_key] = block
        return block

    def parse_project(
        self,
        project: Project,
        macro_manifest: Manifest,
        old_results: Optional[ParseResult],
    ) -> None:
        parsers: List[Parser] = []
        for cls in _parser_types:
            parser = cls(self.results, project, self.root_project,
                         macro_manifest)
            parsers.append(parser)

        # per-project cache.
        self._loaded_file_cache.clear()

        for parser in parsers:
            for path in parser.search():
                self.parse_with_cache(path, parser, old_results)

    def load_only_macros(self) -> Manifest:
        old_results = self.read_parse_results()

        for project in self.all_projects.values():
            parser = MacroParser(self.results, project)
            for path in parser.search():
                self.parse_with_cache(path, parser, old_results)

        # make a manifest with just the macros to get the context
        macro_manifest = Manifest.from_macros(
            macros=self.results.macros,
            files=self.results.files
        )
        self.macro_hook(macro_manifest)
        return macro_manifest

    def load(self, macro_manifest: Manifest):
        old_results = self.read_parse_results()
        if old_results is not None:
            logger.debug('Got an acceptable cached parse result')
        self.results.macros.update(macro_manifest.macros)
        self.results.files.update(macro_manifest.files)

        for project in self.all_projects.values():
            # parse a single project
            self.parse_project(project, macro_manifest, old_results)

    def write_parse_results(self):
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)
        make_directory(self.root_project.target_path)
        with open(path, 'wb') as fp:
            pickle.dump(self.results, fp)

    def matching_parse_results(self, result: ParseResult) -> bool:
        """Compare the global hashes of the read-in parse results' values to
        the known ones, and return if it is ok to re-use the results.
        """
        try:
            if result.dbt_version != __version__:
                logger.debug(
                    'dbt version mismatch: {} != {}, cache invalidated'
                    .format(result.dbt_version, __version__)
                )
                return False
        except AttributeError:
            logger.debug('malformed result file, cache invalidated')
            return False

        valid = True

        if self.results.vars_hash != result.vars_hash:
            logger.debug('vars hash mismatch, cache invalidated')
            valid = False
        if self.results.profile_hash != result.profile_hash:
            logger.debug('profile hash mismatch, cache invalidated')
            valid = False

        missing_keys = {
            k for k in self.results.project_hashes
            if k not in result.project_hashes
        }
        if missing_keys:
            logger.debug(
                'project hash mismatch: values missing, cache invalidated: {}'
                .format(missing_keys)
            )
            valid = False

        for key, new_value in self.results.project_hashes.items():
            if key in result.project_hashes:
                old_value = result.project_hashes[key]
                if new_value != old_value:
                    logger.debug(
                        'For key {}, hash mismatch ({} -> {}), cache '
                        'invalidated'
                        .format(key, old_value, new_value)
                    )
                    valid = False
        return valid

    def _partial_parse_enabled(self):
        # if the CLI is set, follow that
        if flags.PARTIAL_PARSE is not None:
            return flags.PARTIAL_PARSE
        # if the config is set, follow that
        elif self.root_project.config.partial_parse is not None:
            return self.root_project.config.partial_parse
        else:
            return DEFAULT_PARTIAL_PARSE

    def read_parse_results(self) -> Optional[ParseResult]:
        if not self._partial_parse_enabled():
            logger.debug('Partial parsing not enabled')
            return None
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)

        if os.path.exists(path):
            try:
                with open(path, 'rb') as fp:
                    result: ParseResult = pickle.load(fp)
                # keep this check inside the try/except in case something about
                # the file has changed in weird ways, perhaps due to being a
                # different version of dbt
                if self.matching_parse_results(result):
                    return result
            except Exception as exc:
                logger.debug(
                    'Failed to load parsed file from disk at {}: {}'
                    .format(path, exc),
                    exc_info=True
                )

        return None

    def process_manifest(self, manifest: Manifest):
        project_name = self.root_project.project_name
        process_sources(manifest, project_name)
        process_refs(manifest, project_name)
        process_docs(manifest, self.root_project)

    def create_manifest(self) -> Manifest:
        # before we do anything else, patch the sources. This mutates
        # results.disabled, so it needs to come before the final 'disabled'
        # list is created
        sources = patch_sources(self.results, self.root_project)
        disabled = []
        for value in self.results.disabled.values():
            disabled.extend(value)

        nodes: MutableMapping[str, NonSourceNode] = {
            k: v for k, v in self.results.nodes.items()
        }

        manifest = Manifest(
            nodes=nodes,
            sources=sources,
            macros=self.results.macros,
            docs=self.results.docs,
            generated_at=datetime.utcnow(),
            metadata=self.root_project.get_metadata(),
            disabled=disabled,
            files=self.results.files,
        )
        manifest.patch_nodes(self.results.patches)
        manifest.patch_macros(self.results.macro_patches)
        self.process_manifest(manifest)
        return manifest

    @classmethod
    def load_all(
        cls,
        root_config: RuntimeConfig,
        macro_manifest: Manifest,
        macro_hook: Callable[[Manifest], Any],
    ) -> Manifest:
        with PARSING_STATE:
            projects = root_config.load_dependencies()
            v1_configs = []
            for project in projects.values():
                if project.config_version == 1:
                    v1_configs.append(f'\n\n     - {project.project_name}')
            if v1_configs:
                deprecations.warn(
                    'dbt-project-yaml-v1',
                    project_names=''.join(v1_configs)
                )
            loader = cls(root_config, projects, macro_hook)
            loader.load(macro_manifest=macro_manifest)
            loader.write_parse_results()
            manifest = loader.create_manifest()
            _check_manifest(manifest, root_config)
            manifest.build_flat_graph()
            return manifest

    @classmethod
    def load_macros(
        cls,
        root_config: RuntimeConfig,
        macro_hook: Callable[[Manifest], Any],
    ) -> Manifest:
        with PARSING_STATE:
            projects = root_config.load_dependencies()
            loader = cls(root_config, projects, macro_hook)
            return loader.load_only_macros()


def invalid_ref_fail_unless_test(node, target_model_name,
                                 target_model_package, disabled):

    if node.resource_type == NodeType.Test:
        msg = get_target_not_found_or_disabled_msg(
            node, target_model_name, target_model_package, disabled
        )
        if disabled:
            logger.debug(warning_tag(msg))
        else:
            warn_or_error(
                msg,
                log_fmt=warning_tag('{}')
            )
    else:
        ref_target_not_found(
            node,
            target_model_name,
            target_model_package,
            disabled=disabled,
        )


def invalid_source_fail_unless_test(
    node, target_name, target_table_name, disabled
):
    if node.resource_type == NodeType.Test:
        msg = get_source_not_found_or_disabled_msg(
            node, target_name, target_table_name, disabled
        )
        if disabled:
            logger.debug(warning_tag(msg))
        else:
            warn_or_error(
                msg,
                log_fmt=warning_tag('{}')
            )
    else:
        source_target_not_found(
            node,
            target_name,
            target_table_name,
            disabled=disabled
        )


def _check_resource_uniqueness(
    manifest: Manifest,
    config: RuntimeConfig,
) -> None:
    names_resources: Dict[str, NonSourceNode] = {}
    alias_resources: Dict[str, NonSourceNode] = {}

    for resource, node in manifest.nodes.items():
        if node.resource_type not in NodeType.refable():
            continue
        # appease mypy - sources aren't refable!
        assert not isinstance(node, ParsedSourceDefinition)

        name = node.name
        # the full node name is really defined by the adapter's relation
        relation_cls = get_relation_class_by_name(config.credentials.type)
        relation = relation_cls.create_from(config=config, node=node)
        full_node_name = str(relation)

        existing_node = names_resources.get(name)
        if existing_node is not None:
            dbt.exceptions.raise_duplicate_resource_name(
                existing_node, node
            )

        existing_alias = alias_resources.get(full_node_name)
        if existing_alias is not None:
            dbt.exceptions.raise_ambiguous_alias(
                existing_alias, node, full_node_name
            )

        names_resources[name] = node
        alias_resources[full_node_name] = node


def _warn_for_unused_resource_config_paths(
    manifest: Manifest, config: RuntimeConfig
) -> None:
    resource_fqns: Mapping[str, PathSet] = manifest.get_resource_fqns()
    disabled_fqns: PathSet = frozenset(tuple(n.fqn) for n in manifest.disabled)
    config.warn_for_unused_resource_config_paths(resource_fqns, disabled_fqns)


def _check_manifest(manifest: Manifest, config: RuntimeConfig) -> None:
    _check_resource_uniqueness(manifest, config)
    _warn_for_unused_resource_config_paths(manifest, config)


def _load_projects(config, paths):
    for path in paths:
        try:
            project = config.new_project(path)
        except dbt.exceptions.DbtProjectError as e:
            raise dbt.exceptions.DbtProjectError(
                'Failed to read package at {}: {}'
                .format(path, e)
            )
        else:
            yield project.project_name, project


def _get_node_column(node, column_name):
    """Given a ParsedNode, add some fields that might be missing. Return a
    reference to the dict that refers to the given column, creating it if
    it doesn't yet exist.
    """
    if column_name in node.columns:
        column = node.columns[column_name]
    else:
        node.columns[column_name] = ColumnInfo(name=column_name)
        node.columns[column_name] = column

    return column


DocsContextCallback = Callable[
    [Union[ParsedNode, ParsedSourceDefinition]],
    Dict[str, Any]
]


def _process_docs_for_node(
    context: Dict[str, Any],
    node: NonSourceNode,
):
    node.description = get_rendered(node.description, context)
    for column_name, column in node.columns.items():
        column.description = get_rendered(column.description, context)


def _process_docs_for_source(
    context: Dict[str, Any],
    source: ParsedSourceDefinition,
):
    table_description = source.description
    source_description = source.source_description
    table_description = get_rendered(table_description, context)
    source_description = get_rendered(source_description, context)
    source.description = table_description
    source.source_description = source_description

    for column in source.columns.values():
        column_desc = column.description
        column_desc = get_rendered(column_desc, context)
        column.description = column_desc


def _process_docs_for_macro(
    context: Dict[str, Any], macro: ParsedMacro
) -> None:
    macro.description = get_rendered(macro.description, context)
    for arg in macro.arguments:
        arg.description = get_rendered(arg.description, context)


def process_docs(manifest: Manifest, config: RuntimeConfig):
    for node in manifest.nodes.values():
        ctx = generate_runtime_docs(
            config,
            node,
            manifest,
            config.project_name,
        )
        _process_docs_for_node(ctx, node)
    for source in manifest.sources.values():
        ctx = generate_runtime_docs(
            config,
            source,
            manifest,
            config.project_name,
        )
        _process_docs_for_source(ctx, source)
    for macro in manifest.macros.values():
        ctx = generate_runtime_docs(
            config,
            macro,
            manifest,
            config.project_name,
        )
        _process_docs_for_macro(ctx, macro)


def _process_refs_for_node(
    manifest: Manifest, current_project: str, node: NonSourceNode
):
    """Given a manifest and a node in that manifest, process its refs"""
    for ref in node.refs:
        target_model: Optional[Union[Disabled, NonSourceNode]] = None
        target_model_name: str
        target_model_package: Optional[str] = None

        if len(ref) == 1:
            target_model_name = ref[0]
        elif len(ref) == 2:
            target_model_package, target_model_name = ref
        else:
            raise dbt.exceptions.InternalException(
                f'Refs should always be 1 or 2 arguments - got {len(ref)}'
            )

        target_model = manifest.resolve_ref(
            target_model_name,
            target_model_package,
            current_project,
            node.package_name,
        )

        if target_model is None or isinstance(target_model, Disabled):
            # This may raise. Even if it doesn't, we don't want to add
            # this node to the graph b/c there is no destination node
            node.config.enabled = False
            invalid_ref_fail_unless_test(
                node, target_model_name, target_model_package,
                disabled=(isinstance(target_model, Disabled))
            )

            continue

        target_model_id = target_model.unique_id

        node.depends_on.nodes.append(target_model_id)
        # TODO: I think this is extraneous, node should already be the same
        # as manifest.nodes[node.unique_id] (we're mutating node here, not
        # making a new one)
        manifest.update_node(node)


def process_refs(manifest: Manifest, current_project: str):
    for node in manifest.nodes.values():
        _process_refs_for_node(manifest, current_project, node)
    return manifest


def _process_sources_for_node(
    manifest: Manifest, current_project: str, node: NonSourceNode
):
    target_source: Optional[Union[Disabled, ParsedSourceDefinition]] = None
    for source_name, table_name in node.sources:
        target_source = manifest.resolve_source(
            source_name,
            table_name,
            current_project,
            node.package_name,
        )

        if target_source is None or isinstance(target_source, Disabled):
            # this folows the same pattern as refs
            node.config.enabled = False
            invalid_source_fail_unless_test(
                node,
                source_name,
                table_name,
                disabled=(isinstance(target_source, Disabled))
            )
            continue
        target_source_id = target_source.unique_id
        node.depends_on.nodes.append(target_source_id)
        manifest.update_node(node)


def process_sources(manifest: Manifest, current_project: str):
    for node in manifest.nodes.values():
        if node.resource_type == NodeType.Source:
            continue
        assert not isinstance(node, ParsedSourceDefinition)
        _process_sources_for_node(manifest, current_project, node)
    return manifest


def process_macro(
    config: RuntimeConfig, manifest: Manifest, macro: ParsedMacro
) -> None:
    ctx = generate_runtime_docs(
        config,
        macro,
        manifest,
        config.project_name,
    )
    _process_docs_for_macro(ctx, macro)


def process_node(
    config: RuntimeConfig, manifest: Manifest, node: NonSourceNode
):

    _process_sources_for_node(
        manifest, config.project_name, node
    )
    _process_refs_for_node(manifest, config.project_name, node)
    ctx = generate_runtime_docs(config, node, manifest, config.project_name)
    _process_docs_for_node(ctx, node)


def load_macro_manifest(
    config: RuntimeConfig,
    macro_hook: Callable[[Manifest], Any],
) -> Manifest:
    return ManifestLoader.load_macros(config, macro_hook)


def load_manifest(
    config: RuntimeConfig,
    macro_manifest: Manifest,
    macro_hook: Callable[[Manifest], Any],
) -> Manifest:
    return ManifestLoader.load_all(config, macro_manifest, macro_hook)
