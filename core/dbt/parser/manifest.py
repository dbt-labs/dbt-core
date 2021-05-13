from dataclasses import dataclass
from dataclasses import field
import os
import pickle
from typing import (
    Dict, Optional, Mapping, Callable, Any, List, Type, Union
)
import time

import dbt.exceptions
import dbt.tracking
import dbt.flags as flags

from dbt.adapters.factory import (
    get_adapter,
    get_relation_class_by_name,
    get_adapter_package_names,
)
from dbt.helper_types import PathSet
from dbt.logger import GLOBAL_LOGGER as logger, DbtProcessState
from dbt.node_types import NodeType
from dbt.clients.jinja import get_rendered, statically_extract_macro_calls
from dbt.clients.system import make_directory
from dbt.config import Project, RuntimeConfig
from dbt.context.docs import generate_runtime_docs
from dbt.context.macro_resolver import MacroResolver
from dbt.context.base import generate_base_context
from dbt.contracts.files import FileHash, ParseFileType
from dbt.parser.read_files import read_files, load_source_file
from dbt.contracts.graph.compiled import ManifestNode
from dbt.contracts.graph.manifest import (
    Manifest, Disabled, MacroManifest, ManifestStateCheck
)
from dbt.contracts.graph.parsed import (
    ParsedSourceDefinition, ParsedNode, ParsedMacro, ColumnInfo, ParsedExposure
)
from dbt.contracts.util import Writable
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
from dbt.parser.schemas import SchemaParser
from dbt.parser.search import FileBlock
from dbt.parser.seeds import SeedParser
from dbt.parser.snapshots import SnapshotParser
from dbt.parser.sources import patch_sources
from dbt.ui import warning_tag
from dbt.version import __version__

from dbt.dataclass_schema import dbtClassMixin

PARTIAL_PARSE_FILE_NAME = 'partial_parse.pickle'
PARSING_STATE = DbtProcessState('parsing')
DEFAULT_PARTIAL_PARSE = False


# Part of saved performance info
@dataclass
class ParserInfo(dbtClassMixin):
    parser: str
    elapsed: float
    path_count: int = 0


# Part of saved performance info
@dataclass
class ProjectLoaderInfo(dbtClassMixin):
    project_name: str
    elapsed: float
    parsers: List[ParserInfo] = field(default_factory=list)
    path_count: int = 0


# Part of saved performance info
@dataclass
class ManifestLoaderInfo(dbtClassMixin, Writable):
    path_count: int = 0
    is_partial_parse_enabled: Optional[bool] = None
    read_files_elapsed: Optional[float] = None
    load_macros_elapsed: Optional[float] = None
    parse_project_elapsed: Optional[float] = None
    patch_sources_elapsed: Optional[float] = None
    process_manifest_elapsed: Optional[float] = None
    load_all_elapsed: Optional[float] = None
    projects: List[ProjectLoaderInfo] = field(default_factory=list)
    _project_index: Dict[str, ProjectLoaderInfo] = field(default_factory=dict)

    def __post_serialize__(self, dct):
        del dct['_project_index']
        return dct


# The ManifestLoader loads the manifest. The standard way to use the
# ManifestLoader is using the 'get_full_manifest' class method, but
# many tests use abbreviated processes.
class ManifestLoader:
    def __init__(
        self,
        root_project: RuntimeConfig,
        all_projects: Mapping[str, Project],
        macro_hook: Optional[Callable[[Manifest], Any]] = None,
    ) -> None:
        self.root_project: RuntimeConfig = root_project
        self.all_projects: Mapping[str, Project] = all_projects
        self.manifest: Manifest = Manifest({}, {}, {}, {}, {}, {}, [], {})
        self.manifest.metadata = root_project.get_metadata()
        # This is a MacroQueryStringSetter callable, which is called
        # later after we set the MacroManifest in the adapter. It sets
        # up the query headers.
        self.macro_hook: Callable[[Manifest], Any]
        if macro_hook is None:
            self.macro_hook = lambda m: None
        else:
            self.macro_hook = macro_hook

        # State check determines whether the old_manifest and the current
        # manifest match well enough to do partial parsing
        self.manifest.state_check = self.build_manifest_state_check()

        self._perf_info = self.build_perf_info()

        # This is a saved manifest from a previous run that's used for partial parsing
        self.old_manifest: Optional[Manifest] = self.read_saved_manifest()

    # This is the method that builds a complete manifest. We sometimes
    # use an abbreviated process in tests.
    @classmethod
    def get_full_manifest(
        cls,
        config: RuntimeConfig,
        *,
        reset: bool = False,
    ) -> Manifest:

        adapter = get_adapter(config)  # type: ignore
        # reset is set in a TaskManager load_manifest call, since
        # the config and adapter may be persistent.
        if reset:
            config.clear_dependencies()
            adapter.clear_macro_manifest()
        macro_hook = adapter.connections.set_query_header

        with PARSING_STATE:  # set up logbook.Processor for parsing
            # Start performance counting
            start_load_all = time.perf_counter()

            projects = config.load_dependencies()
            loader = ManifestLoader(config, projects, macro_hook)
            loader.load()

            # The goal is to move partial parse writing to after update_manifest
            loader.write_manifest_for_partial_parse()
            manifest = loader.update_manifest()
            # Move write_manifest_for_partial_parse here

            _check_manifest(manifest, config)
            manifest.build_flat_graph()

            # This needs to happen after loading from a partial parse,
            # so that the adapter has the query headers from the macro_hook.
            loader.save_macros_to_adapter(adapter)

            # Save performance info
            loader._perf_info.load_all_elapsed = (
                time.perf_counter() - start_load_all
            )
            loader.track_project_load()

        return manifest

    # This is where the main action happens
    def load(self):

        if self.old_manifest is not None:
            logger.debug('Got an acceptable saved parse result')

        # Read files creates a dictionary of projects to a dictionary
        # of parsers to lists of file strings. The file strings are
        # used to get the SourceFiles from the manifest files.
        # In the future the loaded files will be used to control
        # partial parsing, but right now we're just moving the
        # file loading out of the individual parsers and doing it
        # all at once.
        start_read_files = time.perf_counter()
        project_parser_files = {}
        for project in self.all_projects.values():
            read_files(project, self.manifest.files, project_parser_files)
        self._perf_info.read_files_elapsed = (time.perf_counter() - start_read_files)

        # We need to parse the macros first, so they're resolvable when
        # the other files are loaded
        start_load_macros = time.perf_counter()
        for project in self.all_projects.values():
            parser = MacroParser(project, self.manifest)
            parser_files = project_parser_files[project.project_name]
            for search_key in parser_files['MacroParser']:
                block = FileBlock(self.manifest.files[search_key])
                self.parse_with_cache(block, parser)
        self.reparse_macros()
        # This is where a loop over self.manifest.macros should be performed
        # to set the 'depends_on' information from static rendering.
        self._perf_info.load_macros_elapsed = (time.perf_counter() - start_load_macros)

        # Load the rest of the files except for schema yaml files
        parser_types: List[Type[Parser]] = [
            ModelParser, SnapshotParser, AnalysisParser, DataTestParser,
            SeedParser, DocumentationParser, HookParser]
        for project in self.all_projects.values():
            if project.project_name not in project_parser_files:
                continue
            self.parse_project(project, project_parser_files[project.project_name], parser_types)

        # Load yaml files
        parser_types = [SchemaParser]
        for project in self.all_projects.values():
            if project.project_name not in project_parser_files:
                continue
            self.parse_project(project, project_parser_files[project.project_name], parser_types)

    # Parse every file in this project, except macros (already done)
    def parse_project(
        self,
        project: Project,
        parser_files,
        parser_types: List[Type[Parser]],
    ) -> None:

        project_loader_info = self._perf_info._project_index[project.project_name]
        start_timer = time.perf_counter()
        total_path_count = 0

        # Loop through parsers with loaded files.
        for parser_cls in parser_types:
            parser_name = parser_cls.__name__
            # No point in creating a parser if we don't have files for it
            if parser_name not in parser_files or not parser_files[parser_name]:
                continue

            # Initialize timing info
            parser_path_count = 0
            parser_start_timer = time.perf_counter()

            # Parse the project files for this parser
            parser: Parser = parser_cls(project, self.manifest, self.root_project)
            for file_id in parser_files[parser_name]:
                block = FileBlock(self.manifest.files[file_id])
                if isinstance(parser, SchemaParser):
                    dct = block.file.dict_from_yaml
                    parser.parse_file(block, dct=dct)
                else:
                    parser.parse_file(block)
                parser_path_count = parser_path_count + 1

            # Save timing info
            project_loader_info.parsers.append(ParserInfo(
                parser=parser.resource_type,
                path_count=parser_path_count,
                elapsed=time.perf_counter() - parser_start_timer
            ))
            total_path_count = total_path_count + parser_path_count

        # HookParser doesn't run from loaded files, just dbt_project.yml,
        # so do separately
        if HookParser in parser_types:
            hook_parser = HookParser(project, self.manifest, self.root_project)
            path = hook_parser.get_path()
            file_block = FileBlock(
                load_source_file(path, ParseFileType.Hook, project.project_name)
            )
            hook_parser.parse_file(file_block)

        # Store the performance info
        elapsed = time.perf_counter() - start_timer
        project_loader_info.path_count = project_loader_info.path_count + total_path_count
        project_loader_info.elapsed = project_loader_info.elapsed + elapsed
        self._perf_info.path_count = (
            self._perf_info.path_count + total_path_count
        )

    # Loop through macros in the manifest and statically parse
    # the 'macro_sql' to find depends_on.macros
    def reparse_macros(self):
        internal_package_names = get_adapter_package_names(
            self.root_project.credentials.type
        )
        macro_resolver = MacroResolver(
            self.manifest.macros,
            self.root_project.project_name,
            internal_package_names
        )
        base_ctx = generate_base_context({})
        for macro in self.manifest.macros.values():
            possible_macro_calls = statically_extract_macro_calls(macro.macro_sql, base_ctx)
            for macro_name in possible_macro_calls:
                # adapter.dispatch calls can generate a call with the same name as the macro
                # it ought to be an adapter prefix (postgres_) or default_
                if macro_name == macro.name:
                    continue
                package_name = macro.package_name
                if '.' in macro_name:
                    package_name, macro_name = macro_name.split('.')
                dep_macro_id = macro_resolver.get_macro_id(package_name, macro_name)
                if dep_macro_id:
                    macro.depends_on.add_macro(dep_macro_id)  # will check for dupes

    # This is where we use the partial-parse state from the
    # pickle file (if it exists)
    def parse_with_cache(
        self,
        block: FileBlock,
        parser: BaseParser,
    ) -> None:
        # _get_cached actually copies the nodes, etc, that were
        # generated from the file to the results, in 'sanitized_update'
        if not self._get_cached(block, parser):
            parser.parse_file(block)

    # check if we have a stored parse file, then check if
    # file checksums are the same or not and either return
    # the old ... stuff or return false (not cached)
    def _get_cached(
        self,
        block: FileBlock,
        parser: BaseParser,
    ) -> bool:
        # TODO: handle multiple parsers w/ same files, by
        # tracking parser type vs node type? Or tracking actual
        # parser type during parsing?
        if self.old_manifest is None:
            return False
        # The 'has_file' method is where we check to see if
        # the checksum of the old file is the same as the new
        # file. If the checksum is different, 'has_file' returns
        # false. If it's the same, the file and the things that
        # were generated from it are used.
        if self.old_manifest.has_file(block.file):
            return self.manifest.sanitized_update(
                block.file, self.old_manifest, parser.resource_type
            )
        return False

    def write_manifest_for_partial_parse(self):
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)
        make_directory(self.root_project.target_path)
        with open(path, 'wb') as fp:
            pickle.dump(self.manifest, fp)

    def matching_parse_results(self, manifest: Manifest) -> bool:
        """Compare the global hashes of the read-in parse results' values to
        the known ones, and return if it is ok to re-use the results.
        """
        try:
            if manifest.metadata.dbt_version != __version__:
                logger.debug(
                    'dbt version mismatch: {} != {}, cache invalidated'
                    .format(manifest.metadata.dbt_version, __version__)
                )
                return False
        except AttributeError as exc:
            logger.debug(f"malformed result file, cache invalidated: {exc}")
            return False

        valid = True

        if not self.manifest.state_check or not manifest.state_check:
            return False

        if self.manifest.state_check.vars_hash != manifest.state_check.vars_hash:
            logger.debug('vars hash mismatch, cache invalidated')
            valid = False
        if self.manifest.state_check.profile_hash != manifest.state_check.profile_hash:
            logger.debug('profile hash mismatch, cache invalidated')
            valid = False

        missing_keys = {
            k for k in self.manifest.state_check.project_hashes
            if k not in manifest.state_check.project_hashes
        }
        if missing_keys:
            logger.debug(
                'project hash mismatch: values missing, cache invalidated: {}'
                .format(missing_keys)
            )
            valid = False

        for key, new_value in self.manifest.state_check.project_hashes.items():
            if key in manifest.state_check.project_hashes:
                old_value = manifest.state_check.project_hashes[key]
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

    def read_saved_manifest(self) -> Optional[Manifest]:
        if not self._partial_parse_enabled():
            logger.debug('Partial parsing not enabled')
            return None
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)

        if os.path.exists(path):
            try:
                with open(path, 'rb') as fp:
                    manifest: Manifest = pickle.load(fp)
                # keep this check inside the try/except in case something about
                # the file has changed in weird ways, perhaps due to being a
                # different version of dbt
                if self.matching_parse_results(manifest):
                    return manifest
            except Exception as exc:
                logger.debug(
                    'Failed to load parsed file from disk at {}: {}'
                    .format(path, exc),
                    exc_info=True
                )
        return None

    # This find the sources, refs, and docs and resolves them
    # for nodes and exposures
    def process_manifest(self):
        project_name = self.root_project.project_name
        process_sources(self.manifest, project_name)
        process_refs(self.manifest, project_name)
        process_docs(self.manifest, self.root_project)

    def update_manifest(self) -> Manifest:
        start_patch = time.perf_counter()
        # patch_sources converts the UnparsedSourceDefinitions in the
        # Manifest.sources to ParsedSourceDefinition via 'patch_source'
        # in SourcePatcher
        sources = patch_sources(self.root_project, self.manifest)
        self.manifest.sources = sources
        # ParseResults had a 'disabled' attribute which was a dictionary
        # which is now named '_disabled'. This used to copy from
        # ParseResults to the Manifest. Can this be normalized so
        # there's only one disabled?
        disabled = []
        for value in self.manifest._disabled.values():
            disabled.extend(value)
        self.manifest.disabled = disabled
        self._perf_info.patch_sources_elapsed = (
            time.perf_counter() - start_patch
        )

        self.manifest.selectors = self.root_project.manifest_selectors

        # do the node and macro patches
        self.manifest.patch_nodes()
        self.manifest.patch_macros()

        # process_manifest updates the refs, sources, and docs
        start_process = time.perf_counter()
        self.process_manifest()

        self._perf_info.process_manifest_elapsed = (
            time.perf_counter() - start_process
        )

        return self.manifest

    def build_perf_info(self):
        mli = ManifestLoaderInfo(
            is_partial_parse_enabled=self._partial_parse_enabled()
        )
        for project in self.all_projects.values():
            project_info = ProjectLoaderInfo(
                project_name=project.project_name,
                path_count=0,
                elapsed=0,
            )
            mli.projects.append(project_info)
            mli._project_index[project.project_name] = project_info
        return mli

    # TODO: this should be calculated per-file based on the vars() calls made in
    # parsing, so changing one var doesn't invalidate everything. also there should
    # be something like that for env_var - currently changing env_vars in way that
    # impact graph selection or configs will result in weird test failures.
    # finally, we should hash the actual profile used, not just root project +
    # profiles.yml + relevant args. While sufficient, it is definitely overkill.
    def build_manifest_state_check(self):
        config = self.root_project
        all_projects = self.all_projects
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

        state_check = ManifestStateCheck(
            vars_hash=vars_hash,
            profile_hash=profile_hash,
            project_hashes=project_hashes,
        )
        return state_check

    def save_macros_to_adapter(self, adapter):
        macro_manifest = MacroManifest(self.manifest.macros)
        adapter._macro_manifest_lazy = macro_manifest
        # This executes the callable macro_hook and sets the
        # query headers
        self.macro_hook(macro_manifest)

    # This creates a MacroManifest which contains the macros in
    # the adapter. Only called by the load_macros call from the
    # adapter.
    def create_macro_manifest(self):
        for project in self.all_projects.values():
            # what is the manifest passed in actually used for?
            macro_parser = MacroParser(project, self.manifest)
            for path in macro_parser.get_paths():
                source_file = load_source_file(
                    path, ParseFileType.Macro, project.project_name)
                block = FileBlock(source_file)
                # This does not add the file to the manifest.files,
                # but that shouldn't be necessary here.
                self.parse_with_cache(block, macro_parser)
        macro_manifest = MacroManifest(self.manifest.macros)
        return macro_manifest

    # This is called by the adapter code only, to create the
    # MacroManifest that's stored in the adapter.
    # 'get_full_manifest' uses a persistent ManifestLoader while this
    # creates a temporary ManifestLoader and throws it away.
    # Not sure when this would actually get used except in tests.
    # The ManifestLoader loads macros with other files, then copies
    # into the adapter MacroManifest.
    @classmethod
    def load_macros(
        cls,
        root_config: RuntimeConfig,
        macro_hook: Callable[[Manifest], Any],
    ) -> Manifest:
        with PARSING_STATE:
            projects = root_config.load_dependencies()
            # This creates a loader object, including result,
            # and then throws it away, returning only the
            # manifest
            loader = cls(root_config, projects, macro_hook)
            macro_manifest = loader.create_macro_manifest()

        return macro_manifest

    # Create tracking event for saving performance info
    def track_project_load(self):
        invocation_id = dbt.tracking.active_user.invocation_id
        dbt.tracking.track_project_load({
            "invocation_id": invocation_id,
            "project_id": self.root_project.hashed_name(),
            "path_count": self._perf_info.path_count,
            "read_files_elapsed": self._perf_info.read_files_elapsed,
            "load_macros_elapsed": self._perf_info.load_macros_elapsed,
            "parse_project_elapsed": self._perf_info.parse_project_elapsed,
            "patch_sources_elapsed": self._perf_info.patch_sources_elapsed,
            "process_manifest_elapsed": (
                self._perf_info.process_manifest_elapsed
            ),
            "load_all_elapsed": self._perf_info.load_all_elapsed,
            "is_partial_parse_enabled": (
                self._perf_info.is_partial_parse_enabled
            ),
        })


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
    names_resources: Dict[str, ManifestNode] = {}
    alias_resources: Dict[str, ManifestNode] = {}

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


# This is just used in test cases
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


# node and column descriptions
def _process_docs_for_node(
    context: Dict[str, Any],
    node: ManifestNode,
):
    node.description = get_rendered(node.description, context)
    for column_name, column in node.columns.items():
        column.description = get_rendered(column.description, context)


# source and table descriptions, column descriptions
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


# macro argument descriptions
def _process_docs_for_macro(
    context: Dict[str, Any], macro: ParsedMacro
) -> None:
    macro.description = get_rendered(macro.description, context)
    for arg in macro.arguments:
        arg.description = get_rendered(arg.description, context)


# exposure descriptions
def _process_docs_for_exposure(
    context: Dict[str, Any], exposure: ParsedExposure
) -> None:
    exposure.description = get_rendered(exposure.description, context)


# nodes: node and column descriptions
# sources: source and table descriptions, column descriptions
# macros: macro argument descriptions
# exposures: exposure descriptions
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
    for exposure in manifest.exposures.values():
        ctx = generate_runtime_docs(
            config,
            exposure,
            manifest,
            config.project_name,
        )
        _process_docs_for_exposure(ctx, exposure)


def _process_refs_for_exposure(
    manifest: Manifest, current_project: str, exposure: ParsedExposure
):
    """Given a manifest and a exposure in that manifest, process its refs"""
    for ref in exposure.refs:
        target_model: Optional[Union[Disabled, ManifestNode]] = None
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
            exposure.package_name,
        )

        if target_model is None or isinstance(target_model, Disabled):
            # This may raise. Even if it doesn't, we don't want to add
            # this exposure to the graph b/c there is no destination exposure
            invalid_ref_fail_unless_test(
                exposure, target_model_name, target_model_package,
                disabled=(isinstance(target_model, Disabled))
            )

            continue

        target_model_id = target_model.unique_id

        exposure.depends_on.nodes.append(target_model_id)
        manifest.update_exposure(exposure)


def _process_refs_for_node(
    manifest: Manifest, current_project: str, node: ManifestNode
):
    """Given a manifest and a node in that manifest, process its refs"""
    for ref in node.refs:
        target_model: Optional[Union[Disabled, ManifestNode]] = None
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
        # Q: could we stop doing this?
        manifest.update_node(node)


# Takes references in 'refs' array of nodes and exposures, finds the target
# node, and updates 'depends_on.nodes' with the unique id
def process_refs(manifest: Manifest, current_project: str):
    for node in manifest.nodes.values():
        _process_refs_for_node(manifest, current_project, node)
    for exposure in manifest.exposures.values():
        _process_refs_for_exposure(manifest, current_project, exposure)
    return manifest


def _process_sources_for_exposure(
    manifest: Manifest, current_project: str, exposure: ParsedExposure
):
    target_source: Optional[Union[Disabled, ParsedSourceDefinition]] = None
    for source_name, table_name in exposure.sources:
        target_source = manifest.resolve_source(
            source_name,
            table_name,
            current_project,
            exposure.package_name,
        )
        if target_source is None or isinstance(target_source, Disabled):
            invalid_source_fail_unless_test(
                exposure,
                source_name,
                table_name,
                disabled=(isinstance(target_source, Disabled))
            )
            continue
        target_source_id = target_source.unique_id
        exposure.depends_on.nodes.append(target_source_id)
        manifest.update_exposure(exposure)


def _process_sources_for_node(
    manifest: Manifest, current_project: str, node: ManifestNode
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


# Loops through all nodes and exposures, for each element in
# 'sources' array finds the source node and updates the
# 'depends_on.nodes' array with the unique id
def process_sources(manifest: Manifest, current_project: str):
    for node in manifest.nodes.values():
        if node.resource_type == NodeType.Source:
            continue
        assert not isinstance(node, ParsedSourceDefinition)
        _process_sources_for_node(manifest, current_project, node)
    for exposure in manifest.exposures.values():
        _process_sources_for_exposure(manifest, current_project, exposure)
    return manifest


# This is called in task.rpc.sql_commands when a "dynamic" node is
# created in the manifest, in 'add_refs'
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


# This is called in task.rpc.sql_commands when a "dynamic" node is
# created in the manifest, in 'add_refs'
def process_node(
    config: RuntimeConfig, manifest: Manifest, node: ManifestNode
):

    _process_sources_for_node(
        manifest, config.project_name, node
    )
    _process_refs_for_node(manifest, config.project_name, node)
    ctx = generate_runtime_docs(config, node, manifest, config.project_name)
    _process_docs_for_node(ctx, node)
