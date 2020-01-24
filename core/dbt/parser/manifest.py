import itertools
import os
import pickle
from datetime import datetime
from typing import Dict, Optional, Mapping, Callable, Any

from dbt.include.global_project import PACKAGES
import dbt.exceptions
import dbt.flags

from dbt.logger import GLOBAL_LOGGER as logger, DbtProcessState
from dbt.node_types import NodeType
from dbt.clients.system import make_directory
from dbt.config import Project, RuntimeConfig
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest, FilePath, FileHash
from dbt.parser.base import BaseParser
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
from dbt.parser.util import ParserUtils
from dbt.version import __version__


PARTIAL_PARSE_FILE_NAME = 'partial_parse.pickle'
PARSING_STATE = DbtProcessState('parsing')
DEFAULT_PARTIAL_PARSE = False


_parser_types = [
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
        '\0'.join([
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

    def _load_macros(
        self,
        old_results: Optional[ParseResult],
        internal_manifest: Optional[Manifest] = None,
    ) -> None:
        projects = self.all_projects
        if internal_manifest is not None:
            projects = {
                k: v for k, v in self.all_projects.items() if k not in PACKAGES
            }
            self.results.macros.update(internal_manifest.macros)
            self.results.files.update(internal_manifest.files)

        # TODO: go back to skipping the internal manifest during macro parsing
        for project in projects.values():
            parser = MacroParser(self.results, project)
            for path in parser.search():
                self.parse_with_cache(path, parser, old_results)

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
        parsers = []
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
        self._load_macros(old_results, internal_manifest=None)
        # make a manifest with just the macros to get the context
        macro_manifest = Manifest.from_macros(
            macros=self.results.macros,
            files=self.results.files
        )
        return macro_manifest

    def load(self, internal_manifest: Optional[Manifest] = None):
        old_results = self.read_parse_results()
        if old_results is not None:
            logger.debug('Got an acceptable cached parse result')
        self._load_macros(old_results, internal_manifest=internal_manifest)
        # make a manifest with just the macros to get the context
        macro_manifest = Manifest.from_macros(
            macros=self.results.macros,
            files=self.results.files
        )
        self.macro_hook(macro_manifest)

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
        if dbt.flags.PARTIAL_PARSE is not None:
            return dbt.flags.PARTIAL_PARSE
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

    def create_manifest(self) -> Manifest:
        nodes: Dict[str, CompileResultNode] = {}
        nodes.update(self.results.nodes)
        nodes.update(self.results.sources)
        disabled = []
        for value in self.results.disabled.values():
            disabled.extend(value)
        manifest = Manifest(
            nodes=nodes,
            macros=self.results.macros,
            docs=self.results.docs,
            generated_at=datetime.utcnow(),
            metadata=self.root_project.get_metadata(),
            disabled=disabled,
            files=self.results.files,
        )
        manifest.patch_nodes(self.results.patches)
        manifest = ParserUtils.process_sources(
            manifest, self.root_project.project_name
        )
        manifest = ParserUtils.process_refs(
            manifest, self.root_project.project_name
        )
        manifest = ParserUtils.process_docs(
            manifest, self.root_project.project_name
        )
        return manifest

    @classmethod
    def load_all(
        cls,
        root_config: RuntimeConfig,
        internal_manifest: Optional[Manifest],
        macro_hook: Callable[[Manifest], Any],
    ) -> Manifest:
        with PARSING_STATE:
            projects = load_all_projects(root_config)
            loader = cls(root_config, projects, macro_hook)
            loader.load(internal_manifest=internal_manifest)
            loader.write_parse_results()
            manifest = loader.create_manifest()
            _check_manifest(manifest, root_config)
            manifest.build_flat_graph()
            return manifest

    @classmethod
    def load_internal(cls, root_config: RuntimeConfig) -> Manifest:
        with PARSING_STATE:
            projects = load_internal_projects(root_config)
            loader = cls(root_config, projects)
            return loader.load_only_macros()


def _check_resource_uniqueness(manifest):
    names_resources = {}
    alias_resources = {}

    for resource, node in manifest.nodes.items():
        if node.resource_type not in NodeType.refable():
            continue

        name = node.name
        alias = "{}.{}".format(node.schema, node.alias)

        existing_node = names_resources.get(name)
        if existing_node is not None:
            dbt.exceptions.raise_duplicate_resource_name(
                existing_node, node
            )

        existing_alias = alias_resources.get(alias)
        if existing_alias is not None:
            dbt.exceptions.raise_ambiguous_alias(
                existing_alias, node
            )

        names_resources[name] = node
        alias_resources[alias] = node


def _warn_for_unused_resource_config_paths(manifest, config):
    resource_fqns = manifest.get_resource_fqns()
    disabled_fqns = [n.fqn for n in manifest.disabled]
    config.warn_for_unused_resource_config_paths(resource_fqns, disabled_fqns)


def _check_manifest(manifest, config):
    _check_resource_uniqueness(manifest)
    _warn_for_unused_resource_config_paths(manifest, config)


def internal_project_names():
    return iter(PACKAGES.values())


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


def _project_directories(config):
    root = os.path.join(config.project_root, config.modules_path)

    dependencies = []
    if os.path.exists(root):
        dependencies = os.listdir(root)

    for name in dependencies:
        full_obj = os.path.join(root, name)

        if not os.path.isdir(full_obj) or name.startswith('__'):
            # exclude non-dirs and dirs that start with __
            # the latter could be something like __pycache__
            # for the global dbt modules dir
            continue

        yield full_obj


def load_all_projects(config) -> Mapping[str, Project]:
    all_projects = {config.project_name: config}
    project_paths = itertools.chain(
        internal_project_names(),
        _project_directories(config)
    )
    all_projects.update(_load_projects(config, project_paths))
    return all_projects


def load_internal_projects(config):
    return dict(_load_projects(config, internal_project_names()))


def load_internal_manifest(config: RuntimeConfig) -> Manifest:
    return ManifestLoader.load_internal(config)


def load_manifest(
    config: RuntimeConfig,
    internal_manifest: Optional[Manifest],
    macro_hook: Callable[[Manifest], Any],
) -> Manifest:
    return ManifestLoader.load_all(config, internal_manifest, macro_hook)
