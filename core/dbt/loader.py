import itertools
import os
import pickle
from datetime import datetime
from typing import Dict, Optional

from dbt.include.global_project import PACKAGES
import dbt.exceptions
import dbt.flags

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.clients.system import make_directory
from dbt.config import Project, RuntimeConfig
from dbt.contracts.graph.manifest import Manifest, FilePath, FileHash
from dbt.parser.base import BaseParser
from dbt.parser import AnalysisParser
from dbt.parser import DataTestParser
from dbt.parser import DocumentationParser
from dbt.parser import HookParser
from dbt.parser import MacroParser
from dbt.parser import ModelParser
from dbt.parser import ParseResult
from dbt.parser import SchemaParser
from dbt.parser import SeedParser
from dbt.parser import SnapshotParser
from dbt.parser import ParserUtils
from dbt.parser.search import FileBlock


PARTIAL_PARSE_FILE_NAME = 'partial_parse.pickle'


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
def _make_parse_result(
    config: RuntimeConfig, all_projects: Dict[str, Project]
) -> ParseResult:
    """Make a ParseResult from the project configuration and the profile."""
    # if any of these change, we need to reject the parser
    vars_hash = FileHash.from_contents(
        '\0'.join([
            getattr(config.args, 'vars', '{}') or '{}',
            getattr(config.args, 'profile', '') or '',
            getattr(config.args, 'target', '') or '',
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


class GraphLoader:
    def __init__(
        self, root_project: RuntimeConfig, all_projects: Dict[str, Project]
    ) -> None:
        self.root_project = root_project
        self.all_projects = all_projects

        self.results = _make_parse_result(root_project, all_projects)
        self._loaded_file_cache: Dict[str, FileBlock] = {}

    def _load_macros(
        self, old_results: Optional[ParseResult], internal_manifest=None
    ) -> None:
        # TODO: go back to skipping the internal manifest during macro parsing
        for project in self.all_projects.values():
            parser = MacroParser(self.results, project)
            for path in parser.search():
                self._parse_with_cache(path, parser, old_results)

    def _parse_with_cache(
        self,
        path: FilePath,
        parser: BaseParser,
        old_results: Optional[ParseResult],
    ) -> None:
        block = self._get_file(path, parser)
        if not self._get_cached(block, old_results):
            parser.parse_file(block)

    def _get_cached(
        self, block: FileBlock, old_results: Optional[ParseResult]
    ) -> bool:
        # TODO: handle multiple parsers w/ same files, by
        # tracking parser type vs node type? Or tracking actual
        # parser type during parsing?
        if old_results is None:
            return False
        if old_results.has_file(block.file):
            return self.results.sanitized_update(block.file, old_results)
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
                self._parse_with_cache(path, parser, old_results)

    def load(self, internal_manifest=None):
        old_results = self.read_parse_results()
        self._load_macros(old_results, internal_manifest=internal_manifest)
        # make a manifest with just the macros to get the context
        macro_manifest = Manifest.from_macros(macros=self.results.macros)

        for project in self.all_projects.values():
            # parse a single project
            self.parse_project(project, macro_manifest, old_results)

    def write_parse_results(self):
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)
        make_directory(self.root_project.target_path)
        with open(path, 'wb') as fp:
            pickle.dump(self.results, fp)

    def _matching_parse_results(self, result: ParseResult) -> bool:
        """Compare the global hashes of the read-in parse results' values to
        the known ones, and return if it is ok to re-use the results.
        """
        valid = True
        if self.results.vars_hash != result.vars_hash:
            logger.debug('vars hash collision, cache invalidated')
            valid = False
        if self.results.profile_hash != result.profile_hash:
            logger.debug('profile hash collision, cache invalidated')
            valid = False
        if self.results.project_hashes != result.project_hashes:
            logger.debug('profile hash collision, cache invalidated')
            valid = False
        return valid

    def read_parse_results(self) -> Optional[ParseResult]:
        if not dbt.flags.PARTIAL_PARSE:
            return None
        path = os.path.join(self.root_project.target_path,
                            PARTIAL_PARSE_FILE_NAME)

        result: Optional[ParseResult] = None

        if os.path.exists(path):
            try:
                with open(path, 'rb') as fp:
                    result = pickle.load(fp)
            except Exception as exc:
                logger.debug(
                    'Failed to load parsed file from disk at {}: {}'
                    .format(path, exc),
                    exc_info=True
                )

        if result and self._matching_parse_results(result):
            return result
        return None

    def create_manifest(self):
        nodes = {}
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
            config=self.root_project,
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
    def _load_from_projects(cls, root_config, projects, internal_manifest):
        loader = cls(root_config, projects)
        loader.load(internal_manifest=internal_manifest)
        loader.write_parse_results()
        return loader.create_manifest()

    @classmethod
    def load_all(cls, root_config, internal_manifest=None):
        projects = load_all_projects(root_config)
        manifest = cls._load_from_projects(root_config, projects,
                                           internal_manifest)
        _check_manifest(manifest, root_config)
        return manifest

    @classmethod
    def load_internal(cls, root_config):
        projects = load_internal_projects(root_config)
        return cls._load_from_projects(root_config, projects, None)


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


def load_all_projects(config) -> Dict[str, Project]:
    all_projects = {config.project_name: config}
    project_paths = itertools.chain(
        internal_project_names(),
        _project_directories(config)
    )
    all_projects.update(_load_projects(config, project_paths))
    return all_projects


def load_internal_projects(config):
    return dict(_load_projects(config, internal_project_names()))
