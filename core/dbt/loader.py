import os
import itertools

from dbt import deprecations
from dbt.include.global_project import PACKAGES
import dbt.exceptions
import dbt.flags

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.utils import timestring

from dbt.parser import MacroParser, ModelParser, SeedParser, AnalysisParser, \
    DocumentationParser, DataTestParser, HookParser, ArchiveParser, \
    SchemaParser, ParserUtils

from dbt.contracts.project import ProjectList


class GraphLoader(object):
    def __init__(self, root_project, all_projects):
        self.root_project = root_project
        self.all_projects = all_projects
        self.nodes = {}
        self.docs = {}
        self.macros = {}
        self.tests = {}
        self.patches = {}
        self.disabled = []
        self.macro_manifest = None

    def _load_sql_nodes(self, parser_type, resource_type, relative_dirs_attr,
                        **kwargs):
        parser = parser_type(self.root_project, self.all_projects,
                             self.macro_manifest)

        for project_name, project in self.all_projects.items():
            nodes, disabled = parser.load_and_parse(
                package_name=project_name,
                root_dir=project.project_root,
                relative_dirs=getattr(project, relative_dirs_attr),
                resource_type=resource_type,
                **kwargs
            )
            self.nodes.update(nodes)
            self.disabled.extend(disabled)

    def _load_macros(self, internal_manifest=None):
        # skip any projects in the internal manifest
        all_projects = self.all_projects.copy()
        if internal_manifest is not None:
            for name in internal_project_names():
                all_projects.pop(name, None)
            self.macros.update(internal_manifest.macros)

        # give the macroparser all projects but then only load what we haven't
        # loaded already
        parser = MacroParser(self.root_project, self.all_projects)
        for project_name, project in all_projects.items():
            self.macros.update(parser.load_and_parse(
                package_name=project_name,
                root_dir=project.project_root,
                relative_dirs=project.macro_paths,
                resource_type=NodeType.Macro,
            ))

    def _load_seeds(self):
        parser = SeedParser(self.root_project, self.all_projects,
                            self.macro_manifest)
        for project_name, project in self.all_projects.items():
            self.nodes.update(parser.load_and_parse(
                package_name=project_name,
                root_dir=project.project_root,
                relative_dirs=project.data_paths,
            ))

    def _load_nodes(self):
        self._load_sql_nodes(ModelParser, NodeType.Model, 'source_paths')
        self._load_sql_nodes(AnalysisParser, NodeType.Analysis,
                             'analysis_paths')
        self._load_sql_nodes(DataTestParser, NodeType.Test, 'test_paths',
                             tags=['data'])

        hook_parser = HookParser(self.root_project, self.all_projects,
                                 self.macro_manifest)
        self.nodes.update(hook_parser.load_and_parse())

        archive_parser = ArchiveParser(self.root_project, self.all_projects,
                                       self.macro_manifest)
        self.nodes.update(archive_parser.load_and_parse())

        self._load_seeds()

    def _load_docs(self):
        parser = DocumentationParser(self.root_project, self.all_projects)
        for project_name, project in self.all_projects.items():
            self.docs.update(parser.load_and_parse(
                package_name=project_name,
                root_dir=project.project_root,
                relative_dirs=project.docs_paths
            ))

    def _load_schema_tests(self):
        parser = SchemaParser(self.root_project, self.all_projects,
                              self.macro_manifest)
        for project_name, project in self.all_projects.items():
            tests, patches, sources = parser.load_and_parse(
                package_name=project_name,
                root_dir=project.project_root,
                relative_dirs=project.source_paths
            )

            for unique_id, test in tests.items():
                if unique_id in self.tests:
                    dbt.exceptions.raise_duplicate_resource_name(
                        test, self.tests[unique_id],
                    )
                self.tests[unique_id] = test

            for unique_id, source in sources.items():
                if unique_id in self.nodes:
                    dbt.exceptions.raise_duplicate_resource_name(
                        source, self.nodes[unique_id],
                    )
                self.nodes[unique_id] = source

            for name, patch in patches.items():
                if name in self.patches:
                    dbt.exceptions.raise_duplicate_patch_name(
                        name, patch, self.patches[name]
                    )
                self.patches[name] = patch

    def load(self, internal_manifest=None):
        self._load_macros(internal_manifest=internal_manifest)
        # make a manifest with just the macros to get the context
        self.macro_manifest = Manifest(macros=self.macros, nodes={}, docs={},
                                       generated_at=timestring(), disabled=[])
        self._load_nodes()
        self._load_docs()
        self._load_schema_tests()

    def create_manifest(self):
        manifest = Manifest(
            nodes=self.nodes,
            macros=self.macros,
            docs=self.docs,
            generated_at=timestring(),
            config=self.root_project,
            disabled=self.disabled
        )
        manifest.add_nodes(self.tests)
        manifest.patch_nodes(self.patches)
        manifest = ParserUtils.process_sources(manifest, self.root_project)
        manifest = ParserUtils.process_refs(manifest,
                                            self.root_project.project_name)
        manifest = ParserUtils.process_docs(manifest, self.root_project)
        return manifest

    @classmethod
    def _load_from_projects(cls, root_config, projects, internal_manifest):
        if dbt.flags.STRICT_MODE:
            ProjectList(**projects)

        loader = cls(root_config, projects)
        loader.load(internal_manifest=internal_manifest)
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
                    existing_node, node)

        existing_alias = alias_resources.get(alias)
        if existing_alias is not None:
            dbt.exceptions.raise_ambiguous_alias(
                    existing_alias, node)

        names_resources[name] = node
        alias_resources[alias] = node


def _warn_for_unused_resource_config_paths(manifest, config):
    resource_fqns = manifest.get_resource_fqns()
    disabled_fqns = [n.fqn for n in manifest.disabled]
    config.warn_for_unused_resource_config_paths(resource_fqns, disabled_fqns)


def _warn_for_deprecated_configs(manifest):
    for unique_id, node in manifest.nodes.items():
        is_model = node.resource_type == NodeType.Model
        if is_model and 'sql_where' in node.config:
            deprecations.warn('sql_where')


def _check_manifest(manifest, config):
    _check_resource_uniqueness(manifest)
    _warn_for_unused_resource_config_paths(manifest, config)
    _warn_for_deprecated_configs(manifest)


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


def load_all_projects(config):
    all_projects = {config.project_name: config}
    project_paths = itertools.chain(
        internal_project_names(),
        _project_directories(config)
    )
    all_projects.update(_load_projects(config, project_paths))
    return all_projects


def load_internal_projects(config):
    return dict(_load_projects(config, internal_project_names()))
