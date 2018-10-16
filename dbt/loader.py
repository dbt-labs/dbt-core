import dbt.exceptions

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.utils import timestring

from dbt.parser import MacroParser, ModelParser, SeedParser, AnalysisParser, \
    DocumentationParser, DataTestParser, HookParser, ArchiveParser, \
    SchemaParser, ParserUtils


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

    def _load_macro_nodes(self, resource_type):
        for project_name, project in self.all_projects.items():
            self.macros.update(MacroParser.load_and_parse(
                package_name=project_name,
                root_project=self.root_project,
                all_projects=self.all_projects,
                root_dir=project.project_root,
                relative_dirs=project.macro_paths,
                resource_type=resource_type,
            ))

    def _load_sql_nodes(self, parser, resource_type, relative_dirs_attr,
                        **kwargs):
        for project_name, project in self.all_projects.items():
            nodes, disabled = parser.load_and_parse(
                package_name=project_name,
                root_project=self.root_project,
                all_projects=self.all_projects,
                root_dir=project.project_root,
                relative_dirs=getattr(project, relative_dirs_attr),
                resource_type=resource_type,
                macros=self.macros,
                **kwargs
            )
            self.nodes.update(nodes)
            self.disabled.extend(disabled)

    def _load_macros(self):
        self._load_macro_nodes(NodeType.Macro)
        self._load_macro_nodes(NodeType.Operation)

    def _load_seeds(self):
        for project_name, project in self.all_projects.items():
            self.nodes.update(SeedParser.load_and_parse(
                package_name=project_name,
                root_project=self.root_project,
                all_projects=self.all_projects,
                root_dir=project.project_root,
                relative_dirs=project.data_paths,
                macros=self.macros
            ))

    def _load_nodes(self):
        self._load_sql_nodes(ModelParser, NodeType.Model, 'source_paths')
        self._load_sql_nodes(AnalysisParser, NodeType.Analysis,
                             'analysis_paths')
        self._load_sql_nodes(DataTestParser, NodeType.Test, 'test_paths',
                             tags=['data'])

        self.nodes.update(HookParser.load_and_parse(
            self.root_project, self.all_projects, self.macros
        ))
        self.nodes.update(ArchiveParser.load_and_parse(
            self.root_project, self.all_projects, self.macros
        ))

        self._load_seeds()

    def _load_docs(self):
        for project_name, project in self.all_projects.items():
            self.docs.update(DocumentationParser.load_and_parse(
                package_name=project_name,
                root_project=self.root_project,
                all_projects=self.all_projects,
                root_dir=project.project_root,
                relative_dirs=project.docs_paths
            ))

    def _load_schema_tests(self):
        for project_name, project in self.all_projects.items():
            tests, patches = SchemaParser.load_and_parse(
                package_name=project_name,
                root_project=self.root_project,
                all_projects=self.all_projects,
                root_dir=project.project_root,
                relative_dirs=project.source_paths,
                macros=self.macros
            )

            for unique_id, test in tests.items():
                if unique_id in self.tests:
                    dbt.exceptions.raise_duplicate_resource_name(
                        test, self.tests[unique_id],
                    )
                self.tests[unique_id] = test

            for name, patch in patches.items():
                if name in self.patches:
                    dbt.exceptions.raise_duplicate_patch_name(
                        name, patch, self.patches[name]
                    )
                self.patches[name] = patch

    def load(self):
        self._load_macros()
        self._load_nodes()
        self._load_docs()
        self._load_schema_tests()
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
        manifest = ParserUtils.process_refs(manifest,
                                            self.root_project.project_name)
        manifest = ParserUtils.process_docs(manifest, self.root_project)
        return manifest

    @classmethod
    def load_all(cls, project_config, all_projects):
        return cls(project_config, all_projects).load()
