import dbt.exceptions

from dbt.node_types import NodeType
from dbt.contracts.graph.manifest import Manifest
from dbt.utils import timestring

import dbt.parser


class GraphLoader(object):

    _LOADERS = []

    @classmethod
    def load_all(cls, project_obj, all_projects):
        root_project = project_obj.cfg
        macros = MacroLoader.load_all(root_project, all_projects)
        macros.update(OperationLoader.load_all(root_project, all_projects))
        nodes = {}
        for loader in cls._LOADERS:
            nodes.update(loader.load_all(root_project, all_projects, macros))
        docs = DocumentationLoader.load_all(root_project, all_projects)

        tests, patches = SchemaTestLoader.load_all(root_project, all_projects)

        manifest = Manifest(nodes=nodes, macros=macros, docs=docs,
                            generated_at=timestring(), project=project_obj)
        manifest.add_nodes(tests)
        manifest.patch_nodes(patches)

        manifest = dbt.parser.ParserUtils.process_refs(
            manifest,
            root_project.get('name')
        )
        manifest = dbt.parser.ParserUtils.process_docs(manifest, root_project)
        return manifest

    @classmethod
    def register(cls, loader):
        cls._LOADERS.append(loader)


class ResourceLoader(object):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        to_return = {}

        for project_name, project in all_projects.items():
            to_return.update(cls.load_project(root_project, all_projects,
                                              project, project_name, macros))

        return to_return

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        raise dbt.exceptions.NotImplementedException(
            'load_project is not implemented for this loader!')


class MacroLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.MacroParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('macro-paths', []),
            resource_type=NodeType.Macro)


class ModelLoader(ResourceLoader):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        to_return = {}

        for project_name, project in all_projects.items():
            project_loaded = cls.load_project(root_project,
                                              all_projects,
                                              project, project_name,
                                              macros)

            to_return.update(project_loaded)

        return to_return

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.ModelParser.load_and_parse(
                package_name=project_name,
                root_project=root_project,
                all_projects=all_projects,
                root_dir=project.get('project-root'),
                relative_dirs=project.get('source-paths', []),
                resource_type=NodeType.Model,
                macros=macros)


class OperationLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.MacroParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('macro-paths', []),
            resource_type=NodeType.Operation)


class AnalysisLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.AnalysisParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('analysis-paths', []),
            resource_type=NodeType.Analysis,
            macros=macros)


class SchemaTestLoader(ResourceLoader):
    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        tests = {}
        patches = {}
        for project_name, project in all_projects.items():
            project_tests, project_patches = cls.load_project(
                root_project, all_projects, project, project_name, macros
            )
            for unique_id, test in project_tests.items():
                if unique_id in tests:
                    dbt.exceptions.raise_duplicate_resource_name(
                        test, tests[unique_id],
                    )
                tests[unique_id] = test

            for name, patch in project_patches.items():
                if name in patches:
                    dbt.exceptions.raise_duplicate_patch_name(name, patch,
                                                              patches[name])
                patches[name] = patch
        return tests, patches

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.SchemaParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('source-paths', []),
            macros=macros)


class DataTestLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.DataTestParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('test-paths', []),
            resource_type=NodeType.Test,
            tags=['data'],
            macros=macros)


# ArchiveLoader and RunHookLoader operate on configs, so we just need to run
# them both once, not for each project
class ArchiveLoader(ResourceLoader):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        return cls.load_project(root_project, all_projects, macros)

    @classmethod
    def load_project(cls, root_project, all_projects, macros):
        return dbt.parser.ArchiveParser.load_and_parse(root_project,
                                                       all_projects,
                                                       macros)


class RunHookLoader(ResourceLoader):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        return cls.load_project(root_project, all_projects, macros)

    @classmethod
    def load_project(cls, root_project, all_projects, macros):
        return dbt.parser.HookParser.load_and_parse(root_project, all_projects,
                                                    macros)


class SeedLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.SeedParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('data-paths', []),
            macros=macros)


class DocumentationLoader(ResourceLoader):
    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.DocumentationParser.load_and_parse(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('docs-paths', []))

# node loaders
GraphLoader.register(ModelLoader)
GraphLoader.register(AnalysisLoader)
GraphLoader.register(DataTestLoader)
GraphLoader.register(RunHookLoader)
GraphLoader.register(ArchiveLoader)
GraphLoader.register(SeedLoader)
