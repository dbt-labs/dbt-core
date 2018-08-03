import dbt.exceptions

from dbt.node_types import NodeType
from dbt.contracts.graph.parsed import ParsedManifest

import dbt.parser


class GraphLoader(object):

    _LOADERS = []

    @classmethod
    def load_all(cls, root_project, all_projects):
        macros = MacroLoader.load_all(root_project, all_projects)
        macros.update(OperationLoader.load_all(root_project, all_projects))
        nodes = {}
        for loader in cls._LOADERS:
            nodes.update(loader.load_all(root_project, all_projects, macros))

        manifest = ParsedManifest(nodes=nodes, macros=macros)
        manifest = dbt.parser.ParserUtils.process_refs(manifest, root_project)
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


# node loaders
GraphLoader.register(ModelLoader)
GraphLoader.register(AnalysisLoader)
GraphLoader.register(SchemaTestLoader)
GraphLoader.register(DataTestLoader)
GraphLoader.register(RunHookLoader)
GraphLoader.register(ArchiveLoader)
GraphLoader.register(SeedLoader)
