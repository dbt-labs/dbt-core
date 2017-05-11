import dbt.exceptions
import dbt.parser

from dbt.utils import NodeType


class GraphLoader(object):

    _LOADERS = {'nodes': [], 'macros': []}

    @classmethod
    def load_all(cls, root_project, all_projects):
        to_return = {}

        subgraphs = ['nodes', 'macros']

        for subgraph in subgraphs:
            subgraph_nodes = {}

            for loader in cls._LOADERS[subgraph]:
                subgraph_nodes.update(
                    loader.load_all(root_project, all_projects))

            to_return[subgraph] = subgraph_nodes

        return to_return

    @classmethod
    def register(cls, loader, subgraph='nodes'):
        if subgraph not in ['nodes', 'macros']:
            raise dbt.exceptions.ProgrammingException(
                'Invalid subgraph type {}, should be "nodes" or "macros"!'
                .format(subgraph))

        cls._LOADERS[subgraph].append(loader)


class ResourceLoader(object):

    @classmethod
    def load_all(cls, root_project, all_projects):
        to_return = {}

        for project_name, project in all_projects.items():
            to_return.update(cls.load_project(root_project, all_projects,
                                              project, project_name))

        return to_return

    @classmethod
    def load_project(root_project, all_projects, project, project_name):
        raise dbt.exceptions.NotImplementedException(
            'load_project is not implemented for this loader!')


class MacroLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_macros(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('macro-paths', []),
            resource_type=NodeType.Macro)


class ModelLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_sql(
                package_name=project_name,
                root_project=root_project,
                all_projects=all_projects,
                root_dir=project.get('project-root'),
                relative_dirs=project.get('source-paths', []),
                resource_type=NodeType.Model)


class AnalysisLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_sql(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('analysis-paths', []),
            resource_type=NodeType.Analysis)


class SchemaTestLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_yml(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('source-paths', []))


class DataTestLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_sql(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('test-paths', []),
            resource_type=NodeType.Test,
            tags={'data'})


class ArchiveLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.parse_archives_from_projects(root_project,
                                                       all_projects)


class RunHookLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name):
        return dbt.parser.load_and_parse_run_hooks(root_project, all_projects)


# macro loaders
GraphLoader.register(MacroLoader, 'macros')

# node loaders
GraphLoader.register(ModelLoader, 'nodes')
GraphLoader.register(AnalysisLoader, 'nodes')
GraphLoader.register(SchemaTestLoader, 'nodes')
GraphLoader.register(DataTestLoader, 'nodes')
GraphLoader.register(RunHookLoader, 'nodes')
GraphLoader.register(ArchiveLoader, 'nodes')
