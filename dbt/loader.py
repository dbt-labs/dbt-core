import dbt.exceptions
import dbt.parser

from dbt.node_types import NodeType

from dbt.utils import DBTConfigKeys

from dbt.logger import initialize_logger, GLOBAL_LOGGER as logger

def get_model_config_pqn( config_models, pqn = None ,model_config_pqns = None):
    if pqn is None:
        pqn = []
    # could also write `pqn = pqn or []`
    if model_config_pqns is None:
        model_config_pqns = []
    # why can't I have model_config_pqns = [] here?
    for k,v in config_models.items():
        # If the next level is a dictionary
        if isinstance(v,dict):
            # If the key is a config key, add the list of keys to the model_config_pqns list
            # base case - when you get to a model config key
            if k in DBTConfigKeys: 
                if pqn not in model_config_pqns and pqn:
                    model_config_pqns.append(pqn)
            # Else, keep iterating
            # recursive case
            else:
                get_model_config_pqn( v, pqn + [k], model_config_pqns)
        # If you've reached the end of the path, add the path
        # base case - when you reach the end of the dictionary
        else:
            if pqn not in model_config_pqns and pqn:
                model_config_pqns.append(pqn)

    return model_config_pqns

def is_pqn_in_fqn(pqn, fqn):
    for item in pqn:
        # there's a better word than "item" here...
        # check that the current directory exists in the fqn
        if item in fqn:
            # if it does, then update the fqn to that it now only contains item after that item
            fqn = fqn[fqn.index(item)+1:]
        else:
            # if it doesn't then return false and exit the loop
            return False
            break
    # if the loop doesn't get broken, turn True
    return True

def is_pqn_in_at_least_one_fqn(pqn, model_fqns): 
    for fqn in model_fqns:
        if is_pqn_in_fqn(pqn, fqn):
            return True
            break
    return False

def check_config_pqns(model_config_pqns, model_fqns):
    for pqn in model_config_pqns:
        if is_pqn_in_at_least_one_fqn(pqn, model_fqns):
            pass
        else:
            logger.info("Your config " + str(pqn) + " doesn't point to a model")


class GraphLoader(object):

    _LOADERS = {'nodes': [], 'macros': []}

    @classmethod
    def load_all(cls, root_project, all_projects):
        to_return = {}

        subgraphs = ['nodes', 'macros']

        macros = MacroLoader.load_all(root_project, all_projects)
        for subgraph in subgraphs:
            subgraph_nodes = {}

            for loader in cls._LOADERS[subgraph]:
                subgraph_nodes.update(
                    loader.load_all(root_project, all_projects, macros))

            to_return[subgraph] = subgraph_nodes
        
        fqns = []
        for unique_id, node in to_return['nodes'].items():
            fqns.append(node['fqn'])
        
        pqns = []
        for project_name, project in all_projects.items():
          pqns.extend(get_model_config_pqn(project['models']))
        
        check_config_pqns(pqns, fqns)
        
        # Do we want to exit compilation if there is a bad config?
        
        to_return['macros'] = macros
        return to_return

    @classmethod
    def register(cls, loader, subgraph='nodes'):
        if subgraph not in ['nodes', 'macros']:
            raise dbt.exceptions.InternalException(
                'Invalid subgraph type {}, should be "nodes" or "macros"!'
                .format(subgraph))

        cls._LOADERS[subgraph].append(loader)


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
        return dbt.parser.load_and_parse_macros(
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

        # Check for duplicate model names
        names_models = {}
        for model, attribs in to_return.items():
            name = attribs['name']
            existing_name = names_models.get(name)
            if existing_name is not None:
                raise dbt.exceptions.CompilationException(
                    'Found models with the same name: \n- %s\n- %s' % (
                        model, existing_name))
            names_models[name] = model
        return to_return

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.load_and_parse_sql(
                package_name=project_name,
                root_project=root_project,
                all_projects=all_projects,
                root_dir=project.get('project-root'),
                relative_dirs=project.get('source-paths', []),
                resource_type=NodeType.Model,
                macros=macros)


class AnalysisLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.load_and_parse_sql(
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
        return dbt.parser.load_and_parse_yml(
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
        return dbt.parser.load_and_parse_sql(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('test-paths', []),
            resource_type=NodeType.Test,
            tags={'data'},
            macros=macros)


# ArchiveLoader and RunHookLoader operate on configs, so we just need to run
# them both once, not for each project
class ArchiveLoader(ResourceLoader):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        return cls.load_project(root_project, all_projects, macros)

    @classmethod
    def load_project(cls, root_project, all_projects, macros):
        return dbt.parser.parse_archives_from_projects(root_project,
                                                       all_projects,
                                                       macros)


class RunHookLoader(ResourceLoader):

    @classmethod
    def load_all(cls, root_project, all_projects, macros=None):
        return cls.load_project(root_project, all_projects, macros)

    @classmethod
    def load_project(cls, root_project, all_projects, macros):
        return dbt.parser.load_and_parse_run_hooks(root_project, all_projects,
                                                   macros)


class SeedLoader(ResourceLoader):

    @classmethod
    def load_project(cls, root_project, all_projects, project, project_name,
                     macros):
        return dbt.parser.load_and_parse_seeds(
            package_name=project_name,
            root_project=root_project,
            all_projects=all_projects,
            root_dir=project.get('project-root'),
            relative_dirs=project.get('data-paths', []),
            resource_type=NodeType.Seed,
            macros=macros)


# node loaders
GraphLoader.register(ModelLoader, 'nodes')
GraphLoader.register(AnalysisLoader, 'nodes')
GraphLoader.register(SchemaTestLoader, 'nodes')
GraphLoader.register(DataTestLoader, 'nodes')
GraphLoader.register(RunHookLoader, 'nodes')
GraphLoader.register(ArchiveLoader, 'nodes')
GraphLoader.register(SeedLoader, 'nodes')
