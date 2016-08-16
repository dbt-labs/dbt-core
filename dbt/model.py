
import os.path
import yaml
import jinja2
from dbt.templates import TestCreateTemplate

class SourceConfig(object):
    Materializations = ['view', 'table', 'incremental', 'ephemeral']
    ConfigKeys = ['enabled', 'materialized', 'dist', 'sort', 'sql_where']

    def __init__(self, active_project, own_project, fqn):
        self.active_project = active_project
        self.own_project = own_project
        self.fqn = fqn

        self.in_model_config   = {} # the config options defined within the model

    def _merge(self, *configs):
        merged_config = {}
        for config in configs:
            merged_config.update(config)
        return merged_config

    # this is re-evaluated every time `config` is called.
    # we can cache it, but that complicates things. TODO : see how this fares performance-wise
    @property
    def config(self):
        if self.active_project['name'] != self.own_project['name']:
            own_config = self.load_config_from_own_project()
            default_config = self.own_project['model-defaults'].copy()
        else:
            own_config = {}
            default_config = self.active_project['model-defaults'].copy()

        active_config = self.load_config_from_active_project()

        # if this is a dependency model:
        #   - own default config
        #   - in-model config
        #   - own project config
        #   - active project config
        # if this is a top-level model:
        #   - active default config
        #   - in-model config
        #   - active project config
        return self._merge(default_config, self.in_model_config, own_config, active_config)

    def update_in_model_config(self, config):
        self.in_model_config.update(config)

    def get_project_config(self, project):
        config = {} 
        model_configs = project['models']

        fqn = self.fqn[:]
        for level in fqn:
            level_config = model_configs.get(level, None)
            if level_config is None:
                break
            relevant_configs = {key: level_config[key] for key in level_config if key in self.ConfigKeys}
            config.update(relevant_configs)
            model_configs = model_configs[level]

        return config

    def load_config_from_own_project(self):
        return self.get_project_config(self.own_project)

    def load_config_from_active_project(self):
        return self.get_project_config(self.active_project)

class DBTSource(object):

    def __init__(self, project, top_dir, rel_filepath, own_project):
        self.project = project
        self.own_project = own_project

        self.top_dir = top_dir
        self.rel_filepath = rel_filepath
        self.filepath = os.path.join(top_dir, rel_filepath)
        self.filedir = os.path.dirname(self.filepath)
        self.name = self.fqn[-1]
        self.own_project_name = self.fqn[0]

        self.source_config = SourceConfig(project, own_project, self.fqn)

    @property
    def root_dir(self):
        return os.path.join(self.own_project['project-root'], self.top_dir)

    def compile(self):
        raise RuntimeError("Not implemented!")
    
    def serialize(self):
        serialized = {
            "build_path": os.path.join(self.project['target-path'], self.build_path()),
            "source_path": self.filepath,
            "name": self.name,
            "tmp_name": self.tmp_name(),
            "project_name": self.own_project['name']
        }

        serialized.update(self.config)
        return serialized

    @property
    def contents(self):
        with open(self.filepath) as fh:
            return fh.read().strip()

    @property
    def config(self):
        return self.source_config.config

    def update_in_model_config(self, config):
        self.source_config.update_in_model_config(config)

    @property
    def materialization(self):
        return self.config['materialized']

    @property
    def is_incremental(self):
        return self.materialization == 'incremental'

    @property
    def is_ephemeral(self):
        return self.materialization == 'ephemeral'

    @property
    def is_table(self):
        return self.materialization == 'table'

    @property
    def is_view(self):
        return self.materialization == 'view'

    @property
    def is_enabled(self):
        return self.config['enabled']


    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.own_project['name']] + parts[1:-1] + [name]

    @property
    def original_fqn(self):
        return self.fqn

    def tmp_name(self):
        return "{}__dbt_tmp".format(self.name)

    def rename_query(self, schema):
        opts = {
            "schema": schema,
            "tmp_name": self.tmp_name(),
            "final_name": self.name
        }

        return 'alter table "{schema}"."{tmp_name}" rename to "{final_name}"'.format(**opts)


class Model(DBTSource):
    def __init__(self, project, model_dir, rel_filepath, own_project, create_template):
        self.prologue = []
        self.create_template = create_template
        super(Model, self).__init__(project, model_dir, rel_filepath, own_project)

    def add_to_prologue(self, s):
        self.prologue.append(s)

    def get_prologue_string(self):
        blob = "\n".join("-- {}".format(s) for s in self.prologue)
        return "-- Compiled by DBT\n{}".format(blob)

    def sort_qualifier(self, model_config):
        if 'sort' not in model_config:
            return ''
        sort_keys = model_config['sort']
        if type(sort_keys) == str:
            sort_keys = [sort_keys]

        # remove existing quotes in field name, then wrap in quotes
        formatted_sort_keys = ['"{}"'.format(sort_key.replace('"', '')) for sort_key in sort_keys]
        return "sortkey ({})".format(', '.join(formatted_sort_keys))

    def dist_qualifier(self, model_config):
        if 'dist' not in model_config:
            return ''

        dist_key = model_config['dist']

        if type(dist_key) != str:
            raise RuntimeError("The provided distkey '{}' is not valid!".format(dist_key))

        return 'distkey ("{}")'.format(dist_key)

    def build_path(self):
        build_dir = self.create_template.label
        filename = "{}.sql".format(self.create_template.model_name(self.name))
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def compile(self, rendered_query, project, create_template):
        model_config = self.config

        if self.materialization not in SourceConfig.Materializations:
            raise RuntimeError("Invalid materialize option given: '{}'. Must be one of {}".format(self.materialization, SourceConfig.Materializations))

        ctx = project.context().copy()
        schema = ctx['env'].get('schema', 'public')

        # these are empty strings if configs aren't provided
        dist_qualifier = self.dist_qualifier(model_config)
        sort_qualifier = self.sort_qualifier(model_config)

        if self.materialization == 'incremental':
            identifier = self.name
            ctx['this'] =  '"{}"."{}"'.format(schema, identifier)
            if 'sql_where' not in model_config:
                raise RuntimeError("sql_where not specified in model materialized as incremental: {}".format(self))
            raw_sql_where = model_config['sql_where']
            env = jinja2.Environment()
            sql_where = env.from_string(raw_sql_where).render(ctx)
        else:
            identifier = self.tmp_name()
            ctx['this'] =  '"{}"."{}"'.format(schema, identifier)
            sql_where = None

        opts = {
            "materialization": self.materialization,
            "schema": schema,
            "identifier": identifier,
            "query": rendered_query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier,
            "sql_where": sql_where,
            "prologue": self.get_prologue_string()
        }

        return create_template.wrap(opts)
    
    @property
    def cte_name(self):
        return "__dbt__CTE__{}".format(self.name)

    def __repr__(self):
        return "<Model {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Analysis(Model):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        return super(Analysis, self).__init__(project, target_dir, rel_filepath, own_project)

    def build_path(self):
        build_dir = 'build-analysis'
        filename = "{}.sql".format(self.name)
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def __repr__(self):
        return "<Analysis {}: {}>".format(self.name, self.filepath)

class TestModel(Model):
    def __init__(self, project, target_dir, rel_filepath, own_project, create_template):
        return super(TestModel, self).__init__(project, target_dir, rel_filepath, own_project, create_template)

    def build_path(self):
        build_dir = self.create_template.label
        filename = "{}.sql".format(self.name)
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        test_name = TestCreateTemplate.model_name(name)
        return [self.own_project['name']] + parts[1:-1] + [test_name]

    @property
    def original_fqn(self):
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.project['name']] + parts[1:-1] + [name]

    def __repr__(self):
        return "<TestModel {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Schema(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Schema, self).__init__(project, target_dir, rel_filepath, own_project)
        self.schema = yaml.safe_load(self.contents)

    def get_model(self, project, model_name):
        rel_filepath = self.rel_filepath.replace('schema.yml', '{}.sql'.format(model_name))
        model = Model(project, self.top_dir, rel_filepath)
        return model

    def __repr__(self):
        return "<Schema {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Csv(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Csv, self).__init__(project, target_dir, rel_filepath, own_project)

    def __repr__(self):
        return "<Csv {}.{}: {}>".format(self.project['name'], self.name, self.filepath)
