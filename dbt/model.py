
import os.path
import yaml
import jinja2
from dbt.templates import TestCreateTemplate

class DBTSource(object):
    Materializations = ['view', 'table', 'incremental', 'ephemeral']
    ConfigKeys = ['enabled', 'materialized', 'dist', 'sort', 'sql_where']

    def __init__(self, project, top_dir, rel_filepath, own_project):
        self.project = project
        self.own_project = own_project
        self.top_dir = top_dir
        self.rel_filepath = rel_filepath
        self.filepath = os.path.join(top_dir, rel_filepath)
        self.filedir = os.path.dirname(self.filepath)
        self.name = self.fqn[-1]
        self.config = self.load_config()

    @property
    def root_dir(self):
        return os.path.join(self.project['project-root'], self.top_dir)

    def compile(self):
        raise RuntimeError("Not implemented!")

    @property
    def contents(self):
        with open(self.filepath) as fh:
            return fh.read().strip()

    def load_config(self):
        config_keys = self.ConfigKeys

        def load_from_project(model, the_project, skip_default=False):
            if skip_default:
                config = {}
            else:
                config = the_project['model-defaults'].copy()
            model_configs = the_project['models']
            fqn = model.original_fqn[:]
            while len(fqn) > 0:
                model_group = fqn.pop(0)
                if model_group in model_configs:
                    model_configs = model_configs[model_group]
                    relevant_configs = {key: model_configs[key] for key in config_keys if key in model_configs}
                    config.update(relevant_configs)
                else:
                    break
            return config

        config = load_from_project(self, self.project, skip_default=False)

        # overwrite dep config w/ primary config if different
        if self.project['name'] != self.own_project['name']:
            primary_config = load_from_project(self, self.own_project, skip_default=True)
            config.update(primary_config)
        return config

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
        return self.config.get('enabled')


    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.project['name']] + parts[1:-1] + [name]

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
    def __init__(self, project, model_dir, rel_filepath, own_project):
        super(Model, self).__init__(project, model_dir, rel_filepath, own_project)

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

    def build_path(self, create_template):
        build_dir = create_template.label
        filename = "{}.sql".format(create_template.model_name(self.name))
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def compile(self, rendered_query, project, create_template):
        model_config = self.config

        if self.materialization not in DBTSource.Materializations:
            raise RuntimeError("Invalid materialize option given: '{}'. Must be one of {}".format(self.materialization, DBTSource.Materializations))

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
            "sql_where": sql_where
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

    def build_path(self, create_template):
        build_dir = '{}-analysis'.format(create_template.label)
        filename = "{}.sql".format(create_template.model_name(self.name))
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def __repr__(self):
        return "<Analysis {}: {}>".format(self.name, self.filepath)

class TestModel(Model):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        return super(TestModel, self).__init__(project, target_dir, rel_filepath, own_project)

    def build_path(self, create_template):
        build_dir = create_template.label
        filename = "{}.sql".format(self.name)
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    @property
    def fqn(self):
        "fully-qualified name for model. Includes all subdirs below 'models' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        test_name = TestCreateTemplate.model_name(name)
        return [self.project['name']] + parts[1:-1] + [test_name]

    @property
    def original_fqn(self):
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return [self.project['name']] + parts[1:-1] + [name]

    def __repr__(self):
        return "<TestModel {}.{}: {}>".format(self.project['name'], self.name, self.filepath)


class CompiledModel(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        return super(CompiledModel, self).__init__(project, target_dir, rel_filepath, own_project)

    @property
    def fqn(self):
        "fully-qualified name for compiled model. Includes all subdirs below 'target/build-*' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return parts[2:-1] + [name]

    def __repr__(self):
        return "<CompiledModel {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

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
