
import os.path
import yaml
from dbt.templates import TestCreateTemplate

class DBTSource(object):
    def __init__(self, project, top_dir, rel_filepath):
        self.project = project
        self.top_dir = top_dir
        self.rel_filepath = rel_filepath
        self.filepath = os.path.join(top_dir, rel_filepath)
        self.filedir = os.path.dirname(self.filepath)
        self.name = self.fqn[-1]

    @property
    def root_dir(self):
        return os.path.join(self.project['project-root'], self.top_dir)

    @property
    def contents(self):
        with open(self.filepath) as fh:
            return fh.read().strip()

    def get_config_keys(self):
        return ['enabled', 'materialized', 'dist', 'sort', 'sql_field']

    def compile(self):
        raise RuntimeError("Not implemented!")

    def get_config(self, primary_project):
        config_keys = self.get_config_keys()

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
        if self.project['name'] != primary_project['name']:
            primary_config = load_from_project(self, primary_project, skip_default=True)
            config.update(primary_config)
        return config

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
    def __init__(self, project, model_dir, rel_filepath):
        super(Model, self).__init__(project, model_dir, rel_filepath)

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
        model_config = self.get_config(project)

        valid_materializations = ['view', 'table', 'incremental']
        materialization = model_config['materialized']
        if materialization not in valid_materializations:
            raise RuntimeError("Invalid materialize option given: '{}'. Must be one of {}".format(materialization, valid_materializations))

        ctx = project.context()
        schema = ctx['env'].get('schema', 'public')

        # these are empty strings if configs aren't provided
        dist_qualifier = self.dist_qualifier(model_config)
        sort_qualifier = self.sort_qualifier(model_config)

        if materialization in ('table', 'view'):
            identifier = self.tmp_name()
            sql_field = None
        else:
            identifier = self.name
            if 'sql_field' not in model_config:
                raise RuntimeError("sql_field not specified in model materialized as incremental: {}".format(self))
            sql_field = model_config['sql_field']

        opts = {
            "materialization": materialization,
            "schema": schema,
            "identifier": identifier,
            "query": rendered_query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier,
            "sql_field": sql_field
        }

        return create_template.wrap(opts)

    def __repr__(self):
        return "<Model {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Analysis(Model):
    def __init__(self, project, target_dir, rel_filepath):
        return super(Analysis, self).__init__(project, target_dir, rel_filepath)

    def build_path(self, create_template):
        build_dir = '{}-analysis'.format(create_template.label)
        filename = "{}.sql".format(create_template.model_name(self.name))
        path_parts = [build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def __repr__(self):
        return "<Analysis {}: {}>".format(self.name, self.filepath)

class TestModel(Model):
    def __init__(self, project, target_dir, rel_filepath):
        return super(TestModel, self).__init__(project, target_dir, rel_filepath)

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
    def __init__(self, project, target_dir, rel_filepath):
        return super(CompiledModel, self).__init__(project, target_dir, rel_filepath)

    @property
    def fqn(self):
        "fully-qualified name for compiled model. Includes all subdirs below 'target/build-*' path and the filename"
        parts = self.filepath.split("/")
        name, _ = os.path.splitext(parts[-1])
        return parts[2:-1] + [name]

    def __repr__(self):
        return "<CompiledModel {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Schema(DBTSource):
    def __init__(self, project, target_dir, rel_filepath):
        super(Schema, self).__init__(project, target_dir, rel_filepath)
        self.schema = yaml.safe_load(self.contents)

    def get_model(self, project, model_name):
        rel_filepath = self.rel_filepath.replace('schema.yml', '{}.sql'.format(model_name))
        model = Model(project, self.top_dir, rel_filepath)
        return model

    def __repr__(self):
        return "<Schema {}.{}: {}>".format(self.project['name'], self.name, self.filepath)

class Csv(DBTSource):
    def __init__(self, project, target_dir, rel_filepath):
        super(Csv, self).__init__(project, target_dir, rel_filepath)

    def __repr__(self):
        return "<Csv {}.{}: {}>".format(self.project['name'], self.name, self.filepath)
