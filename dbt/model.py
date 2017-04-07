import os.path
import jinja2.runtime

from dbt.compat import basestring

import dbt.flags

from dbt.templates import BaseCreateTemplate
from dbt.utils import split_path, NodeType
import dbt.project
from dbt.utils import deep_merge, DBTConfigKeys, compiler_error

import dbt.clients.jinja


class SourceConfig(object):
    Materializations = ['view', 'table', 'incremental', 'ephemeral']
    ConfigKeys = DBTConfigKeys

    AppendListFields = ['pre-hook', 'post-hook']
    ExtendDictFields = ['vars']
    ClobberFields = [
        'enabled',
        'materialized',
        'dist',
        'sort',
        'sql_where',
        'unique_key',
        'sort_type'
    ]

    def __init__(self, active_project, own_project, fqn):
        self._config = None
        self.active_project = active_project
        self.own_project = own_project
        self.fqn = fqn

        # the config options defined within the model
        self.in_model_config = {}

        # make sure we categorize all configs
        all_configs = self.AppendListFields + self.ExtendDictFields + \
            self.ClobberFields

        for config in self.ConfigKeys:
            assert config in all_configs, config

    def _merge(self, *configs):
        merged_config = {}
        for config in configs:
            intermediary_merged = deep_merge(
                merged_config.copy(), config.copy()
            )

            merged_config.update(intermediary_merged)
        return merged_config

    # this is re-evaluated every time `config` is called.
    # we can cache it, but that complicates things.
    # TODO : see how this fares performance-wise
    @property
    def config(self):
        """
        Config resolution order:

         if this is a dependency model:
           - own project config
           - in-model config
           - active project config
         if this is a top-level model:
           - active project config
           - in-model config
        """
        defaults = {"enabled": True, "materialized": "view"}
        active_config = self.load_config_from_active_project()

        if self.active_project['name'] == self.own_project['name']:
            cfg = self._merge(defaults, active_config, self.in_model_config)
        else:
            own_config = self.load_config_from_own_project()

            cfg = self._merge(
                defaults, own_config, self.in_model_config, active_config
            )

        # mask this as a table if it's an incremental model with
        # --full-refresh provided
        if cfg.get('materialized') == 'incremental' and self.is_full_refresh():
            cfg['materialized'] = 'table'

        return cfg

    def is_full_refresh(self):
        return dbt.flags.FULL_REFRESH

    def update_in_model_config(self, config):
        config = config.copy()

        # make sure we're not clobbering an array of hooks with a single hook
        # string
        hook_fields = ['pre-hook', 'post-hook']
        for hook_field in hook_fields:
            if hook_field in config:
                config[hook_field] = self.__get_hooks(config, hook_field)

        self.in_model_config.update(config)

    def __get_hooks(self, relevant_configs, key):
        hooks = []

        if key not in relevant_configs:
            return []

        new_hooks = relevant_configs[key]
        if type(new_hooks) not in [list, tuple]:
            new_hooks = [new_hooks]

        for hook in new_hooks:
            if not isinstance(hook, basestring):
                name = ".".join(self.fqn)
                compiler_error(None, "{} for model {} is not a string!".format(
                    key, name
                ))

            hooks.append(hook)
        return hooks

    def smart_update(self, mutable_config, new_configs):
        relevant_configs = {
            key: new_configs[key] for key
            in new_configs if key in self.ConfigKeys
        }

        for key in SourceConfig.AppendListFields:
            new_hooks = self.__get_hooks(relevant_configs, key)
            mutable_config[key].extend([
                h for h in new_hooks if h not in mutable_config[key]
            ])

        for key in SourceConfig.ExtendDictFields:
            dict_val = relevant_configs.get(key, {})
            mutable_config[key].update(dict_val)

        for key in SourceConfig.ClobberFields:
            if key in relevant_configs:
                mutable_config[key] = relevant_configs[key]

        return relevant_configs

    def get_project_config(self, project):
        # most configs are overwritten by a more specific config, but pre/post
        # hooks are appended!
        config = {}
        for k in SourceConfig.AppendListFields:
            config[k] = []
        for k in SourceConfig.ExtendDictFields:
            config[k] = {}

        model_configs = project.get('models')

        if model_configs is None:
            return config

        # mutates config
        self.smart_update(config, model_configs)

        fqn = self.fqn[:]
        for level in fqn:
            level_config = model_configs.get(level, None)
            if level_config is None:
                break

            # mutates config
            relevant_configs = self.smart_update(config, level_config)

            clobber_configs = {
                k: v for (k, v) in relevant_configs.items()
                if k not in SourceConfig.AppendListFields and
                k not in SourceConfig.ExtendDictFields
            }

            config.update(clobber_configs)
            model_configs = model_configs[level]

        return config

    def load_config_from_own_project(self):
        return self.get_project_config(self.own_project)

    def load_config_from_active_project(self):
        return self.get_project_config(self.active_project)


class DBTSource(object):
    dbt_run_type = NodeType.Base

    def __init__(self, project, top_dir, rel_filepath, own_project):
        self._config = None
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
    def absolute_path(self):
        return os.path.join(self.root_dir, self.rel_filepath)

    @property
    def root_dir(self):
        return os.path.join(self.own_project['project-root'], self.top_dir)

    @property
    def is_empty(self):
        return len(self.contents.strip()) == 0

    def compile(self):
        raise RuntimeError("Not implemented!")

    def serialize(self):
        serialized = {
            "build_path": os.path.join(
                self.project['target-path'], self.build_path()
            ),
            "source_path": self.filepath,
            "name": self.name,
            "tmp_name": self.tmp_name(),
            "project_name": self.own_project['name'],
            "dbt_run_type": self.dbt_run_type
        }

        serialized.update(self.config)
        return serialized

    @property
    def contents(self):
        return dbt.clients.system.load_file_contents(self.absolute_path)

    @property
    def config(self):
        if self._config is not None:
            return self._config

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
        enabled = self.config['enabled']
        if enabled not in (True, False):
            compiler_error(
                self,
                "'enabled' config must be either True or False. '{}' given."
                .format(enabled)
            )
        return enabled

    @property
    def fqn(self):
        """
        fully-qualified name for model. Includes all subdirs below 'models'
        path and the filename
        """
        parts = split_path(self.filepath)
        name, _ = os.path.splitext(parts[-1])
        return [self.own_project['name']] + parts[1:-1] + [name]

    @property
    def original_fqn(self):
        return self.fqn

    def tmp_name(self):
        if self.is_non_destructive():
            return self.name
        else:
            return "{}__dbt_tmp".format(self.name)

    def is_non_destructive(self):
        return dbt.flags.NON_DESTRUCTIVE

    def rename_query(self, schema):
        opts = {
            "schema": schema,
            "tmp_name": self.tmp_name(),
            "final_name": self.name
        }

        return 'alter table "{schema}"."{tmp_name}" rename to "{final_name}"' \
            .format(**opts)

    @property
    def nice_name(self):
        return "{}.{}".format(self.fqn[0], self.fqn[-1])


class Model(DBTSource):
    dbt_run_type = NodeType.Model
    build_dir = 'build'
    template = BaseCreateTemplate()

    def __init__(self, project, model_dir, rel_filepath, own_project):
        self.prologue = []
        super(Model, self).__init__(
            project, model_dir, rel_filepath, own_project
        )

    def add_to_prologue(self, s):
        safe_string = s.replace('{{', 'DBT_EXPR(').replace('}}', ')')
        self.prologue.append(safe_string)

    def get_prologue_string(self):
        blob = "\n".join("-- {}".format(s) for s in self.prologue)
        return "-- Compiled by DBT\n{}".format(blob)

    def build_path(self):
        filename = "{}.sql".format(self.name)
        path_parts = [self.build_dir] + self.fqn[:-1] + [filename]
        return os.path.join(*path_parts)

    def compile_string(self, ctx, string):
        # if bool/int/float/etc are passed in, don't compile anything
        if not isinstance(string, basestring):
            return string

        return dbt.clients.jinja.get_rendered(string, ctx, self)

    def get_hooks(self, ctx, hook_key):
        hooks = self.config.get(hook_key, [])
        if isinstance(hooks, basestring):
            hooks = [hooks]

        return [self.compile_string(ctx, hook) for hook in hooks]

    def compile(self, rendered_query, project, ctx):
        model_config = self.config

        if self.materialization not in SourceConfig.Materializations:
            compiler_error(
                self,
                "Invalid materialize option given: '{}'. Must be one of {}"
                .format(self.materialization, SourceConfig.Materializations)
            )

        schema = ctx['env'].get('schema', 'public')

        # these are empty strings if configs aren't provided
        dist_qualifier = self.dist_qualifier(model_config)
        sort_qualifier = self.sort_qualifier(model_config)

        if self.materialization == 'incremental':
            identifier = self.name
            if 'sql_where' not in model_config:
                compiler_error(
                    self,
                    # TODO - this probably won't be an error now?
                    "sql_where not specified in model materialized "
                    "as incremental"
                )
            raw_sql_where = model_config['sql_where']
            sql_where = self.compile_string(ctx, raw_sql_where)

            unique_key = model_config.get('unique_key', None)
        else:
            identifier = self.tmp_name()
            sql_where = None
            unique_key = None

        pre_hooks = self.get_hooks(ctx, 'pre-hook')
        post_hooks = self.get_hooks(ctx, 'post-hook')

        opts = {
            "materialization": self.materialization,
            "schema": schema,
            "identifier": identifier,
            "query": rendered_query,
            "dist_qualifier": dist_qualifier,
            "sort_qualifier": sort_qualifier,
            "sql_where": sql_where,
            "prologue": self.get_prologue_string(),
            "unique_key": unique_key,
            "pre-hooks": pre_hooks,
            "post-hooks": post_hooks,
            "non_destructive": self.is_non_destructive()
        }

        return self.template.wrap(opts)

    @property
    def immediate_name(self):
        if self.materialization == 'incremental':
            return self.name
        else:
            return self.tmp_name()

    @property
    def cte_name(self):
        return "__dbt__CTE__{}".format(self.name)

    def __repr__(self):
        return "<Model {}.{}: {}>".format(
            self.project['name'], self.name, self.filepath
        )


class Csv(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Csv, self).__init__(
            project, target_dir, rel_filepath, own_project
        )

    def __repr__(self):
        return "<Csv {}.{}: {}>".format(
            self.project['name'], self.model_name, self.filepath
        )


class Macro(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Macro, self).__init__(
            project, target_dir, rel_filepath, own_project
        )
        self.filepath = os.path.join(self.root_dir, self.rel_filepath)

    def get_macros(self, ctx):
        template = dbt.clients.jinja.get_template(
            self.contents, ctx, self)

        for key, item in template.module.__dict__.items():
            if type(item) == jinja2.runtime.Macro:
                yield {"project": self.own_project, "name": key, "macro": item}

    def __repr__(self):
        return "<Macro {}.{}: {}>".format(
            self.project['name'], self.name, self.filepath
        )
