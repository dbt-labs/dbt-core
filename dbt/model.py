import os.path

import dbt.exceptions

from dbt.compat import basestring

from dbt.utils import split_path, deep_merge, DBTConfigKeys
from dbt.node_types import NodeType


class SourceConfig(object):
    ConfigKeys = DBTConfigKeys

    AppendListFields = ['pre-hook', 'post-hook']
    ExtendDictFields = ['vars', 'column_types', 'quoting']
    ClobberFields = [
        'alias',
        'schema',
        'enabled',
        'materialized',
        'dist',
        'sort',
        'sql_where',
        'unique_key',
        'sort_type',
        'bind'
    ]

    def __init__(self, active_project, own_project, fqn, node_type):
        self._config = None
        self.active_project = active_project
        self.own_project = own_project
        self.fqn = fqn
        self.node_type = node_type

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

        if self.node_type == NodeType.Seed:
            defaults['materialized'] = 'seed'

        active_config = self.load_config_from_active_project()

        if self.active_project['name'] == self.own_project['name']:
            cfg = self._merge(defaults, active_config,
                              self.in_model_config)
        else:
            own_config = self.load_config_from_own_project()

            cfg = self._merge(
                defaults, own_config, self.in_model_config, active_config
            )

        return cfg

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
        if key not in relevant_configs:
            return []

        hooks = relevant_configs[key]
        if not isinstance(hooks, (list, tuple)):
            hooks = [hooks]

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

        if self.node_type == NodeType.Seed:
            model_configs = project.get('seeds')
        else:
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
    def __init__(self, project, top_dir, rel_filepath, own_project):
        self._config = None
        self.project = project
        self.own_project = own_project

        self.top_dir = top_dir
        self.rel_filepath = rel_filepath
        self.filepath = os.path.join(top_dir, rel_filepath)
        self.name = self.fqn[-1]

        self.source_config = SourceConfig(project, own_project, self.fqn)

    def compile(self):
        raise RuntimeError("Not implemented!")

    @property
    def config(self):
        if self._config is not None:
            return self._config

        return self.source_config.config

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
    def nice_name(self):
        return "{}.{}".format(self.fqn[0], self.fqn[-1])


class Csv(DBTSource):
    def __init__(self, project, target_dir, rel_filepath, own_project):
        super(Csv, self).__init__(
            project, target_dir, rel_filepath, own_project
        )

    def __repr__(self):
        return "<Csv {}.{}: {}>".format(
            self.project['name'], self.model_name, self.filepath
        )
