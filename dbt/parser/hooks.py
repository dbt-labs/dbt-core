
import collections

import dbt.flags
import dbt.contracts.project
import dbt.utils

from dbt.parser.base_sql import BaseSqlParser
from dbt.node_types import NodeType, RunHookType


class HookParser(BaseSqlParser):
    @classmethod
    def get_hooks_from_project(cls, config, hook_type):
        if hook_type == RunHookType.Start:
            hooks = config.on_run_start
        elif hook_type == RunHookType.End:
            hooks = config.on_run_end
        else:
            dbt.exceptions.InternalException(
                'hook_type must be one of "{}" or "{}"'
                .format(RunHookType.Start, RunHookType.End))

        if type(hooks) not in (list, tuple):
            hooks = [hooks]

        return hooks

    @classmethod
    def get_hooks(cls, all_projects, hook_type):
        project_hooks = collections.defaultdict(list)

        for project_name, project in all_projects.items():
            hooks = cls.get_hooks_from_project(project, hook_type)
            project_hooks[project_name].extend(hooks)

        return project_hooks

    @classmethod
    def load_and_parse_run_hook_type(cls, root_project, all_projects,
                                     hook_type, macros=None):

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        project_hooks = cls.get_hooks(all_projects, hook_type)

        result = []
        for project_name, hooks in project_hooks.items():
            for i, hook in enumerate(hooks):
                hook_name = '{}-{}-{}'.format(project_name, hook_type, i)
                hook_path = dbt.utils.get_pseudo_hook_path(hook_name)

                result.append({
                    'name': hook_name,
                    'root_path': "{}/dbt_project.yml".format(project_name),
                    'resource_type': NodeType.Operation,
                    'path': hook_path,
                    'original_file_path': hook_path,
                    'package_name': project_name,
                    'raw_sql': hook,
                    'index': i
                })

        tags = [hook_type]
        hooks, _ = cls.parse_sql_nodes(result, root_project, all_projects,
                                       tags=tags, macros=macros)
        return hooks

    @classmethod
    def load_and_parse(cls, root_project, all_projects, macros=None):
        if macros is None:
            macros = {}

        hook_nodes = {}
        for hook_type in RunHookType.Both:
            project_hooks = cls.load_and_parse_run_hook_type(
                root_project,
                all_projects,
                hook_type,
                macros=macros
            )
            hook_nodes.update(project_hooks)

        return hook_nodes
