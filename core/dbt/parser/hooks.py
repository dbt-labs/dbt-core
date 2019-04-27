
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

    def get_hooks(self, hook_type):
        project_hooks = collections.defaultdict(list)

        for project_name, project in self.all_projects.items():
            hooks = self.get_hooks_from_project(project, hook_type)
            project_hooks[project_name].extend(hooks)

        return project_hooks

    def load_and_parse_run_hook_type(self, hook_type):
        project_hooks = self.get_hooks(hook_type)

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
        hooks, _ = self.parse_sql_nodes(result, tags=tags)
        return hooks

    def load_and_parse(self):
        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**self.all_projects)

        hook_nodes = {}
        for hook_type in RunHookType.Both:
            project_hooks = self.load_and_parse_run_hook_type(
                hook_type,
            )
            hook_nodes.update(project_hooks)

        return hook_nodes
