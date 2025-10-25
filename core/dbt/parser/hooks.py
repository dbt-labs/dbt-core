from dataclasses import dataclass
from typing import Any, Dict, Iterable, Iterator, List, Tuple, Union

from dbt.constants import DEFAULT_HOOK_PRIORITY
from dbt.context.context_config import ContextConfig
from dbt.contracts.files import FilePath
from dbt.contracts.graph.nodes import HookNode
from dbt.node_types import NodeType, RunHookType
from dbt.parser.base import SimpleParser
from dbt.parser.search import FileBlock
from dbt.utils import get_pseudo_hook_path
from dbt_common.exceptions import DbtInternalError


@dataclass
class HookBlock(FileBlock):
    project: str
    value: str
    index: int
    hook_type: RunHookType
    priority: int = DEFAULT_HOOK_PRIORITY

    @property
    def contents(self):
        return self.value

    @property
    def name(self):
        return "{}-{!s}-{!s}".format(self.project, self.hook_type, self.index)


class HookSearcher(Iterable[HookBlock]):
    def __init__(self, project, source_file, hook_type) -> None:
        self.project = project
        self.source_file = source_file
        self.hook_type = hook_type

    def _hook_list(
        self, hooks: Union[str, List[Union[str, Dict]], Tuple[Union[str, Dict], ...]]
    ) -> List[Dict[str, Any]]:
        """Convert hook definitions to a standardized list format."""
        if isinstance(hooks, tuple):
            hooks = list(hooks)
        elif not isinstance(hooks, list):
            hooks = [hooks]

        # Standardize format - ensure all hooks have a consistent structure
        result = []
        for hook in hooks:
            if isinstance(hook, str):
                # Convert string to dict format with default priority
                result.append({"sql": hook, "priority": DEFAULT_HOOK_PRIORITY})
            elif isinstance(hook, dict) and "sql" in hook:
                # Ensure required keys exist with defaults
                hook_dict = {
                    "sql": hook["sql"],
                    "priority": hook.get("priority", DEFAULT_HOOK_PRIORITY),
                }
                result.append(hook_dict)
            elif isinstance(hook, dict):
                # Backward compatibility for any other dict format
                result.append({"sql": str(hook), "priority": DEFAULT_HOOK_PRIORITY})

        return result

    def get_hook_defs(self) -> List[Dict[str, Any]]:
        if self.hook_type == RunHookType.Start:
            hooks = self.project.on_run_start
        elif self.hook_type == RunHookType.End:
            hooks = self.project.on_run_end
        else:
            raise DbtInternalError(
                'hook_type must be one of "{}" or "{}" (got {})'.format(
                    RunHookType.Start, RunHookType.End, self.hook_type
                )
            )
        return self._hook_list(hooks)

    def __iter__(self) -> Iterator[HookBlock]:
        hooks = self.get_hook_defs()
        for index, hook in enumerate(hooks):
            # Extract SQL and priority
            if isinstance(hook, dict):
                sql = hook.get("sql", "")
                priority = hook.get("priority", DEFAULT_HOOK_PRIORITY)
            else:
                # Fallback for any unexpected format
                sql = str(hook)
                priority = DEFAULT_HOOK_PRIORITY

            yield HookBlock(
                file=self.source_file,
                project=self.project.project_name,
                value=sql,
                index=index,
                hook_type=self.hook_type,
                priority=priority,
            )


class HookParser(SimpleParser[HookBlock, HookNode]):

    # Hooks are only in the dbt_project.yml file for the project
    def get_path(self) -> FilePath:
        # There ought to be an existing file object for this, but
        # until that is implemented use a dummy modification time
        path = FilePath(
            project_root=self.project.project_root,
            searched_path=".",
            relative_path="dbt_project.yml",
            modification_time=0.0,
        )
        return path

    def parse_from_dict(self, dct, validate=True) -> HookNode:
        if validate:
            HookNode.validate(dct)
        return HookNode.from_dict(dct)

    @classmethod
    def get_compiled_path(cls, block: HookBlock):
        return get_pseudo_hook_path(block.name)

    def _create_parsetime_node(
        self,
        block: HookBlock,
        path: str,
        config: ContextConfig,
        fqn: List[str],
        name=None,
        **kwargs,
    ) -> HookNode:

        # Create the node using the parent method
        node = super()._create_parsetime_node(
            block=block,
            path=path,
            config=config,
            fqn=fqn,
            index=block.index,
            name=name,
            tags=[str(block.hook_type)],
        )

        # Store the priority in the node's meta
        if not node.config.meta:
            node.config.meta = {}
        node.config.meta["hook_priority"] = block.priority

        return node

    @property
    def resource_type(self) -> NodeType:
        return NodeType.Operation

    def parse_file(self, block: FileBlock) -> None:
        for hook_type in RunHookType:
            for hook in HookSearcher(self.project, block.file, hook_type):
                self.parse_node(hook)
