from typing import (
    Any, Dict, Union
)

from dbt.exceptions import (
    doc_invalid_args,
    doc_target_not_found,
)
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.compiled import CompileResultNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.parsed import ParsedMacro

from dbt.context.base import contextmember
from dbt.context.configured import ConfiguredContext


class DocsParseContext(ConfiguredContext):
    def __init__(
        self,
        config: RuntimeConfig,
        node: Any,
    ) -> None:
        super().__init__(config)
        self.node = node

    @contextmember
    def doc(self, *args: str) -> str:
        # when you call doc(), this is what happens at parse time
        if len(args) != 1 and len(args) != 2:
            doc_invalid_args(self.node, args)
        # At parse time, nothing should care about what doc() returns
        return ''


class DocsRuntimeContext(ConfiguredContext):
    def __init__(
        self,
        config: RuntimeConfig,
        node: Union[ParsedMacro, CompileResultNode],
        manifest: Manifest,
        current_project: str,
    ) -> None:
        super().__init__(config)
        self.node = node
        self.manifest = manifest
        self.current_project = current_project

    @contextmember
    def doc(self, *args: str) -> str:
        # when you call doc(), this is what happens at runtime
        if len(args) == 1:
            doc_package_name = None
            doc_name = args[0]
        elif len(args) == 2:
            doc_package_name, doc_name = args
        else:
            doc_invalid_args(self.node, args)

        target_doc = self.manifest.resolve_doc(
            doc_name,
            doc_package_name,
            self.current_project,
            self.node.package_name,
        )

        if target_doc is None:
            doc_target_not_found(self.node, doc_name, doc_package_name)

        return target_doc.block_contents


def generate_parser_docs(
    config: RuntimeConfig,
    unparsed: Any,
) -> Dict[str, Any]:
    ctx = DocsParseContext(config, unparsed)
    return ctx.to_dict()


def generate_runtime_docs(
    config: RuntimeConfig,
    target: Any,
    manifest: Manifest,
    current_project: str,
) -> Dict[str, Any]:
    ctx = DocsRuntimeContext(config, target, manifest, current_project)
    return ctx.to_dict()
