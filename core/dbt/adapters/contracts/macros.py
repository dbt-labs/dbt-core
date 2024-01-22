from typing import Optional
from typing_extensions import Protocol

from dbt_common.clients.jinja import MacroProtocol


class MacroResolverProtocol(Protocol):
    def find_macro_by_name(
        self, name: str, root_project_name: str, package: Optional[str]
    ) -> Optional[MacroProtocol]:
        raise NotImplementedError("find_macro_by_name not implemented")
