from typing import Any, Dict

from dbt.contracts.connection import HasCredentials

from dbt.context.base import (
    BaseContext, contextproperty
)


class TargetContext(BaseContext):
    def __init__(self, config: HasCredentials, cli_vars: Dict[str, Any]):
        super().__init__(cli_vars=cli_vars)
        self.config = config

    @contextproperty
    def target(self) -> Dict[str, Any]:
        return self.config.to_target_dict()


def generate_target_context(
    config: HasCredentials, cli_vars: Dict[str, Any]
) -> Dict[str, Any]:
    ctx = TargetContext(config, cli_vars)
    return ctx.to_dict()
