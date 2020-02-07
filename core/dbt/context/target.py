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
        target = dict(
            self.config.credentials.connection_info(with_aliases=True)
        )
        target.update({
            'type': self.config.credentials.type,
            'threads': self.config.threads,
            'name': self.config.target_name,
            # not specified, but present for compatibility
            'target_name': self.config.target_name,
            'profile_name': self.config.profile_name,
            'config': self.config.config.to_dict(),
        })
        return target


def generate_target_context(
    config: HasCredentials, cli_vars: Dict[str, Any]
) -> Dict[str, Any]:
    ctx = TargetContext(config, cli_vars)
    return ctx.to_dict()
