from typing import Any, Dict

from dbt.contracts.connection import HasCredentials

from dbt.context.base import (
    BaseContext, contextproperty
)


class TargetContext(BaseContext):
    # subclass is ConfiguredContext
    def __init__(self, config: HasCredentials, cli_vars: Dict[str, Any]):
        super().__init__(cli_vars=cli_vars)
        self.config = config

    @contextproperty
    def target(self) -> Dict[str, Any]:
        """`target` contains information about your connection to the warehouse
        (specified in profiles.yml). Some configs are shared between all
        adapters, while others are adapter-specific.

        Common:

            |----------|-----------|------------------------------------------|
            | Variable |  Example  |                Description               |
            |----------|-----------|------------------------------------------|
            |   name   |    dev    | Name of the active target                |
            |----------|-----------|------------------------------------------|
            |  schema  | dbt_alice | Name of the dbt schema (or, dataset on   |
            |          |           | BigQuery)                                |
            |----------|-----------|------------------------------------------|
            |   type   |  postgres | The active adapter being used.           |
            |----------|-----------|------------------------------------------|
            | threads  |    4      | The number of threads in use by dbt      |
            |----------|-----------|------------------------------------------|

        Snowflake:

            |----------|-----------|------------------------------------------|
            | Variable |  Example  |                Description               |
            |----------|-----------|------------------------------------------|
            | database |    RAW    | The active target's database.            |
            |----------|-----------|------------------------------------------|
            | warehouse| TRANSFORM | The active target's warehouse.           |
            |----------|-----------|------------------------------------------|
            |   user   |  USERNAME | The active target's user                 |
            |----------|-----------|------------------------------------------|
            | role     |  ROLENAME | The active target's role                 |
            |----------|-----------|------------------------------------------|
            | account  |  abc123   | The active target's account              |
            |----------|-----------|------------------------------------------|

        Postgres/Redshift:

            |----------|-------------------|----------------------------------|
            | Variable |  Example          |        Description               |
            |----------|-------------------|----------------------------------|
            |  dbname  | analytics         | The active target's database.    |
            |----------|-------------------|----------------------------------|
            |  host    | abc123.us-west-2. | The active target's host.        |
            |          | redshift.amazonaws|                                  |
            |          |  .com             |                                  |
            |----------|-------------------|----------------------------------|
            |   user   |  dbt_user         |  The active target's user        |
            |----------|-------------------|----------------------------------|
            |   port   |    5439           | The active target's port         |
            |----------|-------------------|----------------------------------|

        BigQuery:

            |----------|-----------|------------------------------------------|
            | Variable |  Example  |                Description               |
            |----------|-----------|------------------------------------------|
            | project  |  abc-123  | The active target's project.             |
            |----------|-----------|------------------------------------------|

        """
        return self.config.to_target_dict()


def generate_target_context(
    config: HasCredentials, cli_vars: Dict[str, Any]
) -> Dict[str, Any]:
    ctx = TargetContext(config, cli_vars)
    return ctx.to_dict()
