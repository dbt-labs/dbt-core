from dataclasses import dataclass, field
from typing import Optional, Set, Iterable

from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation_configs.index import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresMaterializedViewConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    This config follows the specs found here:
    https://www.postgresql.org/docs/current/sql-creatematerializedview.html

    The following parameters are configurable by dbt:
    - table_name: name of the materialized view
    - query: the query that defines the view
    - indexes: the collection (set) of indexes on the materialized view

    Applicable defaults for non-configurable parameters:
    - method: `heap`
    - tablespace_name: `default_tablespace`
    - with_data: `True`
    """

    table_name: Optional[str] = None
    query: Optional[str] = None
    indexes: Set[PostgresIndexConfig] = field(default_factory=set)

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # index rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=self.table_name is None or len(self.table_name) <= 63,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than 63 characters: {self.table_name}"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PostgresMaterializedViewConfig":
        kwargs_dict = {
            "table_name": config_dict.get("table_name"),
            "query": config_dict.get("query"),
            "indexes": {
                PostgresIndexConfig.from_dict(index) for index in config_dict.get("indexes", {})
            },
        }
        materialized_view: "PostgresMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "PostgresMaterializedViewConfig":
        materialized_view_config = cls.parse_model_node(model_node)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Postgres-specific implementation of `RelationConfig.from_model_node()` for materialized views
        """
        config_dict = {
            "table_name": model_node.identifier,
            "query": model_node.compiled_code,
        }

        # create index objects for each index found in the config
        if indexes := model_node.config.extra.get("indexes"):
            index_configs = [PostgresIndexConfig.parse_model_node(index) for index in indexes]
            config_dict.update({"indexes": index_configs})

        return config_dict

    @classmethod
    def from_relation_results(
        cls, relation_results: RelationResults
    ) -> "PostgresMaterializedViewConfig":
        materialized_view_config = cls.parse_relation_results(relation_results)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        """
        Postgres-specific implementation of `RelationConfig.from_relation_results()` for materialized views
        """
        base_config = relation_results.get("base", {})
        config_dict = {
            "table_name": base_config.get("table_name"),
            "query": base_config.get("query"),
        }

        # create index objects for each index found in the config
        if indexes := relation_results.get("indexes"):
            index_configs = [
                PostgresIndexConfig.parse_relation_results({"base": index})
                for index in indexes.rows
            ]
            config_dict.update({"indexes": index_configs})

        return config_dict


@dataclass
class PostgresMaterializedViewConfigChangeCollection:
    indexes: Optional[Set[PostgresIndexConfigChange]] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(index.requires_full_refresh for index in self.indexes)

    @property
    def has_changes(self) -> bool:
        if isinstance(self.indexes, Iterable):
            return any({index.is_change for index in self.indexes})
        return False
