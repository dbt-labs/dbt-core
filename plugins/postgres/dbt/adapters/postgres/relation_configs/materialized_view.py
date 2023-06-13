from dataclasses import dataclass, field
from typing import Set, FrozenSet, List

import agate
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation_configs.constants import MAX_CHARACTERS_IN_IDENTIFIER
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

    table_name: str = ""
    query: str = ""
    indexes: FrozenSet[PostgresIndexConfig] = field(default_factory=frozenset)

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        # index rules get run by default with the mixin
        return {
            RelationConfigValidationRule(
                validation_check=self.table_name is None
                or len(self.table_name) <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than {MAX_CHARACTERS_IN_IDENTIFIER} "
                    f"characters: {self.table_name}"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PostgresMaterializedViewConfig":
        kwargs_dict = {
            "table_name": config_dict.get("table_name"),
            "query": config_dict.get("query"),
            "indexes": frozenset(
                PostgresIndexConfig.from_dict(index) for index in config_dict.get("indexes", {})
            ),
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
        indexes: List[dict] = model_node.config.extra.get("indexes", [])
        config_dict = {
            "table_name": model_node.identifier,
            "query": model_node.compiled_code,
            "indexes": [PostgresIndexConfig.parse_model_node(index) for index in indexes],
        }
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
        indexes: agate.Table = relation_results.get("indexes", agate.Table(rows={}))
        config_dict = {
            "indexes": [
                PostgresIndexConfig.parse_relation_results(index) for index in indexes.rows
            ],
        }
        return config_dict


@dataclass
class PostgresMaterializedViewConfigChangeCollection:
    indexes: Set[PostgresIndexConfigChange] = field(default_factory=set)

    @property
    def requires_full_refresh(self) -> bool:
        return any(index.requires_full_refresh for index in self.indexes)

    @property
    def has_changes(self) -> bool:
        return self.indexes != set()
