from dataclasses import dataclass, field
from typing import Optional, Set, FrozenSet, Dict, Union, List

from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError
from dbt.adapters.relation_configs import (
    RelationConfigBase,
    RelationResults,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChangeAction,
    RelationConfigChange,
)


# A `NodeConfig` instance can have multiple indexes, this is just one index
# e.g. {"columns": ["column_a", "column_b"], "unique": True, "type": "hash"}
Columns = List[str]
ModelNodeEntry = Dict[str, Union[Columns, bool, str]]


class PostgresIndexMethod(StrEnum):
    btree = "btree"
    hash = "hash"
    gist = "gist"
    spgist = "spgist"
    gin = "gin"
    brin = "brin"

    @classmethod
    def default(cls) -> "PostgresIndexMethod":
        return cls.btree


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexConfig(RelationConfigBase, RelationConfigValidationMixin):
    """
    This config fallows the specs found here:
    https://www.postgresql.org/docs/current/sql-createindex.html

    The following parameters are configurable by dbt:
    - name: the name of the index in the database, this isn't predictable since we apply a timestamp
    - unique: checks for duplicate values when the index is created and on data updates
    - method: the index method to be used
    - column_names: the columns in the index

    Applicable defaults for non-configurable parameters:
    - concurrently: `False`
    - nulls_distinct: `True`
    """

    name: Optional[str] = field(default=None, hash=False, compare=False)
    column_names: Optional[FrozenSet[str]] = field(default_factory=set, hash=True)
    unique: Optional[bool] = field(default=False, hash=True)
    method: Optional[PostgresIndexMethod] = field(default=PostgresIndexMethod.btree, hash=True)

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.column_names is not None,
                validation_error=DbtRuntimeError(
                    "Indexes require at least one column, but none were provided"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "PostgresIndexConfig":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "method": config_dict.get("method"),
            "unique": config_dict.get("unique"),
            "column_names": frozenset(column for column in config_dict.get("column_names", {})),
        }
        index: "PostgresIndexConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return index

    @classmethod
    def parse_model_node(cls, model_node_entry: ModelNodeEntry) -> dict:
        config_dict = {
            "unique": model_node_entry.get("unique"),
            "method": model_node_entry.get("type"),
        }

        if column_names := model_node_entry.get("columns", []):
            # TODO: include the QuotePolicy instead of defaulting to lower()
            config_dict.update({"column_names": set(column.lower() for column in column_names)})

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> dict:
        index = relation_results.get("base", {})
        config_dict = {
            "name": index.get("name"),
            # we shouldn't have to adjust the values from the database for the QuotePolicy
            "column_names": set(index.get("column_names", "").split(",")),
            "unique": index.get("unique"),
            "method": index.get("method"),
        }
        return config_dict

    @property
    def as_node_config(self) -> dict:
        """
        Returns: a dictionary that can be passed into `get_create_index_sql()`
        """
        node_config = {
            "columns": list(self.column_names),
            "unique": self.unique,
            "type": self.method.value,
        }
        return node_config


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexConfigChange(RelationConfigChange, RelationConfigValidationMixin):
    """
    Example of an index change:
    {
        "action": "create",
        "context": {
            "name": "",  # we don't know the name since it gets created as a hash at runtime
            "columns": ["column_1", "column_3"],
            "type": "hash",
            "unique": True
        }
    },
    {
        "action": "drop",
        "context": {
            "name": "index_abc",  # we only need this to drop, but we need the rest to compare
            "columns": ["column_1"],
            "type": "btree",
            "unique": True
        }
    }
    """

    context: PostgresIndexConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action
                in {RelationConfigChangeAction.create, RelationConfigChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` changes are supported for indexes."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.create
                    and self.context.column_names == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }
