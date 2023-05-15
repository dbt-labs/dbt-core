from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import agate

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError


Column = str
IndexDef = Dict[str, Union[List[Column], str, bool]]
IndexName = str


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):
    index_default_type = "btree"
    index_default_unique = False

    def __post_init__(self):
        # Check for length of Postgres table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return 63

    def get_index_updates(
        self, indexes: agate.Table, config: NodeConfig
    ) -> List[Optional[Dict[str, Union[str, IndexName, IndexDef]]]]:
        """
        Get the indexes that are changing as a result of the new run. There are four scenarios:

        1. Indexes are equal > don't return these
        2. Index is new > create these, no old index
        3. Index is old > drop these, no new index
        4. Indexes are not equal > return both

        Returns: a dictionary of indexes in the form:
        [
            {"action": "create", "context": {"columns": ["column_1", "column_3"], "type": "hash", "unique": True}},
        ]  # scenario 2
        OR
        [
            {"action": "drop", "context": "index_abc"},
        ]  # scenario 3
        OR
        [
            {"action": "drop", "context": "index_abc"},
            {"action": "create", "context": {"columns": ["column_1", "column_2"], "type": "hash", "unique": True}},
        ]  # scenario 4
        """
        existing_indexes = {
            index.indexname: self.get_index_config_from_create_statement(index.indexdef)
            for index in indexes
        }

        new_indexes = []
        for index in config.get("indexes", []):
            new_index = {
                "columns": sorted(index.pop("columns"), key=lambda x: x.upper()),
                "type": index.pop("type", self.index_default_type),
                "unique": index.pop("unique", self.index_default_unique),
            }
            new_index.update(index)
            new_indexes.append(new_index)

        updates = [
            {"action": "drop", "context": index_name}
            for index_name, index_def in existing_indexes.items()
            if index_def not in new_indexes
        ]
        updates.extend(
            [
                {"action": "create", "context": index}
                for index in new_indexes
                if index not in existing_indexes.values()
            ]
        )
        return updates

    def get_index_config_from_create_statement(self, create_statement: str) -> IndexDef:
        """
        Converts the create statement found in `pg_indexes` into the corresponding config provided by the user.

        For example:
        CREATE UNIQUE INDEX my_view_id_idx ON my_view USING btree (id) ->
        {'columns': [id], 'type': 'btree', 'unique': True}

        Args:
            create_statement: the create statement found in `pg_indexes`

        Returns:
            the corresponding config
        """
        try:
            column_clause = create_statement[
                create_statement.index("(") + 1 : create_statement.index(")")
            ]
        except IndexError:
            raise DbtRuntimeError(
                f"Malformed index create statement. Columns not contained within '()': '{create_statement}'"
            )

        columns = [column.strip() for column in column_clause.split(",")]
        sorted_columns = sorted(columns, key=lambda x: x.upper())

        keywords = create_statement[: create_statement.index("(")]
        keywords = keywords.strip().split(" ")

        if "unique" in keywords:
            unique = True
        else:
            unique = False

        if "using" in keywords:
            try:
                index_type = keywords[keywords.index("using") + 1]
            except IndexError:
                raise DbtRuntimeError(
                    f"Malformed index create statement. USING clause with no type: '{create_statement}'"
                )
        else:
            index_type = self.index_default_type

        return {"column": sorted_columns, "type": index_type, "unique": unique}
