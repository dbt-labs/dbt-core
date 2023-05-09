from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import agate

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError


Column = str
IndexDef = List[Column]
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
    ) -> List[Optional[Dict[str, Union[str, IndexDef]]]]:
        """
        Get the indexes that are changing as a result of the new run. There are four scenarios:

        1. Indexes are equal > don't return these
        2. Index is new > create these, no old index
        3. Index is old > drop these, no new index
        4. Indexes are not equal > return both

        Returns: a dictionary of indexes in the form:
        {
            "<index_name>": {
                "old": ["<column_1_name>", "<column_2_name>", ...]
                "new": ["<column_1_name>", "<column_3_name>", ...]
        }  # scenario 4
        OR
        {
            "<index_name>": {
                "old": None
                "new": ["<column_1_name>", "<column_3_name>", ...]
        }  # scenario 2
        OR
        {
            "<index_name>": {
                "old": ["<column_1_name>", "<column_2_name>", ...]
                "new": None
        }  # scenario 3
        """
        existing_indexes = {
            index.indexname: self.get_index_config_from_create_statement(index.indexdef)
            for index in indexes
        }
        default_index = {
            "columns": [],
            "type": self.index_default_type,
            "unique": self.index_default_unique,
        }
        new_indexes = []
        for index in config.get("indexes", []):
            index_stub = default_index.copy()
            index_stub.update(index)
            new_indexes.append(index_stub)

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

    def get_index_config_from_create_statement(
        self, create_statement: str
    ) -> Dict[str, Union[list, str, bool]]:
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
        keywords, columns = create_statement.replace(")", "").split("(")
        keywords = keywords.split(" ")
        columns = [column.strip() for column in columns.split(",")]
        unique = keywords[1] == "unique"
        if "using" in keywords:
            type = keywords[keywords.index("using") + 1]
        else:
            type = self.index_default_type
        return {"column": columns, "type": type, "unique": unique}
