from dataclasses import dataclass
from typing import Union, Dict

import agate
from dbt.utils import filter_null_values


"""
This is what relation metadata from the database looks like. It's a dictionary because there will be
multiple grains of data for a single object. For example, a materialized view in Postgres has base level information,
like name. But it also can have multiple indexes, which needs to be a separate query. It might look like this:

{
    "base": agate.Row({"table_name": "table_abc", "query": "select * from table_def"})
    "indexes": agate.Table("rows": [
        agate.Row({"name": "index_a", "columns": ["column_a"], "type": "hash", "unique": False}),
        agate.Row({"name": "index_b", "columns": ["time_dim_a"], "type": "btree", "unique": False}),
    ])
}
"""
RelationResults = Dict[str, Union[agate.Row, agate.Table]]


@dataclass(frozen=True)
class RelationConfigBase:
    @classmethod
    def from_dict(cls, kwargs_dict) -> "RelationConfigBase":
        """
        This assumes the subclass of `RelationConfigBase` is flat, in the sense that no attribute is
        itself another subclass of `RelationConfigBase`. If that's not the case, this should be overriden
        to manually manage that complexity.

        Args:
            kwargs_dict: the dict representation of this instance

        Returns: the `RelationConfigBase` representation associated with the provided dict
        """
        return cls(**filter_null_values(kwargs_dict))  # type: ignore

    @classmethod
    def _not_implemented_error(cls) -> NotImplementedError:
        return NotImplementedError(
            "This relation type has not been fully configured for this adapter."
        )
