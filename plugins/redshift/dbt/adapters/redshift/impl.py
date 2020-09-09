from dataclasses import dataclass
from typing import Optional
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftConnectionManager
from dbt.adapters.redshift import RedshiftColumn
from dbt.adapters.redshift import RedshiftRelation
from dbt.logger import GLOBAL_LOGGER as logger  # noqa


@dataclass
class RedshiftConfig(AdapterConfig):
    sort_type: Optional[str] = None
    dist: Optional[str] = None
    sort: Optional[str] = None
    bind: Optional[bool] = None


class RedshiftAdapter(PostgresAdapter):
    Relation = RedshiftRelation
    ConnectionManager = RedshiftConnectionManager
    Column = RedshiftColumn

    AdapterSpecificConfigs = RedshiftConfig

    @classmethod
    def date_function(cls):
        return 'getdate()'

    def drop_relation(self, relation):
        """
        In Redshift, DROP TABLE ... CASCADE should not be used
        inside a transaction. Redshift doesn't prevent the CASCADE
        part from conflicting with concurrent transactions. If we do
        attempt to drop two tables with CASCADE at once, we'll often
        get the dreaded:

            table was dropped by a concurrent transaction

        So, we need to lock around calls to the underlying
        drop_relation() function.

        https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_TABLE.html
        """
        with self.connections.fresh_transaction():
            return super().drop_relation(relation)

    @classmethod
    def convert_text_type(cls, agate_table, col_idx):
        column = agate_table.columns[col_idx]
        # `lens` must be a list, so this can't be a generator expression,
        # because max() raises ane exception if its argument has no members.
        lens = [len(d.encode("utf-8")) for d in column.values_without_nulls()]
        max_len = max(lens) if lens else 64
        return "varchar({})".format(max_len)

    @classmethod
    def convert_time_type(cls, agate_table, col_idx):
        return "varchar(24)"
