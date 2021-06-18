from dataclasses import dataclass
from typing import Optional
from dbt.adapters.base.impl import AdapterConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.base.meta import available
from dbt.adapters.postgres import PostgresAdapter
from dbt.adapters.redshift import RedshiftConnectionManager
from dbt.adapters.redshift import RedshiftColumn
from dbt.adapters.redshift import RedshiftRelation
from dbt.logger import GLOBAL_LOGGER as logger  # noqa
import dbt.exceptions


@dataclass
class RedshiftConfig(AdapterConfig):
    sort_type: Optional[str] = None
    dist: Optional[str] = None
    sort: Optional[str] = None
    bind: Optional[bool] = None


class RedshiftAdapter(PostgresAdapter, SQLAdapter):
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

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        ra3_node = self.config.credentials.ra3_node

        if database.lower() != expected.lower() and not ra3_node:
            raise dbt.exceptions.NotImplementedException(
                'Cross-db references allowed only in RA3.* node. ({} vs {})'
                .format(database, expected)
            )
        # return an empty string on success so macros can call this
        return ''

    def _get_catalog_schemas(self, manifest):
        # redshift(besides ra3) only allow one database (the main one)
        schemas = super(SQLAdapter, self)._get_catalog_schemas(manifest)
        try:
            return schemas.flatten(allow_multiple_databases=self.config.credentials.ra3_node)
        except dbt.exceptions.RuntimeException as exc:
            dbt.exceptions.raise_compiler_error(
                'Cross-db references allowed only in {} RA3.* node. Got {}'
                .format(self.type(), exc.msg)
            )
