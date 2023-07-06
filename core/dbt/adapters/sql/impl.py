import agate
from typing import Any, Optional, Tuple, Type, List

from dbt.contracts.connection import Connection, AdapterResponse
from dbt.exceptions import RelationTypeNullError
from dbt.adapters.base import BaseAdapter, available
from dbt.adapters.cache import _make_ref_key_dict
from dbt.adapters.sql import SQLConnectionManager
from dbt.events.functions import fire_event
from dbt.events.types import ColTypeChange, SchemaCreation, SchemaDrop


from dbt.adapters.base.relation import BaseRelation

LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"
GET_COLUMNS_IN_RELATION_MACRO_NAME = "get_columns_in_relation"
LIST_SCHEMAS_MACRO_NAME = "list_schemas"
CHECK_SCHEMA_EXISTS_MACRO_NAME = "check_schema_exists"
CREATE_SCHEMA_MACRO_NAME = "create_schema"
DROP_SCHEMA_MACRO_NAME = "drop_schema"
RENAME_RELATION_MACRO_NAME = "rename_relation"
TRUNCATE_RELATION_MACRO_NAME = "truncate_relation"
DROP_RELATION_MACRO_NAME = "drop_relation"
ALTER_COLUMN_TYPE_MACRO_NAME = "alter_column_type"
VALIDATE_SQL_MACRO_NAME = "validate_sql"


class SQLAdapter(BaseAdapter):
    """The default adapter with the common agate conversions and some SQL
    methods was implemented. This adapter has a different much shorter list of
    methods to implement, but some more macros that must be implemented.

    To implement a macro, implement "${adapter_type}__${macro_name}". in the
    adapter's internal project.

    Methods to implement:
        - date_function

    Macros to implement:
        - get_catalog
        - list_relations_without_caching
        - get_columns_in_relation
    """

    ConnectionManager: Type[SQLConnectionManager]
    connections: SQLConnectionManager

    @available.parse(lambda *a, **k: (None, None))
    def add_query(
        self,
        sql: str,
        auto_begin: bool = True,
        bindings: Optional[Any] = None,
        abridge_sql_log: bool = False,
    ) -> Tuple[Connection, Any]:
        """Add a query to the current transaction. A thin wrapper around
        ConnectionManager.add_query.

        :param sql: The SQL query to add
        :param auto_begin: If set and there is no transaction in progress,
            begin a new one.
        :param bindings: An optional list of bindings for the query.
        :param abridge_sql_log: If set, limit the raw sql logged to 512
            characters
        """
        return self.connections.add_query(sql, auto_begin, bindings, abridge_sql_log)

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "text"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        # TODO CT-211
        decimals = agate_table.aggregate(agate.MaxPrecision(col_idx))  # type: ignore[attr-defined]
        return "float8" if decimals else "integer"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "boolean"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp without time zone"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "date"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "time"

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def expand_column_types(self, goal, current):
        reference_columns = {c.name: c for c in self.get_columns_in_relation(goal)}

        target_columns = {c.name: c for c in self.get_columns_in_relation(current)}

        for column_name, reference_column in reference_columns.items():
            target_column = target_columns.get(column_name)

            if target_column is not None and target_column.can_expand_to(reference_column):
                col_string_size = reference_column.string_size()
                new_type = self.Column.string_type(col_string_size)
                fire_event(
                    ColTypeChange(
                        orig_type=target_column.data_type,
                        new_type=new_type,
                        table=_make_ref_key_dict(current),
                    )
                )

                self.alter_column_type(current, column_name, new_type)

    def alter_column_type(self, relation, column_name, new_column_type) -> None:
        """
        1. Create a new column (w/ temp name and correct type)
        2. Copy data over to it
        3. Drop the existing column (cascade!)
        4. Rename the new column to existing column
        """
        kwargs = {
            "relation": relation,
            "column_name": column_name,
            "new_column_type": new_column_type,
        }
        self.execute_macro(ALTER_COLUMN_TYPE_MACRO_NAME, kwargs=kwargs)

    def drop_relation(self, relation):
        if relation.type is None:
            raise RelationTypeNullError(relation)

        self.cache_dropped(relation)
        self.execute_macro(DROP_RELATION_MACRO_NAME, kwargs={"relation": relation})

    def truncate_relation(self, relation):
        self.execute_macro(TRUNCATE_RELATION_MACRO_NAME, kwargs={"relation": relation})

    def rename_relation(self, from_relation, to_relation):
        self.cache_renamed(from_relation, to_relation)

        kwargs = {"from_relation": from_relation, "to_relation": to_relation}
        self.execute_macro(RENAME_RELATION_MACRO_NAME, kwargs=kwargs)

    def get_columns_in_relation(self, relation):
        return self.execute_macro(
            GET_COLUMNS_IN_RELATION_MACRO_NAME, kwargs={"relation": relation}
        )

    def create_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaCreation(relation=_make_ref_key_dict(relation)))
        kwargs = {
            "relation": relation,
        }
        self.execute_macro(CREATE_SCHEMA_MACRO_NAME, kwargs=kwargs)
        self.commit_if_has_connection()
        # we can't update the cache here, as if the schema already existed we
        # don't want to (incorrectly) say that it's empty

    def drop_schema(self, relation: BaseRelation) -> None:
        relation = relation.without_identifier()
        fire_event(SchemaDrop(relation=_make_ref_key_dict(relation)))
        kwargs = {
            "relation": relation,
        }
        self.execute_macro(DROP_SCHEMA_MACRO_NAME, kwargs=kwargs)
        self.commit_if_has_connection()
        # we can update the cache here
        self.cache.drop_schema(relation.database, relation.schema)

    def list_relations_without_caching(
        self,
        schema_relation: BaseRelation,
    ) -> List[BaseRelation]:
        kwargs = {"schema_relation": schema_relation}
        results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)

        relations = []
        quote_policy = {"database": True, "schema": True, "identifier": True}
        for _database, name, _schema, _type in results:
            try:
                _type = self.Relation.get_relation_type(_type)
            except ValueError:
                _type = self.Relation.External
            relations.append(
                self.Relation.create(
                    database=_database,
                    schema=_schema,
                    identifier=name,
                    quote_policy=quote_policy,
                    type=_type,
                )
            )
        return relations

    @classmethod
    def quote(self, identifier):
        return '"{}"'.format(identifier)

    def list_schemas(self, database: str) -> List[str]:
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        return [row[0] for row in results]

    def check_schema_exists(self, database: str, schema: str) -> bool:
        information_schema = self.Relation.create(
            database=database,
            schema=schema,
            identifier="INFORMATION_SCHEMA",
            quote_policy=self.config.quoting,
        ).information_schema()

        kwargs = {"information_schema": information_schema, "schema": schema}
        results = self.execute_macro(CHECK_SCHEMA_EXISTS_MACRO_NAME, kwargs=kwargs)
        return results[0][0] > 0

    def validate_sql(self, sql: str) -> AdapterResponse:
        """Submit the given SQL to the engine for validation, but not execution.

        By default we simply prefix the query with the explain keyword and allow the
        exceptions thrown by the underlying engine on invalid SQL inputs to bubble up
        to the exception handler. For adjustments to the explain statement - such as
        for adapters that have different mechanisms for hinting at query validation
        or dry-run - callers may be able to override the validate_sql_query macro with
        the addition of an <adapter>__validate_sql implementation.

        :param sql str: The sql to validate
        """
        kwargs = {
            "sql": sql,
        }
        result = self.execute_macro(VALIDATE_SQL_MACRO_NAME, kwargs=kwargs)
        # The statement macro always returns an AdapterResponse in the output AttrDict's
        # `response` property, and we preserve the full payload in case we want to
        # return fetched output for engines where explain plans are emitted as columnar
        # results. Any macro override that deviates from this behavior may encounter an
        # assertion error in the runtime.
        adapter_response = result.response  # type: ignore[attr-defined]
        assert isinstance(adapter_response, AdapterResponse), (
            f"Expected AdapterResponse from validate_sql macro execution, "
            f"got {type(adapter_response)}."
        )
        return adapter_response

    # This is for use in the test suite
    def run_sql_for_tests(self, sql, fetch, conn):
        cursor = conn.handle.cursor()
        try:
            cursor.execute(sql)
            if hasattr(conn.handle, "commit"):
                conn.handle.commit()
            if fetch == "one":
                return cursor.fetchone()
            elif fetch == "all":
                return cursor.fetchall()
            else:
                return
        except BaseException as e:
            if conn.handle and not getattr(conn.handle, "closed", True):
                conn.handle.rollback()
            print(sql)
            print(e)
            raise
        finally:
            conn.transaction_open = False
