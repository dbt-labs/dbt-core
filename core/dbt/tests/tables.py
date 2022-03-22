from dbt.context import providers
from unittest.mock import patch
from contextlib import contextmanager
from dbt.events.functions import fire_event
from dbt.events.test_types import IntegrationTestDebug

# This code was copied from the earlier test framework in test/integration/base.py
# The goal is to vastly simplify this and replace it with calls to macros.
# For now, we use this to get the tests converted in a more straightforward way.
# Assertions:
#   assert_tables_equal  (old: assertTablesEqual)
#   assert_many_relations_equal  (old: assertManyRelationsEqual)
#   assert_many_tables_equal  (old: assertManyTablesEqual)
#   assert_table_does_not_exist  (old: assertTableDoesNotExist)
#   assert_table_does_exist  (old: assertTableDoesExist)


class TableComparison:
    def __init__(self, adapter, unique_schema, database):
        self.adapter = adapter
        self.unique_schema = unique_schema
        self.default_database = database
        # TODO: We need to get this from somewhere reasonable
        if database == "dbtMixedCase":
            self.quoting = {"database": True, "schema": True, "identifier": True}
        else:
            self.quoting = {"database": False, "schema": False, "identifier": False}

    # assertion used in tests
    def assert_tables_equal(
        self,
        table_a,
        table_b,
        table_a_schema=None,
        table_b_schema=None,
        table_a_db=None,
        table_b_db=None,
    ):
        if table_a_schema is None:
            table_a_schema = self.unique_schema

        if table_b_schema is None:
            table_b_schema = self.unique_schema

        if table_a_db is None:
            table_a_db = self.default_database

        if table_b_db is None:
            table_b_db = self.default_database

        relation_a = self._make_relation(table_a, table_a_schema, table_a_db)
        relation_b = self._make_relation(table_b, table_b_schema, table_b_db)

        self._assert_table_columns_equal(relation_a, relation_b)

        sql = self._assert_tables_equal_sql(relation_a, relation_b)
        result = self.run_sql(sql, fetch="one")

        assert result[0] == 0, "row_count_difference nonzero: " + sql
        assert result[1] == 0, "num_mismatched nonzero: " + sql

    # assertion used in tests
    def assert_many_relations_equal(self, relations, default_schema=None, default_database=None):
        if default_schema is None:
            default_schema = self.unique_schema
        if default_database is None:
            default_database = self.default_database

        specs = []
        for relation in relations:
            if not isinstance(relation, (tuple, list)):
                relation = [relation]

            assert len(relation) <= 3

            if len(relation) == 3:
                relation = self._make_relation(*relation)
            elif len(relation) == 2:
                relation = self._make_relation(relation[0], relation[1], default_database)
            elif len(relation) == 1:
                relation = self._make_relation(relation[0], default_schema, default_database)
            else:
                raise ValueError("relation must be a sequence of 1, 2, or 3 values")

            specs.append(relation)

        with self.get_connection():
            column_specs = self.get_many_relation_columns(specs)

        # make sure everyone has equal column definitions
        first_columns = None
        for relation in specs:
            key = (relation.database, relation.schema, relation.identifier)
            # get a good error here instead of a hard-to-diagnose KeyError
            assert key in column_specs, f"No columns found for {key}"
            columns = column_specs[key]
            if first_columns is None:
                first_columns = columns
            else:
                assert first_columns == columns, f"{str(specs[0])} did not match {str(relation)}"

        # make sure everyone has the same data. if we got here, everyone had
        # the same column specs!
        first_relation = None
        for relation in specs:
            if first_relation is None:
                first_relation = relation
            else:
                sql = self._assert_tables_equal_sql(
                    first_relation, relation, columns=first_columns
                )
                result = self.run_sql(sql, fetch="one")

                assert result[0] == 0, "row_count_difference nonzero: " + sql
                assert result[1] == 0, "num_mismatched nonzero: " + sql

    # assertion used in tests
    def assert_many_tables_equal(self, *args):
        schema = self.unique_schema

        all_tables = []
        for table_equivalencies in args:
            all_tables += list(table_equivalencies)

        all_cols = self.get_table_columns_as_dict(all_tables, schema)

        for table_equivalencies in args:
            first_table = table_equivalencies[0]
            first_relation = self._make_relation(first_table)

            # assert that all tables have the same columns
            base_result = all_cols[first_table]
            assert len(base_result) > 0

            for other_table in table_equivalencies[1:]:
                other_result = all_cols[other_table]
                assert len(other_result) > 0
                assert base_result == other_result

                other_relation = self._make_relation(other_table)
                sql = self._assert_tables_equal_sql(
                    first_relation, other_relation, columns=base_result
                )
                result = self.run_sql(sql, fetch="one")

                assert result[0] == 0, "row_count_difference nonzero: " + sql
                assert result[1] == 0, "num_mismatched nonzero: " + sql

    # assertion used in tests
    def assert_table_does_not_exist(self, table, schema=None, database=None):
        columns = self.get_table_columns(table, schema, database)
        assert len(columns) == 0

    # assertion used in tests
    def assert_table_does_exist(self, table, schema=None, database=None):
        columns = self.get_table_columns(table, schema, database)

        assert len(columns) > 0

    # called by assert_tables_equal
    def _assert_table_columns_equal(self, relation_a, relation_b):
        table_a_result = self.get_relation_columns(relation_a)
        table_b_result = self.get_relation_columns(relation_b)

        assert len(table_a_result) == len(table_b_result)

        for a_column, b_column in zip(table_a_result, table_b_result):
            a_name, a_type, a_size = a_column
            b_name, b_type, b_size = b_column
            assert a_name == b_name, "{} vs {}: column '{}' != '{}'".format(
                relation_a, relation_b, a_name, b_name
            )

            assert a_type == b_type, "{} vs {}: column '{}' has type '{}' != '{}'".format(
                relation_a, relation_b, a_name, a_type, b_type
            )

            assert a_size == b_size, "{} vs {}: column '{}' has size '{}' != '{}'".format(
                relation_a, relation_b, a_name, a_size, b_size
            )

    def get_relation_columns(self, relation):
        with self.get_connection():
            columns = self.adapter.get_columns_in_relation(relation)
        return sorted(((c.name, c.dtype, c.char_size) for c in columns), key=lambda x: x[0])

    def get_table_columns(self, table, schema=None, database=None):
        schema = self.unique_schema if schema is None else schema
        database = self.default_database if database is None else database
        relation = self.adapter.Relation.create(
            database=database,
            schema=schema,
            identifier=table,
            type="table",
            quote_policy=self.quoting,
        )
        return self.get_relation_columns(relation)

    # called by assert_many_table_equal
    def get_table_columns_as_dict(self, tables, schema=None):
        col_matrix = self.get_many_table_columns(tables, schema)
        res = {}
        for row in col_matrix:
            table_name = row[0]
            col_def = row[1:]
            if table_name not in res:
                res[table_name] = []
            res[table_name].append(col_def)
        return res

    # override for presto
    @property
    def column_schema(self):
        return "table_name, column_name, data_type, character_maximum_length"

    # This should be overridden for Snowflake. Called by get_many_table_columns.
    def get_many_table_columns_information_schema(self, tables, schema, database=None):
        columns = self.column_schema

        sql = """
                select {columns}
                from {db_string}information_schema.columns
                where {schema_filter}
                  and ({table_filter})
                order by column_name asc"""

        db_string = ""
        if database:
            db_string = self.quote_as_configured(database, "database") + "."

        table_filters_s = " OR ".join(
            _ilike("table_name", table.replace('"', "")) for table in tables
        )
        schema_filter = _ilike("table_schema", schema)

        sql = sql.format(
            columns=columns,
            schema_filter=schema_filter,
            table_filter=table_filters_s,
            db_string=db_string,
        )

        columns = self.run_sql(sql, fetch="all")
        return list(map(self.filter_many_columns, columns))

    # Snowflake needs a static char_size
    def filter_many_columns(self, column):
        if len(column) == 3:
            table_name, column_name, data_type = column
            char_size = None
        else:
            table_name, column_name, data_type, char_size = column
        return (table_name, column_name, data_type, char_size)

    @contextmanager
    def get_connection(self, name="_test"):
        """Create a test connection context where all executed macros, etc will
        use the adapter created in the schema fixture.
        This allows tests to run normal adapter macros as if reset_adapters()
        were not called by handle_and_check (for asserts, etc)
        """
        with patch.object(providers, "get_adapter", return_value=self.adapter):
            with self.adapter.connection_named(name):
                conn = self.adapter.connections.get_thread_connection()
                yield conn

    def _make_relation(self, identifier, schema=None, database=None):
        if schema is None:
            schema = self.unique_schema
        if database is None:
            database = self.default_database
        return self.adapter.Relation.create(
            database=database, schema=schema, identifier=identifier, quote_policy=self.quoting
        )

    # called by get_many_relation_columns
    def get_many_table_columns(self, tables, schema, database=None):
        result = self.get_many_table_columns_information_schema(tables, schema, database)
        result.sort(key=lambda x: "{}.{}".format(x[0], x[1]))
        return result

    # called by assert_many_relations_equal
    def get_many_relation_columns(self, relations):
        """Returns a dict of (datbase, schema) -> (dict of (table_name -> list of columns))"""
        schema_fqns = {}
        for rel in relations:
            this_schema = schema_fqns.setdefault((rel.database, rel.schema), [])
            this_schema.append(rel.identifier)

        column_specs = {}
        for key, tables in schema_fqns.items():
            database, schema = key
            columns = self.get_many_table_columns(tables, schema, database=database)
            table_columns = {}
            for col in columns:
                table_columns.setdefault(col[0], []).append(col[1:])
            for rel_name, columns in table_columns.items():
                key = (database, schema, rel_name)
                column_specs[key] = columns

        return column_specs

    def _assert_tables_equal_sql(self, relation_a, relation_b, columns=None):
        if columns is None:
            columns = self.get_relation_columns(relation_a)
        column_names = [c[0] for c in columns]
        sql = self.adapter.get_rows_different_sql(relation_a, relation_b, column_names)
        return sql

    # This duplicates code in the TestProjInfo class.
    def run_sql(self, sql, fetch=None):
        if sql.strip() == "":
            return
        # substitute schema and database in sql
        adapter = self.adapter
        kwargs = {
            "schema": self.unique_schema,
            "database": adapter.quote(self.default_database),
        }
        sql = sql.format(**kwargs)

        with self.get_connection("__test") as conn:
            msg = f'test connection "{conn.name}" executing: {sql}'
            fire_event(IntegrationTestDebug(msg=msg))
            with conn.handle.cursor() as cursor:
                try:
                    cursor.execute(sql)
                    conn.handle.commit()
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

    def get_tables_in_schema(self):
        sql = """
                select table_name,
                        case when table_type = 'BASE TABLE' then 'table'
                             when table_type = 'VIEW' then 'view'
                             else table_type
                        end as materialization
                from information_schema.tables
                where {}
                order by table_name
                """

        sql = sql.format(_ilike("table_schema", self.unique_schema))
        result = self.run_sql(sql, fetch="all")

        return {model_name: materialization for (model_name, materialization) in result}


# needs overriding for presto
def _ilike(target, value):
    return "{} ilike '{}'".format(target, value)
