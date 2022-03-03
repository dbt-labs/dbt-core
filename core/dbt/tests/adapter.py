from itertools import chain, repeat
from dbt.context import providers
from unittest.mock import patch

# These functions were extracted from the dbt-adapter-tests spec_file.py.
# They are used in the 'adapter' tests directory. At some point they
# might be moved to dbts.tests.util if they are of general purpose use,
# but leaving here for now to keep the adapter work more contained.
# We may want to consolidate in the future since some of this is kind
# of duplicative of the functionality in dbt.tests.tables.


class TestProcessingException(Exception):
    pass


def relation_from_name(adapter, name: str):
    """reverse-engineer a relation (including quoting) from a given name and
    the adapter. Assumes that relations are split by the '.' character.
    """

    # Different adapters have different Relation classes
    cls = adapter.Relation
    credentials = adapter.config.credentials
    quote_policy = cls.get_default_quote_policy().to_dict()
    include_policy = cls.get_default_include_policy().to_dict()
    kwargs = {}  # This will contain database, schema, identifier

    parts = name.split(".")
    names = ["database", "schema", "identifier"]
    defaults = [credentials.database, credentials.schema, None]
    values = chain(repeat(None, 3 - len(parts)), parts)
    for name, value, default in zip(names, values, defaults):
        # no quote policy -> use the default
        if value is None:
            if default is None:
                include_policy[name] = False
            value = default
        else:
            include_policy[name] = True
            # if we have a value, we can figure out the quote policy.
            trimmed = value[1:-1]
            if adapter.quote(trimmed) == value:
                quote_policy[name] = True
                value = trimmed
            else:
                quote_policy[name] = False
        kwargs[name] = value

    relation = cls.create(
        include_policy=include_policy,
        quote_policy=quote_policy,
        **kwargs,
    )
    return relation


def check_relation_types(adapter, relation_to_type):
    """
    Relation name to table/view
    {
        "base": "table",
        "other": "view",
    }
    """

    expected_relation_values = {}
    found_relations = []
    schemas = set()

    for key, value in relation_to_type.items():
        relation = relation_from_name(adapter, key)
        expected_relation_values[relation] = value
        schemas.add(relation.without_identifier())

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("__test"):
            for schema in schemas:
                found_relations.extend(adapter.list_relations_without_caching(schema))

    for key, value in relation_to_type.items():
        for relation in found_relations:
            # this might be too broad
            if relation.identifier == key:
                assert relation.type == value, (
                    f"Got an unexpected relation type of {relation.type} "
                    f"for relation {key}, expected {value}"
                )


def check_relations_equal(adapter, relation_names):
    if len(relation_names) < 2:
        raise TestProcessingException(
            "Not enough relations to compare",
        )
    relations = [relation_from_name(adapter, name) for name in relation_names]

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("_test"):
            basis, compares = relations[0], relations[1:]
            columns = [c.name for c in adapter.get_columns_in_relation(basis)]

            for relation in compares:
                sql = adapter.get_rows_different_sql(basis, relation, column_names=columns)
                _, tbl = adapter.execute(sql, fetch=True)
                num_rows = len(tbl)
                assert (
                    num_rows == 1
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of rows ({num_rows})"
                num_cols = len(tbl[0])
                assert (
                    num_cols == 2
                ), f"Invalid sql query from get_rows_different_sql: incorrect number of cols ({num_cols})"
                row_count_difference = tbl[0][0]
                assert (
                    row_count_difference == 0
                ), f"Got {row_count_difference} difference in row count betwen {basis} and {relation}"
                rows_mismatched = tbl[0][1]
                assert (
                    rows_mismatched == 0
                ), f"Got {rows_mismatched} different rows between {basis} and {relation}"


def get_unique_ids_in_results(results):
    unique_ids = []
    for result in results:
        unique_ids.append(result.node.unique_id)
    return unique_ids


def check_result_nodes_by_name(results, names):
    result_names = []
    for result in results:
        result_names.append(result.node.name)
    assert set(names) == set(result_names)


def check_result_nodes_by_unique_id(results, unique_ids):
    result_unique_ids = []
    for result in results:
        result_unique_ids.append(result.node.unique_id)
    assert set(unique_ids) == set(result_unique_ids)


def update_rows(adapter, update_rows_config):
    """
    {
      "name": "base",
      "dst_col": "some_date"
      "clause": {
          "type": "add_timestamp",
          "src_col": "some_date",
       "where" "id > 10"
    }
    """
    for key in ["name", "dst_col", "clause"]:
        if key not in update_rows_config:
            raise TestProcessingException(f"Invalid update_rows: no {key}")

    clause = update_rows_config["clause"]
    clause = generate_update_clause(adapter, clause)

    where = None
    if "where" in update_rows_config:
        where = update_rows_config["where"]

    name = update_rows_config["name"]
    dst_col = update_rows_config["dst_col"]
    relation = relation_from_name(adapter, name)

    with patch.object(providers, "get_adapter", return_value=adapter):
        with adapter.connection_named("_test"):
            sql = adapter.update_column_sql(
                dst_name=str(relation),
                dst_column=dst_col,
                clause=clause,
                where_clause=where,
            )
            print(f"--- update_rows sql: {sql}")
            adapter.execute(sql, auto_begin=True)
            adapter.commit_if_has_connection()


def generate_update_clause(adapter, clause) -> str:
    """
    Called by update_rows function. Expects the "clause" dictionary
    documented in 'update_rows.
    """

    if "type" not in clause or clause["type"] not in ["add_timestamp", "add_string"]:
        raise TestProcessingException("invalid update_rows clause: type missing or incorrect")
    clause_type = clause["type"]

    if clause_type == "add_timestamp":
        if "src_col" not in clause:
            raise TestProcessingException("Invalid update_rows clause: no src_col")
        add_to = clause["src_col"]
        kwargs = {k: v for k, v in clause.items() if k in ("interval", "number")}
        with patch.object(providers, "get_adapter", return_value=adapter):
            with adapter.connection_named("_test"):
                return adapter.timestamp_add_sql(add_to=add_to, **kwargs)
    elif clause_type == "add_string":
        for key in ["src_col", "value"]:
            if key not in clause:
                raise TestProcessingException(f"Invalid update_rows clause: no {key}")
        src_col = clause["src_col"]
        value = clause["value"]
        location = clause.get("location", "append")
        with patch.object(providers, "get_adapter", return_value=adapter):
            with adapter.connection_named("_test"):
                return adapter.string_add_sql(src_col, value, location)
    return ""
