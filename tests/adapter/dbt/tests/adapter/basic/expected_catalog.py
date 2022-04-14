def no_stats():
    return {
        "has_stats": {
            "id": "has_stats",
            "label": "Has Stats?",
            "value": False,
            "description": "Indicates whether there are statistics for this table",
            "include": False,
        },
    }


def base_expected_catalog(
    project,
    id_type,
    text_type,
    time_type,
    view_type,
    table_type,
    model_stats,
    seed_stats=None,
    case=None,
    case_columns=False,
    model_database=None,
):

    if case is None:

        def case(x):
            return x

    col_case = case if case_columns else lambda x: x

    if seed_stats is None:
        seed_stats = model_stats

    if model_database is None:
        model_database = project.database
    my_schema_name = project.test_schema
    role = "root"
    alternate_schema = project.test_schema + "_test"

    expected_cols = {
        col_case("id"): {
            "name": col_case("id"),
            "index": 1,
            "type": id_type,
            "comment": None,
        },
        col_case("first_name"): {
            "name": col_case("first_name"),
            "index": 2,
            "type": text_type,
            "comment": None,
        },
        col_case("email"): {
            "name": col_case("email"),
            "index": 3,
            "type": text_type,
            "comment": None,
        },
        col_case("ip_address"): {
            "name": col_case("ip_address"),
            "index": 4,
            "type": text_type,
            "comment": None,
        },
        col_case("updated_at"): {
            "name": col_case("updated_at"),
            "index": 5,
            "type": time_type,
            "comment": None,
        },
    }
    return {
        "nodes": {
            "model.test.model": {
                "unique_id": "model.test.model",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": case("model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "model.test.second_model": {
                "unique_id": "model.test.second_model",
                "metadata": {
                    "schema": alternate_schema,
                    "database": project.database,
                    "name": case("second_model"),
                    "type": view_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": model_stats,
                "columns": expected_cols,
            },
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": case("seed"),
                    "type": table_type,
                    "comment": None,
                    "owner": role,
                },
                "stats": seed_stats,
                "columns": expected_cols,
            },
        },
    }


def expected_references_catalog(project):
    model_database = project.database
    my_schema_name = project.test_schema
    role = "root"
    stats = no_stats()
    summary_columns = {
        "first_name": {
            "name": "first_name",
            "index": 1,
            "type": "text",
            "comment": None,
        },
        "ct": {
            "name": "ct",
            "index": 2,
            "type": "bigint",
            "comment": None,
        },
    }

    seed_columns = {
        "id": {
            "name": "id",
            "index": 1,
            "type": "integer",
            "comment": None,
        },
        "first_name": {
            "name": "first_name",
            "index": 2,
            "type": "text",
            "comment": None,
        },
        "email": {
            "name": "email",
            "index": 3,
            "type": "text",
            "comment": None,
        },
        "ip_address": {
            "name": "ip_address",
            "index": 4,
            "type": "text",
            "comment": None,
        },
        "updated_at": {
            "name": "updated_at",
            "index": 5,
            "type": "timestamp without time zone",
            "comment": None,
        },
    }
    return {
        "nodes": {
            "seed.test.seed": {
                "unique_id": "seed.test.seed",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": "seed",
                    "type": "BASE TABLE",
                    "comment": None,
                    "owner": role,
                },
                "stats": stats,
                "columns": seed_columns,
            },
            "model.test.ephemeral_summary": {
                "unique_id": "model.test.ephemeral_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": "ephemeral_summary",
                    "type": "BASE TABLE",
                    "comment": None,
                    "owner": role,
                },
                "stats": stats,
                "columns": summary_columns,
            },
            "model.test.view_summary": {
                "unique_id": "model.test.view_summary",
                "metadata": {
                    "schema": my_schema_name,
                    "database": model_database,
                    "name": "view_summary",
                    "type": "VIEW",
                    "comment": None,
                    "owner": role,
                },
                "stats": stats,
                "columns": summary_columns,
            },
        },
        "sources": {
            "source.test.my_source.my_table": {
                "unique_id": "source.test.my_source.my_table",
                "metadata": {
                    "schema": my_schema_name,
                    "database": project.database,
                    "name": "seed",
                    "type": "BASE TABLE",
                    "comment": None,
                    "owner": role,
                },
                "stats": stats,
                "columns": seed_columns,
            },
        },
    }
