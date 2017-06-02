import dbt.adapters.postgres as postgres


def reset():
    postgres.connection_cache = {}
