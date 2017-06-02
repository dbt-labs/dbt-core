

def reset():
    # TODO
    try:
        import dbt.adapters.postgres as postgres
        postgres.connection_cache = {}
    except ModuleNotFoundError:
        pass
