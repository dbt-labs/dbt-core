from dbt.adapters.postgres import PostgresAdapter


def get_adapter(target):
    adapters = {
        'postgres': PostgresAdapter,
        'redshift': PostgresAdapter,
    }

    return adapters[target.target_type]
