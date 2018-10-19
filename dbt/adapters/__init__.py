from importlib import import_module

# each of the adapter modules registers itself during its package __init__, so
# we want to import them here to trigger that.

for adapter in ('bigquery', 'postgres', 'redshift', 'snowflake'):
    try:
        import_module('.'+adapter, 'dbt.adapters')
    except ImportError:
        pass
