import json
import os

from dbt.logger import GLOBAL_LOGGER as logger
from dbt.adapters.factory import get_adapter
from dbt.clients.system import write_file
from dbt.compat import bigint
import dbt.ui.printer

from dbt.task.base_task import BaseTask


def get_stripped_prefix(source, prefix):
    """Go through source, extracting every key/value pair where the key starts
    with the given prefix.
    """
    cut = len(prefix)
    return {
        k[cut:]: v for k, v in source.items()
        if k.startswith(prefix)
    }


def unflatten(columns):
    """Given a list of column dictionaries following this layout:

        [{
            'column_comment': None,
            'column_index': Decimal('1'),
            'column_name': 'id',
            'column_type': 'integer',
            'table_comment': None,
            'table_name': 'test_table,
            'table_schema': 'test_schema',
            'table_type': 'BASE TABLE'
        }]

    unflatten will convert them into a dict with this nested structure:

    {
        'test_schema': {
            'test_table': {
                'metadata': {
                    'comment': None,
                    'name': 'table',
                    'type': 'BASE_TABLE'
                    'schema': 'test_schema',
                }
                'columns': [
                    {
                        'type': 'integer',
                        'comment': None,
                        'index': 1,
                    }
                ]
            }
        }
    }

    Note: the docstring for DefaultAdapter.get_catalog_for_schemas discusses
    what keys are guaranteed to exist. This method makes use of those keys.

    Keys prefixed with 'column_' end up in per-column data and keys prefixed
    with 'table_' end up in table metadata. Keys without either prefix are
    ignored.
    """
    structured = {}
    for entry in columns:
        schema_name = entry['table_schema']
        table_name = entry['table_name']

        if schema_name not in structured:
            structured[schema_name] = {}
        schema = structured[schema_name]

        if table_name not in schema:
            metadata = get_stripped_prefix(entry, 'table_')
            metadata.pop('schema')
            schema[table_name] = {'metadata': metadata, 'columns': []}
        table = schema[table_name]

        column = get_stripped_prefix(entry, 'column_')
        # the index should really never be that big so it's ok to end up
        # serializing this to JSON (2^53 is the max safe value there)
        column['index'] = bigint(column['index'])
        table['columns'].append(column)
    return structured


# derive from BaseTask as I don't really want any result interpretation.
class GenerateTask(BaseTask):
    def run(self):
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        # To get a list of schemas, it looks like we'd need to have the
        # compiled project and use node_runners.BaseRunner.get_model_schemas.
        # But I think we don't really want to compile, here, right? Or maybe
        # we do and I need to add all of that? But then we probably need to
        # go through the whole BaseRunner.safe_run path which makes things
        # more complex - need to figure out how to handle all the
        # is_ephemeral_model stuff, etc.
        # TODO: talk to connor/drew about this question.
        try:
            columns = adapter.get_catalog_for_schemas(profile, schemas=None)
            adapter.release_connection(profile)
        finally:
            adapter.cleanup_connections()

        results = unflatten(columns)

        path = os.path.join(self.project['target-path'], 'catalog.json')
        write_file(path, json.dumps(results))

        dbt.ui.printer.print_timestamped_line(
            'Catalog written to {}'.format(os.path.abspath(path))
        )

        return results
