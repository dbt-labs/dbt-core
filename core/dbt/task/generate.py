import os
import shutil

from dbt.adapters.factory import get_adapter
from dbt.clients.system import write_json
from dbt.include.global_project import DOCS_INDEX_FILE_PATH
import dbt.ui.printer
import dbt.utils
import dbt.compilation
import dbt.exceptions

from dbt.task.compile import CompileTask


CATALOG_FILENAME = 'catalog.json'


def get_stripped_prefix(source, prefix):
    """Go through source, extracting every key/value pair where the key starts
    with the given prefix.
    """
    cut = len(prefix)
    return {
        k[cut:]: v for k, v in source.items()
        if k.startswith(prefix)
    }


def format_stats(stats):
    """Given a dictionary following this layout:

        {
            'encoded:label': 'Encoded',
            'encoded:value': 'Yes',
            'encoded:description': 'Indicates if the column is encoded',
            'encoded:include': True,

            'size:label': 'Size',
            'size:value': 128,
            'size:description': 'Size of the table in MB',
            'size:include': True,
        }

    format_stats will convert the dict into this structure:

        {
            'encoded': {
                'id': 'encoded',
                'label': 'Encoded',
                'value': 'Yes',
                'description': 'Indicates if the column is encoded',
                'include': True
            },
            'size': {
                'id': 'size',
                'label': 'Size',
                'value': 128,
                'description': 'Size of the table in MB',
                'include': True
            }
        }
    """
    stats_collector = {}
    for stat_key, stat_value in stats.items():
        stat_id, stat_field = stat_key.split(":")

        stats_collector.setdefault(stat_id, {"id": stat_id})
        stats_collector[stat_id][stat_field] = stat_value

    # strip out all the stats we don't want
    stats_collector = {
        stat_id: stats
        for stat_id, stats in stats_collector.items()
        if stats.get('include', False)
    }

    # we always have a 'has_stats' field, it's never included
    has_stats = {
        'id': 'has_stats',
        'label': 'Has Stats?',
        'value': len(stats_collector) > 0,
        'description': 'Indicates whether there are statistics for this table',
        'include': False,
    }
    stats_collector['has_stats'] = has_stats
    return stats_collector


def unflatten(columns):
    """Given a list of column dictionaries following this layout:

        [{
            'column_comment': None,
            'column_index': Decimal('1'),
            'column_name': 'id',
            'column_type': 'integer',
            'table_comment': None,
            'table_name': 'test_table',
            'table_schema': 'test_schema',
            'table_type': 'BASE TABLE'
        }]

    unflatten will convert them into a dict with this nested structure:

        {
            'test_schema': {
                'test_table': {
                    'metadata': {
                        'comment': None,
                        'name': 'test_table',
                        'type': 'BASE TABLE',
                        'schema': 'test_schema',
                    },
                    'columns': {
                        "id": {
                            'type': 'integer',
                            'comment': None,
                            'index': 1,
                            'name': 'id'
                        }
                    }
                }
            }
        }

    Required keys in each column: table_schema, table_name, column_index

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
            stats = get_stripped_prefix(entry, 'stats:')
            stats_dict = format_stats(stats)

            schema[table_name] = {
                'metadata': metadata,
                'stats': stats_dict,
                'columns': {}
            }

        table = schema[table_name]

        column = get_stripped_prefix(entry, 'column_')

        # the index should really never be that big so it's ok to end up
        # serializing this to JSON (2^53 is the max safe value there)
        column['index'] = int(column['index'])
        table['columns'][column['name']] = column
    return structured


def incorporate_catalog_unique_ids(catalog, manifest):
    nodes = {}

    for schema, tables in catalog.items():
        for table_name, table_def in tables.items():
            unique_ids = manifest.get_unique_ids_for_schema_and_table(
                schema, table_name)

            for unique_id in unique_ids:
                if unique_id in nodes:
                    dbt.exceptions.raise_ambiguous_catalog_match(
                        unique_id, nodes[unique_id], table_def)

                else:
                    table_def_copy = table_def.copy()
                    table_def_copy['unique_id'] = unique_id
                    nodes[unique_id] = table_def_copy

    return nodes


class GenerateTask(CompileTask):
    def _get_manifest(self):
        manifest = dbt.loader.GraphLoader.load_all(self.config)
        return manifest

    def run(self):
        compile_results = None
        if self.args.compile:
            compile_results = super().run()
            if any(r.error is not None for r in compile_results):
                dbt.ui.printer.print_timestamped_line(
                    'compile failed, cannot generate docs'
                )
                return {'compile_results': compile_results}

        shutil.copyfile(
            DOCS_INDEX_FILE_PATH,
            os.path.join(self.config.target_path, 'index.html'))

        adapter = get_adapter(self.config)
        with adapter.connection_named('generate_catalog'):
            manifest = self._get_manifest()

            dbt.ui.printer.print_timestamped_line("Building catalog")
            results = adapter.get_catalog(manifest)

        results = [
            dict(zip(results.column_names, row))
            for row in results
        ]

        nested_results = unflatten(results)
        results = {
            'nodes': incorporate_catalog_unique_ids(nested_results, manifest),
            'generated_at': dbt.utils.timestring(),
        }

        path = os.path.join(self.config.target_path, CATALOG_FILENAME)
        write_json(path, results)

        dbt.ui.printer.print_timestamped_line(
            'Catalog written to {}'.format(os.path.abspath(path))
        )
        # now that we've serialized the data we can add compile_results in to
        # make interpret_results happy.
        results['compile_results'] = compile_results

        return results

    def interpret_results(self, results):
        compile_results = results.get('compile_results')
        if compile_results is None:
            return True

        return super().interpret_results(compile_results)
