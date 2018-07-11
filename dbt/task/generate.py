import json
import os

from dbt.contracts.graph.parsed import ParsedManifest, ParsedNode, ParsedMacro
from dbt.adapters.factory import get_adapter
from dbt.clients.system import write_file
from dbt.compat import bigint
import dbt.ui.printer
import dbt.utils
import dbt.compilation

from dbt.task.base_task import BaseTask


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
                    'columns': [
                        {
                            'type': 'integer',
                            'comment': None,
                            'index': bigint(1),
                            'name': 'id'
                        }
                    ]
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
            schema[table_name] = {'metadata': metadata, 'columns': []}
        table = schema[table_name]

        column = get_stripped_prefix(entry, 'column_')
        # the index should really never be that big so it's ok to end up
        # serializing this to JSON (2^53 is the max safe value there)
        column['index'] = bigint(column['index'])
        table['columns'].append(column)
    return structured


class GenerateTask(BaseTask):
    def _get_manifest(self, project):
        compiler = dbt.compilation.Compiler(project)
        compiler.initialize()

        root_project = project.cfg
        all_projects = compiler.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(root_project, all_projects)
        return manifest

    def run(self):
        manifest = self._get_manifest(self.project)
        profile = self.project.run_environment()
        adapter = get_adapter(profile)

        dbt.ui.printer.print_timestamped_line("Building catalog")
        results = adapter.get_catalog(profile, self.project.cfg, manifest)

        results = [
            dict(zip(results.column_names, row))
            for row in results
        ]
        results = unflatten(results)

        path = os.path.join(self.project['target-path'], CATALOG_FILENAME)
        write_file(path, json.dumps(results))

        dbt.ui.printer.print_timestamped_line(
            'Catalog written to {}'.format(os.path.abspath(path))
        )

        return results
