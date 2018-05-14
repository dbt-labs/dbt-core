import json
import os

from dbt.adapters.factory import get_adapter
from dbt.clients.system import write_file
from dbt.compat import bigint
from dbt.include import GLOBAL_DBT_MODULES_PATH
from dbt.node_runners import OperationRunner
from dbt.node_types import NodeType
from dbt.runner import RunManager
import dbt.ui.printer

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
    def get_all_projects(self):
        root_project = self.project.cfg
        all_projects = {root_project.get('name'): root_project}
        # we only need to load the global deps. We haven't compiled, so our
        # project['module-path'] does not exist.
        dependency_projects = dbt.utils.dependencies_for_path(
            self.project, GLOBAL_DBT_MODULES_PATH
        )

        for project in dependency_projects:
            name = project.cfg.get('name', 'unknown')
            all_projects[name] = project.cfg

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        return all_projects

    def run(self):
        profile = self.project.run_environment()

        # From dbt/compilation.py
        root_project = self.project.cfg
        all_projects = self.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(root_project, all_projects)
        flat_graph = manifest.to_flat_graph()
        operation_name = 'get_catalog_data'
        operation = dbt.utils.get_operation(flat_graph, operation_name)

        adapter = get_adapter(profile)

        runner = OperationRunner(self.project, adapter, operation, 0, 0)
        run_result = runner.safe_run(flat_graph)
        agate_table = run_result.returned
        results = [
            dict(zip(agate_table.column_names, row))
            for row in agate_table
        ]
        results = unflatten(results)

        path = os.path.join(self.project['target-path'], CATALOG_FILENAME)
        write_file(path, json.dumps(results))

        dbt.ui.printer.print_timestamped_line(
            'Catalog written to {}'.format(os.path.abspath(path))
        )

        return results
