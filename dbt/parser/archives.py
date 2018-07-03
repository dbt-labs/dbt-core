
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser

import os


class ArchiveParser(BaseParser):
    @classmethod
    def parse_archives_from_project(cls, project):
        archives = []
        archive_configs = project.get('archive', [])

        for archive_config in archive_configs:
            tables = archive_config.get('tables')

            if tables is None:
                continue

            for table in tables:
                config = table.copy()
                config['source_schema'] = archive_config.get('source_schema')
                config['target_schema'] = archive_config.get('target_schema')

                fake_path = [config['target_schema'], config['target_table']]
                archives.append({
                    'name': table.get('target_table'),
                    'root_path': project.get('project-root'),
                    'resource_type': NodeType.Archive,
                    'path': os.path.join('archive', *fake_path),
                    'original_file_path': 'dbt_project.yml',
                    'package_name': project.get('name'),
                    'config': config,
                    'raw_sql': '{{config(materialized="archive")}} -- noop'
                })

        return archives

    @classmethod
    def load_and_parse(cls, root_project, all_projects, macros=None):
        """Load and parse archives in a list of projects. Returns a dict
           that maps unique ids onto ParsedNodes"""

        archives = []
        to_return = {}

        for name, project in all_projects.items():
            archives = archives + cls.parse_archives_from_project(project)

        # We're going to have a similar issue with parsed nodes, if we want to
        # make parse_node return those.
        for a in archives:
            # archives have a config, but that would make for an invalid
            # UnparsedNode, so remove it and pass it along to parse_node as an
            # argument.
            archive_config = a.pop('config')
            archive = UnparsedNode(**a)
            node_path = cls.get_path(archive.get('resource_type'),
                                     archive.get('package_name'),
                                     archive.get('name'))

            to_return[node_path] = cls.parse_node(
                archive,
                node_path,
                root_project,
                all_projects.get(archive.get('package_name')),
                all_projects,
                macros=macros,
                archive_config=archive_config)

        return to_return
