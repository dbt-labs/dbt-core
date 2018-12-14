
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.node_types import NodeType
from dbt.parser.base import MacrosKnownParser

import os


class ArchiveParser(MacrosKnownParser):
    @classmethod
    def parse_archives_from_project(cls, config):
        archives = []
        archive_configs = config.archive

        for archive_config in archive_configs:
            tables = archive_config.get('tables')

            if tables is None:
                continue

            for table in tables:
                cfg = table.copy()
                cfg['source_schema'] = archive_config.get('source_schema')
                cfg['target_schema'] = archive_config.get('target_schema')

                fake_path = [cfg['target_schema'], cfg['target_table']]
                archives.append({
                    'name': table.get('target_table'),
                    'root_path': config.project_root,
                    'resource_type': NodeType.Archive,
                    'path': os.path.join('archive', *fake_path),
                    'original_file_path': 'dbt_project.yml',
                    'package_name': config.project_name,
                    'config': cfg,
                    'raw_sql': '{{config(materialized="archive")}} -- noop'
                })

        return archives

    def load_and_parse(self):
        """Load and parse archives in a list of projects. Returns a dict
           that maps unique ids onto ParsedNodes"""

        archives = []
        to_return = {}

        for name, project in self.all_projects.items():
            archives = archives + self.parse_archives_from_project(project)

        # We're going to have a similar issue with parsed nodes, if we want to
        # make parse_node return those.
        for a in archives:
            # archives have a config, but that would make for an invalid
            # UnparsedNode, so remove it and pass it along to parse_node as an
            # argument.
            archive_config = a.pop('config')
            archive = UnparsedNode(**a)
            node_path = self.get_path(archive.resource_type,
                                      archive.package_name,
                                      archive.name)

            to_return[node_path] = self.parse_node(
                archive,
                node_path,
                self.all_projects.get(archive.package_name),
                archive_config=archive_config)

        return to_return
