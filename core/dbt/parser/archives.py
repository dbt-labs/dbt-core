from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.contracts.graph.parsed import ParsedArchiveNode
from dbt.node_types import NodeType
from dbt.parser.base import MacrosKnownParser
from dbt.parser.base_sql import BaseSqlParser, SQLParseResult
from dbt.adapters.factory import get_adapter
import dbt.clients.jinja
import dbt.exceptions
import dbt.utils

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
                source_database = archive_config.get(
                    'source_database',
                    config.credentials.database
                )
                cfg['target_database'] = archive_config.get(
                    'target_database',
                    config.credentials.database
                )

                source_schema = archive_config['source_schema']
                cfg['target_schema'] = archive_config.get('target_schema')

                fake_path = [cfg['target_database'], cfg['target_schema'],
                             cfg['target_table']]

                relation = get_adapter(config).Relation.create(
                    database=source_database,
                    schema=source_schema,
                    identifier=table['source_table'],
                    type='table'
                )

                raw_sql = '{{ config(materialized="archive") }}' + \
                          'select * from {!s}'.format(relation)

                archives.append({
                    'name': table.get('target_table'),
                    'root_path': config.project_root,
                    'resource_type': NodeType.Archive,
                    'path': os.path.join('archive', *fake_path),
                    'original_file_path': 'dbt_project.yml',
                    'package_name': config.project_name,
                    'config': cfg,
                    'raw_sql': raw_sql
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


class ArchiveBlockParser(BaseSqlParser):
    def parse_archives_from_file(self, file_node, tags=None):
        # the file node has a 'raw_sql' field that contains the jinja data with
        # (we hope!) `archive` blocks
        try:
            blocks = dbt.clients.jinja.extract_toplevel_blocks(
                file_node['raw_sql']
            )
        except dbt.exceptions.CompilationException as exc:
            if exc.node is None:
                exc.node = file_node
            raise
        for block in blocks:
            if block.block_type_name != NodeType.Archive:
                # non-archive blocks are just ignored
                continue
            name = block.block_name
            raw_sql = block.contents
            updates = {
                'raw_sql': raw_sql,
                'name': name,
            }
            yield dbt.utils.deep_merge(file_node, updates)

    @classmethod
    def get_compiled_path(cls, name, relative_path):
        return relative_path

    @classmethod
    def get_fqn(cls, node, package_project_config, extra=[]):
        parts = dbt.utils.split_path(node.path)
        fqn = [package_project_config.project_name]
        fqn.extend(parts[:-1])
        fqn.extend(extra)
        fqn.append(node.name)

        return fqn

    @staticmethod
    def validate_archives(node):
        if node.resource_type == NodeType.Archive:
            try:
                return ParsedArchiveNode(**node.to_shallow_dict())
            except dbt.exceptions.JSONValidationException as exc:
                raise dbt.exceptions.CompilationException(str(exc), node)
        else:
            return node

    def parse_sql_nodes(self, nodes, tags=None):
        if tags is None:
            tags = []

        results = SQLParseResult()

        # in archives, we have stuff in blocks.
        for file_node in nodes:
            archive_nodes = list(
                self.parse_archives_from_file(file_node, tags=tags)
            )
            found = super(ArchiveBlockParser, self).parse_sql_nodes(
                nodes=archive_nodes, tags=tags
            )
            # make sure our blocks are going to work when we try to archive
            # them!
            found.parsed = {k: self.validate_archives(v) for
                            k, v in found.parsed.items()}

            results.update(found)
        return results
