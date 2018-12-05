
import os

import dbt.contracts.project
import dbt.exceptions
import dbt.clients.system
import dbt.utils
import dbt.flags

from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.parser.base import MacrosKnownParser


class BaseSqlParser(MacrosKnownParser):
    @classmethod
    def get_compiled_path(cls, name, relative_path):
        raise dbt.exceptions.NotImplementedException("Not implemented")

    def load_and_parse(self, package_name, root_dir, relative_dirs,
                       resource_type, tags=None):
        """Load and parse models in a list of directories. Returns a dict
           that maps unique ids onto ParsedNodes"""

        extension = "[!.#~]*.sql"

        if tags is None:
            tags = []

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**self.all_projects)

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = []

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'))

            parts = dbt.utils.split_path(file_match.get('relative_path', ''))
            name, _ = os.path.splitext(parts[-1])

            path = self.get_compiled_path(name,
                                          file_match.get('relative_path'))

            original_file_path = os.path.join(
                file_match.get('searched_path'),
                path)

            result.append({
                'name': name,
                'root_path': root_dir,
                'resource_type': resource_type,
                'path': path,
                'original_file_path': original_file_path,
                'package_name': package_name,
                'raw_sql': file_contents
            })

        return self.parse_sql_nodes(result, tags)

    def parse_sql_nodes(self, nodes, tags=None):

        if tags is None:
            tags = []

        to_return = {}
        disabled = []

        for n in nodes:
            node = UnparsedNode(**n)
            package_name = node.package_name

            node_path = self.get_path(node.resource_type,
                                      package_name,
                                      node.name)

            project = self.all_projects.get(package_name)
            node_parsed = self.parse_node(node, node_path, project, tags=tags)

            # Ignore disabled nodes
            if not node_parsed['config']['enabled']:
                disabled.append(node_parsed)
                continue

            # Check for duplicate model names
            existing_node = to_return.get(node_path)
            if existing_node is not None:
                dbt.exceptions.raise_duplicate_resource_name(
                        existing_node, node_parsed)

            to_return[node_path] = node_parsed

        return to_return, disabled
