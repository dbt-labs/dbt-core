import os

import dbt.flags
import dbt.clients.agate_helper
import dbt.clients.system
import dbt.context.parser
import dbt.contracts.project
import dbt.exceptions

from dbt.node_types import NodeType
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.graph.unparsed import UnparsedNode
from dbt.parser.base import BaseParser


class SeedParser(BaseParser):
    @classmethod
    def parse_seed_file(cls, file_match, root_dir, package_name):
        """Parse the given seed file, returning an UnparsedNode and the agate
        table.
        """
        abspath = file_match['absolute_path']
        logger.debug("Parsing {}".format(abspath))
        table_name = os.path.basename(abspath)[:-4]
        node = UnparsedNode(
            path=file_match['relative_path'],
            name=table_name,
            root_path=root_dir,
            resource_type=NodeType.Seed,
            # Give this raw_sql so it conforms to the node spec,
            # use dummy text so it doesn't look like an empty node
            raw_sql='-- csv --',
            package_name=package_name,
            original_file_path=os.path.join(file_match.get('searched_path'),
                                            file_match.get('relative_path')),
        )
        try:
            table = dbt.clients.agate_helper.from_csv(abspath)
        except ValueError as e:
            dbt.exceptions.raise_compiler_error(str(e), node)
        table.original_abspath = abspath
        return node, table

    @classmethod
    def load_and_parse(cls, package_name, root_project, all_projects, root_dir,
                       relative_dirs, tags=None, macros=None):
        """Load and parse seed files in a list of directories. Returns a dict
           that maps unique ids onto ParsedNodes"""

        extension = "[!.#~]*.csv"
        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = {}
        for file_match in file_matches:
            node, agate_table = cls.parse_seed_file(file_match, root_dir,
                                                    package_name)
            node_path = cls.get_path(NodeType.Seed, package_name, node.name)
            parsed = cls.parse_node(node, node_path, root_project,
                                    all_projects.get(package_name),
                                    all_projects, tags=tags, macros=macros,
                                    agate_table=agate_table)
            result[node_path] = parsed

        return result
