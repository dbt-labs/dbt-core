import os

import jinja2.runtime

import dbt.exceptions
import dbt.flags
import dbt.utils

import dbt.clients.jinja
import dbt.clients.system
import dbt.contracts.project

from dbt.parser.base import BaseParser
from dbt.node_types import NodeType
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.graph.unparsed import UnparsedMacro
from dbt.contracts.graph.parsed import ParsedMacro


class MacroParser(BaseParser):
    def parse_macro_file(self, macro_file_path, macro_file_contents, root_path,
                         package_name, resource_type, tags=None, context=None):

        logger.debug("Parsing {}".format(macro_file_path))

        to_return = {}

        if tags is None:
            tags = []

        context = {}

        # change these to actual kwargs
        base_node = UnparsedMacro(
            path=macro_file_path,
            original_file_path=macro_file_path,
            package_name=package_name,
            raw_sql=macro_file_contents,
            root_path=root_path,
        )

        try:
            ast = dbt.clients.jinja.parse(macro_file_contents)
        except dbt.exceptions.CompilationException as e:
            e.node = base_node
            raise e

        for macro_node in ast.find_all(jinja2.nodes.Macro):
            macro_name = macro_node.name

            node_type = None
            if macro_name.startswith(dbt.utils.MACRO_PREFIX):
                node_type = NodeType.Macro
                name = macro_name.replace(dbt.utils.MACRO_PREFIX, '')

            if node_type != resource_type:
                continue

            unique_id = self.get_path(resource_type, package_name, name)

            merged = dbt.utils.deep_merge(
                base_node.serialize(),
                {
                    'name': name,
                    'unique_id': unique_id,
                    'tags': tags,
                    'resource_type': resource_type,
                    'depends_on': {'macros': []},
                })

            new_node = ParsedMacro(**merged)

            to_return[unique_id] = new_node

        return to_return

    def load_and_parse(self, package_name, root_dir, relative_dirs,
                       resource_type, tags=None):
        extension = "[!.#~]*.sql"

        if tags is None:
            tags = []

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**self.all_projects)

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = {}

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'))

            original_file_path = os.path.join(
                file_match.get('searched_path'),
                file_match.get('relative_path')
            )

            result.update(
                self.parse_macro_file(
                    original_file_path,
                    file_contents,
                    root_dir,
                    package_name,
                    resource_type))

        return result
