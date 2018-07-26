import dbt.exceptions
from dbt.node_types import NodeType
from dbt.parser.base import BaseParser
from dbt.contracts.graph.unparsed import UnparsedDocumentation
from dbt.contracts.graph.parsed import ParsedDocumentation

import jinja2.runtime
import os

class DocumentationParser(BaseParser):
    @classmethod
    def load(cls, package_name, root_dir, relative_dirs):
        """Load and parse documentation in a lsit of projects. Returns a list
        of ParsedNodes.
        """
        extension = "[!.#~]*.md"

        file_matches = dbt.clients.system.find_matching(
            root_dir,
            relative_dirs,
            extension)

        result = []

        for file_match in file_matches:
            file_contents = dbt.clients.system.load_file_contents(
                file_match.get('absolute_path'), strip=False)

            parts = dbt.utils.split_path(file_match.get('relative_path', ''))
            name, _ = os.path.splitext(parts[-1])

            # TODO: this is probably wrong
            path = file_match.get('relative_path')
            original_file_path = os.path.join(
                file_match.get('searched_path'),
                path)

            result.append(UnparsedDocumentation(
                name=name,
                root_path=root_dir,
                resource_type=NodeType.Documentation,
                path=path,
                original_file_path=original_file_path,
                package_name=package_name,
                file_contents=file_contents
            ))

        return result

    @classmethod
    def parse_file(cls, all_projects, root_project_config, unparsed_docs):
        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        context = {}
        try:
            template = dbt.clients.jinja.get_template(unparsed_docs.file_contents, {})
        except dbt.exceptions.CompilationException as e:
            e.node = unparsed_docs
            raise

        profile = dbt.utils.get_profile_from_project(root_project_config)
        schema = profile.get('schema', 'public')

        for key, item in template.module.__dict__.items():
            if type(item) != jinja2.runtime.Macro:
                continue

            if not key.startswith(dbt.utils.DOCS_PREFIX):
                continue

            name = key.replace(dbt.utils.DOCS_PREFIX, '')

            unique_id = cls.get_path(NodeType.Documentation,
                                     unparsed_docs.package_name,
                                     name)
            fqn = cls.get_fqn(unparsed_docs.path,
                              all_projects[unparsed_docs.package_name])

            merged = dbt.utils.deep_merge(
                unparsed_docs.serialize(),
                {
                    'name': name,
                    'unique_id': unique_id,
                }
            )
            yield ParsedDocumentation(**merged)

    @classmethod
    def parse(cls, nodes):
        to_return = {}
        for node in nodes:
            node_path = cls.get_path(node.resource_type, node.package_name,
                                     node.name)
            parsed = cls.parse_docs(node_path, node)
            if node_path in to_return:
                dbt.exceptions.raise_duplicate_resource_name(
                        to_return[node_path], node_parsed)





