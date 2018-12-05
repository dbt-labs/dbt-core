import os

import dbt.exceptions
import dbt.flags
import dbt.include
import dbt.utils
import dbt.hooks
import dbt.clients.jinja
import dbt.context.parser

from dbt.utils import coalesce
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.graph.parsed import ParsedNode
from dbt.parser.source_config import SourceConfig
from dbt.node_types import NodeType


class BaseParser(object):
    def __init__(self, root_project_config, all_projects):
        self.root_project_config = root_project_config
        self.all_projects = all_projects

    @property
    def default_schema(self):
        return getattr(self.root_project_config.credentials, 'schema',
                       'public')

    def load_and_parse(self, *args, **kwargs):
        raise dbt.exceptions.NotImplementedException("Not implemented")

    @classmethod
    def get_path(cls, resource_type, package_name, resource_name):
        """Returns a unique identifier for a resource"""

        return "{}.{}.{}".format(resource_type, package_name, resource_name)

    @classmethod
    def get_fqn(cls, path, package_project_config, extra=[]):
        parts = dbt.utils.split_path(path)
        name, _ = os.path.splitext(parts[-1])
        fqn = ([package_project_config.project_name] +
               parts[:-1] +
               extra +
               [name])

        return fqn


class MacrosKnownParser(BaseParser):
    def __init__(self, root_project_config, all_projects, macro_manifest):
        super(MacrosKnownParser, self).__init__(
            root_project_config=root_project_config,
            all_projects=all_projects
        )
        self.macro_manifest = macro_manifest
        self._get_schema_func = None

    def get_schema_func(self):
        """The get_schema function is set by a few different things:
            - if there is a 'generate_schema_name' macro in the root project,
                it will be used.
            - if that does not exist but there is a 'generate_schema_name'
                macro in the 'dbt' internal project, that will be used
            - if neither of those exist (unit tests?), a function that returns
                the 'default schema' as set in the root project's 'credentials'
                is used

        """
        if self._get_schema_func is not None:
            return self._get_schema_func

        get_schema_macro = self.macro_manifest.find_macro_by_name(
            'generate_schema_name',
            self.root_project_config.project_name
        )
        if get_schema_macro is None:
            get_schema_macro = self.macro_manifest.find_macro_by_name(
                'generate_schema_name',
                dbt.include.GLOBAL_PROJECT_NAME
            )
        if get_schema_macro is None:
            def get_schema(_):
                return self.default_schema
        else:
            # use the macro itself as the 'parsed node' to pass into
            # parser.generate() to get a context.
            macro_node = get_schema_macro.incorporate(
                resource_type=NodeType.Operation
            )
            root_context = dbt.context.parser.generate(
                macro_node, self.root_project_config,
                self.macro_manifest, self.root_project_config
            )
            get_schema = get_schema_macro.generator(root_context)

        self._get_schema_func = get_schema
        return self._get_schema_func

    def parse_node(self, node, node_path, package_project_config, tags=None,
                   fqn_extra=None, fqn=None, agate_table=None,
                   archive_config=None, column_name=None):
        """Parse a node, given an UnparsedNode and any other required information.

        agate_table should be set if the node came from a seed file.
        archive_config should be set if the node is an Archive node.
        column_name should be set if the node is a Test node associated with a
        particular column.
        """
        logger.debug("Parsing {}".format(node_path))

        node = node.serialize()

        if agate_table is not None:
            node['agate_table'] = agate_table
        tags = coalesce(tags, [])
        fqn_extra = coalesce(fqn_extra, [])

        node.update({
            'refs': [],
            'depends_on': {
                'nodes': [],
                'macros': [],
            }
        })

        if fqn is None:
            fqn = self.get_fqn(node.get('path'), package_project_config,
                               fqn_extra)

        config = SourceConfig(
            self.root_project_config,
            package_project_config,
            fqn,
            node['resource_type'])

        node['unique_id'] = node_path
        node['empty'] = (
            'raw_sql' in node and len(node['raw_sql'].strip()) == 0
        )
        node['fqn'] = fqn
        node['tags'] = tags

        # Set this temporarily. Not the full config yet (as config() hasn't
        # been called from jinja yet). But the Var() call below needs info
        # about project level configs b/c they might contain refs.
        # TODO: Restructure this?
        config_dict = coalesce(archive_config, {})
        config_dict.update(config.config)
        node['config'] = config_dict

        # Set this temporarily so get_rendered() has access to a schema & alias
        node['schema'] = self.default_schema
        default_alias = node.get('name')
        node['alias'] = default_alias

        # if there's a column, it should end up part of the ParsedNode
        if column_name is not None:
            node['column_name'] = column_name

        parsed_node = ParsedNode(**node)
        context = dbt.context.parser.generate(
            parsed_node,
            self.root_project_config,
            self.macro_manifest,
            config)

        dbt.clients.jinja.get_rendered(
            parsed_node.raw_sql, context, parsed_node.to_shallow_dict(),
            capture_macros=True)

        # Clean up any open conns opened by adapter functions that hit the db
        db_wrapper = context['adapter']
        adapter = db_wrapper.adapter
        runtime_config = db_wrapper.config
        adapter.release_connection(parsed_node.name)

        # Special macro defined in the global project. Use the root project's
        # definition, not the current package
        schema_override = config.config.get('schema')
        get_schema = self.get_schema_func()
        parsed_node.schema = get_schema(schema_override).strip()
        parsed_node.alias = config.config.get('alias', default_alias)

        # Set tags on node provided in config blocks
        model_tags = config.config.get('tags', [])
        parsed_node.tags.extend(model_tags)

        # Overwrite node config
        config_dict = parsed_node.get('config', {})
        config_dict.update(config.config)
        parsed_node.config = config_dict

        for hook_type in dbt.hooks.ModelHookType.Both:
            parsed_node.config[hook_type] = dbt.hooks.get_hooks(parsed_node,
                                                                hook_type)

        parsed_node.validate()

        return parsed_node
