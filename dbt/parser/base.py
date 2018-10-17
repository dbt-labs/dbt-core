import os

import dbt.exceptions
import dbt.flags
import dbt.model
import dbt.utils
import dbt.hooks
import dbt.clients.jinja
import dbt.context.parser

from dbt.utils import coalesce
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.contracts.graph.parsed import ParsedNode
from dbt.contracts.graph.manifest import Manifest


class BaseParser(object):

    @classmethod
    def load_and_parse(cls, *args, **kwargs):
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

    @classmethod
    def parse_node(cls, node, node_path, root_project_config,
                   package_project_config, all_projects,
                   tags=None, fqn_extra=None, fqn=None, macros=None,
                   agate_table=None, archive_config=None, column_name=None):
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
        macros = coalesce(macros, {})

        node.update({
            'refs': [],
            'depends_on': {
                'nodes': [],
                'macros': [],
            }
        })

        if fqn is None:
            fqn = cls.get_fqn(node.get('path'), package_project_config,
                              fqn_extra)

        config = dbt.model.SourceConfig(
            root_project_config,
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
        default_schema = getattr(root_project_config.credentials, 'schema',
                                 'public')
        node['schema'] = default_schema
        default_alias = node.get('name')
        node['alias'] = default_alias

        # if there's a column, it should end up part of the ParsedNode
        if column_name is not None:
            node['column_name'] = column_name

        # make a manifest with just the macros to get the context
        manifest = Manifest(macros=macros, nodes={}, docs={},
                            generated_at=dbt.utils.timestring(), disabled=[])

        parsed_node = ParsedNode(**node)
        context = dbt.context.parser.generate(parsed_node, root_project_config,
                                              manifest, config)

        dbt.clients.jinja.get_rendered(
            parsed_node.raw_sql, context, parsed_node.to_shallow_dict(),
            capture_macros=True)

        # Clean up any open conns opened by adapter functions that hit the db
        db_wrapper = context['adapter']
        adapter = db_wrapper.adapter
        runtime_config = db_wrapper.config
        adapter.release_connection(parsed_node.name)

        # Special macro defined in the global project
        schema_override = config.config.get('schema')
        get_schema = context.get('generate_schema_name',
                                 lambda x: default_schema)
        parsed_node.schema = get_schema(schema_override)
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
