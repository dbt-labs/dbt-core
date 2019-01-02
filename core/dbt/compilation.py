import itertools
import os
import json
from collections import OrderedDict, defaultdict
import sqlparse

import dbt.utils
import dbt.include
import dbt.tracking

from dbt.utils import get_materialization, NodeType, is_type

from dbt.linker import Linker

import dbt.compat
import dbt.context.runtime
import dbt.contracts.project
import dbt.exceptions
import dbt.flags
import dbt.loader
import dbt.config
from dbt.contracts.graph.compiled import CompiledNode, CompiledGraph

from dbt.clients.system import write_json
from dbt.logger import GLOBAL_LOGGER as logger

graph_file_name = 'graph.gpickle'
manifest_file_name = 'manifest.json'


def print_compile_stats(stats):
    names = {
        NodeType.Model: 'models',
        NodeType.Test: 'tests',
        NodeType.Archive: 'archives',
        NodeType.Analysis: 'analyses',
        NodeType.Macro: 'macros',
        NodeType.Operation: 'operations',
        NodeType.Seed: 'seed files',
    }

    results = {k: 0 for k in names.keys()}
    results.update(stats)

    stat_line = ", ".join(
        ["{} {}".format(ct, names.get(t)) for t, ct in results.items()])

    logger.info("Found {}".format(stat_line))


def _add_prepended_cte(prepended_ctes, new_cte):
    for dct in prepended_ctes:
        if dct['id'] == new_cte['id']:
            dct['sql'] = new_cte['sql']
            return
    prepended_ctes.append(new_cte)


def _extend_prepended_ctes(prepended_ctes, new_prepended_ctes):
    for new_cte in new_prepended_ctes:
        _add_prepended_cte(prepended_ctes, new_cte)


def prepend_ctes(model, manifest):
    model, _, manifest = recursively_prepend_ctes(model, manifest)

    return (model, manifest)


def recursively_prepend_ctes(model, manifest):
    if model.extra_ctes_injected:
        return (model, model.extra_ctes, manifest)

    if dbt.flags.STRICT_MODE:
        # ensure that all the nodes in this manifest are compiled
        CompiledGraph(**manifest.to_flat_graph())

    prepended_ctes = []

    for cte in model.extra_ctes:
        cte_id = cte['id']
        cte_to_add = manifest.nodes.get(cte_id)
        cte_to_add, new_prepended_ctes, manifest = recursively_prepend_ctes(
            cte_to_add, manifest)

        _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)
        new_cte_name = '__dbt__CTE__{}'.format(cte_to_add.get('name'))
        sql = ' {} as (\n{}\n)'.format(new_cte_name, cte_to_add.compiled_sql)
        _add_prepended_cte(prepended_ctes, {'id': cte_id, 'sql': sql})

    model.prepend_ctes(prepended_ctes)

    manifest.nodes[model.unique_id] = model

    return (model, prepended_ctes, manifest)


class Compiler(object):
    def __init__(self, config):
        self.config = config

    def initialize(self):
        dbt.clients.system.make_directory(self.config.target_path)
        dbt.clients.system.make_directory(self.config.modules_path)

    def compile_node(self, node, manifest, extra_context=None):
        if extra_context is None:
            extra_context = {}

        logger.debug("Compiling {}".format(node.get('unique_id')))

        data = node.to_dict()
        data.update({
            'compiled': False,
            'compiled_sql': None,
            'extra_ctes_injected': False,
            'extra_ctes': [],
            'injected_sql': None,
        })
        compiled_node = CompiledNode(**data)

        context = dbt.context.runtime.generate(
            compiled_node, self.config, manifest)
        context.update(extra_context)

        compiled_node.compiled_sql = dbt.clients.jinja.get_rendered(
            node.get('raw_sql'),
            context,
            node)

        compiled_node.compiled = True

        injected_node, _ = prepend_ctes(compiled_node, manifest)

        should_wrap = {NodeType.Test, NodeType.Operation}
        if injected_node.resource_type in should_wrap:
            # data tests get wrapped in count(*)
            # TODO : move this somewhere more reasonable
            if 'data' in injected_node.tags and \
               is_type(injected_node, NodeType.Test):
                injected_node.wrapped_sql = (
                    "select count(*) from (\n{test_sql}\n) sbq").format(
                        test_sql=injected_node.injected_sql)
            else:
                # don't wrap schema tests or analyses.
                injected_node.wrapped_sql = injected_node.injected_sql

        elif is_type(injected_node, NodeType.Archive):
            # unfortunately we do everything automagically for
            # archives. in the future it'd be nice to generate
            # the SQL at the parser level.
            pass

        elif(is_type(injected_node, NodeType.Model) and
             get_materialization(injected_node) == 'ephemeral'):
            pass

        else:
            injected_node.wrapped_sql = None

        return injected_node

    def write_manifest_file(self, manifest):
        """Write the manifest file to disk.

        manifest should be a Manifest.
        """
        filename = manifest_file_name
        manifest_path = os.path.join(self.config.target_path, filename)
        write_json(manifest_path, manifest.serialize())

    def write_graph_file(self, linker, manifest):
        filename = graph_file_name
        graph_path = os.path.join(self.config.target_path, filename)
        linker.write_graph(graph_path, manifest)

    def link_node(self, linker, node, manifest):
        linker.add_node(node.unique_id)

        for dependency in node.depends_on_nodes:
            if manifest.nodes.get(dependency):
                linker.dependency(
                    node.unique_id,
                    (manifest.nodes.get(dependency).unique_id))

            else:
                dbt.exceptions.dependency_not_found(node, dependency)

    def link_graph(self, linker, manifest):
        for node in manifest.nodes.values():
            self.link_node(linker, node, manifest)

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

    def get_all_projects(self):
        all_projects = {self.config.project_name: self.config}
        dependency_projects = dbt.utils.dependency_projects(self.config)

        for project_cfg in dependency_projects:
            name = project_cfg.project_name
            all_projects[name] = project_cfg

        if dbt.flags.STRICT_MODE:
            dbt.contracts.project.ProjectList(**all_projects)

        return all_projects

    def _check_resource_uniqueness(cls, manifest):
        names_resources = {}
        alias_resources = {}

        for resource, node in manifest.nodes.items():
            if node.resource_type not in NodeType.refable():
                continue

            name = node.name
            alias = "{}.{}".format(node.schema, node.alias)

            existing_node = names_resources.get(name)
            if existing_node is not None:
                dbt.exceptions.raise_duplicate_resource_name(
                        existing_node, node)

            existing_alias = alias_resources.get(alias)
            if existing_alias is not None:
                dbt.exceptions.raise_ambiguous_alias(
                        existing_alias, node)

            names_resources[name] = node
            alias_resources[alias] = node

    def warn_for_deprecated_configs(self, manifest):
        for unique_id, node in manifest.nodes.items():
            is_model = node.resource_type == NodeType.Model
            if is_model and 'sql_where' in node.config:
                dbt.deprecations.warn('sql_where')

    def compile(self):
        linker = Linker()

        all_projects = self.get_all_projects()

        manifest = dbt.loader.GraphLoader.load_all(self.config, all_projects)

        self.write_manifest_file(manifest)

        self._check_resource_uniqueness(manifest)

        resource_fqns = manifest.get_resource_fqns()
        disabled_fqns = [n.fqn for n in manifest.disabled]
        self.config.warn_for_unused_resource_config_paths(resource_fqns,
                                                          disabled_fqns)
        self.warn_for_deprecated_configs(manifest)

        self.link_graph(linker, manifest)

        stats = defaultdict(int)

        for node_name, node in itertools.chain(
                manifest.nodes.items(),
                manifest.macros.items()):
            stats[node.resource_type] += 1

        self.write_graph_file(linker, manifest)
        print_compile_stats(stats)

        return manifest, linker
