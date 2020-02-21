import itertools
import os
from collections import defaultdict
from typing import List, Dict

import dbt.utils
import dbt.include
import dbt.tracking

from dbt.node_types import NodeType
from dbt.linker import Linker

from dbt.context.providers import generate_runtime_model
import dbt.contracts.project
import dbt.exceptions
import dbt.flags
import dbt.config
from dbt.contracts.graph.compiled import InjectedCTE, COMPILED_TYPES
from dbt.contracts.graph.parsed import ParsedNode

from dbt.logger import GLOBAL_LOGGER as logger

graph_file_name = 'graph.gpickle'


def _compiled_type_for(model: ParsedNode):
    if model.resource_type not in COMPILED_TYPES:
        raise dbt.exceptions.InternalException(
            'Asked to compile {} node, but it has no compiled form'
            .format(model.resource_type)
        )
    return COMPILED_TYPES[model.resource_type]


def print_compile_stats(stats):
    names = {
        NodeType.Model: 'model',
        NodeType.Test: 'test',
        NodeType.Snapshot: 'snapshot',
        NodeType.Analysis: 'analysis',
        NodeType.Macro: 'macro',
        NodeType.Operation: 'operation',
        NodeType.Seed: 'seed file',
        NodeType.Source: 'source',
    }

    results = {k: 0 for k in names.keys()}
    results.update(stats)

    stat_line = ", ".join([
        dbt.utils.pluralize(ct, names.get(t)) for t, ct in results.items()
        if t in names
    ])

    logger.info("Found {}".format(stat_line))


def _node_enabled(node):
    # Disabled models are already excluded from the manifest
    if node.resource_type == NodeType.Test and not node.config.enabled:
        return False
    else:
        return True


def _generate_stats(manifest):
    stats: Dict[NodeType, int] = defaultdict(int)
    for node_name, node in itertools.chain(
            manifest.nodes.items(),
            manifest.macros.items()):
        if _node_enabled(node):
            stats[node.resource_type] += 1

    return stats


def _add_prepended_cte(prepended_ctes, new_cte):
    for cte in prepended_ctes:
        if cte.id == new_cte.id:
            cte.sql = new_cte.sql
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
        if not isinstance(model, tuple(COMPILED_TYPES.values())):
            raise dbt.exceptions.InternalException(
                'Bad model type: {}'.format(type(model))
            )

    prepended_ctes: List[InjectedCTE] = []

    for cte in model.extra_ctes:
        cte_id = cte.id
        cte_to_add = manifest.nodes.get(cte_id)
        cte_to_add, new_prepended_ctes, manifest = recursively_prepend_ctes(
            cte_to_add, manifest)
        _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)
        new_cte_name = '__dbt__CTE__{}'.format(cte_to_add.name)
        sql = ' {} as (\n{}\n)'.format(new_cte_name, cte_to_add.compiled_sql)
        _add_prepended_cte(prepended_ctes, InjectedCTE(id=cte_id, sql=sql))

    model.prepend_ctes(prepended_ctes)

    manifest.update_node(model)

    return (model, prepended_ctes, manifest)


class Compiler:
    def __init__(self, config):
        self.config = config

    def initialize(self):
        dbt.clients.system.make_directory(self.config.target_path)
        dbt.clients.system.make_directory(self.config.modules_path)

    def compile_node(self, node, manifest, extra_context=None):
        if extra_context is None:
            extra_context = {}

        logger.debug("Compiling {}".format(node.unique_id))

        data = node.to_dict()
        data.update({
            'compiled': False,
            'compiled_sql': None,
            'extra_ctes_injected': False,
            'extra_ctes': [],
            'injected_sql': None,
        })
        compiled_node = _compiled_type_for(node).from_dict(data)

        context = generate_runtime_model(
            compiled_node, self.config, manifest
        )
        context.update(extra_context)

        compiled_node.compiled_sql = dbt.clients.jinja.get_rendered(
            node.raw_sql,
            context,
            node)

        compiled_node.compiled = True

        injected_node, _ = prepend_ctes(compiled_node, manifest)

        should_wrap = {NodeType.Test, NodeType.Operation}
        if injected_node.resource_type in should_wrap:
            # data tests get wrapped in count(*)
            # TODO : move this somewhere more reasonable
            if 'data' in injected_node.tags and \
               injected_node.resource_type == NodeType.Test:
                injected_node.wrapped_sql = (
                    "select count(*) as errors "
                    "from (\n{test_sql}\n) sbq").format(
                        test_sql=injected_node.injected_sql)
            else:
                # don't wrap schema tests or analyses.
                injected_node.wrapped_sql = injected_node.injected_sql

        elif injected_node.resource_type == NodeType.Snapshot:
            # unfortunately we do everything automagically for
            # snapshots. in the future it'd be nice to generate
            # the SQL at the parser level.
            pass

        elif(injected_node.resource_type == NodeType.Model and
             injected_node.get_materialization() == 'ephemeral'):
            pass

        else:
            injected_node.wrapped_sql = None

        return injected_node

    def write_graph_file(self, linker, manifest):
        filename = graph_file_name
        graph_path = os.path.join(self.config.target_path, filename)
        if dbt.flags.WRITE_JSON:
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

    def compile(self, manifest, write=True):
        linker = Linker()

        self.link_graph(linker, manifest)

        stats = _generate_stats(manifest)

        if write:
            self.write_graph_file(linker, manifest)
        print_compile_stats(stats)

        return linker


def compile_manifest(config, manifest, write=True) -> Linker:
    compiler = Compiler(config)
    compiler.initialize()
    return compiler.compile(manifest, write=write)


def _is_writable(node):
    if not node.injected_sql:
        return False

    if node.resource_type == NodeType.Snapshot:
        return False

    return True


def compile_node(adapter, config, node, manifest, extra_context, write=True):
    compiler = Compiler(config)
    node = compiler.compile_node(node, manifest, extra_context)

    if write and _is_writable(node):
        logger.debug('Writing injected SQL for node "{}"'.format(
            node.unique_id))

        node.build_path = node.write_node(
            config.target_path,
            'compiled',
            node.injected_sql
        )

    return node
