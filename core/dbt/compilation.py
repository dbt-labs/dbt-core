import os
from collections import defaultdict
from typing import List, Dict, Any, Tuple, cast, Optional

import networkx as nx  # type: ignore
import sqlparse

from dbt import flags
from dbt.adapters.factory import get_adapter
from dbt.clients import jinja
from dbt.clients.system import make_directory
from dbt.context.providers import generate_runtime_model
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.graph.compiled import (
    InjectedCTE,
    COMPILED_TYPES,
    NonSourceNode,
    NonSourceCompiledNode,
    CompiledDataTestNode,
    CompiledSchemaTestNode,
)
from dbt.contracts.graph.parsed import ParsedNode
from dbt.exceptions import (
    dependency_not_found,
    InternalException,
    RuntimeException,
)
from dbt.graph import Graph
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.node_types import NodeType
from dbt.utils import pluralize

graph_file_name = 'graph.gpickle'


def _compiled_type_for(model: ParsedNode):
    if type(model) not in COMPILED_TYPES:
        raise InternalException(
            f'Asked to compile {type(model)} node, but it has no compiled form'
        )
    return COMPILED_TYPES[type(model)]


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
        pluralize(ct, names.get(t)) for t, ct in results.items()
        if t in names
    ])

    logger.info("Found {}".format(stat_line))


def _node_enabled(node: NonSourceNode):
    # Disabled models are already excluded from the manifest
    if node.resource_type == NodeType.Test and not node.config.enabled:
        return False
    else:
        return True


def _generate_stats(manifest: Manifest):
    stats: Dict[NodeType, int] = defaultdict(int)
    for node in manifest.nodes.values():
        if _node_enabled(node):
            stats[node.resource_type] += 1

    for source in manifest.sources.values():
        stats[source.resource_type] += 1
    for macro in manifest.macros.values():
        stats[macro.resource_type] += 1
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


class Linker:
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.graph = nx.DiGraph(**data)

    def edges(self):
        return self.graph.edges()

    def nodes(self):
        return self.graph.nodes()

    def find_cycles(self):
        try:
            cycle = nx.find_cycle(self.graph)
        except nx.NetworkXNoCycle:
            return None
        else:
            # cycles is a List[Tuple[str, ...]]
            return " --> ".join(c[0] for c in cycle)

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node2, node1)

    def add_node(self, node):
        self.graph.add_node(node)

    def write_graph(self, outfile: str, manifest: Manifest):
        """Write the graph to a gpickle file. Before doing so, serialize and
        include all nodes in their corresponding graph entries.
        """
        out_graph = self.graph.copy()
        for node_id in self.graph.nodes():
            data = manifest.expect(node_id).to_dict()
            out_graph.add_node(node_id, **data)
        nx.write_gpickle(out_graph, outfile)


class Compiler:
    def __init__(self, config):
        self.config = config

    def initialize(self):
        make_directory(self.config.target_path)
        make_directory(self.config.modules_path)

    def _create_node_context(
        self,
        node: NonSourceCompiledNode,
        manifest: Manifest,
        extra_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        context = generate_runtime_model(
            node, self.config, manifest
        )
        context.update(extra_context)
        if isinstance(node, CompiledSchemaTestNode):
            # for test nodes, add a special keyword args value to the context
            jinja.add_rendered_test_kwargs(context, node)

        return context

    def add_ephemeral_prefix(self, name: str):
        adapter = get_adapter(self.config)
        relation_cls = adapter.Relation
        return relation_cls.add_ephemeral_prefix(name)

    def _get_compiled_model(
        self,
        manifest: Manifest,
        cte_id: str,
        extra_context: Dict[str, Any],
    ) -> NonSourceCompiledNode:

        if cte_id not in manifest.nodes:
            raise InternalException(
                f'During compilation, found a cte reference that could not be '
                f'resolved: {cte_id}'
            )
        cte_model = manifest.nodes[cte_id]
        if getattr(cte_model, 'compiled', False):
            assert isinstance(cte_model, tuple(COMPILED_TYPES.values()))
            return cast(NonSourceCompiledNode, cte_model)
        elif cte_model.is_ephemeral_model:
            # this must be some kind of parsed node that we can compile.
            # we know it's not a parsed source definition
            assert isinstance(cte_model, tuple(COMPILED_TYPES))
            # update the node so
            node = self.compile_node(cte_model, manifest, extra_context)
            manifest.sync_update_node(node)
            return node
        else:
            raise InternalException(
                f'During compilation, found an uncompiled cte that '
                f'was not an ephemeral model: {cte_id}'
            )

    def _inject_ctes_into_sql(self, sql: str, ctes: List[InjectedCTE]) -> str:
        """
        `ctes` is a list of InjectedCTEs like:

            [
                InjectedCTE(
                    id="cte_id_1",
                    sql="__dbt__CTE__ephemeral as (select * from table)",
                ),
                InjectedCTE(
                    id="cte_id_2",
                    sql="__dbt__CTE__events as (select id, type from events)",
                ),
            ]

        Given `sql` like:

          "with internal_cte as (select * from sessions)
           select * from internal_cte"

        This will spit out:

          "with __dbt__CTE__ephemeral as (select * from table),
                __dbt__CTE__events as (select id, type from events),
                with internal_cte as (select * from sessions)
           select * from internal_cte"

        (Whitespace enhanced for readability.)
        """
        if len(ctes) == 0:
            return sql

        parsed_stmts = sqlparse.parse(sql)
        parsed = parsed_stmts[0]

        with_stmt = None
        for token in parsed.tokens:
            if token.is_keyword and token.normalized == 'WITH':
                with_stmt = token
                break

        if with_stmt is None:
            # no with stmt, add one, and inject CTEs right at the beginning
            first_token = parsed.token_first()
            with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, 'with')
            parsed.insert_before(first_token, with_stmt)
        else:
            # stmt exists, add a comma (which will come after injected CTEs)
            trailing_comma = sqlparse.sql.Token(
                sqlparse.tokens.Punctuation, ','
            )
            parsed.insert_after(with_stmt, trailing_comma)

        token = sqlparse.sql.Token(
            sqlparse.tokens.Keyword,
            ", ".join(c.sql for c in ctes)
        )
        parsed.insert_after(with_stmt, token)

        return str(parsed)

    def _model_prepend_ctes(
        self,
        model: NonSourceCompiledNode,
        prepended_ctes: List[InjectedCTE]
    ) -> NonSourceCompiledNode:
        if model.compiled_sql is None:
            raise RuntimeException(
                'Cannot prepend ctes to an unparsed node', model
            )
        injected_sql = self._inject_ctes_into_sql(
            model.compiled_sql,
            prepended_ctes,
        )

        model.extra_ctes_injected = True
        model.extra_ctes = prepended_ctes
        model.injected_sql = injected_sql
        model.validate(model.to_dict())
        return model

    def _get_dbt_test_name(self) -> str:
        return 'dbt__CTE__INTERNAL_test'

    def _recursively_prepend_ctes(
        self,
        model: NonSourceCompiledNode,
        manifest: Manifest,
        extra_context: Dict[str, Any],
    ) -> Tuple[NonSourceCompiledNode, List[InjectedCTE]]:
        if model.extra_ctes_injected:
            return (model, model.extra_ctes)

        if flags.STRICT_MODE:
            if not isinstance(model, tuple(COMPILED_TYPES.values())):
                raise InternalException(
                    f'Bad model type: {type(model)}'
                )

        prepended_ctes: List[InjectedCTE] = []

        dbt_test_name = self._get_dbt_test_name()

        for cte in model.extra_ctes:
            if cte.id == dbt_test_name:
                sql = cte.sql
            else:
                cte_model = self._get_compiled_model(
                    manifest,
                    cte.id,
                    extra_context,
                )
                cte_model, new_prepended_ctes = self._recursively_prepend_ctes(
                    cte_model, manifest, extra_context
                )
                _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)

                new_cte_name = self.add_ephemeral_prefix(cte_model.name)
                sql = f' {new_cte_name} as (\n{cte_model.compiled_sql}\n)'
            _add_prepended_cte(prepended_ctes, InjectedCTE(id=cte.id, sql=sql))

        model = self._model_prepend_ctes(model, prepended_ctes)

        manifest.update_node(model)

        return model, prepended_ctes

    def _insert_ctes(
        self,
        compiled_node: NonSourceCompiledNode,
        manifest: Manifest,
        extra_context: Dict[str, Any],
    ) -> NonSourceCompiledNode:
        """Insert the CTEs for the model."""

        # for data tests, we need to insert a special CTE at the end of the
        # list containing the test query, and then have the "real" query be a
        # select count(*) from that model.
        # the benefit of doing it this way is that _insert_ctes() can be
        # rewritten for different adapters to handle databses that don't
        # support CTEs, or at least don't have full support.
        if isinstance(compiled_node, CompiledDataTestNode):
            # the last prepend (so last in order) should be the data test body.
            # then we can add our select count(*) from _that_ cte as the "real"
            # compiled_sql, and do the regular prepend logic from CTEs.
            name = self._get_dbt_test_name()
            cte = InjectedCTE(
                id=name,
                sql=f' {name} as (\n{compiled_node.compiled_sql}\n)'
            )
            compiled_node.extra_ctes.append(cte)
            compiled_node.compiled_sql = f'\nselect count(*) from {name}'

        injected_node, _ = self._recursively_prepend_ctes(
            compiled_node, manifest, extra_context
        )
        return injected_node

    def _compile_node(
        self,
        node: NonSourceNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> NonSourceCompiledNode:
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

        context = self._create_node_context(
            compiled_node, manifest, extra_context
        )

        compiled_node.compiled_sql = jinja.get_rendered(
            node.raw_sql,
            context,
            node,
        )

        compiled_node.compiled = True

        injected_node = self._insert_ctes(
            compiled_node, manifest, extra_context
        )

        return injected_node

    def write_graph_file(self, linker: Linker, manifest: Manifest):
        filename = graph_file_name
        graph_path = os.path.join(self.config.target_path, filename)
        if flags.WRITE_JSON:
            linker.write_graph(graph_path, manifest)

    def link_node(
        self, linker: Linker, node: NonSourceNode, manifest: Manifest
    ):
        linker.add_node(node.unique_id)

        for dependency in node.depends_on_nodes:
            if dependency in manifest.nodes:
                linker.dependency(
                    node.unique_id,
                    (manifest.nodes[dependency].unique_id)
                )
            elif dependency in manifest.sources:
                linker.dependency(
                    node.unique_id,
                    (manifest.sources[dependency].unique_id)
                )
            else:
                dependency_not_found(node, dependency)

    def link_graph(self, linker: Linker, manifest: Manifest):
        for source in manifest.sources.values():
            linker.add_node(source.unique_id)
        for node in manifest.nodes.values():
            self.link_node(linker, node, manifest)

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

    def compile(self, manifest: Manifest, write=True) -> Graph:
        self.initialize()
        linker = Linker()

        self.link_graph(linker, manifest)

        stats = _generate_stats(manifest)

        if write:
            self.write_graph_file(linker, manifest)
        print_compile_stats(stats)

        return Graph(linker.graph)

    def _write_node(self, node: NonSourceCompiledNode) -> NonSourceNode:
        if not _is_writable(node):
            return node
        logger.debug(f'Writing injected SQL for node "{node.unique_id}"')

        if node.injected_sql is None:
            # this should not really happen, but it'd be a shame to crash
            # over it
            logger.error(
                f'Compiled node "{node.unique_id}" had no injected_sql, '
                'cannot write sql!'
            )
        else:
            node.build_path = node.write_node(
                self.config.target_path,
                'compiled',
                node.injected_sql
            )
        return node

    def compile_node(
        self,
        node: NonSourceNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
        write: bool = True,
    ) -> NonSourceCompiledNode:
        node = self._compile_node(node, manifest, extra_context)

        if write and _is_writable(node):
            self._write_node(node)
        return node


def _is_writable(node):
    if not node.injected_sql:
        return False

    if node.resource_type == NodeType.Snapshot:
        return False

    return True
