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
    COMPILED_TYPES,
    CompiledSchemaTestNode,
    GraphMemberNode,
    InjectedCTE,
    ManifestNode,
    NonSourceCompiledNode,
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
import dbt.tracking

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
        NodeType.Exposure: 'exposure',
    }

    results = {k: 0 for k in names.keys()}
    results.update(stats)

    # create tracking event for resource_counts
    if dbt.tracking.active_user is not None:
        resource_counts = {k.pluralize(): v for k, v in results.items()}
        dbt.tracking.track_resource_counts(resource_counts)

    stat_line = ", ".join([
        pluralize(ct, names.get(t)) for t, ct in results.items()
        if t in names
    ])

    logger.info("Found {}".format(stat_line))


def _node_enabled(node: ManifestNode):
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
    for exposure in manifest.exposures.values():
        stats[exposure.resource_type] += 1
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
            data = manifest.expect(node_id).to_dict(omit_none=True)
            out_graph.add_node(node_id, **data)
        nx.write_gpickle(out_graph, outfile)


class Compiler:
    def __init__(self, config):
        self.config = config

    def initialize(self):
        make_directory(self.config.target_path)
        make_directory(self.config.modules_path)

    # creates a ModelContext which is converted to
    # a dict for jinja rendering of SQL
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

    def _get_relation_name(self, node: ParsedNode):
        relation_name = None
        if node.is_relational and not node.is_ephemeral_model:
            adapter = get_adapter(self.config)
            relation_cls = adapter.Relation
            relation_name = str(relation_cls.create_from(self.config, node))
        return relation_name

    def _inject_ctes_into_sql(self, sql: str, ctes: List[InjectedCTE]) -> str:
        """
        `ctes` is a list of InjectedCTEs like:

            [
                InjectedCTE(
                    id="cte_id_1",
                    sql="__dbt__cte__ephemeral as (select * from table)",
                ),
                InjectedCTE(
                    id="cte_id_2",
                    sql="__dbt__cte__events as (select id, type from events)",
                ),
            ]

        Given `sql` like:

          "with internal_cte as (select * from sessions)
           select * from internal_cte"

        This will spit out:

          "with __dbt__cte__ephemeral as (select * from table),
                __dbt__cte__events as (select id, type from events),
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

    def _recursively_prepend_ctes(
        self,
        model: NonSourceCompiledNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]],
    ) -> Tuple[NonSourceCompiledNode, List[InjectedCTE]]:
        """This method is called by the 'compile_node' method. Starting
        from the node that it is passed in, it will recursively call
        itself using the 'extra_ctes'.  The 'ephemeral' models do
        not produce SQL that is executed directly, instead they
        are rolled up into the models that refer to them by
        inserting CTEs into the SQL.
        """
        if model.compiled_sql is None:
            raise RuntimeException(
                'Cannot inject ctes into an unparsed node', model
            )
        if model.extra_ctes_injected:
            return (model, model.extra_ctes)

        # Just to make it plain that nothing is actually injected for this case
        if not model.extra_ctes:
            model.extra_ctes_injected = True
            manifest.update_node(model)
            return (model, model.extra_ctes)

        # This stores the ctes which will all be recursively
        # gathered and then "injected" into the model.
        prepended_ctes: List[InjectedCTE] = []

        # extra_ctes are added to the model by
        # RuntimeRefResolver.create_relation, which adds an
        # extra_cte for every model relation which is an
        # ephemeral model.
        for cte in model.extra_ctes:
            if cte.id not in manifest.nodes:
                raise InternalException(
                    f'During compilation, found a cte reference that '
                    f'could not be resolved: {cte.id}'
                )
            cte_model = manifest.nodes[cte.id]

            if not cte_model.is_ephemeral_model:
                raise InternalException(f'{cte.id} is not ephemeral')

            # This model has already been compiled, so it's been
            # through here before
            if getattr(cte_model, 'compiled', False):
                assert isinstance(cte_model, tuple(COMPILED_TYPES.values()))
                cte_model = cast(NonSourceCompiledNode, cte_model)
                new_prepended_ctes = cte_model.extra_ctes

            # if the cte_model isn't compiled, i.e. first time here
            else:
                # This is an ephemeral parsed model that we can compile.
                # Compile and update the node
                cte_model = self._compile_node(
                    cte_model, manifest, extra_context)
                # recursively call this method
                cte_model, new_prepended_ctes = \
                    self._recursively_prepend_ctes(
                        cte_model, manifest, extra_context
                    )
                # Save compiled SQL file and sync manifest
                self._write_node(cte_model)
                manifest.sync_update_node(cte_model)

            _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)

            new_cte_name = self.add_ephemeral_prefix(cte_model.name)
            rendered_sql = (
                cte_model._pre_injected_sql or cte_model.compiled_sql
            )
            sql = f' {new_cte_name} as (\n{rendered_sql}\n)'

            _add_prepended_cte(prepended_ctes, InjectedCTE(id=cte.id, sql=sql))

        injected_sql = self._inject_ctes_into_sql(
            model.compiled_sql,
            prepended_ctes,
        )
        model._pre_injected_sql = model.compiled_sql
        model.compiled_sql = injected_sql
        model.extra_ctes_injected = True
        model.extra_ctes = prepended_ctes
        model.validate(model.to_dict(omit_none=True))

        manifest.update_node(model)

        return model, prepended_ctes

    # creates a compiled_node from the ManifestNode passed in,
    # creates a "context" dictionary for jinja rendering,
    # and then renders the "compiled_sql" using the node, the
    # raw_sql and the context.
    def _compile_node(
        self,
        node: ManifestNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> NonSourceCompiledNode:
        if extra_context is None:
            extra_context = {}

        logger.debug("Compiling {}".format(node.unique_id))

        data = node.to_dict(omit_none=True)
        data.update({
            'compiled': False,
            'compiled_sql': None,
            'extra_ctes_injected': False,
            'extra_ctes': [],
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

        compiled_node.relation_name = self._get_relation_name(node)

        compiled_node.compiled = True

        return compiled_node

    def write_graph_file(self, linker: Linker, manifest: Manifest):
        filename = graph_file_name
        graph_path = os.path.join(self.config.target_path, filename)
        if flags.WRITE_JSON:
            linker.write_graph(graph_path, manifest)

    def link_node(
        self, linker: Linker, node: GraphMemberNode, manifest: Manifest
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
        for exposure in manifest.exposures.values():
            self.link_node(linker, exposure, manifest)
            # linker.add_node(exposure.unique_id)

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

    # writes the "compiled_sql" into the target/compiled directory
    def _write_node(self, node: NonSourceCompiledNode) -> ManifestNode:
        if (not node.extra_ctes_injected or
                node.resource_type == NodeType.Snapshot):
            return node
        logger.debug(f'Writing injected SQL for node "{node.unique_id}"')

        if node.compiled_sql:
            node.compiled_path = node.write_node(
                self.config.target_path,
                'compiled',
                node.compiled_sql
            )
        return node

    def compile_node(
        self,
        node: ManifestNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
        write: bool = True,
    ) -> NonSourceCompiledNode:
        """This is the main entry point into this code. It's called by
        CompileRunner.compile, GenericRPCRunner.compile, and
        RunTask.get_hook_sql. It calls '_compile_node' to convert
        the node into a compiled node, and then calls the
        recursive method to "prepend" the ctes.
        """
        node = self._compile_node(node, manifest, extra_context)

        node, _ = self._recursively_prepend_ctes(
            node, manifest, extra_context
        )
        if write:
            self._write_node(node)
        return node
