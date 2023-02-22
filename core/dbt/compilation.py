import os
from collections import defaultdict
from typing import List, Dict, Any, Tuple, Optional

import networkx as nx  # type: ignore
import pickle
import sqlparse

from dbt import flags
from dbt.adapters.factory import get_adapter
from dbt.clients import jinja
from dbt.clients.system import make_directory
from dbt.context.providers import generate_runtime_model_context
from dbt.contracts.graph.manifest import Manifest, UniqueID
from dbt.contracts.graph.nodes import (
    ManifestNode,
    ManifestSQLNode,
    GenericTestNode,
    GraphMemberNode,
    InjectedCTE,
    SeedNode,
)
from dbt.exceptions import (
    GraphDependencyNotFoundError,
    DbtInternalError,
    DbtRuntimeError,
)
from dbt.graph import Graph
from dbt.events.functions import fire_event
from dbt.events.types import FoundStats, WritingInjectedSQLForNode
from dbt.events.contextvars import get_node_info
from dbt.node_types import NodeType, ModelLanguage
from dbt.events.format import pluralize
import dbt.tracking

graph_file_name = "graph.gpickle"


def print_compile_stats(stats):
    names = {
        NodeType.Model: "model",
        NodeType.Test: "test",
        NodeType.Snapshot: "snapshot",
        NodeType.Analysis: "analysis",
        NodeType.Macro: "macro",
        NodeType.Operation: "operation",
        NodeType.Seed: "seed file",
        NodeType.Source: "source",
        NodeType.Exposure: "exposure",
        NodeType.Metric: "metric",
    }

    results = {k: 0 for k in names.keys()}
    results.update(stats)

    # create tracking event for resource_counts
    if dbt.tracking.active_user is not None:
        resource_counts = {k.pluralize(): v for k, v in results.items()}
        dbt.tracking.track_resource_counts(resource_counts)

    stat_line = ", ".join([pluralize(ct, names.get(t)) for t, ct in results.items() if t in names])

    fire_event(FoundStats(stat_line=stat_line))


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
    for metric in manifest.metrics.values():
        stats[metric.resource_type] += 1
    for macro in manifest.macros.values():
        stats[macro.resource_type] += 1
    return stats


def _add_prepended_cte(prepended_ctes, new_cte):
    for cte in prepended_ctes:
        if cte.id == new_cte.id and new_cte.sql:
            cte.sql = new_cte.sql
            return
    if new_cte.sql:
        prepended_ctes.append(new_cte)


def _extend_prepended_ctes(prepended_ctes, new_prepended_ctes):
    for new_cte in new_prepended_ctes:
        _add_prepended_cte(prepended_ctes, new_cte)


def _get_tests_for_node(manifest: Manifest, unique_id: UniqueID) -> List[UniqueID]:
    """Get a list of tests that depend on the node with the
    provided unique id"""

    tests = []
    if unique_id in manifest.child_map:
        for child_unique_id in manifest.child_map[unique_id]:
            if child_unique_id.startswith("test."):
                tests.append(child_unique_id)

    return tests


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
        for node_id in self.graph:
            data = manifest.expect(node_id).to_dict(omit_none=True)
            out_graph.add_node(node_id, **data)
        with open(outfile, "wb") as outfh:
            pickle.dump(out_graph, outfh, protocol=pickle.HIGHEST_PROTOCOL)


class Compiler:
    def __init__(self, config):
        self.config = config

    def initialize(self):
        make_directory(self.config.target_path)
        make_directory(self.config.packages_install_path)

    # creates a ModelContext which is converted to
    # a dict for jinja rendering of SQL
    def _create_node_context(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Dict[str, Any],
    ) -> Dict[str, Any]:

        context = generate_runtime_model_context(node, self.config, manifest)
        context.update(extra_context)

        if isinstance(node, GenericTestNode):
            # for test nodes, add a special keyword args value to the context
            jinja.add_rendered_test_kwargs(context, node)

        return context

    def add_ephemeral_prefix(self, name: str):
        adapter = get_adapter(self.config)
        relation_cls = adapter.Relation
        return relation_cls.add_ephemeral_prefix(name)

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
            if token.is_keyword and token.normalized == "WITH":
                with_stmt = token
                break

        if with_stmt is None:
            # no with stmt, add one, and inject CTEs right at the beginning
            first_token = parsed.token_first()
            with_stmt = sqlparse.sql.Token(sqlparse.tokens.Keyword, "with")
            parsed.insert_before(first_token, with_stmt)
        else:
            # stmt exists, add a comma (which will come after injected CTEs)
            trailing_comma = sqlparse.sql.Token(sqlparse.tokens.Punctuation, ",")
            parsed.insert_after(with_stmt, trailing_comma)

        token = sqlparse.sql.Token(sqlparse.tokens.Keyword, ", ".join(c.sql for c in ctes))
        parsed.insert_after(with_stmt, token)

        return str(parsed)

    def _recursively_prepend_ctes(
        self,
        model: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]],
    ) -> Tuple[ManifestSQLNode, List[InjectedCTE]]:
        """This method is called by the 'compile_node' method. Starting
        from the node that it is passed in, it will recursively call
        itself using the 'extra_ctes'.  The 'ephemeral' models do
        not produce SQL that is executed directly, instead they
        are rolled up into the models that refer to them by
        inserting CTEs into the SQL.
        """
        if model.compiled_code is None:
            raise DbtRuntimeError("Cannot inject ctes into an uncompiled node", model)

        # extra_ctes_injected flag says that we've already recursively injected the ctes
        if model.extra_ctes_injected:
            return (model, model.extra_ctes)

        # Just to make it plain that nothing is actually injected for this case
        if len(model.extra_ctes) == 0:
            # SeedNodes don't have compilation attributes
            if not isinstance(model, SeedNode):
                model.extra_ctes_injected = True
            return (model, [])

        # This stores the ctes which will all be recursively
        # gathered and then "injected" into the model.
        prepended_ctes: List[InjectedCTE] = []

        # extra_ctes are added to the model by
        # RuntimeRefResolver.create_relation, which adds an
        # extra_cte for every model relation which is an
        # ephemeral model. InjectedCTEs have a unique_id and sql.
        # extra_ctes start out with sql set to None, and the sql is set in this loop.
        for cte in model.extra_ctes:
            if cte.id not in manifest.nodes:
                raise DbtInternalError(
                    f"During compilation, found a cte reference that "
                    f"could not be resolved: {cte.id}"
                )
            cte_model = manifest.nodes[cte.id]
            assert not isinstance(cte_model, SeedNode)

            if not cte_model.is_ephemeral_model:
                raise DbtInternalError(f"{cte.id} is not ephemeral")

            # This model has already been compiled and extra_ctes_injected, so it's been
            # through here before. We already checked above for extra_ctes_injected, but
            # checking again because updates maybe have happened in another thread.
            if cte_model.compiled is True and cte_model.extra_ctes_injected is True:
                new_prepended_ctes = cte_model.extra_ctes

            # if the cte_model isn't compiled, i.e. first time here
            else:
                # This is an ephemeral parsed model that we can compile.
                # Render the raw_code and set compiled to True
                cte_model = self._compile_code(cte_model, manifest, extra_context)
                # recursively call this method, sets extra_ctes_injected to True
                cte_model, new_prepended_ctes = self._recursively_prepend_ctes(
                    cte_model, manifest, extra_context
                )
                # Write compiled SQL file
                self._write_node(cte_model)

            _extend_prepended_ctes(prepended_ctes, new_prepended_ctes)

            new_cte_name = self.add_ephemeral_prefix(cte_model.name)
            rendered_sql = cte_model._pre_injected_sql or cte_model.compiled_code
            sql = f" {new_cte_name} as (\n{rendered_sql}\n)"

            _add_prepended_cte(prepended_ctes, InjectedCTE(id=cte.id, sql=sql))

        injected_sql = self._inject_ctes_into_sql(
            model.compiled_code,
            prepended_ctes,
        )
        # Check again before updating for multi-threading
        if not model.extra_ctes_injected:
            model._pre_injected_sql = model.compiled_code
            model.compiled_code = injected_sql
            model.extra_ctes = prepended_ctes
            model.extra_ctes_injected = True

        # if model.extra_ctes is not set to prepended ctes, something went wrong
        return model, model.extra_ctes

    # Sets compiled_code and compiled flag in the ManifestSQLNode passed in,
    # creates a "context" dictionary for jinja rendering,
    # and then renders the "compiled_code" using the node, the
    # raw_code and the context.
    def _compile_code(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
    ) -> ManifestSQLNode:
        if extra_context is None:
            extra_context = {}

        if node.language == ModelLanguage.python:
            # TODO could we also 'minify' this code at all? just aesthetic, not functional

            # quoating seems like something very specific to sql so far
            # for all python implementations we are seeing there's no quating.
            # TODO try to find better way to do this, given that
            original_quoting = self.config.quoting
            self.config.quoting = {key: False for key in original_quoting.keys()}
            context = self._create_node_context(node, manifest, extra_context)

            postfix = jinja.get_rendered(
                "{{ py_script_postfix(model) }}",
                context,
                node,
            )
            # we should NOT jinja render the python model's 'raw code'
            node.compiled_code = f"{node.raw_code}\n\n{postfix}"
            # restore quoting settings in the end since context is lazy evaluated
            self.config.quoting = original_quoting

        else:
            context = self._create_node_context(node, manifest, extra_context)
            node.compiled_code = jinja.get_rendered(
                node.raw_code,
                context,
                node,
            )

        node.compiled = True

        # relation_name is set at parse time, except for tests without store_failures,
        # but cli param can turn on store_failures, so we set here.
        if (
            node.resource_type == NodeType.Test
            and node.relation_name is None
            and node.is_relational
        ):
            adapter = get_adapter(self.config)
            relation_cls = adapter.Relation
            relation_name = str(relation_cls.create_from(self.config, node))
            node.relation_name = relation_name

        return node

    def write_graph_file(self, linker: Linker, manifest: Manifest):
        filename = graph_file_name
        graph_path = os.path.join(self.config.target_path, filename)
        if flags.WRITE_JSON:
            linker.write_graph(graph_path, manifest)

    def link_node(self, linker: Linker, node: GraphMemberNode, manifest: Manifest):
        linker.add_node(node.unique_id)

        for dependency in node.depends_on_nodes:
            if dependency in manifest.nodes:
                linker.dependency(node.unique_id, (manifest.nodes[dependency].unique_id))
            elif dependency in manifest.sources:
                linker.dependency(node.unique_id, (manifest.sources[dependency].unique_id))
            elif dependency in manifest.metrics:
                linker.dependency(node.unique_id, (manifest.metrics[dependency].unique_id))
            else:
                raise GraphDependencyNotFoundError(node, dependency)

    def link_graph(self, linker: Linker, manifest: Manifest, add_test_edges: bool = False):
        for source in manifest.sources.values():
            linker.add_node(source.unique_id)
        for node in manifest.nodes.values():
            self.link_node(linker, node, manifest)
        for exposure in manifest.exposures.values():
            self.link_node(linker, exposure, manifest)
        for metric in manifest.metrics.values():
            self.link_node(linker, metric, manifest)

        cycle = linker.find_cycles()

        if cycle:
            raise RuntimeError("Found a cycle: {}".format(cycle))

        if add_test_edges:
            manifest.build_parent_and_child_maps()
            self.add_test_edges(linker, manifest)

    def add_test_edges(self, linker: Linker, manifest: Manifest) -> None:
        """This method adds additional edges to the DAG. For a given non-test
        executable node, add an edge from an upstream test to the given node if
        the set of nodes the test depends on is a subset of the upstream nodes
        for the given node."""

        # Given a graph:
        # model1 --> model2 --> model3
        #   |             |
        #   |            \/
        #  \/          test 2
        # test1
        #
        # Produce the following graph:
        # model1 --> model2 --> model3
        #   |       /\    |      /\ /\
        #   |       |    \/      |  |
        #  \/       |  test2 ----|  |
        # test1 ----|---------------|

        for node_id in linker.graph:
            # If node is executable (in manifest.nodes) and does _not_
            # represent a test, continue.
            if (
                node_id in manifest.nodes
                and manifest.nodes[node_id].resource_type != NodeType.Test
            ):
                # Get *everything* upstream of the node
                all_upstream_nodes = nx.traversal.bfs_tree(linker.graph, node_id, reverse=True)
                # Get the set of upstream nodes not including the current node.
                upstream_nodes = set([n for n in all_upstream_nodes if n != node_id])

                # Get all tests that depend on any upstream nodes.
                upstream_tests = []
                for upstream_node in upstream_nodes:
                    upstream_tests += _get_tests_for_node(manifest, upstream_node)

                for upstream_test in upstream_tests:
                    # Get the set of all nodes that the test depends on
                    # including the upstream_node itself. This is necessary
                    # because tests can depend on multiple nodes (ex:
                    # relationship tests). Test nodes do not distinguish
                    # between what node the test is "testing" and what
                    # node(s) it depends on.
                    test_depends_on = set(manifest.nodes[upstream_test].depends_on_nodes)

                    # If the set of nodes that an upstream test depends on
                    # is a subset of all upstream nodes of the current node,
                    # add an edge from the upstream test to the current node.
                    if test_depends_on.issubset(upstream_nodes):
                        linker.graph.add_edge(upstream_test, node_id)

    def compile(self, manifest: Manifest, write=True, add_test_edges=False) -> Graph:
        self.initialize()
        linker = Linker()

        self.link_graph(linker, manifest, add_test_edges)

        stats = _generate_stats(manifest)

        if write:
            self.write_graph_file(linker, manifest)
        print_compile_stats(stats)

        return Graph(linker.graph)

    # writes the "compiled_code" into the target/compiled directory
    def _write_node(self, node: ManifestSQLNode) -> ManifestSQLNode:
        if not node.extra_ctes_injected or node.resource_type in (
            NodeType.Snapshot,
            NodeType.Seed,
        ):
            return node
        fire_event(WritingInjectedSQLForNode(node_info=get_node_info()))

        if node.compiled_code:
            node.compiled_path = node.write_node(
                self.config.target_path, "compiled", node.compiled_code
            )
        return node

    def compile_node(
        self,
        node: ManifestSQLNode,
        manifest: Manifest,
        extra_context: Optional[Dict[str, Any]] = None,
        write: bool = True,
    ) -> ManifestSQLNode:
        """This is the main entry point into this code. It's called by
        CompileRunner.compile, GenericRPCRunner.compile, and
        RunTask.get_hook_sql. It calls '_compile_code' to render
        the node's raw_code into compiled_code, and then calls the
        recursive method to "prepend" the ctes.
        """
        node = self._compile_code(node, manifest, extra_context)

        node, _ = self._recursively_prepend_ctes(node, manifest, extra_context)
        if write:
            self._write_node(node)
        return node
