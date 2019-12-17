from queue import PriorityQueue
from typing import Iterable, Set, Optional
import networkx as nx  # type: ignore
import threading


from dbt.contracts.graph.manifest import Manifest
from dbt.node_types import NodeType


def from_file(graph_file):
    linker = Linker()
    linker.read_graph(graph_file)

    return linker


def is_blocking_dependency(node):
    return node.resource_type == NodeType.Model


def is_ephemeral_dependency(node):
    return (node.resource_type == NodeType.Model and
            node.get_materialization() == 'ephemeral')


class GraphQueue:
    """A fancy queue that is backed by the dependency graph.
    Note: this will mutate input!

    This queue is thread-safe for `mark_done` calls, though you must ensure
    that separate threads do not call `.empty()` or `__len__()` and `.get()` at
    the same time, as there is an unlocked race!
    """
    def __init__(self, graph, manifest):
        self.graph = graph
        self.manifest = manifest
        # store the queue as a priority queue.
        self.inner: PriorityQueue = PriorityQueue()
        # things that have been popped off the queue but not finished
        # and worker thread reservations
        self.in_progress = set()
        # things that are in the queue
        self.queued = set()
        # this lock controls most things
        self.lock = threading.Lock()
        # store the 'score' of each node as a number. Lower is higher priority.
        self._scores = self._calculate_scores()
        # populate the initial queue
        self._find_new_additions()

    def _include_in_cost(self, node_id):
        node = self.manifest.expect(node_id)
        if not is_blocking_dependency(node):
            return False
        if node.get_materialization() == 'ephemeral':
            return False
        return True

    def _calculate_scores(self):
        """Calculate the 'value' of each node in the graph based on how many
        blocking descendants it has. We use this score for the internal
        priority queue's ordering, so the quality of this metric is important.

        The score is stored as a negative number because the internal
        PriorityQueue picks lowest values first.

        We could do this in one pass over the graph instead of len(self.graph)
        passes but this is easy. For large graphs this may hurt performance.

        This operates on the graph, so it would require a lock if called from
        outside __init__.

        :return Dict[str, int]: The score dict, mapping unique IDs to integer
            scores. Lower scores are higher priority.
        """
        scores = {}
        for node in self.graph.nodes():
            score = -1 * len([
                d for d in nx.descendants(self.graph, node)
                if self._include_in_cost(d)
            ])
            scores[node] = score
        return scores

    def get(self, block=True, timeout=None):
        """Get a node off the inner priority queue. By default, this blocks.

        This takes the lock, but only for part of it.

        :param bool block: If True, block until the inner queue has data
        :param Optional[float] timeout: If set, block for timeout seconds
            waiting for data.
        :return ParsedNode: The node as present in the manifest.

        See `queue.PriorityQueue` for more information on `get()` behavior and
        exceptions.
        """
        _, node_id = self.inner.get(block=block, timeout=timeout)
        with self.lock:
            self._mark_in_progress(node_id)
        return self.manifest.expect(node_id)

    def __len__(self):
        """The length of the queue is the number of tasks left for the queue to
        give out, regardless of where they are. Incomplete tasks are not part
        of the length.

        This takes the lock.
        """
        with self.lock:
            return len(self.graph) - len(self.in_progress)

    def empty(self):
        """The graph queue is 'empty' if it all remaining nodes in the graph
        are in progress.

        This takes the lock.
        """
        return len(self) == 0

    def _already_known(self, node):
        """Decide if a node is already known (either handed out as a task, or
        in the queue).

        Callers must hold the lock.

        :param str node: The node ID to check
        :returns bool: If the node is in progress/queued.
        """
        return node in self.in_progress or node in self.queued

    def _find_new_additions(self):
        """Find any nodes in the graph that need to be added to the internal
        queue and add them.

        Callers must hold the lock.
        """
        for node, in_degree in self.graph.in_degree():
            if not self._already_known(node) and in_degree == 0:
                self.inner.put((self._scores[node], node))
                self.queued.add(node)

    def mark_done(self, node_id):
        """Given a node's unique ID, mark it as done.

        This method takes the lock.

        :param str node_id: The node ID to mark as complete.
        """
        with self.lock:
            self.in_progress.remove(node_id)
            self.graph.remove_node(node_id)
            self._find_new_additions()
            self.inner.task_done()

    def _mark_in_progress(self, node_id):
        """Mark the node as 'in progress'.

        Callers must hold the lock.

        :param str node_id: The node ID to mark as in progress.
        """
        self.queued.remove(node_id)
        self.in_progress.add(node_id)

    def join(self):
        """Join the queue. Blocks until all tasks are marked as done.

        Make sure not to call this before the queue reports that it is empty.
        """
        self.inner.join()


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
        # There's a networkx find_cycle function, but there's a bug in the
        # nx 1.11 release that prevents us from using it. We should use that
        # function when we upgrade to 2.X. More info:
        #     https://github.com/networkx/networkx/pull/2473
        cycles = list(nx.simple_cycles(self.graph))

        if len(cycles) > 0:
            cycle_nodes = cycles[0]
            cycle_nodes.append(cycle_nodes[0])
            return " --> ".join(cycle_nodes)

        return None

    def build_subset_graph(self, include_nodes: Iterable[str]):
        """Create and return a new graph that is a shallow copy of the graph,
        but with only the nodes in include_nodes. Transitive edges across
        removed nodes are preserved as explicit new edges.
        """
        new_graph = nx.algorithms.transitive_closure(self.graph)

        include_nodes = set(include_nodes)

        for node in self.graph.nodes():
            if node not in include_nodes:
                new_graph.remove_node(node)

        for node in include_nodes:
            if node not in new_graph:
                raise RuntimeError(
                    "Couldn't find model '{}' -- does it exist or is "
                    "it disabled?".format(node)
                )
        return new_graph

    def as_graph_queue(
        self, manifest: Manifest, limit_to: Optional[Iterable[str]] = None
    ) -> GraphQueue:
        """Returns a queue over nodes in the graph that tracks progress of
        dependecies.
        """
        if limit_to is None:
            graph_nodes = self.graph.nodes()
        else:
            graph_nodes = limit_to

        new_graph = self.build_subset_graph(graph_nodes)
        return GraphQueue(new_graph, manifest)

    def sorted_ephemeral_ancestors(
        self, manifest: Manifest, unique_id: str
    ) -> Iterable[str]:
        """Get the ephemeral ancestors of unique_id, stopping at the first
        non-ephemeral node in each chain, in graph-topological order.
        """
        to_check: Set[str] = {unique_id}
        ephemerals: Set[str] = set()
        visited: Set[str] = set()

        while to_check:
            # note that this avoids collecting unique_id itself
            nextval = to_check.pop()
            for pred in self.graph.predecessors(nextval):
                if pred in visited:
                    continue
                visited.add(pred)
                node = manifest.expect(pred)

                if node.resource_type != NodeType.Model:
                    continue
                if node.get_materialization() != 'ephemeral':  # type: ignore
                    continue
                # this is an ephemeral model! We have to find everything it
                # refs and do it all over again until we exhaust them all
                ephemerals.add(pred)
                to_check.add(pred)

        ephemeral_graph = self.build_subset_graph(ephemerals)
        # we can just topo sort this because we know there are no cycles.
        return nx.topological_sort(ephemeral_graph)

    def get_dependent_nodes(self, node):
        return nx.descendants(self.graph, node)

    def dependency(self, node1, node2):
        "indicate that node1 depends on node2"
        self.graph.add_node(node1)
        self.graph.add_node(node2)
        self.graph.add_edge(node2, node1)

    def add_node(self, node):
        self.graph.add_node(node)

    def remove_node(self, node):
        children = nx.descendants(self.graph, node)
        self.graph.remove_node(node)
        return children

    def write_graph(self, outfile, manifest):
        """Write the graph to a gpickle file. Before doing so, serialize and
        include all nodes in their corresponding graph entries.
        """
        out_graph = _updated_graph(self.graph, manifest)
        nx.write_gpickle(out_graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_gpickle(infile)


def _updated_graph(graph, manifest):
    graph = graph.copy()
    for node_id in graph.nodes():
        data = manifest.expect(node_id).to_dict()
        graph.add_node(node_id, **data)
    return graph
