import networkx as nx
from collections import defaultdict
import threading

import dbt.utils
from dbt.compat import PriorityQueue


GRAPH_SERIALIZE_BLACKLIST = [
    'agate_table'
]


def from_file(graph_file):
    linker = Linker()
    linker.read_graph(graph_file)

    return linker


class GraphQueue(object):
    """A fancy queue that is backed by the dependency graph.
    Note: this will mutate input!
    """
    def __init__(self, graph, manifest):
        self.graph = graph
        self.manifest = manifest
        # store the queue as a priority queue.
        self.inner = PriorityQueue()
        # things that have been popped off the queue but not finished
        # and worker thread reservations
        self.in_progress = set()
        # things that are in the queue
        self.queued = set()
        # this lock controls most things
        self.lock = threading.Lock()
        # store the number of descendants for each node.
        self._descendants = self._calculate_descendants()
        # populate the initial queue
        self._find_new_additions()

    def get_node(self, node_id):
        return self.manifest.nodes[node_id]

    def _include_in_cost(self, node_id):
        node = self.get_node(node_id)
        if not dbt.utils.is_blocking_dependency(node):
            return False
        if node.get_materialization() == 'ephemeral':
            return False
        return True

    def _calculate_descendants(self):
        """We could do this in one pass over the graph instead of
        len(self.graph) passes but this is easy. For large graphs this may hurt
        performance.
        """
        costs = {}
        for node in self.graph.nodes():
            cost = len([
                d for d in nx.descendants(self.graph, node)
                if self._include_in_cost(d)
            ])
            costs[node] = cost
        return costs

    def get(self, block=True, timeout=None):
        """Get a node off the inner priority queue. By default, this blocks.
        """
        # It's unsafe to call get() and empty() from different threads, so
        # don't do that
        _, node_id = self.inner.get(block=block, timeout=timeout)
        with self.lock:
            self._mark_in_progress(node_id)
        return self.get_node(node_id)

    def __len__(self):
        with self.lock:
            return len(self.graph) - len(self.in_progress)

    def empty(self):
        """The graph queue is 'empty' if it all remaining nodes are in progress
        """
        return len(self) == 0

    def _already_known(self, node):
        """Callers must hold the lock
        """
        return node in self.in_progress or node in self.queued

    def _find_new_additions(self):
        """Find any nodes in the graph that need to be added to the internal
        queue.

        Note: callers must hold the lock
        """
        for node, in_degree in self.graph.in_degree_iter():
            if not self._already_known(node) and in_degree == 0:
                # lower score = more descendants = higher priority
                score = -1 * self._descendants[node]
                self.inner.put((score, node))
                self.queued.add(node)

    def mark_done(self, task_id):
        with self.lock:
            self.in_progress.remove(task_id)
            self.graph.remove_node(task_id)
            self._find_new_additions()
            self.inner.task_done()

    def _mark_in_progress(self, task_id):
        self.queued.remove(task_id)
        self.in_progress.add(task_id)

    def join(self):
        self.inner.join()


class Linker(object):
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

    def as_graph_queue(self, manifest, limit_to=None):
        """Returns a queue over nodes in the graph that tracks progress of
        dependecies.
        """
        if limit_to is None:
            graph_nodes = self.graph.nodes()
        else:
            graph_nodes = limit_to

        new_graph = nx.DiGraph(self.graph)

        to_remove = []
        graph_nodes_lookup = set(graph_nodes)
        for node in new_graph.nodes():
            if node not in graph_nodes_lookup:
                to_remove.append(node)

        for node in to_remove:
            new_graph.remove_node(node)

        for node in graph_nodes:
            if node not in new_graph:
                raise RuntimeError(
                    "Couldn't find model '{}' -- does it exist or is "
                    "it disabled?".format(node)
                )
        return GraphQueue(new_graph, manifest)

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
        # serialize() removes the agate table
        data = manifest.nodes[node_id].serialize()
        for key in GRAPH_SERIALIZE_BLACKLIST:
            if key in data:
                del data[key]
        graph.add_node(node_id, data)
    return graph
