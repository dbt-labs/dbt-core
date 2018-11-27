import networkx as nx
from collections import defaultdict

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
    def __init__(self, graph):
        self.graph = graph
        # the initial size of the graph, as the PriorityQueue class picks the
        # lowest value but we want the highest. We need this so we don't have
        # to re-score all queue entries every time we remove something from
        # the graph
        self._initial_size = len(graph)
        # store the queue as a priority queue.
        self.inner = PriorityQueue()
        # things that have been popped off the queue but not finished
        self.in_progress = set()
        # things that are in the queue
        self.queued = set()
        self.mutex = threading.Lock()
        # store the number of descendants for each node.
        self._descendants = self._calculate_descendants()

    def _is_ephemeral(self, node):
        materialization = dbt.utils.get_materialization(self.node.node[node])
        return materialization == 'ephemeral'

    def _include_in_count(self, node):
        """TODO: I'm pretty sure this is really inefficient."""
        if not dbt.utils.is_blocking_dependency(self.node.node[node]):
            return False
        if self._is_ephemeral(node):
            return False
        return True

    def _calculate_descendants(self):
        """We could do this in one pass over the graph instead of
        len(self.graph) passes but this is easy. For large graphs this may hurt
        performance.
        """
        descendants = {}
        for node in self.graph.nodes():
            count = len(
                d for d in nx.descendants(self.graph, node)
                if self._include_in_count(d)
            )
            descendants[node] = count
        return descendants

    def get(self):
        """Get a node off the inner priority queue. If a node is ephemeral,
        mark it done immediately and get again. This blocks!
        """
        task_id = self.inner.get()

    def empty(self):
        """The graph queue is 'empty' if it all remaining nodes are in progress
        """
        return (len(self.graph) - len(self.in_progress)) == 0

    def _find_new_additions(self):
        """Find any nodes in the graph that need to be added to the internal
        queue.
        """
        for node, in_degree in self.graph.in_degree_iter():
            if node not in (self.in_progress | self.queued) and in_degree == 0:
                # lower score = more descendants = higher priority
                score = self._initial_size - self._descendants[node]
                self.inner.put((score, node))
                self.queued.add(node)

    def mark_done(self, task_id):
        with self.mutex:
            self.in_progress.remove(task_id)
            self.graph.remove_node(task_id)
            self._find_new_additions()
            self.queue.task_done()

    def _mark_in_progress(self, task_id):
        self.queued.remove(task_id)
        self.in_progress.add(task_id)

    def get(self):
        with self.mutex:
            value = self.inner.get()

    def join(self):
        self.queue.join()


class Linker(object):
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.graph = nx.DiGraph(**data)

    def edges(self):
        return self.graph.edges()

    def nodes(self):
        return self.graph.nodes()

    def get_node(self, node):
        return self.graph.node[node]

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

    def as_graph_queue(self, limit_to=None, ephemeral_only=False):
        """Returns a queue over nodes in the graph that tracks progress of
        dependecies.

        TODO: handle/understand ephemeral_only / is_blocking_dependency
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
            new_graph.remove(node)

        for node in graph_nodes:
            if node not in new_graph:
                raise RuntimeError(
                    "Couldn't find model '{}' -- does it exist or is "
                    "it disabled?".format(node)
                )
        return GraphQueue(new_graph)

    def as_dependency_list(self, limit_to=None, ephemeral_only=False):
        """returns a list of list of nodes, eg. [[0,1], [2], [4,5,6]]. Each
        element contains nodes whose dependenices are subsumed by the union of
        all lists before it. In this way, all nodes in list `i` can be run
        simultaneously assuming that all lists before list `i` have been
        completed"""

        depth_nodes = defaultdict(list)

        if limit_to is None:
            graph_nodes = self.graph.nodes()
        else:
            graph_nodes = limit_to

        for node in graph_nodes:
            if node not in self.graph:
                raise RuntimeError(
                    "Couldn't find model '{}' -- does it exist or is "
                    "it disabled?".format(node)
                )

            num_ancestors = len([
                ancestor for ancestor in
                nx.ancestors(self.graph, node)
                if (dbt.utils.is_blocking_dependency(
                        self.get_node(ancestor)) and
                    (ephemeral_only is False or
                     dbt.utils.get_materialization(
                         self.get_node(ancestor)) == 'ephemeral'))
            ])

            depth_nodes[num_ancestors].append(node)

        dependency_list = []
        for depth in sorted(depth_nodes.keys()):
            dependency_list.append(depth_nodes[depth])

        return dependency_list

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

    def update_node_data(self, node, data):
        self.graph.add_node(node, data)

    def write_graph(self, outfile):
        out_graph = self.remove_blacklisted_attributes_from_nodes(self.graph)
        nx.write_gpickle(out_graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_gpickle(infile)

    @classmethod
    def remove_blacklisted_attributes_from_nodes(cls, graph):
        graph = graph.copy()
        for node_name, node in graph.node.items():
            slim_node = node.copy()
            for key in GRAPH_SERIALIZE_BLACKLIST:
                if key in slim_node:
                    del slim_node[key]

            graph.node[node_name] = slim_node
        return graph
