
import networkx as nx
from collections import defaultdict

class Linker(object):
    def __init__(self, data=None):
        if data is None:
            data = {}
        self.graph = nx.DiGraph(**data)
        self.cte_map = defaultdict(set)

    def nodes(self):
        return self.graph.nodes()

    def run_type(self):
        return self.graph.graph['dbt_run_type']

    def get_node(self, node):
        return self.graph.node[node]

    def as_topological_ordering(self, limit_to=None):
        try:
            return nx.topological_sort(self.graph, nbunch=limit_to)
        except KeyError as e:
            raise RuntimeError("Couldn't find model '{}' -- does it exist or is it diabled?".format(e))
        except nx.exception.NetworkXUnfeasible as e:
            cycle = " --> ".join([".".join(node) for node in  nx.algorithms.find_cycle(self.graph)[0]])
            raise RuntimeError("Can't compile -- cycle exists in model graph\n{}".format(cycle))

    def as_dependency_list(self, limit_to=None):
        """returns a list of list of nodes, eg. [[0,1], [2], [4,5,6]]. Each element contains nodes whose
        dependenices are subsumed by the union of all lists before it. In this way, all nodes in list `i`
        can be run simultaneously assuming that all lists before list `i` have been completed"""

        if limit_to is None:
            graph_nodes = set(self.graph.nodes())
        else:
            graph_nodes = set()
            for node in limit_to:
                graph_nodes.add(node)
                if node in self.graph:
                    graph_nodes.update(nx.descendants(self.graph, node))
                else:
                    raise RuntimeError("Couldn't find model '{}' -- does it exist or is it diabled?".format(node))

        depth_nodes = defaultdict(list)

        for node in graph_nodes:
            num_ancestors = len(nx.ancestors(self.graph, node))
            depth_nodes[num_ancestors].append(node)

        dependency_list = []
        for depth in sorted(depth_nodes.keys()):
            dependency_list.append(depth_nodes[depth])

        return dependency_list

    def inject_cte(self, source, cte_model):
        self.cte_map[source].add(cte_model)

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
        nx.write_yaml(self.graph, outfile)

    def read_graph(self, infile):
        self.graph = nx.read_yaml(infile)

