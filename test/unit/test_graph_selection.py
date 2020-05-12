import unittest
from unittest import mock

import string
import dbt.exceptions
import dbt.graph.selector as graph_selector

import networkx as nx


class BaseGraphSelectionTest(unittest.TestCase):
    def create_graph(self):
        raise NotImplementedError

    def add_tags(self, nodes):
        pass

    def setUp(self):
        self.package_graph = graph_selector.Graph(self.create_graph())
        nodes = {
            node: mock.MagicMock(fqn=node.split('.')[1:], tags=[])
            for node in self.package_graph
        }
        self.add_tags(nodes)
        self.manifest = mock.MagicMock(nodes=nodes)
        self.selector = graph_selector.NodeSelector(self.package_graph, self.manifest)


class GraphSelectionTest(BaseGraphSelectionTest):
    def create_graph(self):
        integer_graph = nx.balanced_tree(2, 2, nx.DiGraph())

        package_mapping = {
            i: 'm.' + ('X' if i % 2 == 0 else 'Y') + '.' + letter
            for (i, letter) in enumerate(string.ascii_lowercase)
        }

        # Edges: [(X.a, Y.b), (X.a, X.c), (Y.b, Y.d), (Y.b, X.e), (X.c, Y.f), (X.c, X.g)]
        return nx.relabel_nodes(integer_graph, package_mapping)

    def add_tags(self, nodes):
        nodes['m.X.a'].tags = ['abc']
        nodes['m.Y.b'].tags = ['abc', 'bcef']
        nodes['m.X.c'].tags = ['abc', 'bcef']
        nodes['m.Y.d'].tags = []
        nodes['m.X.e'].tags = ['efg', 'bcef']
        nodes['m.Y.f'].tags = ['efg', 'bcef']
        nodes['m.X.g'].tags = ['efg']

    def run_specs_and_assert(self, graph, include, exclude, expected):
        selected = self.selector.select_nodes(
            graph,
            include,
            exclude
        )

        self.assertEqual(selected, expected)

    def test__single_node_selection_in_package(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['X.a'],
            [],
            set(['m.X.a'])
        )

    def test__select_by_tag(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:abc'],
            [],
            set(['m.X.a', 'm.Y.b', 'm.X.c'])
        )

    def test__exclude_by_tag(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['*'],
            ['tag:abc'],
            set(['m.Y.d', 'm.X.e', 'm.Y.f', 'm.X.g'])
        )

    def test__select_by_tag_and_model_name(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:abc', 'a'],
            [],
            set(['m.X.a', 'm.Y.b', 'm.X.c'])
        )

        self.run_specs_and_assert(
            self.package_graph,
            ['tag:abc', 'd'],
            [],
            set(['m.X.a', 'm.Y.b', 'm.X.c', 'm.Y.d'])
        )

    def test__multiple_node_selection_in_package(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['X.a', 'b'],
            [],
            set(['m.X.a', 'm.Y.b'])
        )

    def test__select_children_except_in_package(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['X.a+'],
            ['b'],
            set(['m.X.a','m.X.c', 'm.Y.d','m.X.e','m.Y.f','m.X.g']))

    def test__select_children_except_tag(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['X.a+'],
            ['tag:efg'],
            set(['m.X.a','m.Y.b','m.X.c', 'm.Y.d']))

    def test__select_childrens_parents(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['@X.c'],
            [],
            set(['m.X.a', 'm.X.c', 'm.Y.f', 'm.X.g'])
        )

    def test__select_same_model_intersection(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['a,a'],
            [],
            set(['m.X.a'])
        )

    def test__select_layer_intersection(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['+c,c+'],
            [],
            set(['m.X.c'])
        )

    def test__select_intersection_lack(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['a,b'],
            [],
            set()
        )

    def test__select_tags_intersection(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:abc,tag:bcef'],
            [],
            set(['m.Y.b', 'm.X.c'])
        )

    def test__select_intersection_triple_descending(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['*,tag:abc,a'],
            [],
            set(['m.X.a'])
        )

    def test__select_intersection_triple_ascending(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['a,tag:abc,*'],
            [],
            set(['m.X.a'])
        )

    def test__select_intersection_with_exclusion(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:abc,tag:bcef'],
            ['c'],
            set(['m.Y.b'])
        )

    def test__select_intersection_exclude_intersection(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:bcef,tag:efg'],
            ['tag:bcef,@b'],
            set(['m.Y.f'])
        )

    def test__select_intersection_exclude_intersection_lack(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['tag:bcef,tag:efg'],
            ['tag:bcef,@a'],
            set()
        )

    def test__select_intersection_exclude_triple_intersection(self):
        self.run_specs_and_assert(
            self.package_graph,
            ['*,@a,+b'],
            ['*,tag:abc,tag:bcef'],
            set(['m.X.a'])
        )

    def parse_spec_and_assert(self, spec, parents, children, filter_type, filter_value, childrens_parents):
        parsed = graph_selector.SelectionCriteria(spec)
        self.assertEqual(parsed.select_parents, parents)
        self.assertEqual(parsed.select_children, children)
        self.assertEqual(parsed.selector_type, filter_type)
        self.assertEqual(parsed.selector_value, filter_value)
        self.assertEqual(parsed.select_childrens_parents, childrens_parents)

    def invalid_spec(self, spec):
        with self.assertRaises(dbt.exceptions.RuntimeException):
            graph_selector.SelectionCriteria(spec)

    def test__spec_parsing(self):
        self.parse_spec_and_assert('a', False, False, 'fqn', 'a', False)
        self.parse_spec_and_assert('+a', True, False, 'fqn', 'a', False)
        self.parse_spec_and_assert('a+', False, True, 'fqn', 'a', False)
        self.parse_spec_and_assert('+a+', True, True, 'fqn', 'a', False)
        self.parse_spec_and_assert('@a', False, False, 'fqn', 'a', True)
        self.invalid_spec('@a+')

        self.parse_spec_and_assert('a.b', False, False, 'fqn', 'a.b', False)
        self.parse_spec_and_assert('+a.b', True, False, 'fqn', 'a.b', False)
        self.parse_spec_and_assert('a.b+', False, True, 'fqn', 'a.b', False)
        self.parse_spec_and_assert('+a.b+', True, True, 'fqn', 'a.b', False)
        self.parse_spec_and_assert('@a.b', False, False, 'fqn', 'a.b', True)
        self.invalid_spec('@a.b+')

        self.parse_spec_and_assert('a.b.*', False, False, 'fqn', 'a.b.*', False)
        self.parse_spec_and_assert('+a.b.*', True, False, 'fqn', 'a.b.*', False)
        self.parse_spec_and_assert('a.b.*+', False, True, 'fqn', 'a.b.*', False)
        self.parse_spec_and_assert('+a.b.*+', True, True, 'fqn', 'a.b.*', False)
        self.parse_spec_and_assert('@a.b.*', False, False, 'fqn', 'a.b.*', True)
        self.invalid_spec('@a.b*+')

        self.parse_spec_and_assert('tag:a', False, False, 'tag', 'a', False)
        self.parse_spec_and_assert('+tag:a', True, False, 'tag', 'a', False)
        self.parse_spec_and_assert('tag:a+', False, True, 'tag', 'a', False)
        self.parse_spec_and_assert('+tag:a+', True, True, 'tag', 'a', False)
        self.parse_spec_and_assert('@tag:a', False, False, 'tag', 'a', True)
        self.invalid_spec('@tag:a+')

        self.parse_spec_and_assert('source:a', False, False, 'source', 'a', False)
        self.parse_spec_and_assert('source:a+', False, True, 'source', 'a', False)
        self.parse_spec_and_assert('@source:a', False, False, 'source', 'a', True)
        self.invalid_spec('@source:a+')

    def test__package_name_getter(self):
        found = graph_selector.get_package_names(self.package_graph.nodes())

        expected = set(['X', 'Y'])
        self.assertEqual(found, expected)

    def assert_is_selected_node(self, node, spec, should_work):
        self.assertEqual(
            graph_selector.is_selected_node(node, spec),
            should_work
        )

    def test__is_selected_node(self):
        self.assert_is_selected_node(('X', 'a'), ('a'), True)
        self.assert_is_selected_node(('X', 'a'), ('X', 'a'), True)
        self.assert_is_selected_node(('X', 'a'), ('*'), True)
        self.assert_is_selected_node(('X', 'a'), ('X', '*'), True)

        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', '*'), True)
        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', 'a', '*'), True)
        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', 'a', 'b', '*'), True)
        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', 'a', 'b', 'c'), True)
        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', 'a'), True)
        self.assert_is_selected_node(('X', 'a', 'b', 'c'), ('X', 'a', 'b'), True)

        self.assert_is_selected_node(('X', 'a'), ('b'), False)
        self.assert_is_selected_node(('X', 'a'), ('X', 'b'), False)
        self.assert_is_selected_node(('X', 'a'), ('X', 'a', 'b'), False)
        self.assert_is_selected_node(('X', 'a'), ('Y', '*'), False)
