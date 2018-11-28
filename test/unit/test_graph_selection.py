import unittest
import mock

import os
import string
import dbt.graph.selector as graph_selector

import networkx as nx


class GraphSelectionTest(unittest.TestCase):

    def setUp(self):
        integer_graph = nx.balanced_tree(2, 2, nx.DiGraph())

        package_mapping = {
            i: 'm.' + ('X' if i % 2 == 0 else 'Y') + '.' + letter
            for (i, letter) in enumerate(string.ascii_lowercase)
        }

        # Edges: [(X.a, Y.b), (X.a, X.c), (Y.b, Y.d), (Y.b, X.e), (X.c, Y.f), (X.c, X.g)]
        self.package_graph = nx.relabel_nodes(integer_graph, package_mapping)
        nodes = {
            node: mock.MagicMock(fqn=node.split('.')[1:], tags=[])
            for node in self.package_graph
        }

        nodes['m.X.a'].tags = ['abc']
        nodes['m.Y.b'].tags = ['abc']
        nodes['m.X.c'].tags = ['abc']
        nodes['m.Y.d'].tags = []
        nodes['m.X.e'].tags = ['efg']
        nodes['m.Y.f'].tags = ['efg']
        nodes['m.X.g'].tags = ['efg']

        self.manifest = mock.MagicMock(nodes=nodes)
        self.linker = mock.MagicMock(graph=self.package_graph)
        self.selector = graph_selector.NodeSelector(self.linker, self.manifest)


    def run_specs_and_assert(self, graph, include, exclude, expected):
        selected = self.selector.select_nodes(
            graph,
            include,
            exclude
        )

        self.assertEquals(selected, expected)


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

    def parse_spec_and_assert(self, spec, parents, children, filter_type, filter_value):
        parsed = graph_selector.parse_spec(spec)
        self.assertEquals(
            parsed,
            {
                "select_parents": parents,
                "select_children": children,
                "filter": {
                    'type': filter_type,
                    'value': filter_value
                },
                "raw": spec
            }
        )

    def test__spec_parsing(self):
        self.parse_spec_and_assert('a', False, False, 'fqn', 'a')
        self.parse_spec_and_assert('+a', True, False, 'fqn', 'a')
        self.parse_spec_and_assert('a+', False, True, 'fqn', 'a')
        self.parse_spec_and_assert('+a+', True, True, 'fqn', 'a')

        self.parse_spec_and_assert('a.b', False, False, 'fqn', 'a.b')
        self.parse_spec_and_assert('+a.b', True, False, 'fqn', 'a.b')
        self.parse_spec_and_assert('a.b+', False, True, 'fqn', 'a.b')
        self.parse_spec_and_assert('+a.b+', True, True, 'fqn', 'a.b')

        self.parse_spec_and_assert('a.b.*', False, False, 'fqn', 'a.b.*')
        self.parse_spec_and_assert('+a.b.*', True, False, 'fqn', 'a.b.*')
        self.parse_spec_and_assert('a.b.*+', False, True, 'fqn', 'a.b.*')
        self.parse_spec_and_assert('+a.b.*+', True, True, 'fqn', 'a.b.*')

        self.parse_spec_and_assert('tag:a', False, False, 'tag', 'a')
        self.parse_spec_and_assert('+tag:a', True, False, 'tag', 'a')
        self.parse_spec_and_assert('tag:a+', False, True, 'tag', 'a')
        self.parse_spec_and_assert('+tag:a+', True, True, 'tag', 'a')

    def test__package_name_getter(self):
        found = graph_selector.get_package_names(self.package_graph)

        expected = set(['X', 'Y'])
        self.assertEquals(found, expected)

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
