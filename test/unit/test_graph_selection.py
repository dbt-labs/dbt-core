import unittest

import os
import string
import dbt.graph.selector as graph_selector
import dbt.project

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

        for node in self.package_graph:
            self.package_graph.node[node]['fqn'] = node.split('.')[1:]

        self.project = self.get_project()

    def get_project(self, extra_cfg=None):
        if extra_cfg is None:
            extra_cfg = {}

        cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': os.path.abspath('.'),
        }

        profiles = {
            'test': {
                'outputs': {
                    'test': {
                        'type': 'postgres',
                        'threads': 4,
                        'host': 'database',
                        'port': 5432,
                        'user': 'root',
                        'pass': 'password',
                        'dbname': 'dbt',
                        'schema': 'dbt_test'
                    }
                },
                'target': 'test'
            }
        }

        cfg.update(extra_cfg)

        project = dbt.project.Project(
            cfg=cfg,
            profiles=profiles,
            profiles_dir=None)

        project.validate()
        return project

    def run_specs_and_assert(self, graph, include, exclude, expected):
        selected = graph_selector.select_nodes(
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

    def parse_spec_and_assert(self, spec, parents, children, qualified_node_name):
        parsed = graph_selector.parse_spec(spec)
        self.assertEquals(
            parsed,
            {
                "select_parents": parents,
                "select_children": children,
                "qualified_node_name": qualified_node_name,
                "raw": spec
            }
        )

    def test__spec_parsing(self):
        self.parse_spec_and_assert('a', False, False, ['a'])
        self.parse_spec_and_assert('+a', True, False, ['a'])
        self.parse_spec_and_assert('a+', False, True, ['a'])
        self.parse_spec_and_assert('+a+', True, True, ['a'])

        self.parse_spec_and_assert('a.b', False, False, ['a', 'b'])
        self.parse_spec_and_assert('+a.b', True, False, ['a', 'b'])
        self.parse_spec_and_assert('a.b+', False, True, ['a', 'b'])
        self.parse_spec_and_assert('+a.b+', True, True, ['a', 'b'])

        self.parse_spec_and_assert('a.b.*', False, False, ['a', 'b', '*'])
        self.parse_spec_and_assert('+a.b.*', True, False, ['a', 'b', '*'])
        self.parse_spec_and_assert('a.b.*+', False, True, ['a', 'b', '*'])
        self.parse_spec_and_assert('+a.b.*+', True, True, ['a', 'b', '*'])

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
        test = self.assert_is_selected_node

        test(('X', 'a'), ('a'), True)
        test(('X', 'a'), ('X', 'a'), True)
        test(('X', 'a'), ('*'), True)
        test(('X', 'a'), ('X', '*'), True)

        test(('X', 'a', 'b', 'c'), ('X', '*'), True)
        test(('X', 'a', 'b', 'c'), ('X', 'a', '*'), True)
        test(('X', 'a', 'b', 'c'), ('X', 'a', 'b', '*'), True)
        test(('X', 'a', 'b', 'c'), ('X', 'a', 'b', 'c'), True)
        test(('X', 'a', 'b', 'c'), ('X', 'a'), True)
        test(('X', 'a', 'b', 'c'), ('X', 'a', 'b'), True)

        test(('X', 'a'), ('b'), False)
        test(('X', 'a'), ('X', 'b'), False)
        test(('X', 'a'), ('X', 'a', 'b'), False)
        test(('X', 'a'), ('Y', '*'), False)
