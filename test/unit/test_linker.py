import unittest

from dbt.compilation import Linker

class LinkerTest(unittest.TestCase):

    def setUp(self):
        self.linker = Linker()

    def test_linker_add_node(self):
        expected_nodes = ['A', 'B', 'C']
        for node in expected_nodes:
            self.linker.add_node(node)

        actual_nodes = self.linker.nodes()
        for node in expected_nodes:
            self.assertIn(node, actual_nodes)

        self.assertEqual(len(actual_nodes), len(expected_nodes))

    def test_linker_add_dependency(self):
        actual_deps = [('A', 'B'), ('A', 'C'), ('B', 'C')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        expected_dep_list = [['C'], ['B'], ['A']]
        actual_dep_list = self.linker.as_dependency_list()
        self.assertEqual(expected_dep_list, actual_dep_list)

    def test_linker_add_disjoint_dependencies(self):
        actual_deps = [('A', 'B')]
        additional_node = 'Z'

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)
        self.linker.add_node(additional_node)

        # has to be one of these two
        possible = [
                [['Z', 'B'], ['A']],
                [['B', 'Z'], ['A']],
        ]

        actual = self.linker.as_dependency_list()

        for expected in possible:
            if expected == actual:
                return
        self.assertTrue(False, actual)

    def test_linker_dependencies_limited_to_some_nodes(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        actual_limit = self.linker.as_dependency_list(['B'])
        expected_limit = [['B'], ['A']]
        self.assertEqual(expected_limit, actual_limit)

    def test_linker_bad_limit_throws_runtime_error(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        self.assertRaises(RuntimeError, self.linker.as_dependency_list, ['ZZZ'])
