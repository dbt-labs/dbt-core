import mock
import unittest

import dbt.utils

from dbt.compilation import Linker


class LinkerTest(unittest.TestCase):

    def setUp(self):
        self.real_is_blocking_dependency = dbt.utils.is_blocking_dependency
        self.linker = Linker()

        dbt.utils.is_blocking_dependency = mock.MagicMock(return_value=True)

    def tearDown(self):
        dbt.utils.is_blocking_dependency = self.real_is_blocking_dependency

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
        expected_limit = [['B']]
        self.assertEqual(expected_limit, actual_limit)

        actual_limit_2 = self.linker.as_dependency_list(['A', 'B'])
        expected_limit_2 = [['B'], ['A']]
        self.assertEqual(expected_limit_2, actual_limit_2)

    def test_linker_bad_limit_throws_runtime_error(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        self.assertRaises(RuntimeError,
                          self.linker.as_dependency_list, ['ZZZ'])

    def test__find_cycles__cycles(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'A')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        self.assertIsNotNone(self.linker.find_cycles())

    def test__find_cycles__no_cycles(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        self.assertIsNone(self.linker.find_cycles())
