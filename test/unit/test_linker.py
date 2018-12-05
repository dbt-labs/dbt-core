import mock
import unittest

import dbt.utils

from dbt import linker
try:
    from queue import Empty
except KeyError:
    from Queue import Empty


def _mock_manifest(nodes):
    return mock.MagicMock(nodes={
        n: mock.MagicMock(unique_id=n) for n in nodes
    })

class LinkerTest(unittest.TestCase):

    def setUp(self):
        self.patcher = mock.patch.object(linker, 'is_blocking_dependency')
        self.is_blocking_dependency = self.patcher.start()
        self.is_blocking_dependency.return_value = True
        self.linker = linker.Linker()

    def tearDown(self):
        self.patcher.stop()

    def test_linker_add_node(self):
        expected_nodes = ['A', 'B', 'C']
        for node in expected_nodes:
            self.linker.add_node(node)

        actual_nodes = self.linker.nodes()
        for node in expected_nodes:
            self.assertIn(node, actual_nodes)

        self.assertEqual(len(actual_nodes), len(expected_nodes))

    def assert_would_join(self, queue):
        """test join() without timeout risk"""
        self.assertEqual(queue.inner.unfinished_tasks, 0)

    def test_linker_add_dependency(self):
        actual_deps = [('A', 'B'), ('A', 'C'), ('B', 'C')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        manifest = _mock_manifest('ABC')
        queue = self.linker.as_graph_queue(manifest)

        got = queue.get(block=False)
        self.assertEqual(got.unique_id, 'C')
        with self.assertRaises(Empty):
            queue.get(block=False)
        self.assertFalse(queue.empty())
        queue.mark_done('C')
        self.assertFalse(queue.empty())

        got = queue.get(block=False)
        self.assertEqual(got.unique_id, 'B')
        with self.assertRaises(Empty):
            queue.get(block=False)
        self.assertFalse(queue.empty())
        queue.mark_done('B')
        self.assertFalse(queue.empty())

        got = queue.get(block=False)
        self.assertEqual(got.unique_id, 'A')
        with self.assertRaises(Empty):
            queue.get(block=False)
        self.assertTrue(queue.empty())
        queue.mark_done('A')
        self.assert_would_join(queue)
        self.assertTrue(queue.empty())

    def test_linker_add_disjoint_dependencies(self):
        actual_deps = [('A', 'B')]
        additional_node = 'Z'

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)
        self.linker.add_node(additional_node)


        manifest = _mock_manifest('ABZ')
        queue = self.linker.as_graph_queue(manifest)

        # the first one we get must be B, it has the longest dep chain
        first = queue.get(block=False)
        self.assertEqual(first.unique_id, 'B')
        self.assertFalse(queue.empty())
        queue.mark_done('B')
        self.assertFalse(queue.empty())

        second = queue.get(block=False)
        self.assertIn(second.unique_id, {'A', 'Z'})
        self.assertFalse(queue.empty())
        queue.mark_done(second.unique_id)
        self.assertFalse(queue.empty())

        third = queue.get(block=False)
        self.assertIn(third.unique_id, {'A', 'Z'})
        with self.assertRaises(Empty):
            queue.get(block=False)
        self.assertNotEqual(second.unique_id, third.unique_id)
        self.assertTrue(queue.empty())
        queue.mark_done(third.unique_id)
        self.assert_would_join(queue)
        self.assertTrue(queue.empty())

    def test_linker_dependencies_limited_to_some_nodes(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        queue = self.linker.as_graph_queue(_mock_manifest('ABCD'), ['B'])
        got = queue.get(block=False)
        self.assertEqual(got.unique_id, 'B')
        self.assertTrue(queue.empty())
        queue.mark_done('B')
        self.assert_would_join(queue)

        queue_2 = self.linker.as_graph_queue(_mock_manifest('ABCD'), ['A', 'B'])
        got = queue_2.get(block=False)
        self.assertEqual(got.unique_id, 'B')
        self.assertFalse(queue_2.empty())
        with self.assertRaises(Empty):
            queue_2.get(block=False)
        queue_2.mark_done('B')
        self.assertFalse(queue_2.empty())

        got = queue_2.get(block=False)
        self.assertEqual(got.unique_id, 'A')
        self.assertTrue(queue_2.empty())
        with self.assertRaises(Empty):
            queue_2.get(block=False)
        self.assertTrue(queue_2.empty())
        queue_2.mark_done('A')
        self.assert_would_join(queue_2)

    def test_linker_bad_limit_throws_runtime_error(self):
        actual_deps = [('A', 'B'), ('B', 'C'), ('C', 'D')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        with self.assertRaises(RuntimeError):
            self.linker.as_graph_queue(_mock_manifest('ABCD'), ['ZZZ'])

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
