import os
import tempfile
import unittest
from unittest import mock

from dbt import compilation
try:
    from queue import Empty
except ImportError:
    from Queue import Empty

from dbt.graph.selector import NodeSelector
from dbt.graph.cli import parse_difference


def _mock_manifest(nodes):
    config = mock.MagicMock(enabled=True)
    manifest = mock.MagicMock(nodes={
        n: mock.MagicMock(
            unique_id=n,
            package_name='pkg',
            name=n,
            empty=False,
            config=config,
            fqn=['pkg', n],
        ) for n in nodes
    })
    manifest.expect.side_effect = lambda n: mock.MagicMock(unique_id=n)
    return manifest


class LinkerTest(unittest.TestCase):

    def setUp(self):
        self.linker = compilation.Linker()

    def test_linker_add_node(self):
        expected_nodes = ['A', 'B', 'C']
        for node in expected_nodes:
            self.linker.add_node(node)

        actual_nodes = self.linker.nodes()
        for node in expected_nodes:
            self.assertIn(node, actual_nodes)

        self.assertEqual(len(actual_nodes), len(expected_nodes))

    def test_linker_write_graph(self):
        expected_nodes = ['A', 'B', 'C']
        for node in expected_nodes:
            self.linker.add_node(node)

        manifest = _mock_manifest('ABC')
        (fd, fname) = tempfile.mkstemp()
        os.close(fd)
        try:
            self.linker.write_graph(fname, manifest)
            assert os.path.exists(fname)
        finally:
            os.unlink(fname)

    def assert_would_join(self, queue):
        """test join() without timeout risk"""
        self.assertEqual(queue.inner.unfinished_tasks, 0)

    def _get_graph_queue(self, manifest, include=None, exclude=None):
        graph = compilation.Graph(self.linker.graph)
        selector = NodeSelector(graph, manifest)
        spec = parse_difference(include, exclude)
        return selector.get_graph_queue(spec)

    def test_linker_add_dependency(self):
        actual_deps = [('A', 'B'), ('A', 'C'), ('B', 'C')]

        for (l, r) in actual_deps:
            self.linker.dependency(l, r)

        queue = self._get_graph_queue(_mock_manifest('ABC'))

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

        queue = self._get_graph_queue(_mock_manifest('ABCZ'))
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

        queue = self._get_graph_queue(_mock_manifest('ABCD'), ['B'])
        got = queue.get(block=False)
        self.assertEqual(got.unique_id, 'B')
        self.assertTrue(queue.empty())
        queue.mark_done('B')
        self.assert_would_join(queue)

        queue_2 = queue = self._get_graph_queue(_mock_manifest('ABCD'), ['A', 'B'])
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