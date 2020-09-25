import threading
from queue import PriorityQueue
from typing import (
    Dict, Set, Optional
)

import networkx as nx  # type: ignore

from .graph import UniqueId
from dbt.contracts.graph.parsed import ParsedSourceDefinition, ParsedExposure
from dbt.contracts.graph.compiled import GraphMemberNode
from dbt.contracts.graph.manifest import Manifest
from dbt.node_types import NodeType


class GraphQueue:
    """A fancy queue that is backed by the dependency graph.
    Note: this will mutate input!

    This queue is thread-safe for `mark_done` calls, though you must ensure
    that separate threads do not call `.empty()` or `__len__()` and `.get()` at
    the same time, as there is an unlocked race!
    """
    def __init__(
        self, graph: nx.DiGraph, manifest: Manifest, selected: Set[UniqueId]
    ):
        self.graph = graph
        self.manifest = manifest
        self._selected = selected
        # store the queue as a priority queue.
        self.inner: PriorityQueue = PriorityQueue()
        # things that have been popped off the queue but not finished
        # and worker thread reservations
        self.in_progress: Set[UniqueId] = set()
        # things that are in the queue
        self.queued: Set[UniqueId] = set()
        # this lock controls most things
        self.lock = threading.Lock()
        # store the 'score' of each node as a number. Lower is higher priority.
        self._scores = self._calculate_scores()
        # populate the initial queue
        self._find_new_additions()
        # awaits after task end
        self.some_task_done = threading.Condition(self.lock)

    def get_selected_nodes(self) -> Set[UniqueId]:
        return self._selected.copy()

    def _include_in_cost(self, node_id: UniqueId) -> bool:
        node = self.manifest.expect(node_id)
        if node.resource_type != NodeType.Model:
            return False
        # must be a Model - tell mypy this won't be a Source or Exposure
        assert not isinstance(node, (ParsedSourceDefinition, ParsedExposure))
        if node.is_ephemeral:
            return False
        return True

    def _calculate_scores(self) -> Dict[UniqueId, int]:
        """Calculate the 'value' of each node in the graph based on how many
        blocking descendants it has. We use this score for the internal
        priority queue's ordering, so the quality of this metric is important.

        The score is stored as a negative number because the internal
        PriorityQueue picks lowest values first.

        We could do this in one pass over the graph instead of len(self.graph)
        passes but this is easy. For large graphs this may hurt performance.

        This operates on the graph, so it would require a lock if called from
        outside __init__.

        :return Dict[str, int]: The score dict, mapping unique IDs to integer
            scores. Lower scores are higher priority.
        """
        scores = {}
        for node in self.graph.nodes():
            score = -1 * len([
                d for d in nx.descendants(self.graph, node)
                if self._include_in_cost(d)
            ])
            scores[node] = score
        return scores

    def get(
        self, block: bool = True, timeout: Optional[float] = None
    ) -> GraphMemberNode:
        """Get a node off the inner priority queue. By default, this blocks.

        This takes the lock, but only for part of it.

        :param block: If True, block until the inner queue has data
        :param timeout: If set, block for timeout seconds waiting for data.
        :return: The node as present in the manifest.

        See `queue.PriorityQueue` for more information on `get()` behavior and
        exceptions.
        """
        _, node_id = self.inner.get(block=block, timeout=timeout)
        with self.lock:
            self._mark_in_progress(node_id)
        return self.manifest.expect(node_id)

    def __len__(self) -> int:
        """The length of the queue is the number of tasks left for the queue to
        give out, regardless of where they are. Incomplete tasks are not part
        of the length.

        This takes the lock.
        """
        with self.lock:
            return len(self.graph) - len(self.in_progress)

    def empty(self) -> bool:
        """The graph queue is 'empty' if it all remaining nodes in the graph
        are in progress.

        This takes the lock.
        """
        return len(self) == 0

    def _already_known(self, node: UniqueId) -> bool:
        """Decide if a node is already known (either handed out as a task, or
        in the queue).

        Callers must hold the lock.

        :param str node: The node ID to check
        :returns bool: If the node is in progress/queued.
        """
        return node in self.in_progress or node in self.queued

    def _find_new_additions(self) -> None:
        """Find any nodes in the graph that need to be added to the internal
        queue and add them.

        Callers must hold the lock.
        """
        for node, in_degree in self.graph.in_degree():
            if not self._already_known(node) and in_degree == 0:
                self.inner.put((self._scores[node], node))
                self.queued.add(node)

    def mark_done(self, node_id: UniqueId) -> None:
        """Given a node's unique ID, mark it as done.

        This method takes the lock.

        :param str node_id: The node ID to mark as complete.
        """
        with self.lock:
            self.in_progress.remove(node_id)
            self.graph.remove_node(node_id)
            self._find_new_additions()
            self.inner.task_done()
            self.some_task_done.notify_all()

    def _mark_in_progress(self, node_id: UniqueId) -> None:
        """Mark the node as 'in progress'.

        Callers must hold the lock.

        :param str node_id: The node ID to mark as in progress.
        """
        self.queued.remove(node_id)
        self.in_progress.add(node_id)

    def join(self) -> None:
        """Join the queue. Blocks until all tasks are marked as done.

        Make sure not to call this before the queue reports that it is empty.
        """
        self.inner.join()

    def wait_until_something_was_done(self) -> int:
        """Block until a task is done, then return the number of unfinished
        tasks.
        """
        with self.lock:
            self.some_task_done.wait()
            return self.inner.unfinished_tasks
