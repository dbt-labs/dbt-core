import signal
import threading
import time

import networkx as nx
import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.graph.queue import GraphQueue
from tests.unit.utils import MockNode, make_manifest


class TestGraphQueue:
    @pytest.fixture(scope="class")
    def manifest(self) -> Manifest:
        return make_manifest(
            nodes=[
                MockNode(package="test_package", name="upstream_model"),
                MockNode(package="test_package", name="downstream_model"),
            ]
        )

    @pytest.fixture(scope="class")
    def graph(self) -> nx.DiGraph:
        graph = nx.DiGraph()
        graph.add_edge("model.test_package.upstream_model", "model.test_package.downstream_model")
        return graph

    def test_init_graph_queue(self, manifest, graph):
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})

        assert graph_queue.manifest == manifest
        assert graph_queue.graph == graph
        assert graph_queue.inner.queue == [(0, "model.test_package.upstream_model")]
        assert graph_queue.in_progress == set()
        assert graph_queue.queued == {"model.test_package.upstream_model"}
        assert graph_queue.lock

    def test_init_graph_queue_preserve_edges_false(self, manifest, graph):
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={}, preserve_edges=False)

        # when preserve_edges is set to false, dependencies between nodes are no longer tracked in the priority queue
        assert list(graph_queue.graph.edges) == []
        assert graph_queue.inner.queue == [
            (0, "model.test_package.downstream_model"),
            (0, "model.test_package.upstream_model"),
        ]
        assert graph_queue.queued == {
            "model.test_package.upstream_model",
            "model.test_package.downstream_model",
        }

    def test_join_with_timeout_returns_while_tasks_unfinished(self, manifest, graph):
        # A bounded join must return instead of blocking forever when work
        # is still pending.
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})
        assert graph_queue.inner.unfinished_tasks  # sanity: work is pending

        start = time.time()
        graph_queue.join(timeout=0.05)
        elapsed = time.time() - start

        assert elapsed < 2  # returned promptly rather than hanging
        assert graph_queue.inner.unfinished_tasks  # tasks are still not done

    def test_wait_until_something_was_done_with_timeout_returns(self, manifest, graph):
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})

        start = time.time()
        remaining = graph_queue.wait_until_something_was_done(timeout=0.05)
        elapsed = time.time() - start

        assert elapsed < 2
        assert remaining == graph_queue.inner.unfinished_tasks

    @pytest.mark.skipif(
        not hasattr(signal, "pthread_kill"), reason="requires pthread_kill (Unix only)"
    )
    @pytest.mark.parametrize("wait_style", ["join", "fail_fast"])
    def test_bounded_waits_service_sigint_from_non_main_thread(self, manifest, graph, wait_style):
        """SIGINT delivered to a non-main thread must still cancel a run.

        The kernel may hand a process-directed signal to any thread, but the
        Python handler only raises KeyboardInterrupt on the main thread. A
        bounded wait lets the main thread return to the interpreter and service
        the pending signal; an unbounded wait would hang forever. We force the
        losing case deterministically with pthread_kill against a worker thread.
        """
        graph_queue = GraphQueue(graph=graph, manifest=manifest, selected={})
        assert graph_queue.inner.unfinished_tasks  # ensure the wait blocks

        main_ident = threading.get_ident()
        stop = threading.Event()
        worker_ready = threading.Event()

        def worker():
            worker_ready.set()
            while not stop.is_set():
                time.sleep(0.02)

        w = threading.Thread(target=worker, daemon=True)
        w.start()
        worker_ready.wait()
        assert w.ident is not None and w.ident != main_ident

        def send_sigint():
            time.sleep(0.3)
            signal.pthread_kill(w.ident, signal.SIGINT)  # deliver to NON-main thread

        # Watchdog: if the wait is silently unbounded (a regressed fix), force it
        # to return so the test fails on the assert below instead of hanging CI.
        released = threading.Event()

        def watchdog():
            if not released.wait(timeout=8):
                graph_queue.inner.task_done()  # drops unfinished_tasks -> join returns
                with graph_queue.lock:
                    graph_queue.some_task_done.notify_all()  # release fail-fast wait

        def do_wait():
            if wait_style == "join":
                while graph_queue.inner.unfinished_tasks:
                    graph_queue.join(timeout=0.1)
            else:
                while graph_queue.wait_until_something_was_done(timeout=0.1):
                    pass

        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, signal.default_int_handler)
        interrupted = False
        try:
            threading.Thread(target=watchdog, daemon=True).start()
            threading.Thread(target=send_sigint, daemon=True).start()
            start = time.time()
            try:
                do_wait()
            except KeyboardInterrupt:
                interrupted = True
            elapsed = time.time() - start
        finally:
            released.set()
            stop.set()
            signal.signal(signal.SIGINT, original_handler)

        assert interrupted, "SIGINT to a non-main thread was not serviced"
        assert elapsed < 5  # cancelled quickly, did not hang
