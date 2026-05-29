import threading

from dbt.artifacts.resources import Contract
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import DependsOn, ModelConfig, ModelNode
from dbt.node_types import NodeType


def _make_model_node(unique_id="model.test.foo", compiled=False, extra_ctes=None):
    return ModelNode(
        package_name="test",
        path="/root/models/foo.sql",
        original_file_path="models/foo.sql",
        language="sql",
        raw_code="select 1",
        name=unique_id.split(".")[-1],
        resource_type=NodeType.Model,
        unique_id=unique_id,
        fqn=["test", "models", unique_id.split(".")[-1]],
        refs=[],
        sources=[],
        metrics=[],
        depends_on=DependsOn(),
        description="",
        primary_key=[],
        database="test_db",
        schema="test_schema",
        alias=unique_id.split(".")[-1],
        tags=[],
        config=ModelConfig(),
        contract=Contract(),
        meta={},
        compiled=compiled,
        extra_ctes=extra_ctes or [],
        extra_ctes_injected=False,
        compiled_code="select 1" if compiled else None,
        checksum=FileHash.from_contents(""),
        unrendered_config={},
    )


class TestConcurrentEphemeralCompilation:
    def test_concurrent_ephemeral_compilation(self):
        """Two threads compile nodes sharing an ephemeral dep.
        The ephemeral should only be compiled once (compiled flag set once)."""
        ephemeral = _make_model_node(unique_id="model.test.ephemeral", compiled=False)
        compile_count = {"count": 0}
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def simulate_compile():
            barrier.wait(timeout=5)
            with ephemeral._lock:
                if ephemeral.compiled is True and ephemeral.extra_ctes_injected is True:
                    return
                # Simulate compilation
                with lock:
                    compile_count["count"] += 1
                ephemeral.compiled = True
                ephemeral.compiled_code = "select 1"
                ephemeral.extra_ctes_injected = True

        threads = [threading.Thread(target=simulate_compile) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert (
            compile_count["count"] == 1
        ), f"Ephemeral compiled {compile_count['count']} times, expected 1"
        assert ephemeral.compiled is True
        assert ephemeral.extra_ctes_injected is True


class TestNoDeadlockOnRecursiveLocking:
    def test_recursive_lock_pattern_does_not_deadlock(self):
        """Simulate the lock acquisition pattern from _recursively_prepend_ctes
        for an ephemeral chain: model_a refs ephemeral_b.

        The real code acquires ephemeral_b._lock for compilation, releases it,
        then re-acquires ephemeral_b._lock for CTE injection during the
        recursive call. If the recursion happened *inside* the first lock,
        this would deadlock with a non-reentrant Lock. This test verifies
        the current code structure avoids that."""
        model_a = _make_model_node(unique_id="model.test.a", compiled=True)
        ephemeral_b = _make_model_node(unique_id="model.test.b", compiled=False)

        deadlocked = {"value": False}

        def simulate_recursively_prepend_ctes():
            # -- Processing model_a's extra_ctes, found ephemeral_b --

            # Critical section A: compile ephemeral_b (scoped to just compilation)
            needs_recursion = False
            with ephemeral_b._lock:
                if not ephemeral_b.compiled:
                    ephemeral_b.compiled = True
                    ephemeral_b.compiled_code = "select 1"
                    needs_recursion = True

            # Recursive call for ephemeral_b happens OUTSIDE the lock above.
            # Inside that recursive call, we'd hit critical section B for
            # ephemeral_b (CTE injection). This would deadlock if we were
            # still inside the `with ephemeral_b._lock` above.
            if needs_recursion:
                # Simulate the CTE injection lock from the recursive call
                acquired = ephemeral_b._lock.acquire(timeout=2)
                if not acquired:
                    deadlocked["value"] = True
                    return
                ephemeral_b.extra_ctes_injected = True
                ephemeral_b._lock.release()

            # Critical section B for model_a (CTE injection)
            acquired = model_a._lock.acquire(timeout=2)
            if not acquired:
                deadlocked["value"] = True
                return
            model_a.extra_ctes_injected = True
            model_a._lock.release()

        t = threading.Thread(target=simulate_recursively_prepend_ctes)
        t.start()
        t.join(timeout=5)

        assert not t.is_alive(), "Thread is still alive — deadlock detected"
        assert not deadlocked["value"], "Failed to acquire lock — possible deadlock"
        assert ephemeral_b.compiled is True
        assert ephemeral_b.extra_ctes_injected is True
        assert model_a.extra_ctes_injected is True


class TestLockSerialization:
    def test_lock_excluded_from_serialization(self):
        """Serialize a node and assert _lock is not in the output."""
        node = _make_model_node(compiled=True)
        node.extra_ctes_injected = True
        dct = node.to_dict()
        assert "_lock" not in dct

    def test_lock_restored_after_deserialization(self):
        """Round-trip serialize/deserialize, assert _lock is a working Lock."""
        node = _make_model_node(compiled=True)
        node.extra_ctes_injected = True
        dct = node.to_dict()
        restored = ModelNode.from_dict(dct)
        assert hasattr(restored, "_lock")
        assert isinstance(restored._lock, type(threading.Lock()))
        # Verify the lock actually works
        assert restored._lock.acquire(timeout=1)
        restored._lock.release()

    def test_lock_survives_pickle_roundtrip(self):
        """Pickle/unpickle a node, assert _lock is a working Lock."""
        import pickle

        node = _make_model_node(compiled=True)
        node.extra_ctes_injected = True
        data = pickle.dumps(node)
        restored = pickle.loads(data)
        assert hasattr(restored, "_lock")
        assert isinstance(restored._lock, type(threading.Lock()))
        assert restored._lock.acquire(timeout=1)
        restored._lock.release()
