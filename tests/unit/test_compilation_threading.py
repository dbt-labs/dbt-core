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


class TestSetCteConcurrentDeduplication:
    def test_set_cte_concurrent_deduplication(self):
        """N threads call set_cte with the same id concurrently.
        Only one CTE should be appended."""
        node = _make_model_node()
        barrier = threading.Barrier(20)
        errors = []

        def call_set_cte():
            try:
                barrier.wait(timeout=5)
                node.set_cte("model.test.ephemeral", "select 1")
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=call_set_cte) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert not errors, f"Errors in threads: {errors}"
        assert (
            len(node.extra_ctes) == 1
        ), f"Expected 1 CTE, got {len(node.extra_ctes)}: {node.extra_ctes}"
        assert node.extra_ctes[0].id == "model.test.ephemeral"


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
