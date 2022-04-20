from unittest.mock import ANY
from dbt.tests.util import AnyFloat


def expected_run_results():
    """
    The expected results of this run.
    """

    return [
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "model.test.model",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "model.test.second_model",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "seed.test.seed",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "snapshot.test.snapshot_seed",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "test.test.not_null_model_id.d01cc630e6",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "test.test.test_nothing_model_.5d38568946",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "test.test.unique_model_id.67b76558ff",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
    ]


def expected_references_run_results():
    return [
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "model.test.ephemeral_summary",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "model.test.view_summary",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "seed.test.seed",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
        {
            "status": "success",
            "message": None,
            "execution_time": AnyFloat(),
            "unique_id": "snapshot.test.snapshot_seed",
            "adapter_response": ANY,
            "thread_id": ANY,
            "timing": [ANY, ANY],
            "failures": ANY,
        },
    ]
