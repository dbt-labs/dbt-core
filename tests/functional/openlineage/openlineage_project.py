import json
import os
from unittest.mock import Mock

import pytest

from dbt.tests.util import run_dbt
from tests.functional.openlineage.fixtures import OpenLineageJaffleShopProject
from tests.openlineage_utils import assert_ol_events_match, ol_event_to_dict


class TestOpenLineage(OpenLineageJaffleShopProject):
    @pytest.mark.parametrize(
        "dbt_command, expected_ol_events_path, expect_pass",
        [
            (["run", "-s", "orders"], "./data/postgres/results/dbt_run_orders.json", True),
            (["test", "-s", "orders"], "./data/postgres/results/dbt_test_orders.json", True),
            (
                ["snapshot", "-s", "orders_snapshot"],
                "./data/postgres/results/dbt_snapshot_orders.json",
                True,
            ),
            (["build", "-s", "orders"], "./data/postgres/results/dbt_build_orders.json", True),
            # model has SQL syntax error
            (
                ["run", "-s", "orders_with_syntax_error"],
                "./data/postgres/results/dbt_run_orders_with_syntax_error.json",
                False,
            ),
            # data test fails
            (
                ["build", "-s", "orders_with_failed_test"],
                "./data/postgres/results/dbt_build_orders_with_failed_test.json",
                False,
            ),
            # snapshot has SQL syntax error
            (
                ["snapshot", "-s", "orders_snapshot_with_syntax_error"],
                "./data/postgres/results/dbt_snapshot_orders_snapshot_with_syntax_error.json",
                False,
            ),
            (
                ["seed", "-s", "raw_countries"],
                "./data/postgres/results/dbt_seed_raw_countries.json",
                True,
            ),
        ],
        ids=[
            "dbt_run_orders",
            "dbt_test_orders",
            "dbt_snapshot_orders",
            "dbt_build_orders",
            "dbt_run_orders_with_syntax_error",
            "dbt_build_orders_with_failed_test",
            "dbt_snapshot_orders_snapshot_with_syntax_error",
            "dbt_seed_raw_countries",
        ],
    )
    def test_openlineage_events(
        self,
        dbt_command,
        expected_ol_events_path,
        expect_pass,
        build_jaffle_shop_project,
        openlineage_handler_with_dummy_emit,
        openlineage_handler_with_raise_exception,
    ):

        def get_expected_ol_events(expected_ol_events_path):
            jsonl_file_path = os.path.join(os.path.dirname(__file__), expected_ol_events_path)
            return json.loads(open(jsonl_file_path, "r").read())

        run_dbt(dbt_command, expect_pass=expect_pass)

        expected_ol_events = get_expected_ol_events(expected_ol_events_path)
        actual_ol_events = [
            ol_event_to_dict(e)
            for e in openlineage_handler_with_dummy_emit.emitted_events
            if e is not None
        ]

        assert_ol_events_match(expected_event=expected_ol_events, actual_event=actual_ol_events)

    def test_openlineage_doesnt_make_dbt_fail(self, build_jaffle_shop_project, monkeypatch):
        def raise_exception(*args, **kwargs):
            raise Exception("Fake exception raised in OpenLineage")

        dummy_handle_exception = Mock()
        monkeypatch.setattr(
            "dbt.openlineage.handler.OpenLineageHandler.handle_unsafe", raise_exception
        )
        monkeypatch.setattr(
            "dbt.openlineage.handler.OpenLineageHandler._handle_exception", dummy_handle_exception
        )

        dbt_command = ["run", "-s", "orders"]
        run_dbt(dbt_command, expect_pass=True)

        assert dummy_handle_exception.call_count
