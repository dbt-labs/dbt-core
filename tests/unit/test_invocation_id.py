from dbt.cli.main import dbtRunner


class TestInvocationId:
    def test_invocation_id(self):
        """Runs dbt programmatically twice, checking that invocation_id is
        consistent within an invocation, but changes for the second invocation."""

        runner = dbtRunner()

        # Run once...
        first_ids = set()
        runner.callbacks = [lambda e: first_ids.add(e.info.invocation_id)]
        runner.invoke(["debug"])

        # ...run twice...
        second_ids = set()
        runner.callbacks = [lambda e: second_ids.add(e.info.invocation_id)]
        runner.invoke(["debug"])

        # ...check that the results were nice.
        assert len(first_ids) == 1  # There was one consistent invocation_id for first run...
        assert len(second_ids) == 1  # ...as well as the second...
        assert len(first_ids.intersection(second_ids)) == 0
