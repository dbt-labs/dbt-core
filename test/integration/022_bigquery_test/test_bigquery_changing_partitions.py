from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import json

class TestChangingPartitions(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "partition-models"

    def test_change(self, before, after):
        results = self.run_dbt(['run', '--vars', json.dumps(before)])
        self.assertEqual(len(results), 1)

        results = self.run_dbt(['run', '--vars', json.dumps(after)])
        self.assertEqual(len(results), 1)

    def test_add_partition(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": "date(cur_time)", "cluster_by": None}
        self.test_change(before, after)

    def test_remove_partition(self):
        before = {"partition_by": "date(cur_time)", "cluster_by": None}
        after = {"partition_by": None, "cluster_by": None}
        self.test_change(before, after)

    def test_change_partitions(self):
        before = {"partition_by": "date(cur_time)", "cluster_by": None}
        after = {"partition_by": "cur_date", "cluster_by": None}
        self.test_change(before, after)

    def test_add_clustering(self):
        before = {"partition_by": "date(cur_time)", "cluster_by": None}
        after = {"partition_by": "cur_date", "cluster_by": "id"}
        self.test_change(before, after)

    def test_remove_clustering(self):
        before = {"partition_by": "date(cur_time)", "cluster_by": "id"}
        after = {"partition_by": "cur_date", "cluster_by": None}
        self.test_change(before, after)

    def test_change_clustering(self):
        before = {"partition_by": "date(cur_time)", "cluster_by": "id"}
        after = {"partition_by": "cur_date", "cluster_by": "name"}
        self.test_change(before, after)
