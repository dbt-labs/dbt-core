from test.integration.base import DBTIntegrationTest, FakeArgs, use_profile
import json


class TestChangingPartitions(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test_022"

    @property
    def models(self):
        return "partition-models"

    def run_changes(self, before, after):
        results = self.run_dbt(['run', '--vars', json.dumps(before)])
        self.assertEqual(len(results), 1)

        results = self.run_dbt(['run', '--vars', json.dumps(after)])
        self.assertEqual(len(results), 1)

    def test_partitions(self, expected):
        test_results = self.run_dbt(['test', '--vars', json.dumps(expected)])

        for result in test_results:
            self.assertIsNone(result.error)
            self.assertFalse(result.skipped)
            # status = # of failing rows
            self.assertEqual(result.status, 0)

    @use_profile('bigquery')
    def test_bigquery_add_partition(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_add_partition_year(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp', 'granularity': 'year'}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_add_partition_month(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp', 'granularity': 'month'}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_add_partition_hour(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp', 'granularity': 'hour'}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_add_partition_hour(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp', 'granularity': 'day'}, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp', 'granularity': 'hour'}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_remove_partition(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": None}
        after = {"partition_by": None, "cluster_by": None}
        self.run_changes(before, after)

    @use_profile('bigquery')
    def test_bigquery_change_partitions(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": None}
        after = {"partition_by": {'field': "cur_date"}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})
        self.run_changes(after, before)
        self.test_partitions({"expected": 1})

    @use_profile('bigquery')
    def test_bigquery_change_partitions_from_int(self):
        before = {"partition_by": {"field": "id", "data_type": "int64", "range": {"start": 0, "end": 10, "interval": 1}}, "cluster_by": None}
        after = {"partition_by": {"field": "cur_date", "data_type": "date"}, "cluster_by": None}
        self.run_changes(before, after)
        self.test_partitions({"expected": 1})
        self.run_changes(after, before)
        self.test_partitions({"expected": 2})

    @use_profile('bigquery')
    def test_bigquery_add_clustering(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": None}
        after = {"partition_by": {'field': "cur_date"}, "cluster_by": "id"}
        self.run_changes(before, after)

    @use_profile('bigquery')
    def test_bigquery_remove_clustering(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": "id"}
        after = {"partition_by": {'field': "cur_date"}, "cluster_by": None}
        self.run_changes(before, after)

    @use_profile('bigquery')
    def test_bigquery_change_clustering(self):
        before = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": "id"}
        after = {"partition_by": {'field': "cur_date"}, "cluster_by": "name"}
        self.run_changes(before, after)

    @use_profile('bigquery')
    def test_bigquery_change_clustering_strict(self):
        before = {'partition_by': {'field': 'cur_time', 'data_type': 'timestamp'}, 'cluster_by': 'id'}
        after = {'partition_by': {'field': 'cur_date', 'data_type': 'date'}, 'cluster_by': 'name'}
        self.run_changes(before, after)
