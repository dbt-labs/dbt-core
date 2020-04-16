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


    @use_profile('bigquery')
    def test_bigquery_add_partition(self):
        before = {"partition_by": None, "cluster_by": None}
        after = {"partition_by": {'field': 'cur_time', 'data_type': 'timestamp'}, "cluster_by": None}
        self.run_changes(before, after)

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
        self.run_changes(after, before)

    @use_profile('bigquery')
    def test_bigquery_change_partitions_from_int(self):
        before = {"partition_by": {"field": "id", "data_type": "int64", "range": {"start": 0, "end": 10, "interval": 1}}, "cluster_by": None}
        after = {"partition_by": {"field": "cur_date", "data_type": "date"}, "cluster_by": None}
        self.run_changes(before, after)
        self.run_changes(after, before)

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
