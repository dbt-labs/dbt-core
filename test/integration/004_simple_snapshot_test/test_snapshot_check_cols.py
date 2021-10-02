from test.integration.base import DBTIntegrationTest, use_profile


class TestSimpleSnapshotFiles(DBTIntegrationTest):
    NUM_SNAPSHOT_MODELS = 1

    @property
    def schema(self):
        return "simple_snapshot_004"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            "snapshot-paths": ['check-snapshots'],
            "test-paths": ['check-snapshots-expected'],
            "source-paths": [],
        }

    def test_snapshot_check_cols_cycle(self):
        results = self.run_dbt(["snapshot", '--vars', 'version: 1'])
        self.assertEqual(len(results), 1)

        results = self.run_dbt(["snapshot", '--vars', 'version: 2'])
        self.assertEqual(len(results), 1)

        results = self.run_dbt(["snapshot", '--vars', 'version: 3'])
        self.assertEqual(len(results), 1)

    def assert_expected(self):
        self.run_dbt(['test', '--select', 'test_type:singular', '--vars', 'version: 3'])

    @use_profile('postgres')
    def test__postgres__simple_snapshot(self):
        self.test_snapshot_check_cols_cycle()
        self.assert_expected()
