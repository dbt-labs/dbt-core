from tests.integration.base import DBTIntegrationTest, use_profile
import json

class TestBaseBigQueryRun(DBTIntegrationTest):

    @property
    def schema(self):
        return "bigquery_test"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'macro-paths': ['macros'],
        }

    @use_profile('bigquery')
    def test__bigquery_fetch_nested_records(self):
        sql = """
        select
          struct(
            cast('Michael' as string) as fname,
            cast('Stonebreaker' as string) as lname
          ) as user,
          [
            struct(1 as val_1, cast(2.12 as numeric) as val_2),
            struct(3 as val_1, cast(4.83 as numeric) as val_2)
          ] as val

        union all

        select
          struct(
            cast('Johnny' as string) as fname,
            cast('Brickmaker' as string) as lname
          ) as user,
          [
            struct(7 as val_1, cast(8 as numeric) as val_2),
            struct(9 as val_1, cast(null as numeric) as val_2)
          ] as val
        """


        status, res = self.adapter.execute(sql, fetch=True)

        self.assertEqual(len(res), 2, "incorrect row count")

        expected = {
            "user": [
                '{"fname": "Michael", "lname": "Stonebreaker"}',
                '{"fname": "Johnny", "lname": "Brickmaker"}'
            ],
            "val": [
                '[{"val_1": 1, "val_2": 2.12}, {"val_1": 3, "val_2": 4.83}]',
                '[{"val_1": 7, "val_2": 8}, {"val_1": 9, "val_2": null}]'
            ]
        }

        for i, key in enumerate(expected):
            line = "row {} for key {} ({} vs {})".format(i, key, expected[key][i], res[i][key])
            # py2 serializes these in an unordered way - deserialize to compare
            v1 = expected[key][i]
            v2 = res[i][key]
            self.assertEqual(json.loads(v1), json.loads(v2), line)
