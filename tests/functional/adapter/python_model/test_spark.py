import pytest
from dbt.tests.util import run_dbt

PANDAS_MODEL = """
import pandas as pd

def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = pd.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""
PYSPARK_MODEL = """
def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = spark.createDataFrame(
        [
            ("Buenos Aires", "Argentina", -34.58, -58.66),
            ("Brasilia", "Brazil", -15.78, -47.91),
            ("Santiago", "Chile", -33.45, -70.66),
            ("Bogota", "Colombia", 4.60, -74.08),
            ("Caracas", "Venezuela", 10.48, -66.86),
        ],
        ["City", "Country", "Latitude", "Longitude"]
    )

    return df
"""

PANDAS_ON_SPARK_MODEL = """
import pyspark.pandas as ps


def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = ps.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""

KOALAS_MODEL = """
import databricks.koalas as ks


def model(dbt, session):
    dbt.config(
        materialized="table",
    )

    df = ks.DataFrame(
        {'City': ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas'],
        'Country': ['Argentina', 'Brazil', 'Chile', 'Colombia', 'Venezuela'],
        'Latitude': [-34.58, -15.78, -33.45, 4.60, 10.48],
        'Longitude': [-58.66, -47.91, -70.66, -74.08, -66.86]}
        )

    return df
"""


class BasePySparkTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "pandas_df.py": PANDAS_MODEL,
            "pyspark_df.py": PYSPARK_MODEL,
            "pandas_on_spark_df.py": PANDAS_ON_SPARK_MODEL,
            "koalas_df.py": KOALAS_MODEL,
        }

    def test_different_dataframes(self, project):
        # test
        results = run_dbt(["run"])
        assert len(results) == 4
