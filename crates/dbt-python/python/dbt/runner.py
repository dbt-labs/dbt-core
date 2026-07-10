# Back-compat shim. The canonical import path is `dbt.cli.main`, matching
# legacy dbt-core (`from dbt.cli.main import dbtRunner, dbtRunnerResult`).
from dbt.cli.main import dbtRunner, dbtRunnerResult  # noqa: F401
