dbt-core's API documentation
============================
Programmatic invocations
--------------------------------------------

In v1.5, dbt-core added support for programmatic invocations. The intent of this entry point is provide **exact parity** with CLI functionality, callable from within a Python script or application.

The main entry point is a ``dbtRunner`` class that wraps around ``dbt-core``'s CLI, and allows you to "invoke" CLI commands as Python methods. Each command returns a `dbtRunnerResult` object, which has three attributes:

* ``success`` (bool): Whether the command succeeded.
* ``result``: If the command completed (successfully or with handled errors), its result(s). Return type varies by command.
* ``exception``: If the dbt invocation encountered an unhandled error and did not complete, the exception it encountered.

.. code-block:: python

    from dbt.cli.main import dbtRunner, dbtRunnerResult

    # initialize
    dbt = dbtRunner()

    # create CLI args as a list of strings
    cli_args = ["run", "--select", "tag:my_tag"]

    # run the command
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # inspect the results
    for r in res.result:
        print(f"{r.node.name}: {r.status}")


For more information and examples, consult the documentation: https://docs.getdbt.com/reference/programmatic-invocations

API documentation
-----------------

.. dbt_click:: dbt.cli.main:cli
