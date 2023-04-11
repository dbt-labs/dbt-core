dbt-core's API documentation
============================
How to invoke dbt commands in python runtime
--------------------------------------------

Right now the best way to invoke a command from python runtime is to use the `dbtRunner` we exposed

.. code-block:: python

    from dbt.cli.main import dbtRunner
    cli_args = ['run', '--project-dir', 'jaffle_shop']

    # initialize the dbt runner
    dbt = dbtRunner()
    # run the command
    res = dbt.invoke(args)

You can also pass in pre constructed object into dbtRunner, and we will use those objects instead of loading up from the disk.

.. code-block:: python

    # preload profile and project
    profile = load_profile(project_dir, {}, 'testing-postgres')
    project = load_project(project_dir, False, profile, {})

    # initialize the runner with pre-loaded profile and project
    dbt = dbtRunner(profile=profile, project=project)
    # run the command, this will use the pre-loaded profile and project instead of loading
    res = dbt.invoke(cli_args)


For the full example code, you can refer to `core/dbt/cli/example.py`

API documentation
-----------------

.. dbt_click:: dbt.cli.main:cli
