dbt-core's API documentation
============================
How to invoke dbt commands in python runtime
--------------------------------------------

All dbt commands are under the root group `dbt_cli`, we can import the `dbt_cli` group and access the commands under it.

.. code-block:: python

    from pprint import pprint
    from dbt.cli import dbt_cli

    pprint(dbt_cli.commands)

this would show us all of the dbt commands that are currently implemented.

.. code-block:: python

    {'build': <Command build>,
    'clean': <Command clean>,
    'compile': <Command compile>,
    'debug': <Command debug>,
    'deps': <Command deps>,
    'docs': <Group docs>,
    'init': <Command init>,
    'list': <Command list>,
    'parse': <Command parse>,
    'run': <Command run>,
    'run-operation': <Command run-operation>,
    'seed': <Command seed>,
    'snapshot': <Command snapshot>,
    'source': <Group source>,
    'test': <Command test>}


Right now the best way to invoke a command from python runtime is to use the `make_context` function from `dbt.cli.context` to create a context for the command we are running, then do the setup that would normally happen in the `cli` group, and involke the dbt command with that click context we just built.

For make context, We would need to pass in the arguments that we want to invoke the command with, also the command that we are building this context for.


.. code-block:: python

    from dbt.cli import dbt_cli
    from dbt.cli.context import make_context
    from dbt.tracking import track_run
    from dbt.adapters.factory import adapter_management
    from dbt.profiler import profiler
    from dbt.config.runtime import load_project

    command = 'run'
    cli_args = ['--project-dir', 'jaffle_shop']
    # make context
    ctx = make_context(cli_args, dbt_cli.commands[command])

    # do the setup that would normally happen in the cli group
    # for this step we are looking to provide proper api soon
    ctx.with_resource(track_run(run_command=command))
    ctx.with_resource(adapter_management())
    ctx.with_resource(profiler(enable=True, outfile="output.profile"))
    project_dir = os.path.expanduser(ctx.params.get('project_dir'))
    ctx.obj["project"] = load_project(project_dir, True, None, None)

    # invoke the command
    dbt_cli.commands[command].invoke(ctx)


For the full code example, you can refer to `example.py <https://github.com/dbt-labs/dbt-core/blob/feature/click-cli/core/dbt/cli/example.py>`_

API documentation
-----------------

.. dbt_click:: dbt.cli.main:cli
