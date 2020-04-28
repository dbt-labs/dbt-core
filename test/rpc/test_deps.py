from .util import (
    get_querier,
    ProjectDefinition,
)


def deps_with_packages(packages, bad_packages, project_dir, profiles_dir, schema):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
        },
        packages={'packages': packages},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_dir,
        profiles_dir=profiles_dir,
        schema=schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        # we should be able to run sql queries at startup
        querier.async_wait_for_result(querier.run_sql('select 1 as id'))

        # the status should be something positive
        querier.is_result(querier.status())

        # deps should pass
        querier.async_wait_for_result(querier.deps())

        # queries should work after deps
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now break the project
        project.packages['packages'] = bad_packages
        project.write_packages(project_dir, remove=True)

        # queries should still work because we haven't reloaded
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # now run deps again, it should be sad
        querier.async_wait_for_error(querier.deps())
        # it should also not be running.
        result = querier.is_result(querier.ps(active=True, completed=False))
        assert result['rows'] == []

        # fix packages again
        project.packages['packages'] = packages
        project.write_packages(project_dir, remove=True)
        # keep queries broken, we haven't run deps yet
        querier.is_error(querier.run())

        # deps should pass now
        querier.async_wait_for_result(querier.deps())
        querier.is_result(querier.status())

        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))


def test_rpc_deps_packages(project_root, profiles_root, postgres_profile, unique_schema):
    packages = [{
        'package': 'fishtown-analytics/dbt_utils',
        'version': '0.2.1',
    }]
    bad_packages = [{
        'package': 'fishtown-analytics/dbt_util',
        'version': '0.2.1',
    }]
    deps_with_packages(packages, bad_packages, project_root, profiles_root, unique_schema)


def test_rpc_deps_git(project_root, profiles_root, postgres_profile, unique_schema):
    packages = [{
        'git': 'https://github.com/fishtown-analytics/dbt-utils.git',
        'revision': '0.2.1'
    }]
    # if you use a bad URL, git thinks it's a private repo and prompts for auth
    bad_packages = [{
        'git': 'https://github.com/fishtown-analytics/dbt-utils.git',
        'revision': 'not-a-real-revision'
    }]
    deps_with_packages(packages, bad_packages, project_root, profiles_root, unique_schema)

