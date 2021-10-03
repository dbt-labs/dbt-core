import pytest

from .util import (
    get_querier,
    ProjectDefinition,
)


@pytest.mark.parametrize(
    "packages, bad_packages",
    # from dbt hub
    [(
        [{
            'package': 'dbt-labs/dbt_utils',
            'version': '0.5.0',
        }],
        # wrong package name
        [{
            'package': 'dbt-labs/dbt_util',
            'version': '0.5.0',
        }],
    ),
    # from git release/tag/branch
    (
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': '0.5.0',
        }],
        # if you use a bad URL, git thinks it's a private repo and prompts for auth
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': 'not-a-real-revision',
        }],
    ),
    # from git commit
    (
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': 'b736cf6acdbf80d2de69b511a51c8d7fe214ee79',
        }],
        # don't use short commits
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': 'b736cf6',
        }],
    ),
    # from git release and subdirectory
    (
        [{
            'git': 'https://github.com/dbt-labs/dbt-labs-experimental-features.git',
            'revision': '0.0.1',
            'subdirectory': 'materialized-views',
        }],
        [{
            'git': 'https://github.com/dbt-labs/dbt-labs-experimental-features.git',
            'revision': '0.0.1',
            'subdirectory': 'path/to/nonexistent/dir',
        }],
    ),
    # from git commit and subdirectory
    (
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': 'f4f84e9110db26aba22f756abbae9f1f8dbb15da',
            'subdirectory': 'dbt_projects/dbt_utils',
        }],
        [{
            'git': 'https://github.com/dbt-labs/dbt-utils.git',
            'revision': 'f4f84e9110db26aba22f756abbae9f1f8dbb15da',
            'subdirectory': 'path/to/nonexistent/dir',
        }],
    )],
    ids=[
        "from dbt hub",
        "from git release/tag/branch",
        "from git commit",
        "from git release and subdirectory",
        "from git commit and subdirectory",
    ],
)
@pytest.mark.supported('postgres')
def test_rpc_deps_packages(project_root, profiles_root, dbt_profile, unique_schema, packages, bad_packages):
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
        },
        packages={'packages': packages},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
        criteria='error',
    )
    with querier_ctx as querier:
        # we should be able to run sql queries at startup
        querier.is_error(querier.run_sql('select 1 as id'))

        # the status should be an error as deps wil not be defined
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
        project.write_packages(project_root, remove=True)

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
        project.write_packages(project_root, remove=True)
        # keep queries broken, we haven't run deps yet
        querier.is_error(querier.run())

        # deps should pass now
        querier.async_wait_for_result(querier.deps())
        querier.is_result(querier.status())

        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))


@pytest.mark.supported('postgres')
def test_rpc_deps_after_list(project_root, profiles_root, dbt_profile, unique_schema):
    packages = [{
        'package': 'dbt-labs/dbt_utils',
        'version': '0.5.0',
    }]
    project = ProjectDefinition(
        models={
            'my_model.sql': 'select 1 as id',
        },
        packages={'packages': packages},
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
        criteria='error',
    )
    with querier_ctx as querier:
        # we should be able to run sql queries at startup
        querier.is_error(querier.run_sql('select 1 as id'))

        # the status should be an error as deps wil not be defined
        querier.is_result(querier.status())

        # deps should pass
        querier.async_wait_for_result(querier.deps())

        # queries should work after deps
        tok1 = querier.is_async_result(querier.run())
        tok2 = querier.is_async_result(querier.run_sql('select 1 as id'))

        querier.is_result(querier.async_wait(tok2))
        querier.is_result(querier.async_wait(tok1))

        # list should pass
        querier.list()

        # deps should pass
        querier.async_wait_for_result(querier.deps())
