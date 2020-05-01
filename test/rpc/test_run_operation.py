from .util import (
    get_querier,
    ProjectDefinition,
)

macros_data = '''
{% macro foo() %}
    {{ return(1) }}
{% endmacro %}
{% macro bar(value) %}
    {{ return(value + 1) }}
{% endmacro %}
{% macro quux(value) %}
    {{ return(asdf) }}
{% endmacro %}
'''


def test_run_operation(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
        }
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='foo', args={})
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='bar', args={'value': 10})
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='baz', args={}),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.run_operation(macro='quux', args={})
        )
        assert 'success' in poll_result
        assert poll_result['success'] is True


def test_run_operation_cli(
    project_root, profiles_root, postgres_profile, unique_schema
):
    project = ProjectDefinition(
        models={'my_model.sql': 'select 1 as id'},
        macros={
            'my_macros.sql': macros_data,
        }
    )
    querier_ctx = get_querier(
        project_def=project,
        project_dir=project_root,
        profiles_dir=profiles_root,
        schema=unique_schema,
        test_kwargs={},
    )

    with querier_ctx as querier:
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation foo')
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        bar_cmd = '''run-operation bar --args="{'value': 10}"'''
        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli=bar_cmd)
        )

        assert 'success' in poll_result
        assert poll_result['success'] is True

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation baz'),
            state='failed',
        )
        assert 'state' in poll_result
        assert poll_result['state'] == 'failed'

        poll_result = querier.async_wait_for_result(
            querier.cli_args(cli='run-operation quux')
        )
        assert 'success' in poll_result
        assert poll_result['success'] is True

