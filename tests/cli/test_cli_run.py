from click.testing import CliRunner

from dbt.cli.main import cli


def test_build():
    runner = CliRunner()
    clean_result = runner.invoke(cli, ['clean'])
    assert 'target' in clean_result.output
    assert 'dbt_packages' in clean_result.output
    assert 'logs' in clean_result.output

    deps_result = runner.invoke(cli, ['deps'])
    assert 'dbt-labs/dbt_utils' in deps_result.output
    assert '1.0.0' in deps_result.output

    ls_result = runner.invoke(cli, ['ls'])
    assert '1 seed' in ls_result.output
    assert '1 model' in ls_result.output
    assert '4 tests' in ls_result.output
    assert '1 snapshot' in ls_result.output

    build_result = runner.invoke(cli, ['build'])
    # 1 seed, 1 model, 2 tests
    assert 'PASS=4' in build_result.output
    # 2 tests
    assert 'ERROR=2' in build_result.output
    # 1 snapshot
    assert 'SKIP=1' in build_result.output

    docs_generate_result = runner.invoke(cli, ['docs', 'generate'])
    assert 'Building catalog' in docs_generate_result.output
    assert 'Catalog written' in docs_generate_result.output
