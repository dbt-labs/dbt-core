import pytest

from dbt.exceptions import raise_duplicate_macro_name, CompilationError
from .utils import MockMacro


def test_raise_duplicate_macros_different_package():
    macro_1 = MockMacro(package='dbt', name='some_macro')
    macro_2 = MockMacro(package='dbt-myadapter', name='some_macro')

    with pytest.raises(CompilationError) as exc:
        raise_duplicate_macro_name(
            node_1=macro_1,
            node_2=macro_2,
            namespace='dbt',
        )
    assert 'dbt-myadapter' in str(exc.value)
    assert 'some_macro' in str(exc.value)
    assert 'namespace "dbt"' in str(exc.value)
    assert '("dbt" and "dbt-myadapter" are both in the "dbt" namespace)' in str(exc.value)


def test_raise_duplicate_macros_same_package():
    macro_1 = MockMacro(package='dbt', name='some_macro')
    macro_2 = MockMacro(package='dbt', name='some_macro')

    with pytest.raises(CompilationError) as exc:
        raise_duplicate_macro_name(
            node_1=macro_1,
            node_2=macro_2,
            namespace='dbt',
        )
    assert 'some_macro' in str(exc.value)
    assert 'namespace "dbt"' in str(exc.value)
    assert "are both in" not in str(exc.value)
