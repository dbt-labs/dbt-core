from dbt.internal_deprecations import deprecated
from dbt.flags import set_from_args
from argparse import Namespace


@deprecated(reason="just because", version="1.23.0", suggested_action="Make some updates")
def to_be_decorated():
    return 5


# simple test that the return value is not modified
def test_deprecated_func():
    set_from_args(Namespace(WARN_ERROR=False), None)
    assert hasattr(to_be_decorated, "__wrapped__")
    assert to_be_decorated() == 5
