import pytest

from dbt.tests.adapter.store_test_failures_tests.basic import (
    StoreTestFailuresAsInteractions,
    StoreTestFailuresAsProjectLevelOff,
    StoreTestFailuresAsProjectLevelView,
    StoreTestFailuresAsProjectLevelEphemeral,
    StoreTestFailuresAsGeneric,
    StoreTestFailuresAsExceptions,
)


class PostgresMixin:
    audit_schema: str

    @pytest.fixture(scope="function", autouse=True)
    def setup_audit_schema(self, project, setup_method):
        # postgres only supports schema names of 63 characters
        # a schema with a longer name still gets created, but the name gets truncated
        self.audit_schema = self.audit_schema[:63]


class TestStoreTestFailuresAsInteractions(StoreTestFailuresAsInteractions, PostgresMixin):
    pass


class TestStoreTestFailuresAsProjectLevelOff(StoreTestFailuresAsProjectLevelOff, PostgresMixin):
    pass


class TestStoreTestFailuresAsProjectLevelView(StoreTestFailuresAsProjectLevelView, PostgresMixin):
    pass


class TestStoreTestFailuresAsProjectLevelEphemeral(
    StoreTestFailuresAsProjectLevelEphemeral, PostgresMixin
):
    pass


class TestStoreTestFailuresAsGeneric(StoreTestFailuresAsGeneric, PostgresMixin):
    pass


class TestStoreTestFailuresAsExceptions(StoreTestFailuresAsExceptions, PostgresMixin):
    pass
