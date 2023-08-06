import pytest

from dbt.contracts.graph.nodes import AccessType


class TestAccess:
    @pytest.mark.parametrize(
        "access_type1,access_type2,expected_lt",
        [
            (AccessType.Public, AccessType.Public, False),
            (AccessType.Protected, AccessType.Public, True),
            (AccessType.Private, AccessType.Public, True),
            (AccessType.Public, AccessType.Protected, False),
            (AccessType.Protected, AccessType.Protected, False),
            (AccessType.Private, AccessType.Protected, True),
            (AccessType.Public, AccessType.Private, False),
            (AccessType.Protected, AccessType.Private, False),
            (AccessType.Private, AccessType.Private, False),
        ],
    )
    def test_access_comparison(self, access_type1, access_type2, expected_lt):
        assert (access_type1 < access_type2) == expected_lt
