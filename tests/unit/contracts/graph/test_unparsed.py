import pytest

from dbt.contracts.graph.unparsed import UnparsedColumn, HasColumnTests
from dbt.exceptions import ParsingError
from dbt.parser.schemas import ParserRef


def test_column_parse():
    unparsed_col = HasColumnTests(
        columns=[UnparsedColumn(name="TestCol", constraints=[{"type": "!INVALID!"}])]
    )

    with pytest.raises(ParsingError):
        ParserRef.from_target(unparsed_col)
