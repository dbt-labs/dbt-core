from __future__ import annotations


# try:
#     import dbt_prql  # type: ignore
# except ImportError:
#     dbt_prql = None

# dict of ref_type (e.g. source, ref) -> (dict of (package, table) -> literal)
# references_type = dict[str, dict[tuple[str, str], str]]

# I can't get the above to work on CI; I had thought that it was fine with
# from __future__ import annotations, but it seems not. So, we'll just use Dict.
from typing import Dict, Tuple

references_type = Dict[str, Dict[Tuple[str, str], str]]


class LanguageProvider:
    """
    A LanguageProvider is a class that can parse a given language.

    Currently implemented only for PRQL, but we could extend this to other languages (in
    the medium term)

    TODO: See notes in `ModelParser.render_update`; the current implementation has some
    missmatches.
    """

    # def compile(self, code: str) -> ParsedNode:
    def compile(self, code: str) -> str:
        """
        Compile a given block into a ParsedNode.
        """
        raise NotImplementedError("compile")

    def list_references(self, code: str) -> references_type:
        """
        List all references (e.g. sources / refs) in a given block.
        """
        raise NotImplementedError("list_references")


class PrqlProvider:
    def __init__(self) -> None:
        # TODO: Uncomment when dbt-prql is released
        # if not dbt_prql:
        #     raise ImportError(
        #         "dbt_prql is required and not found; try running `pip install dbt_prql`"
        #     )
        pass

    def compile(self, code: str, references: dict) -> str:
        from . import _dbt_prql as dbt_prql

        return dbt_prql.compile(code, references=references)

    def list_references(self, code: str) -> references_type:
        from . import _dbt_prql as dbt_prql

        return dbt_prql.list_references(code)
