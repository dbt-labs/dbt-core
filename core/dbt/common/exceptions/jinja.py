from dbt.common.exceptions import CompilationError


class BlockDefinitionNotAtTopError(CompilationError):
    def __init__(self, tag_parser, tag_start) -> None:
        self.tag_parser = tag_parser
        self.tag_start = tag_start
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        position = self.tag_parser.linepos(self.tag_start)
        msg = (
            f"Got a block definition inside control flow at {position}. "
            "All dbt block definitions must be at the top level"
        )
        return msg


class MissingCloseTagError(CompilationError):
    def __init__(self, block_type_name: str, linecount: int) -> None:
        self.block_type_name = block_type_name
        self.linecount = linecount
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f"Reached EOF without finding a close tag for {self.block_type_name} (searched from line {self.linecount})"
        return msg


class MissingControlFlowStartTagError(CompilationError):
    def __init__(self, tag, expected_tag: str, tag_parser) -> None:
        self.tag = tag
        self.expected_tag = expected_tag
        self.tag_parser = tag_parser
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        linepos = self.tag_parser.linepos(self.tag.start)
        msg = (
            f"Got an unexpected control flow end tag, got {self.tag.block_type_name} but "
            f"expected {self.expected_tag} next (@ {linepos})"
        )
        return msg


class NestedTagsError(CompilationError):
    def __init__(self, outer, inner) -> None:
        self.outer = outer
        self.inner = inner
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = (
            f"Got nested tags: {self.outer.block_type_name} (started at {self.outer.start}) did "
            f"not have a matching {{{{% end{self.outer.block_type_name} %}}}} before a "
            f"subsequent {self.inner.block_type_name} was found (started at {self.inner.start})"
        )
        return msg


class UnexpectedControlFlowEndTagError(CompilationError):
    def __init__(self, tag, expected_tag: str, tag_parser) -> None:
        self.tag = tag
        self.expected_tag = expected_tag
        self.tag_parser = tag_parser
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        linepos = self.tag_parser.linepos(self.tag.start)
        msg = (
            f"Got an unexpected control flow end tag, got {self.tag.block_type_name} but "
            f"never saw a preceeding {self.expected_tag} (@ {linepos})"
        )
        return msg


class UnexpectedMacroEOFError(CompilationError):
    def __init__(self, expected_name: str, actual_name: str) -> None:
        self.expected_name = expected_name
        self.actual_name = actual_name
        super().__init__(msg=self.get_message())

    def get_message(self) -> str:
        msg = f'unexpected EOF, expected {self.expected_name}, got "{self.actual_name}"'
        return msg
