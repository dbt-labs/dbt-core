import re
from collections import namedtuple

import dbt.exceptions


def regex(pat):
    return re.compile(pat, re.DOTALL | re.MULTILINE)


class BlockData:
    """raw plaintext data from the top level of the file."""

    def __init__(self, contents):
        self.block_type_name = "__dbt__data"
        self.contents = contents
        self.full_block = contents


class BlockTag:
    def __init__(self, block_type_name, block_name, contents=None, full_block=None, **kw):
        self.block_type_name = block_type_name
        self.block_name = block_name
        self.contents = contents
        self.full_block = full_block

    def __str__(self):
        return "BlockTag({!r}, {!r})".format(self.block_type_name, self.block_name)

    def __repr__(self):
        return str(self)

    @property
    def end_block_type_name(self):
        return "end{}".format(self.block_type_name)

    def end_pat(self):
        # we don't want to use string formatting here because jinja uses most
        # of the string formatting operators in its syntax...
        pattern = "".join(
            (
                r"(?P<endblock>((?:\s*\{\%\-|\{\%)\s*",
                self.end_block_type_name,
                r"\s*(?:\-\%\}\s*|\%\})))",
            )
        )
        return regex(pattern)


Tag = namedtuple("Tag", "block_type_name block_name start end")


_NAME_PATTERN = r"[A-Za-z_][A-Za-z_0-9]*"

COMMENT_START_PATTERN = regex(r"(?:(?P<comment_start>(\s*\{\#)))")
COMMENT_END_PATTERN = regex(r"(.*?)(\s*\#\})")
RAW_START_PATTERN = regex(r"(?:\s*\{\%\-|\{\%)\s*(?P<raw_start>(raw))\s*(?:\-\%\}\s*|\%\})")
EXPR_START_PATTERN = regex(r"(?P<expr_start>(\{\{\s*))")
EXPR_END_PATTERN = regex(r"(?P<expr_end>(\s*\}\}))")

BLOCK_START_PATTERN = regex(
    "".join(
        (
            r"(?:\s*\{\%\-|\{\%)\s*",
            r"(?P<block_type_name>({}))".format(_NAME_PATTERN),
            # some blocks have a 'block name'.
            r"(?:\s+(?P<block_name>({})))?".format(_NAME_PATTERN),
        )
    )
)


RAW_BLOCK_PATTERN = regex(
    "".join(
        (
            r"(?:\s*\{\%\-|\{\%)\s*raw\s*(?:\-\%\}\s*|\%\})",
            r"(?:.*?)",
            r"(?:\s*\{\%\-|\{\%)\s*endraw\s*(?:\-\%\}\s*|\%\})",
        )
    )
)

TAG_CLOSE_PATTERN = regex(r"(?:(?P<tag_close>(\-\%\}\s*|\%\})))")

# stolen from jinja's lexer. Note that we've consumed all prefix whitespace by
# the time we want to use this.
STRING_PATTERN = regex(r"(?P<string>('([^'\\]*(?:\\.[^'\\]*)*)'|" r'"([^"\\]*(?:\\.[^"\\]*)*)"))')

QUOTE_START_PATTERN = regex(r"""(?P<quote>(['"]))""")


class TagIterator:
    def __init__(self, data):
        self.data = data
        self.blocks = []
        self._parenthesis_stack = []
        self.pos = 0

    def linepos(self, end=None) -> str:
        """Given an absolute position in the input data, return a pair of
        line number + relative position to the start of the line.
        """
        end_val: int = self.pos if end is None else end
        data = self.data[:end_val]
        # if not found, rfind returns -1, and -1+1=0, which is perfect!
        last_line_start = data.rfind("\n") + 1
        # it's easy to forget this, but line numbers are 1-indexed
        line_number = data.count("\n") + 1
        return f"{line_number}:{end_val - last_line_start}"

    def advance(self, new_position):
        self.pos = new_position

    def rewind(self, amount=1):
        self.pos -= amount

    def _search(self, pattern):
        return pattern.search(self.data, self.pos)

    def _match(self, pattern):
        return pattern.match(self.data, self.pos)

    def _first_match(self, *patterns, **kwargs):
        matches = []
        for pattern in patterns:
            # default to 'search', but sometimes we want to 'match'.
            if kwargs.get("method", "search") == "search":
                match = self._search(pattern)
            else:
                match = self._match(pattern)
            if match:
                matches.append(match)
        if not matches:
            return None
        # if there are multiple matches, pick the least greedy match
        # TODO: do I need to account for m.start(), or is this ok?
        return min(matches, key=lambda m: m.end())

    def _expect_match(self, expected_name, *patterns, **kwargs):
        match = self._first_match(*patterns, **kwargs)
        if match is None:
            msg = 'unexpected EOF, expected {}, got "{}"'.format(
                expected_name, self.data[self.pos :]
            )
            dbt.exceptions.raise_compiler_error(msg)
        return match

    def handle_expr(self, match):
        """Handle an expression. At this point we're at a string like:
            {{ 1 + 2 }}
            ^ right here

        And the match contains "{{ "

        We expect to find a `}}`, but we might find one in a string before
        that. Imagine the case of `{{ 2 * "}}" }}`...

        You're not allowed to have blocks or comments inside an expr so it is
        pretty straightforward, I hope: only strings can get in the way.
        """
        self.advance(match.end())
        while True:
            match = self._expect_match("}}", EXPR_END_PATTERN, QUOTE_START_PATTERN)
            if match.groupdict().get("expr_end") is not None:
                break
            else:
                # it's a quote. we haven't advanced for this match yet, so
                # just slurp up the whole string, no need to rewind.
                match = self._expect_match("string", STRING_PATTERN)
                self.advance(match.end())

        self.advance(match.end())

    def handle_comment(self, match):
        self.advance(match.end())
        match = self._expect_match("#}", COMMENT_END_PATTERN)
        self.advance(match.end())

    def _expect_block_close(self):
        """Search for the tag close marker.
        To the right of the type name, there are a few possiblities:
           - a name (handled by the regex's 'block_name')
           - any number of: `=`, `(`, `)`, strings, etc (arguments)
           - nothing

        followed eventually by a %}

        So the only characters we actually have to worry about in this context
        are quote and `%}` - nothing else can hide the %} and be valid jinja.
        """
        while True:
            end_match = self._expect_match(
                'tag close ("%}")', QUOTE_START_PATTERN, TAG_CLOSE_PATTERN
            )
            self.advance(end_match.end())
            if end_match.groupdict().get("tag_close") is not None:
                return
            # must be a string. Rewind to its start and advance past it.
            self.rewind()
            string_match = self._expect_match("string", STRING_PATTERN)
            self.advance(string_match.end())

    def handle_raw(self):
        # raw blocks are super special, they are a single complete regex
        match = self._expect_match("{% raw %}...{% endraw %}", RAW_BLOCK_PATTERN)
        self.advance(match.end())
        return match.end()

    def handle_tag(self, match):
        """The tag could be one of a few things:

            {% mytag %}
            {% mytag x = y %}
            {% mytag x = "y" %}
            {% mytag x.y() %}
            {% mytag foo("a", "b", c="d") %}

        But the key here is that it's always going to be `{% mytag`!
        """
        groups = match.groupdict()
        # always a value
        block_type_name = groups["block_type_name"]
        # might be None
        block_name = groups.get("block_name")
        start_pos = self.pos
        if block_type_name == "raw":
            match = self._expect_match("{% raw %}...{% endraw %}", RAW_BLOCK_PATTERN)
            self.advance(match.end())
        else:
            self.advance(match.end())
            self._expect_block_close()
        return Tag(
            block_type_name=block_type_name, block_name=block_name, start=start_pos, end=self.pos
        )

    def find_tags(self):
        while True:
            match = self._first_match(
                BLOCK_START_PATTERN, COMMENT_START_PATTERN, EXPR_START_PATTERN
            )
            if match is None:
                break

            self.advance(match.start())
            # start = self.pos

            groups = match.groupdict()
            comment_start = groups.get("comment_start")
            expr_start = groups.get("expr_start")
            block_type_name = groups.get("block_type_name")

            if comment_start is not None:
                self.handle_comment(match)
            elif expr_start is not None:
                self.handle_expr(match)
            elif block_type_name is not None:
                yield self.handle_tag(match)
            else:
                raise dbt.exceptions.InternalException(
                    "Invalid regex match in next_block, expected block start, "
                    "expr start, or comment start"
                )

    def __iter__(self):
        return self.find_tags()


duplicate_tags = (
    "Got nested tags: {outer.block_type_name} (started at {outer.start}) did "
    "not have a matching {{% end{outer.block_type_name} %}} before a "
    "subsequent {inner.block_type_name} was found (started at {inner.start})"
)


_CONTROL_FLOW_TAGS = {
    "if": "endif",
    "for": "endfor",
}

_CONTROL_FLOW_END_TAGS = {v: k for k, v in _CONTROL_FLOW_TAGS.items()}


class BlockIterator:
    def __init__(self, data):
        self.tag_parser = TagIterator(data)
        self.current = None
        self.stack = []
        self.last_position = 0

    @property
    def current_end(self):
        if self.current is None:
            return 0
        else:
            return self.current.end

    @property
    def data(self):
        return self.tag_parser.data

    def is_current_end(self, tag):
        return (
            tag.block_type_name.startswith("end")
            and self.current is not None
            and tag.block_type_name[3:] == self.current.block_type_name
        )

    def find_blocks(self, allowed_blocks=None, collect_raw_data=True):
        """Find all top-level blocks in the data."""
        if allowed_blocks is None:
            allowed_blocks = {"snapshot", "macro", "materialization", "docs"}

        for tag in self.tag_parser.find_tags():
            if tag.block_type_name in _CONTROL_FLOW_TAGS:
                self.stack.append(tag.block_type_name)
            elif tag.block_type_name in _CONTROL_FLOW_END_TAGS:
                found = None
                if self.stack:
                    found = self.stack.pop()
                else:
                    expected = _CONTROL_FLOW_END_TAGS[tag.block_type_name]
                    dbt.exceptions.raise_compiler_error(
                        (
                            "Got an unexpected control flow end tag, got {} but "
                            "never saw a preceeding {} (@ {})"
                        ).format(tag.block_type_name, expected, self.tag_parser.linepos(tag.start))
                    )
                expected = _CONTROL_FLOW_TAGS[found]
                if expected != tag.block_type_name:
                    dbt.exceptions.raise_compiler_error(
                        (
                            "Got an unexpected control flow end tag, got {} but "
                            "expected {} next (@ {})"
                        ).format(tag.block_type_name, expected, self.tag_parser.linepos(tag.start))
                    )

            if tag.block_type_name in allowed_blocks:
                if self.stack:
                    dbt.exceptions.raise_compiler_error(
                        (
                            "Got a block definition inside control flow at {}. "
                            "All dbt block definitions must be at the top level"
                        ).format(self.tag_parser.linepos(tag.start))
                    )
                if self.current is not None:
                    dbt.exceptions.raise_compiler_error(
                        duplicate_tags.format(outer=self.current, inner=tag)
                    )
                if collect_raw_data:
                    raw_data = self.data[self.last_position : tag.start]
                    self.last_position = tag.start
                    if raw_data:
                        yield BlockData(raw_data)
                self.current = tag

            elif self.is_current_end(tag):
                self.last_position = tag.end
                assert self.current is not None
                yield BlockTag(
                    block_type_name=self.current.block_type_name,
                    block_name=self.current.block_name,
                    contents=self.data[self.current.end : tag.start],
                    full_block=self.data[self.current.start : tag.end],
                )
                self.current = None

        if self.current:
            linecount = self.data[: self.current.end].count("\n") + 1
            dbt.exceptions.raise_compiler_error(
                (
                    "Reached EOF without finding a close tag for " "{} (searched from line {})"
                ).format(self.current.block_type_name, linecount)
            )

        if collect_raw_data:
            raw_data = self.data[self.last_position :]
            if raw_data:
                yield BlockData(raw_data)

    def lex_for_blocks(self, allowed_blocks=None, collect_raw_data=True):
        return list(
            self.find_blocks(allowed_blocks=allowed_blocks, collect_raw_data=collect_raw_data)
        )
