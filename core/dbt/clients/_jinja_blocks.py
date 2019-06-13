import re

import dbt.exceptions


def regex(pat):
    return re.compile(pat, re.DOTALL | re.MULTILINE)


class BlockData(object):
    """raw plaintext data from the top level of the file."""
    def __init__(self, contents):
        self.block_type_name = '__dbt__data'
        self.contents = contents
        self.full_block = contents


class BlockTag(object):
    def __init__(self, block_type_name, block_name, contents=None,
                 full_block=None, **kw):
        self.block_type_name = block_type_name
        self.block_name = block_name
        self.contents = contents
        self.full_block = full_block

    def __str__(self):
        return 'BlockTag({!r}, {!r})'.format(self.block_type_name,
                                             self.block_name)

    def __repr__(self):
        return str(self)

    @property
    def end_block_type_name(self):
        return 'end{}'.format(self.block_type_name)

    def end_pat(self):
        # we don't want to use string formatting here because jinja uses most
        # of the string formatting operators in its syntax...
        pattern = ''.join((
            r'(?P<endblock>((?:\s*\{\%\-|\{\%)\s*',
            self.end_block_type_name,
            r'\s*(?:\-\%\}\s*|\%\})))',
        ))
        return regex(pattern)


_NAME_PATTERN = r'[A-Za-z_][A-Za-z_0-9]*'

COMMENT_START_PATTERN = regex(r'(?:(?P<comment_start>(\s*\{\#)))')
COMMENT_END_PATTERN = regex(r'(.*?)(\s*\#\})')
RAW_START_PATTERN = regex(
    r'(?:\s*\{\%\-|\{\%)\s*(?P<raw_start>(raw))\s*(?:\-\%\}\s*|\%\})'
)
EXPR_START_PATTERN = regex(r'(?P<expr_start>(\{\{\s*))')
EXPR_END_PATTERN = regex(r'(?P<expr_end>(\s*\}\}))')

BLOCK_START_PATTERN = regex(''.join((
    r'(?:\s*\{\%\-|\{\%)\s*',
    r'(?P<block_type_name>({}))'.format(_NAME_PATTERN),
    # some blocks have a 'block name'.
    r'(?:\s+(?P<block_name>({})))?'.format(_NAME_PATTERN),
)))


TAG_CLOSE_PATTERN = regex(r'(?:(?P<tag_close>(\-\%\}\s*|\%\})))')
# if you do {% materialization foo, adapter="myadapter' %} and end up with
# mismatched quotes this will still match, but jinja will fail somewhere
# since the adapter= argument has to be an adapter name, and none have quotes
# or anything else in them. So this should be fine.
MATERIALIZATION_ARGS_PATTERN = regex(
    r'\s*,\s*'
    r'''(?P<adpater_arg>(adapter=(?:['"]{}['"])|default))'''
    .format(_NAME_PATTERN)
)
# macros an stuff like macros get open parents, followed by a very complicated
# argument spec! In fact, it's easiest to parse it in tiny little chunks
# because we have to handle awful stuff like string parsing ;_;
MACRO_ARGS_START_PATTERN = regex(r'\s*(?P<macro_start>\()\s*')
MACRO_ARGS_END_PATTERN = regex(r'\s*(?P<macro_end>(\)))\s*')

# macros can be like {% macro foo(bar) %} or {% macro foo(bar, baz) %} or
# {% macro foo(bar, baz="quux") %} or ...
# I think jinja disallows default values after required (like Python), but we
# can ignore that and let jinja deal
MACRO_ARG_PATTERN = regex(''.join((
    r'\s*(?P<macro_arg_name>({}))\s*',
    r'((?P<value>=)|(?P<more_args>,)?)\s*'.format(_NAME_PATTERN),
)))

# stolen from jinja's lexer. Note that we've consumed all prefix whitespace by
# the time we want to use this.
STRING_PATTERN = regex(
    r"(?P<string>('([^'\\]*(?:\\.[^'\\]*)*)'|"
    r'"([^"\\]*(?:\\.[^"\\]*)*)"))'
)

QUOTE_START_PATTERN = regex(r'''(?P<quote>(['"]))''')

# any number of non-quote characters, followed by:
# - quote: a quote mark indicating start of a string (you'll want to backtrack
#          the regex end on quotes and then match with the string pattern)
# - a comma (so there will be another full argument)
# - a closing parenthesis (you can now expect a closing tag)
NON_STRING_MACRO_ARGS_PATTERN = regex(
    # anything, followed by a quote, open/close paren, or comma
    r'''(.*?)'''
    r'''((?P<quote>(['"]))|(?P<open>(\())|(?P<close>(\)))|(?P<comma>(\,)))'''
)


NON_STRING_DO_BLOCK_MEMBER_PATTERN = regex(
    # anything, followed by a quote, paren, or a tag end
    r'''(.*?)'''
    r'''((?P<quote>(['"]))|(?P<open>(\())|(?P<close>(\))))'''
)


class BlockIterator(object):
    def __init__(self, data):
        self.data = data
        self.blocks = []
        self._parenthesis_stack = []
        self.pos = 0

    def advance(self, new_position):
        self.pos = new_position

    def rewind(self, amount=1):
        self.pos -= amount

    def _search(self, pattern):
        return pattern.search(self.data, self.pos)

    def _match(self, pattern):
        return pattern.match(self.data, self.pos)

    def handle_raw(self, match):
        start = match.end()
        end_pat = BlockTag('raw', None).end_pat()
        end_match = self._search(end_pat)
        if end_match is None:
            dbt.exceptions.raise_compiler_error(
                'unexpected EOF, expected {% endraw %}'
            )
        self.advance(end_match.end())
        return BlockData(self.data[start:end_match.start()])

    def _first_match(self, *patterns, **kwargs):
        matches = []
        for pattern in patterns:
            # default to 'search', but sometimes we want to 'match'.
            if kwargs.get('method', 'search') == 'search':
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
                expected_name, self.data[self.pos:]
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
            match = self._expect_match('}}',
                                       EXPR_END_PATTERN,
                                       QUOTE_START_PATTERN)
            if match.groupdict().get('expr_end') is not None:
                break
            else:
                # it's a quote. we haven't advanced for this match yet, so
                # just slurp up the whole string, no need to rewind.
                match = self._expect_match('string', STRING_PATTERN)
                self.advance(match.end())

        self.advance(match.end())

    def _advance_to_tag_close(self):
        endtag = self._expect_match('%}', TAG_CLOSE_PATTERN)
        self.advance(endtag.end())

    def _consume_to_tag(self, tag_name, start):
        def block_handler(block_match):
            block_type_name = block_match.groupdict()['block_type_name']
            if block_type_name == tag_name:
                self.advance(block_match.end())
                self._advance_to_tag_close()
                return self.data[start:block_match.start()]
            else:
                # must be a new block
                self.handle_block(block_match)

        return self.next_block(block_handler)

    def _consume_block_contents(self, match):
        """Find a block's end. The current state of the parser should be after
        the open block is completed:

            {% blk foo %}my data {% endblk %}
                         ^ right here

        This will recurse!

        :param re.Match match: The regex match for the block tag to look for
            the end of.
        :return re.Match: An end pattern match for the block. Should be an
            re.Match correspondding to
            `BlockTag(**match.groupdict()).end_pat()`
        """
        block = BlockTag(**match.groupdict())

        contents = self._consume_to_tag(block.end_block_type_name, self.pos)
        if contents is None:
            dbt.exceptions.raise_compiler_error(
                'Never found a close tag for the open tag {} ("{}")'
                .format(block.block_type_name, block.end_block_type_name)
            )
        block.contents = contents
        return block

    def handle_top_block(self, match):
        """Handle a block. The current state of the parser should be after the
        block open tag is completed:
            {% blk foo %}my data {% endblk %}
                         ^ right here
        """
        # we have to handle comments inside blocks because you could do this:
        # {% blk foo %}asdf {# {% endblk %} #} {%endblk%}
        # they still end up in the data/raw_data of the block itself, but we
        # have to know to ignore stuff until the end comment marker!

        # the full block started at the given match start, which may include
        # prefixed whitespace! we'll strip it later
        block_start = match.start()
        found = self.handle_block(match)
        found.full_block = self.data[block_start:self.pos]
        return found

    def handle_materialization_block(self, match):
        """Handle a materialization block. The current status of the parser
        should be:
            {% materialization custom, default %}
            ^ right here

        And the match should be for "{% materialization custom"
        """
        self.advance(match.end())
        self._expect_match('materialization args',
                           MATERIALIZATION_ARGS_PATTERN)
        self._advance_to_tag_close()
        # Now we're just looking for {% endmaterialization %}
        return self._consume_block_contents(match)

    def handle_do_block(self, match, expect_block):
        if expect_block:
            # we might be wrong to expect a block ({% do (...) %}, for example)
            # so see if there's more data before the tag closes. if there
            # isn't, we expect a block.
            close_match = self._expect_match('%}', TAG_CLOSE_PATTERN)
            unprocessed = self.data[match.end():close_match.start()].strip()
            expect_block = not unprocessed

        if expect_block:
            # if we're here, expect_block is True and we must have set
            # close_match
            self.advance(close_match.end())
            block = self._consume_block_contents(match)
        else:
            # we have a do-statement like {% do thing() %}, so no {% enddo %}
            # also, we don't want to advance to the end of the match, as it
            # might be inside a string or something! So go back and figure out
            # this should get us past "{% do"
            self.advance(match.end())
            self._process_rval_components()
            block = BlockTag('do', None,
                             full_block=self.data[match.start():self.pos])
        return block

    def handle_set_block(self, match):
        self.advance(match.end())
        equal_or_close = self._expect_match('%} or =',
                                            TAG_CLOSE_PATTERN, regex(r'='))
        self.advance(equal_or_close.end())
        if equal_or_close.groupdict().get('tag_close') is None:
            # it's an equals sign, must be like {% set x = 1 %}
            self._process_rval_components()
            # watch out, order matters here on python 2
            block = BlockTag(full_block=self.data[match.start():self.pos],
                             **match.groupdict())
        else:
            # it's a tag close, must be like {% set x %}...{% endset %}
            block = self._consume_block_contents(match)
        return block

    def handle_if_block(self, match):
        self.advance(match.end())
        self._process_rval_components()
        return self._consume_block_contents(match)

    def handle_elif_block(self, match):
        self.advance(match.end())
        self._process_rval_components()

    def handle_else_block(self, match):
        self.advance(match.end())
        self._advance_to_tag_close()

    def handle_comment(self, match):
        self.advance(match.end())
        match = self._expect_match('#}', COMMENT_END_PATTERN)
        self.advance(match.end())

    def handle_normal_block(self, match):
        # once we've advanced to the end of the given match, we're somewhere
        # like this: {% block_type_name block_type
        # we've either got arguments, a close of tag (%}), or bad input.
        # we've handled materializations already (they're weird!)
        # thankfully, comments aren't allowed *inside* a block def...
        self.advance(match.end())
        block_end_match = self._expect_match('%} or (...)',
                                             TAG_CLOSE_PATTERN,
                                             MACRO_ARGS_START_PATTERN)
        self.advance(block_end_match.end())
        if block_end_match.groupdict().get('macro_start') is not None:
            # we've hit our first parenthesis!
            self._parenthesis_stack = [True]
            self._process_macro_args()
            self._advance_to_tag_close()

        # tag close time!
        return self._consume_block_contents(match)

    def handle_block(self, match):
        matchgroups = match.groupdict()
        if 'block_type_name' not in matchgroups:
            raise dbt.exceptions.InternalException(
                'Got bad matchgroups {} in handle_block'.format(matchgroups)
            )
        block_type_name = matchgroups['block_type_name']
        if block_type_name == 'raw':
            block = self.handle_raw(match)
        elif block_type_name == 'materialization':
            block = self.handle_materialization_block(match)
        elif block_type_name == 'do':
            # if there is a "block_name" in the match groups, we don't expect a
            # block as the "block name" is actually part of the do-statement.
            # we need to do this to handle the (weird and probably wrong!) case
            # of a do-statement that is only a single identifier - techincally
            # allowed in jinja. (for example, {% do thing %})
            expect_block = matchgroups.get('block_name') is None
            block = self.handle_do_block(match, expect_block=expect_block)
        elif block_type_name == 'set':
            block = self.handle_set_block(match)
        elif block_type_name == 'if':
            block = self.handle_if_block(match)
        elif block_type_name == 'elif':
            block = self.handle_elif_block(match)
        elif block_type_name == 'else':
            block = self.handle_else_block(match)
        else:
            block = self.handle_normal_block(match)
        return block

    def next_block(self, block_handler, include_toplevel=False):
        """Return the next block, advancing the position as appropriate.

        If include_toplevel is True, next_block will return a BlockData
        containing any extra pre-match space

        """
        while True:
            match = self._first_match(
                BLOCK_START_PATTERN,
                COMMENT_START_PATTERN,
                EXPR_START_PATTERN)

            if match is None:
                # raise StopIteration?
                return None

            raw_toplevel = self.data[self.pos:match.start()]
            if len(raw_toplevel) > 0:
                self.advance(match.start())
                if include_toplevel:
                    return BlockData(raw_toplevel)

            start = self.pos

            groups = match.groupdict()
            comment_start = groups.get('comment_start')
            expr_start = groups.get('expr_start')
            block_type_name = groups.get('block_type_name')

            if comment_start is not None:
                self.handle_comment(match)
                if include_toplevel:
                    return BlockData(self.data[start:self.pos])
            elif expr_start is not None:
                self.handle_expr(match)
                if include_toplevel:
                    return BlockData(self.data[start:self.pos])
            elif block_type_name is not None:
                block = block_handler(match)
                if block is not None:
                    return block
                elif include_toplevel:
                    return BlockData(self.data[start:self.pos])
            else:
                raise dbt.exceptions.InternalException(
                    'Invalid regex match in next_block, expected block start, '
                    'expr start, or comment start'
                )

    def find_blocks(self):
        def block_handler(match):
            block_start = match.start()
            block = self.handle_block(match)
            block.full_block = self.data[block_start:self.pos]
            return block

        while self.data[self.pos:]:
            block = self.next_block(block_handler, include_toplevel=True)
            if block is None:
                break
            yield block

        raw_toplevel = self.data[self.pos:]
        if len(raw_toplevel) > 0:
            yield BlockData(raw_toplevel)

    def _process_rval_components(self):
        """This is suspiciously similar to _process_macro_default_arg, probably
        want to figure out how to merge the two.

        Process the rval of an assignment statement or a do-block
        """
        while True:
            match = self._expect_match(
                'do block component',
                # you could have a string, though that would be weird
                STRING_PATTERN,
                # a quote or an open/close parenthesis
                NON_STRING_DO_BLOCK_MEMBER_PATTERN,
                # a tag close
                TAG_CLOSE_PATTERN
            )
            matchgroups = match.groupdict()
            self.advance(match.end())
            if matchgroups.get('string') is not None:
                continue
            elif matchgroups.get('quote') is not None:
                self.rewind()
                # now look for a string
                match = self._expect_match('any string', STRING_PATTERN)
                self.advance(match.end())
            elif matchgroups.get('open'):
                self._parenthesis_stack.append(True)
            elif matchgroups.get('close'):
                self._parenthesis_stack.pop()
            elif matchgroups.get('tag_close'):
                if self._parenthesis_stack:
                    msg = ('Found "%}", expected ")"')
                    dbt.exceptions.raise_compiler_error(msg)
                return
            # else whitespace

    def _process_macro_default_arg(self):
        """Handle the bit after an '=' in a macro default argument. This is
        probably the trickiest thing. The goal here is to accept all strings
        jinja would accept and always handle block start/end correctly: It's
        fine to have false positives, jinja can fail later.

        Return True if there are more arguments expected.
        """
        while self._parenthesis_stack:
            match = self._expect_match(
                'macro argument',
                # you could have a string
                STRING_PATTERN,
                # a quote, a comma, or a open/close parenthesis
                NON_STRING_MACRO_ARGS_PATTERN,
                # we want to "match", not "search"
                method='match'
            )
            matchgroups = match.groupdict()
            self.advance(match.end())
            if matchgroups.get('string') is not None:
                # we got a string value. There could be more data.
                continue
            elif matchgroups.get('quote') is not None:
                # we got a bunch of data and then a string opening value.
                # put the quote back on the menu
                self.rewind()
                # now look for a string
                match = self._expect_match('any string', STRING_PATTERN)
                self.advance(match.end())
            elif matchgroups.get('comma') is not None:
                # small hack: if we hit a comma and there is one parenthesis
                # left, return to look for a new name. otherwise we're still
                # looking for the parameter close.
                if len(self._parenthesis_stack) == 1:
                    return
            elif matchgroups.get('open'):
                self._parenthesis_stack.append(True)
            elif matchgroups.get('close'):
                self._parenthesis_stack.pop()
            else:
                raise dbt.exceptions.InternalException(
                    'unhandled regex in _process_macro_default_arg(), no match'
                    ': {}'.format(matchgroups)
                )

    def _process_macro_args(self):
        """Macro args are pretty tricky! Arg names themselves are simple, but
        you can set arbitrary default values, including doing stuff like:
        {% macro my_macro(arg="x" + ("}% {# {% endmacro %}" * 2)) %}

        Which makes you a jerk, but is valid jinja.
        """
        # we are currently after the first parenthesis (+ any whitespace) after
        # the macro args started. You can either have the close paren, or a
        # name.
        while self._parenthesis_stack:
            match = self._expect_match('macro arguments',
                                       MACRO_ARGS_END_PATTERN,
                                       MACRO_ARG_PATTERN)
            self.advance(match.end())
            matchgroups = match.groupdict()
            if matchgroups.get('macro_end') is not None:
                self._parenthesis_stack.pop()
            # we got an argument. let's see what it has
            elif matchgroups.get('value') is not None:
                # we have to process a single macro argument. This mutates
                # the parenthesis stack! If it finds a comma, it will continue
                # the loop.
                self._process_macro_default_arg()
            elif matchgroups.get('more_args') is not None:
                continue
            else:
                raise dbt.exceptions.InternalException(
                    'unhandled regex in _process_macro_args(), no match: {}'
                    .format(matchgroups)
                )
            # if there are more arguments or a macro arg end we'll catch them
            # on the next loop around

    def lex_for_blocks(self):
        return list(self.find_blocks())
