import click
import inspect
import typing as t
from click import Context
from dbt.cli.option_types import ChoiceTuple


# Implementation from: https://stackoverflow.com/a/48394004
# Note MultiOption options must be specified with type=tuple or type=ChoiceTuple (https://github.com/pallets/click/issues/2012)
class MultiOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.save_other_options = kwargs.pop("save_other_options", True)
        nargs = kwargs.pop("nargs", -1)
        assert nargs == -1, "nargs, if set, must be -1 not {}".format(nargs)
        super(MultiOption, self).__init__(*args, **kwargs)
        self._previous_parser_process = None
        self._eat_all_parser = None

        # validate that multiple=True
        multiple = kwargs.pop("multiple", None)
        msg = f"MultiOption named `{self.name}` must have multiple=True (rather than {multiple})"
        assert multiple, msg

        # validate that type=tuple or type=ChoiceTuple
        option_type = kwargs.pop("type", None)
        msg = f"MultiOption named `{self.name}` must be tuple or ChoiceTuple (rather than {option_type})"
        if inspect.isclass(option_type):
            assert issubclass(option_type, tuple), msg
        else:
            assert isinstance(option_type, ChoiceTuple), msg

    def add_to_parser(self, parser, ctx):
        def parser_process(value, state):
            # method to hook to the parser.process
            done = False
            value = [value]
            if self.save_other_options:
                # grab everything up to the next option
                while state.rargs and not done:
                    for prefix in self._eat_all_parser.prefixes:
                        if state.rargs[0].startswith(prefix):
                            done = True
                    if not done:
                        value.append(state.rargs.pop(0))
            else:
                # grab everything remaining
                value += state.rargs
                state.rargs[:] = []
            value = tuple(value)
            # call the actual process
            self._previous_parser_process(value, state)

        retval = super(MultiOption, self).add_to_parser(parser, ctx)
        for name in self.opts:
            our_parser = parser._long_opt.get(name) or parser._short_opt.get(name)
            if our_parser:
                self._eat_all_parser = our_parser
                self._previous_parser_process = our_parser.process
                our_parser.process = parser_process
                break
        return retval

    def type_cast_value(self, ctx: Context, value: t.Any) -> t.Any:
        def flatten(data):
            if isinstance(data, tuple):
                for x in data:
                    yield from flatten(x)
            else:
                yield data

        # there will be nested tuples to flatten when multiple=True
        value = super(MultiOption, self).type_cast_value(ctx, value)
        if value:
            value = tuple(flatten(value))
        return value
