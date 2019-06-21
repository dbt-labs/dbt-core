from dbt.clients.jinja import get_rendered
from dbt.context.common import generate_config_context
from dbt.exceptions import DbtProfileError
from dbt.exceptions import DbtProjectError
from dbt.exceptions import RecursionException
from dbt.utils import deep_map


class ConfigRenderer:
    """A renderer provides configuration rendering for a given set of cli
    variables and a render type.
    """
    def __init__(self, cli_vars):
        self.context = generate_config_context(cli_vars)

    @staticmethod
    def _is_hook_or_model_vars_path(keypath):
        if not keypath:
            return False

        first = keypath[0]
        # run hooks
        if first in {'on-run-start', 'on-run-end'}:
            return True
        # models have two things to avoid
        if first in {'seeds', 'models'}:
            # model-level hooks
            if 'pre-hook' in keypath or 'post-hook' in keypath:
                return True
            # model-level 'vars' declarations
            if 'vars' in keypath:
                return True

        return False

    def _render_project_entry(self, value, keypath):
        """Render an entry, in case it's jinja. This is meant to be passed to
        deep_map.

        If the parsed entry is a string and has the name 'port', this will
        attempt to cast it to an int, and on failure will return the parsed
        string.

        :param value Any: The value to potentially render
        :param key str: The key to convert on.
        :return Any: The rendered entry.
        """
        # hooks should be treated as raw sql, they'll get rendered later.
        # Same goes for 'vars' declarations inside 'models'/'seeds'.
        if self._is_hook_or_model_vars_path(keypath):
            return value

        return self.render_value(value)

    def render_value(self, value, keypath=None):
        # keypath is ignored.
        # if it wasn't read as a string, ignore it
        if not isinstance(value, str):
            return value
        return str(get_rendered(value, self.context))

    def _render_profile_data(self, value, keypath):
        result = self.render_value(value)
        if len(keypath) == 1 and keypath[-1] == 'port':
            try:
                result = int(result)
            except ValueError:
                # let the validator or connection handle this
                pass
        return result

    def _render_schema_source_data(self, value, keypath):
        # things to not render:
        # - descriptions
        if len(keypath) > 0 and keypath[-1] == 'description':
            return value

        return self.render_value(value)

    def render_project(self, as_parsed):
        """Render the parsed data, returning a new dict (or whatever was read).
        """
        try:
            return deep_map(self._render_project_entry, as_parsed)
        except RecursionException:
            raise DbtProjectError(
                'Cycle detected: Project input has a reference to itself',
                project=as_parsed
            )

    def render_profile_data(self, as_parsed):
        """Render the chosen profile entry, as it was parsed."""
        try:
            return deep_map(self._render_profile_data, as_parsed)
        except RecursionException:
            raise DbtProfileError(
                'Cycle detected: Profile input has a reference to itself',
                project=as_parsed
            )

    def render_schema_source(self, as_parsed):
        try:
            return deep_map(self._render_schema_source_data, as_parsed)
        except RecursionException:
            raise DbtProfileError(
                'Cycle detected: schema.yml input has a reference to itself',
                project=as_parsed
            )
