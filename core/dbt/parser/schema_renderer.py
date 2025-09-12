from typing import Any, Dict

from dbt.config.renderer import BaseRenderer, Keypath


# This class renders dictionaries derived from "schema" yaml files.
# It calls Jinja on strings (in deep_map_render), except for certain
# keys which are skipped because they need to be rendered later
# (tests and description). Test configs are rendered in the
# generic test builder code, but skips the keyword args. The test
# keyword args are rendered to capture refs in render_test_update.
# Keyword args are finally rendered at compilation time.
# Descriptions are not rendered until 'process_docs'.
# Pre- and post-hooks in configs are late-rendered.
class SchemaYamlRenderer(BaseRenderer):
    def __init__(self, context: Dict[str, Any], key: str) -> None:
        super().__init__(context)
        self.key = key

    @property
    def name(self):
        return "Rendering yaml"

    def _is_norender_key(self, keypath: Keypath) -> bool:
        """
        models:
            - name: blah
              description: blah
              data_tests: ...
              columns:
                - name:
                  description: blah
                  data_tests: ...

        Return True if it's tests, data_tests or description - those aren't rendered now
        because they're rendered later in parse_generic_tests or process_docs.
        "tests" and "data_tests" are both currently supported but "tests" has been deprecated
        """
        # top level descriptions and data_tests
        if len(keypath) >= 1 and keypath[0] in (
            "tests",
            "data_tests",
            "description",
            "loaded_at_query",
        ):
            return True

        # columns descriptions and data_tests
        if len(keypath) == 2 and keypath[1] in (
            "tests",
            "data_tests",
            "description",
            "loaded_at_query",
        ):
            return True

        # config: pre- and post-hooks, and loaded_at_query
        if (
            len(keypath) >= 2
            and keypath[0] == "config"
            and keypath[1] in ("pre_hook", "post_hook", "loaded_at_query")
        ):
            return True

        # versions
        if len(keypath) == 5 and keypath[4] == "description":
            return True

        if (
            len(keypath) >= 3
            and keypath[0] in ("columns", "dimensions", "measures", "entities")
            and keypath[2] in ("tests", "data_tests", "description")
        ):
            return True

        return False

    # don't render descriptions or test keyword arguments
    def should_render_keypath(self, keypath: Keypath) -> bool:
        if len(keypath) < 1:
            return True
        if self.key == "sources":
            if keypath[0] in ("description", "loaded_at_query"):
                return False
            if len(keypath) >= 2 and keypath[0] == "config" and keypath[1] == "loaded_at_query":
                return False
            if keypath[0] == "tables":
                if self._is_norender_key(keypath[2:]):
                    return False
        elif self.key == "macros":
            if keypath[0] == "arguments":
                if self._is_norender_key(keypath[1:]):
                    return False
            elif self._is_norender_key(keypath[0:]):
                return False
        elif self.key == "metrics":
            # This ensures that metric filters are skipped
            if keypath[-1] == "filter" or len(keypath) > 1 and keypath[-2] == "filter":
                return False
            elif self._is_norender_key(keypath[0:]):
                return False
        elif self.key == "saved_queries":
            # This ensures that saved query filters are skipped
            if keypath[0] == "query_params" and len(keypath) > 1 and keypath[1] == "where":
                return False
            elif self._is_norender_key(keypath[0:]):
                return False
        else:  # models, seeds, snapshots, analyses
            if self._is_norender_key(keypath[0:]):
                return False
        return True
