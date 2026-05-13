"""Unit tests for exception classes in dbt.exceptions.

These cover user-facing message rendering — small enough that we want them
fast and adapter-free.
"""
import pytest

from dbt.exceptions import AmbiguousCatalogMatchError


def _make_match(schema: str, name: str) -> dict:
    return {"metadata": {"schema": schema, "name": name}}


class TestAmbiguousCatalogMatchError:
    """Regression coverage for issue #12629.

    The error fires for any node type whose alias collides — sources, seeds,
    snapshots, models. The original wording hardcoded "model" which was
    misleading when the node was, e.g., a source.
    """

    def test_message_uses_source_label_for_source_unique_id(self):
        err = AmbiguousCatalogMatchError(
            unique_id="source.dbt_pipeline.raw_dcrm.subject",
            match_1=_make_match("raw_dcrm", "subject"),
            match_2=_make_match("raw_dcrm_other", "subject"),
        )
        msg = str(err)
        # Must NOT mislead the user by calling a source a "model".
        assert "created by the model" not in msg
        # Should call out the actual resource type.
        assert "created by the source" in msg
        assert "source.dbt_pipeline.raw_dcrm.subject" in msg

    def test_message_uses_seed_label_for_seed_unique_id(self):
        err = AmbiguousCatalogMatchError(
            unique_id="seed.proj.my_seed",
            match_1=_make_match("public", "my_seed"),
            match_2=_make_match("staging", "my_seed"),
        )
        msg = str(err)
        assert "created by the seed" in msg
        assert "created by the model" not in msg

    def test_message_keeps_model_label_for_model_unique_id(self):
        err = AmbiguousCatalogMatchError(
            unique_id="model.proj.my_model",
            match_1=_make_match("public", "my_model"),
            match_2=_make_match("staging", "my_model"),
        )
        msg = str(err)
        assert "created by the model" in msg

    def test_message_falls_back_to_node_when_unique_id_lacks_prefix(self):
        err = AmbiguousCatalogMatchError(
            unique_id="bare_id_no_prefix",
            match_1=_make_match("public", "x"),
            match_2=_make_match("staging", "x"),
        )
        msg = str(err)
        assert "created by the node" in msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
