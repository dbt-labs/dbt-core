
from dbt.exceptions import AmbiguousCatalogMatchError


class TestAmbiguousCatalogMatchError:
    def _make_error(self, unique_id: str) -> AmbiguousCatalogMatchError:
        return AmbiguousCatalogMatchError(
            unique_id,
            {"metadata": {"schema": "raw_dcrm", "name": "Subject"}},
            {"metadata": {"schema": "raw_dcrm", "name": "subject"}},
        )

    def test_message_for_source(self):
        err = self._make_error("source.dbt_pipeline.raw_dcrm.subject")
        msg = err.get_message()
        assert 'associated with the source "source.dbt_pipeline.raw_dcrm.subject"' in msg
        assert "created by the model" not in msg

    def test_message_for_model(self):
        err = self._make_error("model.my_project.my_model")
        msg = err.get_message()
        assert 'associated with the model "model.my_project.my_model"' in msg

    def test_message_for_seed(self):
        err = self._make_error("seed.my_project.my_seed")
        msg = err.get_message()
        assert 'associated with the seed "seed.my_project.my_seed"' in msg

    def test_message_for_snapshot(self):
        err = self._make_error("snapshot.my_project.my_snapshot")
        msg = err.get_message()
        assert 'associated with the snapshot "snapshot.my_project.my_snapshot"' in msg

    def test_message_contains_both_relations(self):
        err = self._make_error("source.dbt_pipeline.raw_dcrm.subject")
        msg = err.get_message()
        assert "raw_dcrm.Subject" in msg
        assert "raw_dcrm.subject" in msg

    def test_message_contains_similar_identifiers_warning(self):
        err = self._make_error("source.dbt_pipeline.raw_dcrm.subject")
        msg = err.get_message()
        assert "similar database identifiers" in msg
