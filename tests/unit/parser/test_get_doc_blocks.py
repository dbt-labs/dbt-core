from unittest.mock import MagicMock

from dbt.parser.doc_processor import _get_doc_blocks


class TestGetDocBlocks:
    def test_name_node_arg_does_not_raise(self):
        """doc() with a variable reference (Name node, no .value) must not raise AttributeError."""
        manifest = MagicMock()
        # {{ doc(my_variable) }} produces a jinja2.nodes.Name arg, not a Const
        result = _get_doc_blocks("{{ doc(my_variable) }}", manifest, "my_package")
        assert result == []

    def test_const_arg_resolves_doc_block(self):
        """doc() with a string literal still resolves to a doc_block unique_id."""
        manifest = MagicMock()
        manifest.metadata.project_name = "my_project"
        resolved = MagicMock()
        resolved.unique_id = "doc.my_project.my_doc"
        manifest.resolve_doc.return_value = resolved

        result = _get_doc_blocks("{{ doc('my_doc') }}", manifest, "my_package")

        assert result == ["doc.my_project.my_doc"]
        manifest.resolve_doc.assert_called_once_with("my_doc", None, "my_project", "my_package")

    def test_mixed_args_skips_name_nodes(self):
        """When one arg is a Name node and others are Const, only Const values are used."""
        manifest = MagicMock()
        manifest.metadata.project_name = "my_project"
        manifest.resolve_doc.return_value = None

        # doc(my_var) — one Name arg → doc_args becomes [] → falls through to else: continue
        result = _get_doc_blocks("{{ doc(my_var) }}", manifest, "my_package")
        assert result == []
        manifest.resolve_doc.assert_not_called()
