from dbt.contracts.graph.node_args import ModelNodeArgs


class TestModelNodeArgs:
    def test_model_node_args_unique_id(self) -> None:
        model_node_args = ModelNodeArgs(
            name="name", package_name="package", identifier="identifier", schema="schema"
        )
        assert model_node_args.unique_id == "model.package.name"

    def test_model_node_args_unique_id_with_version(self) -> None:
        model_node_args = ModelNodeArgs(
            name="name",
            package_name="package",
            identifier="identifier",
            schema="schema",
            version="1",
        )
        assert model_node_args.unique_id == "model.package.name.v1"

    def test_model_node_args_fqn(self) -> None:
        model_node_args = ModelNodeArgs(
            name="name",
            package_name="package",
            identifier="identifier",
            schema="schema",
        )
        assert model_node_args.fqn == ["package", "name"]

    def test_model_node_args_fqn_with_version(self) -> None:
        model_node_args = ModelNodeArgs(
            name="name",
            package_name="package",
            identifier="identifier",
            schema="schema",
            version="1",
        )
        assert model_node_args.fqn == ["package", "name", "v1"]
