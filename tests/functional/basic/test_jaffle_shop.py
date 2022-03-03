from dbt.tests.util import run_dbt, get_manifest


from tests.fixtures.jaffle_shop import JaffleShopProject


class TestBasic(JaffleShopProject):
    def test_basic(self, project):
        # Create the data from seeds
        results = run_dbt(["seed"])

        # Tests that the jaffle_shop project runs
        results = run_dbt(["run"])
        assert len(results) == 5
        manifest = get_manifest(project.project_root)
        assert "model.jaffle_shop.orders" in manifest.nodes
