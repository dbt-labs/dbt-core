import os
import random
import shutil
import string

import pytest

from dbt.tests.util import run_dbt, update_config_file, write_file, get_manifest

from dbt.exceptions import CompilationError, ModelContractError

from tests.functional.defer_state.fixtures import (
    seed_csv,
    table_model_sql,
    view_model_sql,
    ephemeral_model_sql,
    schema_yml,
    exposures_yml,
    macros_sql,
    infinite_macros_sql,
    contract_schema_yml,
    modified_contract_schema_yml,
)


class BaseModifiedState:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "table_model.sql": table_model_sql,
            "view_model.sql": view_model_sql,
            "ephemeral_model.sql": ephemeral_model_sql,
            "schema.yml": schema_yml,
            "exposures.yml": exposures_yml,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "macros.sql": macros_sql,
            "infinite_macros.sql": infinite_macros_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": seed_csv,
        }

    @property
    def project_config_update(self):
        return {
            "seeds": {
                "test": {
                    "quote_columns": False,
                }
            }
        }

    def copy_state(self):
        if not os.path.exists("state"):
            os.makedirs("state")
        shutil.copyfile("target/manifest.json", "state/manifest.json")

    def run_and_save_state(self):
        run_dbt(["seed"])
        run_dbt(["run"])
        self.copy_state()


class TestChangedSeedContents(BaseModifiedState):
    def test_changed_seed_contents_state(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 0

        # add a new row to the seed
        changed_seed_contents = seed_csv + "\n" + "3,carl"
        write_file(changed_seed_contents, "seeds", "seed.csv")

        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"]
        )
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "state:modified", "--state", "./state"])
        assert len(results) == 1
        assert results[0] == "test.seed"

        results = run_dbt(["ls", "--select", "state:modified+", "--state", "./state"])
        assert len(results) == 7
        assert set(results) == {
            "test.seed",
            "test.table_model",
            "test.view_model",
            "test.ephemeral_model",
            "test.not_null_view_model_id",
            "test.unique_view_model_id",
            "exposure:test.my_exposure",
        }

        shutil.rmtree("./state")
        self.copy_state()

        # make a very big seed
        # assume each line is ~2 bytes + len(name)
        target_size = 1 * 1024 * 1024
        line_size = 64
        num_lines = target_size // line_size
        maxlines = num_lines + 4
        seed_lines = [seed_csv]
        for idx in range(4, maxlines):
            value = "".join(random.choices(string.ascii_letters, k=62))
            seed_lines.append(f"{idx},{value}")
        seed_contents = "\n".join(seed_lines)
        write_file(seed_contents, "seeds", "seed.csv")

        # now if we run again, we should get a warning
        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"]
        )
        assert len(results) == 1
        assert results[0] == "test.seed"

        with pytest.raises(CompilationError) as exc:
            run_dbt(
                [
                    "--warn-error",
                    "ls",
                    "--resource-type",
                    "seed",
                    "--select",
                    "state:modified",
                    "--state",
                    "./state",
                ]
            )
        assert ">1MB" in str(exc.value)

        shutil.rmtree("./state")
        self.copy_state()

        # once it"s in path mode, we don"t mark it as modified if it changes
        write_file(seed_contents + "\n1,test", "seeds", "seed.csv")

        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 0


class TestChangedSeedConfig(BaseModifiedState):
    def test_changed_seed_config(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 0

        update_config_file({"seeds": {"test": {"quote_columns": False}}}, "dbt_project.yml")

        # quoting change -> seed changed
        results = run_dbt(
            ["ls", "--resource-type", "seed", "--select", "state:modified", "--state", "./state"]
        )
        assert len(results) == 1
        assert results[0] == "test.seed"


class TestUnrenderedConfigSame(BaseModifiedState):
    def test_unrendered_config_same(self, project):
        self.run_and_save_state()
        results = run_dbt(
            ["ls", "--resource-type", "model", "--select", "state:modified", "--state", "./state"],
            expect_pass=True,
        )
        assert len(results) == 0

        # although this is the default value, dbt will recognize it as a change
        # for previously-unconfigured models, because it"s been explicitly set
        update_config_file({"models": {"test": {"materialized": "view"}}}, "dbt_project.yml")
        results = run_dbt(
            ["ls", "--resource-type", "model", "--select", "state:modified", "--state", "./state"]
        )
        assert len(results) == 1
        assert results[0] == "test.view_model"


class TestChangedModelContents(BaseModifiedState):
    def test_changed_model_contents(self, project):
        self.run_and_save_state()
        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 0

        table_model_update = """
        {{ config(materialized="table") }}

        select * from {{ ref("seed") }}
        """

        write_file(table_model_update, "models", "table_model.sql")

        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 1
        assert results[0].node.name == "table_model"


class TestNewMacro(BaseModifiedState):
    def test_new_macro(self, project):
        self.run_and_save_state()

        new_macro = """
            {% macro my_other_macro() %}
            {% endmacro %}
        """

        # add a new macro to a new file
        write_file(new_macro, "macros", "second_macro.sql")

        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 0

        os.remove("macros/second_macro.sql")
        # add a new macro to the existing file
        with open("macros/macros.sql", "a") as fp:
            fp.write(new_macro)

        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 0


class TestChangedMacroContents(BaseModifiedState):
    def test_changed_macro_contents(self, project):
        self.run_and_save_state()

        # modify an existing macro
        updated_macro = """
        {% macro my_macro() %}
            {% do log("in a macro", info=True) %}
        {% endmacro %}
        """
        write_file(updated_macro, "macros", "macros.sql")

        # table_model calls this macro
        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 1


class TestChangedExposure(BaseModifiedState):
    def test_changed_exposure(self, project):
        self.run_and_save_state()

        # add an "owner.name" to existing exposure
        updated_exposure = exposures_yml + "\n      name: John Doe\n"
        write_file(updated_exposure, "models", "exposures.yml")

        results = run_dbt(["run", "--models", "+state:modified", "--state", "./state"])
        assert len(results) == 1
        assert results[0].node.name == "view_model"


class TestChangedContract(BaseModifiedState):
    def test_changed_contract(self, project):
        self.run_and_save_state()

        # update contract for table_model
        write_file(contract_schema_yml, "models", "schema.yml")

        # This will find the table_model node modified both through a config change
        # and by a non-breaking change to contract: true
        results = run_dbt(["run", "--models", "state:modified", "--state", "./state"])
        assert len(results) == 1
        assert results[0].node.name == "table_model"
        manifest = get_manifest(project.project_root)
        model_unique_id = "model.test.table_model"
        model = manifest.nodes[model_unique_id]
        expected_unrendered_config = {"contract": {"enforced": True}, "materialized": "table"}
        assert model.unrendered_config == expected_unrendered_config

        # Run it again with "state:modified:contract", still finds modified due to contract: true
        results = run_dbt(["run", "--models", "state:modified.contract", "--state", "./state"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_unique_id]
        first_contract_checksum = model.contract.checksum
        assert first_contract_checksum
        # save a new state
        self.copy_state()

        # This should raise because a column name has changed
        write_file(modified_contract_schema_yml, "models", "schema.yml")
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_unique_id]
        second_contract_checksum = model.contract.checksum
        # double check different contract_checksums
        assert first_contract_checksum != second_contract_checksum
        with pytest.raises(ModelContractError):
            results = run_dbt(["run", "--models", "state:modified.contract", "--state", "./state"])

        # Go back to schema file without contract. Should raise an error.
        write_file(schema_yml, "models", "schema.yml")
        with pytest.raises(ModelContractError):
            results = run_dbt(["run", "--models", "state:modified.contract", "--state", "./state"])
