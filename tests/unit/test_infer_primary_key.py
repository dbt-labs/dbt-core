from dbt_common.contracts.constraints import (
    ConstraintType,
    ModelLevelConstraint,
    ColumnLevelConstraint,
)

from .fixtures import model_node, generic_test_node

from dbt.contracts.graph.model_config import (
    TestConfig,
)
from dbt.contracts.graph.nodes import (
    ColumnInfo,
)
from dbt.artifacts.resources import TestMetadata


def test_no_primary_key():
    model = model_node()
    assert model.infer_primary_key([]) == []


def test_primary_key_model_constraint():
    model = model_node()
    model.constraints = [ModelLevelConstraint(type=ConstraintType.primary_key, columns=["pk"])]
    assertSameContents(model.infer_primary_key([]), ["pk"])

    model.constraints = [
        ModelLevelConstraint(type=ConstraintType.primary_key, columns=["pk1", "pk2"])
    ]
    assertSameContents(model.infer_primary_key([]), ["pk1", "pk2"])


def test_primary_key_column_constraint():
    model = model_node()
    model.columns = {
        "column1": ColumnInfo(
            "column1", constraints=[ColumnLevelConstraint(type=ConstraintType.primary_key)]
        ),
        "column2": ColumnInfo("column2"),
    }
    assertSameContents(model.infer_primary_key([]), ["column1"])


def test_unique_non_null_single():
    model = model_node()
    test1 = generic_test_node()
    test1.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column1"})
    test2 = generic_test_node()
    test2.test_metadata = TestMetadata(name="not_null", kwargs={"column_name": "column1"})
    test3 = generic_test_node()
    test3.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column2"})
    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1"])


def test_unique_non_null_multiple():
    model = model_node()
    tests = []
    for i in range(2):
        for enabled in [True, False]:
            test1 = generic_test_node()
            test1.test_metadata = TestMetadata(
                name="unique", kwargs={"column_name": "column" + str(i) + str(enabled)}
            )
            test1.config = TestConfig(enabled=enabled)
            test2 = generic_test_node()
            test2.test_metadata = TestMetadata(
                name="not_null", kwargs={"column_name": "column" + str(i) + str(enabled)}
            )
            test2.config = TestConfig(enabled=enabled)
            tests.extend([test1, test2])

    assertSameContents(
        model.infer_primary_key(tests),
        ["column0True", "column1True", "column0False", "column1False"],
    )


def test_enabled_unique_single():
    model = model_node()
    test1 = generic_test_node()
    test1.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column1"})
    test2 = generic_test_node()
    test2.config = TestConfig(enabled=False)
    test2.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column3"})

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1"])


def test_enabled_unique_multiple():
    model = model_node()
    test1 = generic_test_node()
    test1.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column1"})
    test2 = generic_test_node()
    test2.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column2 || column3"})

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1", "column2 || column3"])


def test_enabled_unique_combo_single():
    model = model_node()
    test1 = generic_test_node()
    test1.test_metadata = TestMetadata(
        name="unique_combination_of_columns",
        kwargs={"combination_of_columns": ["column1", "column2"]},
    )
    test2 = generic_test_node()
    test2.config = TestConfig(enabled=False)
    test2.test_metadata = TestMetadata(
        name="unique_combination_of_columns",
        kwargs={"combination_of_columns": ["column3", "column4"]},
    )

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1", "column2"])


def test_enabled_unique_combo_multiple():
    model = model_node()
    test1 = generic_test_node()
    test1.test_metadata = TestMetadata(
        name="unique", kwargs={"combination_of_columns": ["column1", "column2"]}
    )
    test2 = generic_test_node()
    test2.test_metadata = TestMetadata(
        name="unique", kwargs={"combination_of_columns": ["column3", "column4"]}
    )

    tests = [test1, test2]
    assertSameContents(
        model.infer_primary_key(tests), ["column1", "column2", "column3", "column4"]
    )


def test_disabled_unique_single():
    model = model_node()
    test1 = generic_test_node()
    test1.config = TestConfig(enabled=False)
    test1.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column1"})
    test2 = generic_test_node()
    test2.test_metadata = TestMetadata(name="not_null", kwargs={"column_name": "column2"})

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1"])


def test_disabled_unique_multiple():
    model = model_node()
    test1 = generic_test_node()
    test1.config = TestConfig(enabled=False)
    test1.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column1"})
    test2 = generic_test_node()
    test2.config = TestConfig(enabled=False)
    test2.test_metadata = TestMetadata(name="unique", kwargs={"column_name": "column2 || column3"})

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1", "column2 || column3"])


def test_disabled_unique_combo_single():
    model = model_node()
    test1 = generic_test_node()
    test1.config = TestConfig(enabled=False)
    test1.test_metadata = TestMetadata(
        name="unique", kwargs={"combination_of_columns": ["column1", "column2"]}
    )
    test2 = generic_test_node()
    test2.config = TestConfig(enabled=False)
    test2.test_metadata = TestMetadata(
        name="random", kwargs={"combination_of_columns": ["column3", "column4"]}
    )

    tests = [test1, test2]
    assertSameContents(model.infer_primary_key(tests), ["column1", "column2"])


def test_disabled_unique_combo_multiple():
    model = model_node()
    test1 = generic_test_node()
    test1.config = TestConfig(enabled=False)
    test1.test_metadata = TestMetadata(
        name="unique", kwargs={"combination_of_columns": ["column1", "column2"]}
    )
    test2 = generic_test_node()
    test2.config = TestConfig(enabled=False)
    test2.test_metadata = TestMetadata(
        name="unique", kwargs={"combination_of_columns": ["column3", "column4"]}
    )

    tests = [test1, test2]
    assertSameContents(
        model.infer_primary_key(tests), ["column1", "column2", "column3", "column4"]
    )


def assertSameContents(list1, list2):
    assert sorted(list1) == sorted(list2)
