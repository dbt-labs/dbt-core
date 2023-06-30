from dataclasses import dataclass, field
from dbt.dataclass_schema import dbtClassMixin
from typing import List, Dict
from dbt.contracts.graph.model_config import MergeBehavior, ShowBehavior, CompareBehavior


@dataclass
class ThingWithMergeBehavior(dbtClassMixin):
    default_behavior: int
    appended: List[str] = field(metadata={"merge": MergeBehavior.Append})
    updated: Dict[str, int] = field(metadata={"merge": MergeBehavior.Update})
    clobbered: str = field(metadata={"merge": MergeBehavior.Clobber})
    keysappended: Dict[str, int] = field(metadata={"merge": MergeBehavior.DictKeyAppend})


def test_merge_behavior_meta():
    existing = {"foo": "bar"}
    initial_existing = existing.copy()
    assert set(MergeBehavior) == {
        MergeBehavior.Append,
        MergeBehavior.Update,
        MergeBehavior.Clobber,
        MergeBehavior.DictKeyAppend,
    }
    for behavior in MergeBehavior:
        assert behavior.meta() == {"merge": behavior}
        assert behavior.meta(existing) == {"merge": behavior, "foo": "bar"}
        assert existing == initial_existing


def test_merge_behavior_from_field():
    fields = [f[0] for f in ThingWithMergeBehavior._get_fields()]
    fields = {name: f for f, name in ThingWithMergeBehavior._get_fields()}
    assert set(fields) == {"default_behavior", "appended", "updated", "clobbered", "keysappended"}
    assert MergeBehavior.from_field(fields["default_behavior"]) == MergeBehavior.Clobber
    assert MergeBehavior.from_field(fields["appended"]) == MergeBehavior.Append
    assert MergeBehavior.from_field(fields["updated"]) == MergeBehavior.Update
    assert MergeBehavior.from_field(fields["clobbered"]) == MergeBehavior.Clobber
    assert MergeBehavior.from_field(fields["keysappended"]) == MergeBehavior.DictKeyAppend


@dataclass
class ThingWithShowBehavior(dbtClassMixin):
    default_behavior: int
    hidden: str = field(metadata={"show_hide": ShowBehavior.Hide})
    shown: float = field(metadata={"show_hide": ShowBehavior.Show})


def test_show_behavior_meta():
    existing = {"foo": "bar"}
    initial_existing = existing.copy()
    assert set(ShowBehavior) == {ShowBehavior.Hide, ShowBehavior.Show}
    for behavior in ShowBehavior:
        assert behavior.meta() == {"show_hide": behavior}
        assert behavior.meta(existing) == {"show_hide": behavior, "foo": "bar"}
        assert existing == initial_existing


def test_show_behavior_from_field():
    fields = [f[0] for f in ThingWithShowBehavior._get_fields()]
    fields = {name: f for f, name in ThingWithShowBehavior._get_fields()}
    assert set(fields) == {"default_behavior", "hidden", "shown"}
    assert ShowBehavior.from_field(fields["default_behavior"]) == ShowBehavior.Show
    assert ShowBehavior.from_field(fields["hidden"]) == ShowBehavior.Hide
    assert ShowBehavior.from_field(fields["shown"]) == ShowBehavior.Show


@dataclass
class ThingWithCompareBehavior(dbtClassMixin):
    default_behavior: int
    included: float = field(metadata={"compare": CompareBehavior.Include})
    excluded: str = field(metadata={"compare": CompareBehavior.Exclude})


def test_compare_behavior_meta():
    existing = {"foo": "bar"}
    initial_existing = existing.copy()
    assert set(CompareBehavior) == {CompareBehavior.Include, CompareBehavior.Exclude}
    for behavior in CompareBehavior:
        assert behavior.meta() == {"compare": behavior}
        assert behavior.meta(existing) == {"compare": behavior, "foo": "bar"}
        assert existing == initial_existing


def test_compare_behavior_from_field():
    fields = [f[0] for f in ThingWithCompareBehavior._get_fields()]
    fields = {name: f for f, name in ThingWithCompareBehavior._get_fields()}
    assert set(fields) == {"default_behavior", "included", "excluded"}
    assert CompareBehavior.from_field(fields["default_behavior"]) == CompareBehavior.Include
    assert CompareBehavior.from_field(fields["included"]) == CompareBehavior.Include
    assert CompareBehavior.from_field(fields["excluded"]) == CompareBehavior.Exclude
