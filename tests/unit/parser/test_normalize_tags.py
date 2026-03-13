from dbt.parser.common import normalize_tags


def test_normalize_tags_none():
    assert normalize_tags(None) == []


def test_normalize_tags_empty_list():
    assert normalize_tags([]) == []


def test_normalize_tags_string():
    assert normalize_tags("single") == ["single"]


def test_normalize_tags_list_sorted():
    assert normalize_tags(["b", "a", "c"]) == ["a", "b", "c"]


def test_normalize_tags_deduplication():
    assert normalize_tags(["a", "a", "b"]) == ["a", "b"]


def test_normalize_tags_dedup_and_sort():
    assert normalize_tags(["z", "a", "z", "b"]) == ["a", "b", "z"]


def test_normalize_tags_single_item_list():
    assert normalize_tags(["only"]) == ["only"]


def test_normalize_tags_empty_string():
    assert normalize_tags("") == [""]
    assert normalize_tags([""]) == [""]
