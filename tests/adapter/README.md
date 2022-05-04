<p align="center">
  <img src="https://raw.githubusercontent.com/dbt-labs/dbt-core/fa1ea14ddfb1d5ae319d5141844910dd53ab2834/etc/dbt-core.svg" alt="dbt logo" width="750"/>
</p>

# dbt-tests-adapter

For context and guidance on using this package, please read: ["Testing a new adapter"](https://docs.getdbt.com/docs/contributing/testing-a-new-adapter)

## What is it?

This package includes reusable test cases that reinforce behaviors common to all or many adapter plugins. There are two categories of tests:

1. **Basic tests** that every adapter plugin is expected to pass. These are defined in `tests.adapter.basic`. Given differences across data platforms, these may require slight modification or reimplementation. Significantly overriding or disabling these tests should be with good reason, since each represents basic functionality expected by dbt users. For example, if your adapter does not support incremental models, you should disable the test, [by marking it with `skip` or `xfail`](https://docs.pytest.org/en/latest/how-to/skipping.html), as well as noting that limitation in any documentation, READMEs, and usage guides that accompany your adapter.

2. **Optional tests**, for second-order functionality that is common across plugins, but not required for basic use. Your plugin can opt into these test cases by inheriting existing ones, or reimplementing them with adjustments. For now, this category includes all tests located outside the `basic` subdirectory. More tests will be added as we convert older tests defined on dbt-core and mature plugins to use the standard framework.

## How to use it?

Each test case in this repo is packaged as a class, prefixed `Base`. To enable a test case to run with your adapter plugin, you should inherit the base class into a new class, prefixed `Test`. That test class will be discovered and run by `pytest`. It can also makes modifications if needed.

```python
class TestSimpleMaterializations(BaseSimpleMaterializations):
    pass
```

## Distribution

To install:

```sh
pip install dbt-tests-adapter
```

This package is versioned in lockstep with `dbt-core`, and [the same versioning guidelines](https://docs.getdbt.com/docs/core-versions) apply:
- New "basic" test cases MAY be added in minor versions ONLY. They may not be included in patch releases.
- Breaking changes to existing test cases MAY be included and communicated as part of minor version upgrades ONLY. They MAY NOT be included in patch releases. We will aim to avoid these whenever possible.
- New "optional" test cases, and non-breaking fixes to existing test cases, MAY be added in minor or patch versions.

Assuming you adapter plugin is pinned to a specific minor version of `dbt-core` (e.g. `~=1.1.0`), you can use the same pin for `dbt-tests-adapter`.

**Note:** This is packaged as a plugin using a python namespace package. It cannot have an `__init__.py` file in the part of the hierarchy to which it needs to be attached.
