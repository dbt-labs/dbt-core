# Testing Guide for dbt-core Contributors

This is a living document intended to help external contributors understand
how to write and add tests when submitting a Pull Request to dbt-core.

---

## 🧪 When to Add a Unit Test

Unit tests are fast, isolated tests that validate a single function
or class in Python without running dbt end-to-end.

**Add a unit test when:**
- You are fixing a bug in a Python utility or helper function
- You are adding new logic to an existing class/method
- Your change does not require a database or dbt project to verify

**Location:** `tests/unit/`

**Examples to follow:**
- [`tests/unit/test_graph.py`](tests/unit/test_graph.py)
- [`tests/unit/test_contracts.py`](tests/unit/test_contracts.py)

**How to run unit tests:**
```bash
python -m pytest tests/unit
```

---

## 🔗 When to Add a Functional / Integration Test

Functional tests run dbt against a real (or in-memory) project and validate
end-to-end behavior — compiling, running models, parsing YAML, etc.

**Add a functional test when:**
- Your change affects dbt's CLI behavior or output
- You are modifying how models, sources, or configs are parsed
- You want to verify correct behavior across an entire dbt run

**Location:** `tests/functional/`

**Examples to follow:**
- [`tests/functional/sources/`](tests/functional/sources/)
- [`tests/functional/configs/`](tests/functional/configs/)

**How to run functional tests:**
```bash
python -m pytest tests/functional
```

**Considerations:**
- Functional tests are slower — only add them when unit tests are insufficient
- Each test folder typically has a `fixtures/` subfolder with sample dbt projects
- Use `project` fixtures provided by `dbt-tests-adapter` where possible

---

## 🏃 Running All Tests

```bash
python -m pytest tests/unit           # fast
python -m pytest tests/functional     # slower
python -m pytest tests/                # everything
```

---

## 💡 Not Sure Which Test to Write?

When in doubt, ask in your Pull Request and a maintainer will guide you.
You can also check similar merged PRs for patterns.