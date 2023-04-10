# Exception Handling

## `requires.py`

### `postflight`
In the postflight decorator, the click command is invoked (i.e. `func(*args, **kwargs)`) and wrapped in a `try/except` block to handle any exceptions thrown.
Any exceptions thrown from `postflight` are wrapped by custom exceptions from the `dbt.cli.exceptions` module (i.e. `ResultExit`, `ExceptionExit`) to instruct click to complete execution with a particular exit code.

Some `dbt-core` handled exceptions have an attribute named `results` which contains results from running nodes (e.g. `FailFastError`). These are wrapped in the `ResultExit` exception to represent runs that have failed in a way that `dbt-core` expects.
If the invocation of the command does not throw any exceptions but does not succeed, `postflight` will still raise the `ResultExit` exception to make use of the exit code.
These exceptions produce an exit code of `1`.

Exceptions wrapped with `ExceptionExit` may be thrown by `dbt-core` intentionally (i.e. an exception that inherits from `dbt.exceptions.Exception`) or unintentionally (i.e. exceptions thrown by the python runtime). In either case these are considered errors that `dbt-core` did not expect and are treated as genuine exceptions.
These exceptions produce an exit code of `2`.

If no exceptions are thrown from invoking the command and the command succeeds, `postflight` will not raise any exceptions.
When no exceptions are raised an exit code of `0` is produced.

## `main.py`

### `dbtRunner`
`dbtRunner` provides a programmatic interface for our click CLI and wraps the invocation of the click commands to handle any exceptions thrown.

`dbtRunner.invoke` should ideally only ever return an instantiated `dbtRunnerResult` which contains the following fields:
- `success`: A boolean representing whether the command invocation was successful
- `result`: The optional result of the command invoked. This attribute can have many types, please see the definition of `dbtRunnerResult` for more information
- `exception`: If an exception was thrown during command invocation it will be saved here, otherwise it will be `None`. Please note that the exceptions held in this attribute are not the exceptions thrown by `preflight` but instead the exceptions that `ResultExit` and `ExceptionExit` wrap

Programmatic exception handling might look like the following:
```python
res = dbtRunner().invoke(["run"])
if not res.success:
    ...
if type(res.exception) == SomeExceptionType:
    ...
```

## `dbt/tests/util.py`

### `run_dbt`
In many of our functional and integration tests, we want to be sure that an invocation of `dbt` raises a certain exception.
A common pattern for these assertions:
```python
class TestSomething:
    def test_something(self, project):
        with pytest.raises(SomeException):
            run_dbt(["run"])
```
To allow these tests to assert that exceptions have been thrown, the `run_dbt` function will raise any exceptions it recieves from the invocation of a `dbt` command.
