from codecs import BOM_UTF8

import agate
import datetime
import isodate
import json
import dbt.utils
from typing import Iterable, List, Dict, Union, Optional, Any

from dbt.exceptions import DbtRuntimeError

BOM = BOM_UTF8.decode("utf-8")  # '\ufeff'


class Integer(agate.data_types.DataType):
    def cast(self, d):
        # by default agate will cast none as a Number
        # but we need to cast it as an Integer to preserve
        # the type when merging and unioning tables
        if type(d) == int or d is None:
            return d
        else:
            raise agate.exceptions.CastError('Can not parse value "%s" as Integer.' % d)

    def jsonify(self, d):
        return d


class Number(agate.data_types.Number):
    # undo the change in https://github.com/wireservice/agate/pull/733
    # i.e. do not cast True and False to numeric 1 and 0
    def cast(self, d):
        if type(d) == bool:
            raise agate.exceptions.CastError("Do not cast True to 1 or False to 0.")
        else:
            return super().cast(d)


class ISODateTime(agate.data_types.DateTime):
    def cast(self, d):
        # this is agate.data_types.DateTime.cast with the "clever" bits removed
        # so we only handle ISO8601 stuff
        if isinstance(d, datetime.datetime) or d is None:
            return d
        elif isinstance(d, datetime.date):
            return datetime.datetime.combine(d, datetime.time(0, 0, 0))
        elif isinstance(d, str):
            d = d.strip()
            if d.lower() in self.null_values:
                return None
        try:
            return isodate.parse_datetime(d)
        except:  # noqa
            pass

        raise agate.exceptions.CastError('Can not parse value "%s" as datetime.' % d)


def build_type_tester(
    text_columns: Iterable[str], string_null_values: Optional[Iterable[str]] = ("null", "")
) -> agate.TypeTester:

    types = [
        Integer(null_values=("null", "")),
        Number(null_values=("null", "")),
        agate.data_types.Date(null_values=("null", ""), date_format="%Y-%m-%d"),
        agate.data_types.DateTime(null_values=("null", ""), datetime_format="%Y-%m-%d %H:%M:%S"),
        ISODateTime(null_values=("null", "")),
        agate.data_types.Boolean(
            true_values=("true",), false_values=("false",), null_values=("null", "")
        ),
        agate.data_types.Text(null_values=string_null_values),
    ]
    force = {k: agate.data_types.Text(null_values=string_null_values) for k in text_columns}
    return agate.TypeTester(force=force, types=types)


DEFAULT_TYPE_TESTER = build_type_tester(())


def table_from_rows(
    rows: List[Any],
    column_names: Iterable[str],
    text_only_columns: Optional[Iterable[str]] = None,
) -> agate.Table:
    if text_only_columns is None:
        column_types = DEFAULT_TYPE_TESTER
    else:
        # If text_only_columns are present, prevent coercing empty string or
        # literal 'null' strings to a None representation.
        column_types = build_type_tester(text_only_columns, string_null_values=())

    return agate.Table(rows, column_names, column_types=column_types)


def table_from_data(data, column_names: Iterable[str]) -> agate.Table:
    "Convert a list of dictionaries into an Agate table"

    # The agate table is generated from a list of dicts, so the column order
    # from `data` is not preserved. We can use `select` to reorder the columns
    #
    # If there is no data, create an empty table with the specified columns

    if len(data) == 0:
        return agate.Table([], column_names=column_names)
    else:
        table = agate.Table.from_object(data, column_types=DEFAULT_TYPE_TESTER)
        return table.select(column_names)


def table_from_data_flat(data, column_names: Iterable[str]) -> agate.Table:
    """
    Convert a list of dictionaries into an Agate table. This method does not
    coerce string values into more specific types (eg. '005' will not be
    coerced to '5'). Additionally, this method does not coerce values to
    None (eg. '' or 'null' will retain their string literal representations).
    """

    rows = []
    text_only_columns = set()
    for _row in data:
        row = []
        for col_name in column_names:
            value = _row[col_name]
            if isinstance(value, (dict, list, tuple)):
                # Represent container types as json strings
                value = json.dumps(value, cls=dbt.utils.JSONEncoder)
                text_only_columns.add(col_name)
            elif isinstance(value, str):
                text_only_columns.add(col_name)
            row.append(value)

        rows.append(row)

    return table_from_rows(
        rows=rows, column_names=column_names, text_only_columns=text_only_columns
    )


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath, text_columns, delimiter=","):
    type_tester = build_type_tester(text_columns=text_columns)
    with open(abspath, encoding="utf-8") as fp:
        if fp.read(1) != BOM:
            fp.seek(0)
        return agate.Table.from_csv(fp, column_types=type_tester, delimiter=delimiter)


class _NullMarker:
    pass


NullableAgateType = Union[agate.data_types.DataType, _NullMarker]


class ColumnTypeBuilder(Dict[str, NullableAgateType]):
    def __init__(self) -> None:
        super().__init__()

    def __setitem__(self, key, value):
        if key not in self:
            super().__setitem__(key, value)
            return

        existing_type = self[key]
        if isinstance(existing_type, _NullMarker):
            # overwrite
            super().__setitem__(key, value)
        elif isinstance(value, _NullMarker):
            # use the existing value
            return
        # when one table column is Number while another is Integer, force the column to Number on merge
        elif isinstance(value, Integer) and isinstance(existing_type, agate.data_types.Number):
            # use the existing value
            return
        elif isinstance(existing_type, Integer) and isinstance(value, agate.data_types.Number):
            # overwrite
            super().__setitem__(key, value)
        elif not isinstance(value, type(existing_type)):
            # actual type mismatch!
            raise DbtRuntimeError(
                f"Tables contain columns with the same names ({key}), "
                f"but different types ({value} vs {existing_type})"
            )

    def finalize(self) -> Dict[str, agate.data_types.DataType]:
        result: Dict[str, agate.data_types.DataType] = {}
        for key, value in self.items():
            if isinstance(value, _NullMarker):
                # agate would make it a Number but we'll make it Integer so that if this column
                # gets merged with another Integer column, it won't get forced to a Number
                result[key] = Integer()
            else:
                result[key] = value
        return result


def _merged_column_types(tables: List[agate.Table]) -> Dict[str, agate.data_types.DataType]:
    # this is a lot like agate.Table.merge, but with handling for all-null
    # rows being "any type".
    new_columns: ColumnTypeBuilder = ColumnTypeBuilder()
    for table in tables:
        for i in range(len(table.columns)):
            column_name: str = table.column_names[i]
            column_type: NullableAgateType = table.column_types[i]
            # avoid over-sensitive type inference
            if all(x is None for x in table.columns[column_name]):
                column_type = _NullMarker()
            new_columns[column_name] = column_type

    return new_columns.finalize()


def merge_tables(tables: List[agate.Table]) -> agate.Table:
    """This is similar to agate.Table.merge, but it handles rows of all 'null'
    values more gracefully during merges.
    """
    new_columns = _merged_column_types(tables)
    column_names = tuple(new_columns.keys())
    column_types = tuple(new_columns.values())

    rows: List[agate.Row] = []
    for table in tables:
        if table.column_names == column_names and table.column_types == column_types:
            rows.extend(table.rows)
        else:
            for row in table.rows:
                data = [row.get(name, None) for name in column_names]
                rows.append(agate.Row(data, column_names))
    # _is_fork to tell agate that we already made things into `Row`s.
    return agate.Table(rows, column_names, column_types, _is_fork=True)


def get_column_value_uncased(column_name: str, row: agate.Row) -> Any:
    """Get the value of a column in this row, ignoring the casing of the column name."""
    for key, value in row.items():
        if key.casefold() == column_name.casefold():
            return value

    raise KeyError
