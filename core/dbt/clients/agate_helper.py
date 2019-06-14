from codecs import BOM_UTF8

import dbt.compat

import agate

BOM = BOM_UTF8.decode('utf-8')  # '\ufeff'

DEFAULT_TYPES = [
    agate.data_types.Number(null_values=('null', '')),
    agate.data_types.TimeDelta(null_values=('null', '')),
    agate.data_types.Date(null_values=('null', '')),
    agate.data_types.DateTime(null_values=('null', '')),
    agate.data_types.Boolean(true_values=('true',),
                             false_values=('false',),
                             null_values=('null', '')),
    agate.data_types.Text(null_values=('null', ''))
]


def table_from_data(data, column_names):
    "Convert list of dictionaries into an Agate table"

    # The agate table is generated from a list of dicts, so the column order
    # from `data` is not preserved. We can use `select` to reorder the columns
    #
    # If there is no data, create an empty table with the specified columns

    if len(data) == 0:
        return agate.Table([], column_names=column_names)
    else:
        type_tester = agate.TypeTester(types=DEFAULT_TYPES)
        table = agate.Table.from_object(data, column_types=type_tester)
        return table.select(column_names)


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath, text_columns):
    text_tester = agate.data_types.Text(null_values=('null', ''))
    type_tester = agate.TypeTester(
        force={column: text_tester for column in text_columns},
        types=DEFAULT_TYPES
    )
    with dbt.compat.open_file(abspath) as fp:
        if fp.read(1) != BOM:
            fp.seek(0)
        t = agate.Table.from_csv(fp, column_types=type_tester)
        return t
