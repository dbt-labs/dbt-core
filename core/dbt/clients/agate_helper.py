from codecs import BOM_UTF8

import agate

BOM = BOM_UTF8.decode('utf-8')  # '\ufeff'

DEFAULT_TYPE_TESTER = agate.TypeTester(types=[
    agate.data_types.Number(null_values=('null', '')),
    agate.data_types.TimeDelta(null_values=('null', '')),
    agate.data_types.Date(null_values=('null', '')),
    agate.data_types.DateTime(null_values=('null', '')),
    agate.data_types.Boolean(true_values=('true',),
                             false_values=('false',),
                             null_values=('null', '')),
    agate.data_types.Text(null_values=('null', ''))
])


def table_from_data(data, column_names):
    "Convert list of dictionaries into an Agate table"

    # The agate table is generated from a list of dicts, so the column order
    # from `data` is not preserved. We can use `select` to reorder the columns
    #
    # If there is no data, create an empty table with the specified columns

    if len(data) == 0:
        return agate.Table([], column_names=column_names)
    else:
        table = agate.Table.from_object(data, column_types=DEFAULT_TYPE_TESTER)
        return table.select(column_names)


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath):
    with open(abspath, encoding='utf-8') as fp:
        if fp.read(1) != BOM:
            fp.seek(0)
        return agate.Table.from_csv(fp, column_types=DEFAULT_TYPE_TESTER)
