
import agate

DEFAULT_TYPE_TESTER = agate.TypeTester(types=[
    agate.data_types.Number(),
    agate.data_types.Date(),
    agate.data_types.DateTime(),
    agate.data_types.Boolean(),
    agate.data_types.Text()
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
    return agate.Table.from_csv(abspath, column_types=DEFAULT_TYPE_TESTER)
