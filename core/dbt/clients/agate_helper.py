from codecs import BOM_UTF8

import agate
import datetime
import isodate
import json
from typing import Iterable


BOM = BOM_UTF8.decode('utf-8')  # '\ufeff'


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

        raise agate.exceptions.CastError(
            'Can not parse value "%s" as datetime.' % d
        )


def build_type_tester(text_columns: Iterable[str]):
    types = [
        agate.data_types.Number(null_values=('null', '')),
        agate.data_types.Date(null_values=('null', ''),
                              date_format='%Y-%m-%d'),
        agate.data_types.DateTime(null_values=('null', ''),
                                  datetime_format='%Y-%m-%d %H:%M:%S'),
        ISODateTime(null_values=('null', '')),
        agate.data_types.Boolean(true_values=('true',),
                                 false_values=('false',),
                                 null_values=('null', '')),
        agate.data_types.Text(null_values=('null', ''))
    ]
    force = {
        k: agate.data_types.Text(null_values=('null', ''))
        for k in text_columns
    }
    return agate.TypeTester(force=force, types=types)


DEFAULT_TYPE_TESTER = build_type_tester(())


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


def table_from_data_flat(data, column_names):
    "Convert list of dictionaries into an Agate table"

    rows = []
    for _row in data:
        row = []
        for value in list(_row.values()):
            if isinstance(value, (dict, list, tuple)):
                row.append(json.dumps(value))
            else:
                row.append(value)
        rows.append(row)

    return agate.Table(rows, column_names, column_types=DEFAULT_TYPE_TESTER)


def empty_table():
    "Returns an empty Agate table. To be used in place of None"

    return agate.Table(rows=[])


def as_matrix(table):
    "Return an agate table as a matrix of data sans columns"

    return [r.values() for r in table.rows.values()]


def from_csv(abspath, text_columns):
    type_tester = build_type_tester(text_columns=text_columns)
    with open(abspath, encoding='utf-8') as fp:
        if fp.read(1) != BOM:
            fp.seek(0)
        return agate.Table.from_csv(fp, column_types=type_tester)
