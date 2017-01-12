from dbt.adapters.postgres import PostgresAdapter


class RedshiftAdapter(PostgresAdapter):

    date_function = 'getdate()'

    @classmethod
    def dist_qualifier(cls, dist):
        dist_key = dist_key.strip().lower()

        if dist_key in ['all', 'even']:
            return 'diststyle({})'.format(dist_key)
        else:
            return 'diststyle key distkey("{}")'.format(dist_key)

    @classmethod
    def sort_qualifier(cls, sort_type, sort):
        valid_sort_types = ['compound', 'interleaved']
        if sort_type not in valid_sort_types:
            raise RuntimeError(
                "Invalid sort_type given: {} -- must be one of {}"
                .format(sort_type, valid_sort_types)
            )

        if type(sort_keys) == str:
            sort_keys = [sort_keys]

        formatted_sort_keys = ['"{}"'.format(sort_key)
                               for sort_key in sort_keys]
        keys_csv = ', '.join(formatted_sort_keys)

        return "{sort_type} sortkey({keys_csv})".format(
            sort_type=sort_type, keys_csv=keys_csv
        )
