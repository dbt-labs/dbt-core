# Database-specific optimizations

In addition to the configuration parameters that apply to all database adapters, there are certain configuration options that apply only to specific databases.

## Amazon Redshift

### Using sortkey and distkey

Tables in Amazon Redshift have two powerful optimizations to improve query performance: distkeys and sortkeys. Supplying these values as model-level configurations apply the corresponding settings in the generated `CREATE TABLE` DDL. Note that these settings will have no effect for models set to `view` or `ephemeral` models.

- `dist` can have a setting of `all`, `even`, or the name of a key.
- `sort` accepts a list of sort keys, for example: `['timestamp', 'userid']`. dbt will build the sort key in the same order the fields are supplied.
- `sort_type` can have a setting of `interleaved` or `compound`. if no setting is specified, sort_type defaults to `compound`.

For more information on distkeys and sortkeys, view Amazon's docs.
