# Archival

Commonly, analysts need to "look back in time" at some previous state of data in their mutable tables. While some systems are built in a way that makes accessing historical data possible, this is often not the case. dbt provides a mechanism, `dbt archive`, to record changes to a mutable table over time. 

To use `dbt archive`, declare the tables you want to archive in your `dbt_project.yml` file:

```yml
archive:
    - source_schema: production_data         # schema to look for tables in (declared below)
      target_schema: dbt_archive             # where to archive the data to
      tables:                                # list of tables to archive
        - source_table: users                # table to archive (production_data.users)
          target_table: users_archived       # table to insert archived data into (dbt_archive.users_archived)
          updated_at: updated_at             # used to determine when data has changed
          unique_key: id                     # used to generate archival query

        - source_table: some_other_table            # production_data.some_other_table
           target_table: some_other_table_archive   # dbt_archive.some_other_table_archive
           updated_at: "updatedAt"
           unique_key: "expressions || work || LOWER(too)"

    - source_schema: some_other_schema
      ....
```

The archived tables will mirror the schema of the source tables they are generated from. In addition, three fields are added to the archive table:

1. `valid_from`: The timestamp when this archived row was inserted (and first considered valid)
2. `valid_to`: The timestamp when this archived row became invalidated. The first archived record for a given `unique_key` has `valid_to = NULL`. When newer data is archived for that `unique_key`, the `valid_to` field of the old record is set to the `valid_from` field of the new record.
3. `scd_id`: A unique key generated for each archive record. Scd = Slowly Changing Dimension.

dbt models can be built on top of these archived tables. The most recent record for a given `unique_key` is the one where `valid_to` is `null`.

To run this archive process, use the command `dbt archive`. After testing and confirming that the archival works, you should schedule this process to run on a recurring basis.
