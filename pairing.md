### Requirements

- [p0*] It is possible to configure a top-level event_time key as a top-level property of a model
  - TODO: consider backward compatability; perhaps call it an Any and do more aggressive validation + skipping if not str

- [p0*] It is possible to hard-core a lower and upper bound time window to apply to all microbatch  runs within an invocation via CLI flags (--event-start-time, --event-end-time)
    - default: open on the left, closed on the right
        - 1 < x <= 2 is open on the left, closed on the right
        - so t=1, t=5 → update [2,3,4,5]

- It is possible to *automatically* read (via `ref` and `source`) just the “new” data for inputs with `event_time` defined in the context of a microbatch model
    - [p0*] “New” data is defined by dynamic checkpoints: current_timestamp as upper bound, lower bound as a partition-aware offset of that

- [p0*] It is possible to configure a “lookback period” that applies to the read window of a microbatch model.

- [p0*] It is possible to efficiently *write* entire partitions representing the newly computed data for a given microbatch model run.
    - https://docs.getdbt.com/docs/build/incremental-strategy
    - Target warehouses:
        - [p0*]`insert_overwrite` dbt-bigquery, dbt-spark, dbt-databricks
        - [p0*]`delete+insert` dbt-snowflake
