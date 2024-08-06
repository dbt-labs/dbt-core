select
  {{ dbt.date_trunc('second', dbt.current_timestamp()) }} as ts_second
