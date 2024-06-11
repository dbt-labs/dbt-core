select
  {{ dbt.date_trunc('day', dbt.current_timestamp()) }} as date_day
