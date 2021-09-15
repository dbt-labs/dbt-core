{% test project_for_job_id(model, region, unique_schema_id, project_id) %}
select 1
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where date(creation_time) = current_date
  and job_project = {{project_id}}
  and destination_table.dataset_id = {{unique_schema_id}}
{% endtest %}
