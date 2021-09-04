{% test project_for_job_id(model, region, project_id, job_id) %}
select job_project = {{project_id}} as id
FROM `{{region}}`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
where job_id = {{job_id}}
{% test %}
