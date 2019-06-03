{% archive good_archive %}
    {{ config(target_schema=schema, target_database=database, strategy='timestamp', unique_key='id', updated_at='updated_at')}}
    select * from {{ schema }}.good
{% endarchive %}
