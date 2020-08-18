{{ config(copy_materialization='incremental') }}
{{ ref('original') }}