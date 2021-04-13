{{ config(
    enabled = var('enabled_direct', False),
    severity = var('severity_direct', 'WARN')
) }}

select 1 as fun
