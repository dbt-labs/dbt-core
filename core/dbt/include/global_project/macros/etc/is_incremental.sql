
{% macro is_incremental() %}
    {#-- do not run introspective queries in parsing #}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized == 'incremental'
                  and not flags.FULL_REFRESH
                  and not model.config.get(force_model_full_refresh, False)) }}
    {% endif %}
{% endmacro %}
