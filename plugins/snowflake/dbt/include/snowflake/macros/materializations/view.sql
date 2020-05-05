{% materialization view, adapter='snowflake' -%}
    {% set to_return = create_or_replace_view() %}

    {% set target_relation = this.incorporate(type='view') %}
    {% do persist_docs(target_relation, model, for_columns=false) %}

    {% do return(to_return) %}

{%- endmaterialization %}
