{% macro ref(model_name) %}

    -- {#  #}
    {% set rel = builtins.ref("failed_to_detect_ref_macro") %}
    {% do return(rel) %}

{% endmacro %}
