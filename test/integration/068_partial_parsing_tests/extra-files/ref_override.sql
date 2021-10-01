- Macro to override ref
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname)) %}
{% endmacro %}
