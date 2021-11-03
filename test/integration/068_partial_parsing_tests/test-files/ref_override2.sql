- Macro to override ref xxxx
{% macro ref(modelname) %}
{% do return(builtins.ref(modelname)) %}
{% endmacro %}
